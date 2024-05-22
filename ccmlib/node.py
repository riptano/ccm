# ccm node
from __future__ import absolute_import, with_statement

import errno
import glob
import locale
import logging
import os
import psutil
import re
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import time
import warnings
from collections import namedtuple
from datetime import datetime
from distutils.version import LooseVersion  #pylint: disable=import-error, no-name-in-module

import yaml
from six import print_, string_types

from ccmlib import common, extension
from ccmlib.repository import setup
from six.moves import xrange

logger = logging.getLogger(__name__)

NODE_WAIT_TIMEOUT_IN_SECS = 90

class Status():
    UNINITIALIZED = "UNINITIALIZED"
    UP = "UP"
    DOWN = "DOWN"
    DECOMMISSIONED = "DECOMMISSIONED"


class NodeError(Exception):

    def __init__(self, msg, process=None):
        Exception.__init__(self, msg)
        self.process = process


class TimeoutError(Exception):

    def __init__(self, data):
        Exception.__init__(self, str(data))

    @staticmethod
    def raise_if_passed(start, timeout, msg, node=None):
        if start + timeout < time.time():
            raise TimeoutError.create(start, timeout, msg, node)

    @staticmethod
    def create(start, timeout, msg, node=None):
        tstamp = time.strftime("%d %b %Y %H:%M:%S", time.gmtime())
        duration = round(time.time() - start, 2)
        node_s = " [{}]".format(node) if node else ""
        msg = "{tstamp}{nodes} after {d}/{t} seconds {msg}".format(
            tstamp=tstamp, nodes=node_s, msg=msg, d=duration, t=timeout)
        return TimeoutError(msg)


class ToolError(Exception):

    def __init__(self, command, exit_status, stdout=None, stderr=None):
        self.command = command
        self.exit_status = exit_status
        self.stdout = stdout
        self.stderr = stderr

        message = "Subprocess {} exited with non-zero status; exit status: {}".format(command, exit_status)
        if stdout:
            message += "; \nstdout: "
            message += self.__decode(stdout)
        if stderr:
            message += "; \nstderr: "
            message += self.__decode(stderr)

        Exception.__init__(self, message)

    def __decode(self, value):
        if isinstance(value, bytes):
            return bytes.decode(value, locale.getpreferredencoding(False))
        return value

# Groups: 1 = cf, 2 = tmp or none, 3 = suffix (Compacted or Data.db)
_sstable_regexp = re.compile('((?P<keyspace>[^\s-]+)-(?P<cf>[^\s-]+)-)?(?P<tmp>tmp(link)?-)?(?P<version>[^\s-]+)-(?P<number>\d+)-(?P<format>([a-z]+)-)?(?P<suffix>[a-zA-Z]+)\.[a-zA-Z0-9]+$')


class Node(object):

    """
    Provides interactions to a Cassandra node.
    """

    def __init__(self,
                 name,
                 cluster,
                 auto_bootstrap,
                 thrift_interface,
                 storage_interface,
                 jmx_port,
                 remote_debug_port,
                 initial_token,
                 save=True,
                 binary_interface=None,
                 byteman_port='0',
                 environment_variables=None,
                 byteman_startup_script=None,
                 derived_cassandra_version=None):
        """
        Create a new Node.
          - name: the name for that node
          - cluster: the cluster this node is part of
          - auto_bootstrap: whether or not this node should be set for auto-bootstrap
          - thrift_interface: the (host, port) tuple for thrift
          - storage_interface: the (host, port) tuple for internal cluster communication
          - jmx_port: the port for JMX to bind to
          - remote_debug_port: the port for remote debugging
          - initial_token: the token for this node. If None, use Cassandra token auto-assignment
          - save: copy all data useful for this node to the right position.  Leaving this true
            is almost always the right choice.
        """
        self.name = name
        self.cluster = cluster
        self.status = Status.UNINITIALIZED
        self.auto_bootstrap = auto_bootstrap
        self.network_interfaces = {'thrift': common.normalize_interface(thrift_interface),
                                   'storage': common.normalize_interface(storage_interface),
                                   'binary': common.normalize_interface(binary_interface)}
        (self.ip_addr, _) = self.network_interfaces['thrift'] if thrift_interface else self.network_interfaces['binary']
        self.jmx_port = jmx_port
        self.remote_debug_port = remote_debug_port
        self.byteman_port = byteman_port
        self.byteman_startup_script = byteman_startup_script
        self.initial_token = initial_token
        self.pid = None
        self.data_center = None
        self.workloads = []
        self._dse_config_options = {}
        self.__config_options = {}
        self._topology = [('default', 'dc1')]
        self.__install_dir = None
        self.__global_log_level = None
        self.__classes_log_level = {}
        self.__environment_variables = environment_variables or {}
        self.__environment_variables = self.__environment_variables.copy()
        self.__original_java_home = None
        self.__original_path = None
        self.__conf_updated = False

        if derived_cassandra_version:
            self._cassandra_version = derived_cassandra_version
        else:
            try:
                self._cassandra_version = common.get_version_from_build(self.get_install_dir(), cassandra=True)
            except common.CCMError:
                self._cassandra_version = self.cluster.cassandra_version()

        if save:
            self.import_config_files()
            self.import_bin_files()
            if common.is_win():
                self.__clean_bat()

    @staticmethod
    def load(path, name, cluster):
        """
        Load a node from from the path on disk to the config files, the node name and the
        cluster the node is part of.
        """
        node_path = os.path.join(path, name)
        filename = os.path.join(node_path, 'node.conf')
        with open(filename, 'r') as f:
            data = yaml.safe_load(f)
        try:
            itf = data['interfaces']
            initial_token = None
            if 'initial_token' in data:
                initial_token = data['initial_token']
            cassandra_version = None
            if 'cassandra_version' in data:
                cassandra_version = LooseVersion(data['cassandra_version'])
            remote_debug_port = 2000
            if 'remote_debug_port' in data:
                remote_debug_port = data['remote_debug_port']
            binary_interface = None
            if 'binary' in itf and itf['binary'] is not None:
                binary_interface = tuple(itf['binary'])
            thrift_interface = None
            if 'thrift' in itf and itf['thrift'] is not None:
                thrift_interface = tuple(itf['thrift'])
            node = cluster.create_node(data['name'], data['auto_bootstrap'], thrift_interface, tuple(itf['storage']), data['jmx_port'], remote_debug_port, initial_token, save=False, binary_interface=binary_interface, byteman_port=data['byteman_port'], derived_cassandra_version=cassandra_version)
            node.status = data['status']
            if 'pid' in data:
                node.pid = int(data['pid'])
            if 'install_dir' in data:
                node.__install_dir = data['install_dir']
            if 'config_options' in data:
                node.__config_options = data['config_options']
            if 'dse_config_options' in data:
                node._dse_config_options = data['dse_config_options']
            if 'environment_variables' in data:
                node.__environment_variables = data['environment_variables']
            if 'data_center' in data:
                node.data_center = data['data_center']
            if 'workloads' in data:
                node.workloads = data['workloads']
            return node
        except KeyError as k:
            raise common.LoadError("Error Loading " + filename + ", missing property: " + str(k))

    def get_path(self):
        """
        Returns the path to this node top level directory (where config/data is stored)
        """
        return os.path.join(self.cluster.get_path(), self.name)

    def get_bin_dir(self):
        """
        Returns the path to the directory where Cassandra scripts are located
        """
        return os.path.join(self.get_path(), 'bin')

    def get_tool(self, toolname):
        return common.join_bin(self.get_install_dir(), 'bin', toolname)

    def get_tool_args(self, toolname):
        return [common.join_bin(self.get_install_dir(), 'bin', toolname)]

    def get_env(self):
        update_conf = not self.__conf_updated
        if update_conf:
            self.__conf_updated = True
        env = common.make_cassandra_env(self.get_install_dir(), self.get_path(), update_conf)
        env = common.update_java_version(jvm_version=None,
                                         install_dir=self.get_install_dir(),
                                         cassandra_version=self.get_cassandra_version(),
                                         env=env,
                                         info_message=self.name)
        for (key, value) in self.__environment_variables.items():
            env[key] = value
        return env

    def get_install_cassandra_root(self):
        return self.get_install_dir()

    def get_node_cassandra_root(self):
        return self.get_path()

    def get_conf_dir(self):
        """
        Returns the path to the directory where Cassandra config are located
        """
        return os.path.join(self.get_path(), 'conf')

    def get_conf_file(self):
        """
        Returns the path to the configuration yaml file
        """
        conf_name = self.cluster.configuration_yaml
        return os.path.join(self.get_conf_dir(), conf_name if conf_name is not None else common.CASSANDRA_CONF)

    def address_for_current_version_slashy(self):
        """
        Returns the address formatted for the current version (InetAddress/InetAddressAndPort.toString)
        """
        if self.get_cassandra_version() >= '4.0':
            return "/{}".format(str(self.address_and_port()))
        else:
            return "/{}".format(str(self.address()));

    def address_for_version(self, version):
        """
        Returns the address formatted for the specified (InetAddress.getHostAddress or
        InetAddressAndPort.getHostAddressAndPort)
        """
        if version >= '4.0':
            return self.address_and_port()
        else:
            return "{}".format(str(self.address()));

    def address_for_current_version(self):
        """
        Returns the address formatted for the current version.
        """
        return self.address_for_version(self.get_cassandra_version())

    def address(self):
        """
        Returns the IP use by this node for internal communication
        """
        return self.network_interfaces['storage'][0]

    def address_and_port(self):
        """
        Returns the IP used for internal communication along with ports
        """
        address = self.network_interfaces['storage'][0]
        port = self.network_interfaces['storage'][1]
        if address.find(":") != -1:
            return "[{}]:{}".format(address, port)
        else:
            return str(address) + ':' + str(port)

    def get_install_dir(self):
        """
        Returns the path to the cassandra source directory used by this node.
        """
        if self.__install_dir is None:
            return self.cluster.get_install_dir()
        else:
            common.validate_install_dir(self.__install_dir)
            return self.__install_dir

    def node_setup(self, version, verbose):
        dir, v = setup(version, verbose=verbose)
        return dir

    def set_install_dir(self, install_dir=None, version=None, verbose=False):
        """
        Sets the path to the cassandra source directory for use by this node.
        """
        if version is None:
            self.__install_dir = install_dir
            if install_dir is not None:
                common.validate_install_dir(install_dir)
        else:
            self.__install_dir = self.node_setup(version, verbose=verbose)

        self._cassandra_version = common.get_version_from_build(self.__install_dir, cassandra=True)

        if self.get_base_cassandra_version() >= 4.0:
            self.network_interfaces['thrift'] = None

        self.import_config_files()
        self.import_bin_files()
        self.__conf_updated = False

        if self.get_cassandra_version() >= '4':
            self.set_configuration_options(values={'start_rpc': None}, delete_empty=True, delete_always=True)
            self.set_configuration_options(values={'rpc_port': None}, delete_empty=True, delete_always=True)
        else:
            self.set_configuration_options(common.CCM_40_YAML_OPTIONS, delete_empty=True, delete_always=True)

        return self

    def set_workloads(self, workloads):
        raise common.ArgumentError("Cannot set workloads on a cassandra node")

    def get_cassandra_version(self):
        return self._cassandra_version

    def get_base_cassandra_version(self):
        version = self.get_cassandra_version()
        return float('.'.join(re.split('\.|-',version.vstring)[:2]))

    def set_configuration_options(self, values=None, delete_empty=False, delete_always=False):
        """
        Set Cassandra configuration options.
        ex:
            node.set_configuration_options(values={
                'hinted_handoff_enabled' : True,
                'concurrent_writes' : 64,
            })
        """
        if (not hasattr(self,'__config_options') and not hasattr(self,'_Node__config_options')) or self.__config_options is None:
            self.__config_options = {}

        if values is not None:
            self.__config_options = common.merge_configuration(self.__config_options, values, delete_empty=delete_empty, delete_always=delete_always)

        self.import_config_files()

    def set_environment_variable(self, key, value):
        self.__environment_variables[key] = value
        self.import_config_files()

    def set_batch_commitlog(self, enabled=False, use_batch_window=True):
        """
        The batch_commitlog option gives an easier way to switch to batch
        commitlog (since it requires setting 2 options and unsetting one).
        """
        if enabled:
            if use_batch_window:
                values = {
                    "commitlog_sync": "batch",
                    "commitlog_sync_batch_window_in_ms": 5,
                    "commitlog_sync_period_in_ms": None
                }
            else:
               values = {
                    "commitlog_sync": "batch",
                    "commitlog_sync_period_in_ms": None
               }
        else:
            if use_batch_window:
                values = {
                    "commitlog_sync": "periodic",
                    "commitlog_sync_batch_window_in_ms": None,
                    "commitlog_sync_period_in_ms": 10000
                }
            else:
                values = {
                    "commitlog_sync": "periodic",
                    "commitlog_sync_period_in_ms": 10000
                }

        self.set_configuration_options(values)

    def set_dse_configuration_options(self, values=None):
        pass

    def show(self, only_status=False, show_cluster=True):
        """
        Print infos on this node configuration.
        """
        self.__update_status()
        indent = ''.join([" " for i in xrange(0, len(self.name) + 2)])
        print_("{}: {}".format(self.name, self.__get_status_string()))
        if not only_status:
            if show_cluster:
                print_("{}{}={}".format(indent, 'cluster', self.cluster.name))
            print_("{}{}={}".format(indent, 'auto_bootstrap', self.auto_bootstrap))
            if self.network_interfaces['thrift'] is not None:
                print_("{}{}={}".format(indent, 'thrift', self.network_interfaces['thrift']))
            if self.network_interfaces['binary'] is not None:
                print_("{}{}={}".format(indent, 'binary', self.network_interfaces['binary']))
            print_("{}{}={}".format(indent, 'storage', self.network_interfaces['storage']))
            print_("{}{}={}".format(indent, 'jmx_port', self.jmx_port))
            print_("{}{}={}".format(indent, 'remote_debug_port', self.remote_debug_port))
            print_("{}{}={}".format(indent, 'byteman_port', self.byteman_port))
            print_("{}{}={}".format(indent, 'initial_token', self.initial_token))
            if self.pid:
                print_("{}{}={}".format(indent, 'pid', self.pid))

    def is_running(self):
        """
        Return true if the node is running
        """
        self.__update_status()
        return self.status == Status.UP or self.status == Status.DECOMMISSIONED

    def is_live(self):
        """
        Return true if the node is live (it's run and is not decommissioned).
        """
        self.__update_status()
        return self.status == Status.UP

    def log_directory(self):
        return os.path.join(self.get_path(), 'logs')

    def logfilename(self):
        """
        Return the path to the current Cassandra log of this node.
        """
        return os.path.join(self.log_directory(), 'system.log')

    def debuglogfilename(self):
        return os.path.join(self.log_directory(), 'debug.log')

    def gclogfilename(self):
        return os.path.join(self.log_directory(), 'gc.log.0.current')

    def compactionlogfilename(self):
        return os.path.join(self.log_directory(), 'compaction.log')

    def envfilename(self):
        return os.path.join(
            self.get_conf_dir(),
            common.CASSANDRA_WIN_ENV if common.is_win() else common.CASSANDRA_ENV
        )

    def grep_log(self, expr, filename='system.log', from_mark=None):
        """
        Returns a list of lines matching the regular expression in parameter
        in the Cassandra log of this node
        """
        matchings = []
        pattern = re.compile(expr)
        with open(os.path.join(self.log_directory(), filename)) as f:
            if from_mark:
                f.seek(from_mark)
            for line in f:
                m = pattern.search(line)
                if m:
                    matchings.append((line, m))
        return matchings

    def grep_log_for_errors(self, filename='system.log'):
        """
        Returns a list of errors with stack traces
        in the Cassandra log of this node
        """
        return self.grep_log_for_errors_from(seek_start=getattr(self, 'error_mark', 0))

    def grep_log_for_errors_from(self, filename='system.log', seek_start=0):
        with open(os.path.join(self.log_directory(), filename)) as f:
            f.seek(seek_start)
            return _grep_log_for_errors(f.read())

    def mark_log_for_errors(self, filename='system.log'):
        """
        Ignore errors behind this point when calling
        node.grep_log_for_errors()
        """
        self.error_mark = self.mark_log(filename)

    def mark_log(self, filename='system.log'):
        """
        Returns "a mark" to the current position of this node Cassandra log.
        This is for use with the from_mark parameter of watch_log_for_* methods,
        allowing to watch the log from the position when this method was called.
        """
        log_file = os.path.join(self.log_directory(), filename)
        if not os.path.exists(log_file):
            return 0
        with open(log_file) as f:
            f.seek(0, os.SEEK_END)
            return f.tell()

    def print_process_output(self, name, proc, verbose=False):
        # If stderr_file exists on the process, we opted to
        # store stderr in a separate temporary file, consume that.
        if hasattr(proc, 'stderr_file') and proc.stderr_file is not None:
            proc.stderr_file.seek(0)
            stderr = proc.stderr_file.read()
        else:
            try:
                stderr = proc.communicate()[1]
            except ValueError:
                stderr = ''

        if len(stderr) > 1:
            print_("[{} ERROR] {}".format(name, stderr.strip()))

    # This will return when exprs are found or it timeouts
    def watch_log_for(self, exprs, from_mark=None, timeout=600,
                      process=None, verbose=False, filename='system.log',
                      error_on_pid_terminated=False):
        """
        Watch the log until one or more (regular) expressions are found or timeouts (a
        TimeoutError is then raised). On successful completion, a list of pair (line matched,
        match object) is returned.

        Will raise NodeError if error_on_pit_terminated is True and C* pid is not running.
        """
        start = time.time()
        tofind = [exprs] if isinstance(exprs, string_types) else exprs
        tofind = [re.compile(e) for e in tofind]
        matchings = []
        reads = ""
        if len(tofind) == 0:
            return None

        log_file = os.path.join(self.log_directory(), filename)
        output_read = False
        while not os.path.exists(log_file):
            time.sleep(.5)
            TimeoutError.raise_if_passed(start=start, timeout=timeout, node=self.name,
                                         msg="Timed out waiting for {} to be created.".format(log_file))

            if process and not output_read:
                process.poll()
                if process.returncode is not None:
                    self.print_process_output(self.name, process, verbose)
                    output_read = True
                    if process.returncode != 0:
                        raise RuntimeError()  # Shouldn't reuse RuntimeError but I'm lazy

        with open(log_file) as f:
            if from_mark:
                f.seek(from_mark)

            while True:
                # First, if we have a process to check, then check it.
                # Skip on Windows - stdout/stderr is cassandra.bat
                if not common.is_win() and not output_read:
                    if process:
                        process.poll()
                        if process.returncode is not None:
                            self.print_process_output(self.name, process, verbose)
                            output_read = True
                            if process.returncode != 0:
                                raise RuntimeError()  # Shouldn't reuse RuntimeError but I'm lazy

                line = f.readline()
                if line:
                    reads = reads + line
                    for e in tofind:
                        m = e.search(line)
                        if m:
                            matchings.append((line, m))
                            tofind.remove(e)
                            if len(tofind) == 0:
                                return matchings[0] if isinstance(exprs, string_types) else matchings
                else:
                    # wait for the situation to clarify, either stop or just a pause in log production
                    time.sleep(1)

                    if error_on_pid_terminated:
                        self.raise_node_error_if_cassandra_process_is_terminated()

                    TimeoutError.raise_if_passed(start=start, timeout=timeout, node=self.name,
                                                 msg="Missing: {exprs} not found in {f}:\n Head: {head}\n Tail: {tail}"
                                                 .format(
                                                     exprs=[e.pattern for e in tofind], f=filename,
                                                     head=reads[:50], tail="..."+reads[len(reads)-150:]))

                    # Checking "process" is tricky, as it may be itself terminated e.g. after "verbose"
                    # or if there is some race condition between log checking and start process finish
                    # so if the "error_on_pid_terminated" is requested we will give it a chance
                    # and will not check parent process termination
                    if process and not error_on_pid_terminated:
                        if common.is_win():
                            if not self.is_running():
                                return None
                        else:
                            process.poll()
                            if process.returncode == 0:
                                common.debug("{pid} or its child process terminated. watch_for_logs() for {l} will not continue.".format(
                                    pid=process.pid, l=[e.pattern for e in tofind]))
                                return None

    def watch_log_for_no_errors(self, exprs, from_mark=None, timeout=600, process=None, verbose=False, filename='system.log'):
        """
        Watch the log until one or more (regular) expressions are found or timeouts (a
        TimeoutError is then raised). On successful completion, a list of pair (line matched,
        match object) is returned.

        This method is different from watch_log_for as it will raise a AssertionError if the
        log contain an error; this assertion will contain the errors found in the log.
        """
        start = time.time()
        tofind = [exprs] if isinstance(exprs, string_types) else exprs
        tofind = [re.compile(e) for e in tofind]
        seek_start=0
        if from_mark:
            seek_start = from_mark
        while True:
            TimeoutError.raise_if_passed(start=start, timeout=timeout, node=self.name,
                                         msg="Missing: {exprs} not found in {f}".format(
                                             exprs=[e.pattern for e in tofind], f=filename))
            try:
                # process is bin/cassandra so choosing to ignore this argument to avoid early termination
                return self.watch_log_for(tofind, from_mark=from_mark, timeout=5, verbose=verbose, filename=filename)
            except TimeoutError:
                logger.debug("waited 5s watching for '{}' but was not found; checking for errors".format(tofind))
                # since the api doesn't return the mark read it isn't thread safe to use mark
                # as the length of the file can change between calls which may mean we skip over
                # errors; to avoid this keep reading the whole file over and over again...
                # unless a explicit mark is given to the method, will read from that offset
                errors = self.grep_log_for_errors_from(filename=filename, seek_start=seek_start)
                if errors:
                    msg = "Errors were found in the logs while watching for '{}'; attempting to fail the test".format(tofind)
                    logger.debug(msg)
                    raise AssertionError("{}:\n".format(msg) + '\n\n'.join(['\n'.join(msg) for msg in errors]))

    def watch_log_for_death(self, nodes, from_mark=None, timeout=600, filename='system.log'):
        """
        Watch the log of this node until it detects that the provided other
        nodes are marked dead. This method returns nothing but throw a
        TimeoutError if all the requested node have not been found to be
        marked dead before timeout sec.
        A mark as returned by mark_log() can be used as the from_mark
        parameter to start watching the log from a given position. Otherwise
        the log is watched from the beginning.
        """
        tofind = nodes if isinstance(nodes, list) else [nodes]
        tofind = ["%s is now [dead|DOWN]" % node.address_for_version(self.get_cassandra_version()) for node in tofind]
        self.watch_log_for(tofind, from_mark=from_mark, timeout=timeout, filename=filename)

    def watch_log_for_alive(self, nodes, from_mark=None, timeout=120, filename='system.log'):
        """
        Watch the log of this node until it detects that the provided other
        nodes are marked UP. This method works similarly to watch_log_for_death.
        """
        tofind = nodes if isinstance(nodes, list) else [nodes]
        tofind = ["%s.* is now UP" % node.address_for_version(self.get_cassandra_version()) for node in tofind]
        self.watch_log_for(tofind, from_mark=from_mark, timeout=timeout, filename=filename)

    def raise_node_error_if_cassandra_process_is_terminated(self):
        if not self._is_pid_running():
            msg = "C* process with {pid} is terminated".format(pid=self.pid)
            common.debug(msg)
            raise NodeError(msg)

    def wait_for_binary_interface(self, **kwargs):
        """
        Waits for the Binary CQL interface to be listening.  If > 1.2 will check
        log for 'Starting listening for CQL clients' before checking for the
        interface to be listening.

        Emits a warning if not listening after given timeout (default NODE_WAIT_TIMEOUT_IN_SECS) in seconds.
        Raises NodeError if C* process terminates during start.
        Raises TimeoutError if log message for "starting listening" is not found in logs in given timeout.
        """
        timeout = kwargs.get('timeout', NODE_WAIT_TIMEOUT_IN_SECS)
        kwargs['timeout'] = timeout

        if self.pid:
            kwargs['error_on_pid_terminated'] = True

        if self.cluster.version() >= '1.2':
            self.watch_log_for("Starting listening for CQL clients", **kwargs)

        binary_itf = self.network_interfaces['binary']
        if not common.check_socket_listening(binary_itf, timeout=timeout):
            warnings.warn("Binary interface %s:%s is not listening after %s seconds, node may have failed to start."
                          % (binary_itf[0], binary_itf[1], timeout))

    def wait_for_thrift_interface(self, **kwargs):
        """
        Waits for the Thrift interface to be listening.

        Emits a warning if not listening after given timeout (default NODE_WAIT_TIMEOUT_IN_SECS) in seconds.
        """
        if self.cluster.version() >= '4':
            return

        timeout = kwargs.get('timeout', NODE_WAIT_TIMEOUT_IN_SECS)
        kwargs['timeout'] = timeout

        self.watch_log_for("Listening for thrift clients...", **kwargs)

        thrift_itf = self.network_interfaces['thrift']
        if not common.check_socket_listening(thrift_itf, timeout=timeout):
            warnings.warn(
                "Thrift interface {}:{} is not listening after {} seconds, node may have failed to start.".format(
                    thrift_itf[0], thrift_itf[1], timeout))

    def get_launch_bin(self):
        cdir = self.get_install_dir()
        launch_bin = common.join_bin(cdir, 'bin', 'cassandra')
        # Copy back the cassandra scripts since profiling may have modified it the previous time
        shutil.copy(launch_bin, self.get_bin_dir())
        return common.join_bin(self.get_path(), 'bin', 'cassandra')

    def add_custom_launch_arguments(self, args):
        pass

    def start(self,
              join_ring=True,
              no_wait=False,
              verbose=False,
              update_pid=True,
              wait_other_notice=True,
              replace_token=None,
              replace_address=None,
              jvm_args=None,
              wait_for_binary_proto=False,
              profile_options=None,
              use_jna=False,
              quiet_start=False,
              allow_root=False,
              set_migration_task=True,
              jvm_version=None):
        """
        Start the node. Options includes:
          - join_ring: if false, start the node with -Dcassandra.join_ring=False
          - no_wait: by default, this method returns when the node is started and listening to clients.
            If no_wait=True, the method returns sooner.
          - wait_other_notice: if truthy, this method returns only when all other live node of the cluster
            have marked this node UP. if an integer, sets the timeout for how long to wait
          - replace_token: start the node with the -Dcassandra.replace_token option.
          - replace_address: start the node with the -Dcassandra.replace_address option.
        """
        if jvm_args is None:
            jvm_args = []

        if set_migration_task and self.cluster.cassandra_version() >= '3.0.1':
            jvm_args += ['-Dcassandra.migration_task_wait_in_seconds={}'.format(len(self.cluster.nodes) * 2)]

        # Validate Windows env
        if common.is_modern_windows_install(self.cluster.version()) and not common.is_ps_unrestricted():
            raise NodeError("PS Execution Policy must be unrestricted when running C* 2.1+")

        if not common.is_win() and quiet_start:
            common.warning("Tried to set Windows quiet start behavior, but we're not running on Windows.")

        if self.is_running():
            raise NodeError("{} is already running".format(self.name))

        for itf in list(self.network_interfaces.values()):
            if itf is not None and replace_address is None:
                common.assert_socket_available(itf)

        if wait_other_notice:
            marks = [(node, node.mark_log()) for node in list(self.cluster.nodes.values()) if node.is_live()]
        else:
            marks = []

        self.mark = self.mark_log()

        launch_bin = self.get_launch_bin()

        # If Windows, change entries in .bat file to split conf from binaries
        if common.is_win():
            self.__clean_bat()

        if profile_options is not None:
            config = common.get_config()
            if 'yourkit_agent' not in config:
                raise NodeError("Cannot enable profile. You need to set 'yourkit_agent' to the path of your agent in a ~/.ccm/config")
            cmd = '-agentpath:{}'.format(config['yourkit_agent'])
            if 'options' in profile_options:
                cmd = cmd + '=' + profile_options['options']
            print_(cmd)
            # Yes, it's fragile as shit
            pattern = r'cassandra_parms="-Dlog4j.configuration=log4j-server.properties -Dlog4j.defaultInitOverride=true'
            common.replace_in_file(launch_bin, pattern, '    ' + pattern + ' ' + cmd + '"')

        os.chmod(launch_bin, os.stat(launch_bin).st_mode | stat.S_IEXEC)

        env = self.get_env()

        extension.append_to_server_env(self, env)

        if common.is_win():
            self._clean_win_jmx()

        pidfile = os.path.join(self.get_path(), 'cassandra.pid')
        args = [launch_bin]

        self.add_custom_launch_arguments(args)

        args = args + ['-p', pidfile, '-Dcassandra.join_ring=%s' % str(join_ring)]

        args.append('-Dcassandra.logdir=%s' % os.path.join(self.get_path(), 'logs'))
        if replace_token is not None:
            args.append('-Dcassandra.replace_token=%s' % str(replace_token))
        if replace_address is not None:
            args.append('-Dcassandra.replace_address=%s' % str(replace_address))
        if use_jna is False:
            args.append('-Dcassandra.boot_without_jna=true')
        if allow_root:
            args.append('-R')
        env['JVM_EXTRA_OPTS'] = env.get('JVM_EXTRA_OPTS', "") + " " + " ".join(jvm_args)

        # Upgrade scenarios may want to test upgrades to a specific Java version. In case a prior C* version
        # requires a lower Java version, we would keep its JAVA_HOME in self.__environment_variables and therefore
        # prevent using the "intended" Java version for the C* version to upgrade to.
        if not self.__original_java_home:
            # Save the "original" JAVA_HOME + PATH to restore it.
            self.__original_java_home = os.environ['JAVA_HOME']
            self.__original_path = os.environ['PATH']
            logger.info("Saving original JAVA_HOME={} PATH={}".format(self.__original_java_home, self.__original_path))
        else:
            # Restore the "original" JAVA_HOME + PATH to restore it.
            env['JAVA_HOME'] = self.__original_java_home
            env['PATH'] = self.__original_path
            logger.info("Restoring original JAVA_HOME={} PATH={}".format(self.__original_java_home, self.__original_path))

        env = common.update_java_version(jvm_version=jvm_version,
                                         install_dir=self.get_install_dir(),
                                         cassandra_version=self.get_cassandra_version(),
                                         env=env,
                                         info_message=self.name)
        # Need to update the node's environment for nodetool and other tools.
        # (e.g. the host's JAVA_HOME points to Java 11, but the node's software is only for Java 8)
        for k in 'JAVA_HOME', 'PATH':
            self.__environment_variables[k] = env[k]

        common.info("Starting {} with JAVA_HOME={} java_version={} cassandra_version={}, install_dir={}"
                    .format(self.name, env['JAVA_HOME'], common.get_jdk_version_int(env=env),
                            self.get_cassandra_version(), self.get_install_dir()))

        # In case we are restarting a node
        # we risk reading the old cassandra.pid file
        self._delete_old_pid()

        # Always write the stdout+stderr of the launched process to log files to make finding startup issues easier.
        start_time = time.time()
        stdout_sink = open(os.path.join(self.log_directory(), 'startup-{}-stdout.log'.format(start_time)), "w+")
        stderr_sink = open(os.path.join(self.log_directory(), 'startup-{}-stderr.log'.format(start_time)), "w+")

        if common.is_win():
            # clean up any old dirty_pid files from prior runs
            dirty_pid_path = os.path.join(self.get_path() + "dirty_pid.tmp")
            if (os.path.isfile(dirty_pid_path)):
                os.remove(dirty_pid_path)

            if quiet_start and self.cluster.version() >= '2.2.4':
                args.append('-q')

            process = subprocess.Popen(args, cwd=self.get_bin_dir(), env=env, stdout=stdout_sink, stderr=stderr_sink)
        else:
            process = subprocess.Popen(args, env=env, stdout=stdout_sink, stderr=stderr_sink)

        process.stderr_file = stderr_sink

        if verbose:
            common.debug("verbose mode: waiting for the start process out/err (and termination)")
            stdout, stderr = process.communicate()
            print_(str(stdout))
            print_(str(stderr))

        # Our modified batch file writes a dirty output with more than just the pid - clean it to get in parity
        # with *nix operation here.
        if common.is_win():
            self.__clean_win_pid()
            self._update_pid(process)
            print_("Started: {0} with pid: {1}".format(self.name, self.pid), file=sys.stderr, flush=True)

        if update_pid or wait_for_binary_proto:
            # at this moment we should have PID and it should be running...
            if not self._wait_for_running(process, timeout_s=7):
                raise NodeError("Node {n} is not running".format(n=self.name), process)

        # if requested wait for other nodes to observe this one (via gossip)
        if common.is_int_not_bool(wait_other_notice):
            for node, mark in marks:
                node.watch_log_for_alive(self, from_mark=mark, timeout=wait_other_notice)
        elif wait_other_notice:
            for node, mark in marks:
                node.watch_log_for_alive(self, from_mark=mark)

        # if requested wait for binary protocol to start
        if common.is_int_not_bool(wait_for_binary_proto):
            self.wait_for_binary_interface(from_mark=self.mark, timeout=wait_for_binary_proto)
        elif wait_for_binary_proto:
            self.wait_for_binary_interface(from_mark=self.mark)

        return process

    def _wait_for_running(self, process, timeout_s):
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if self.is_running():
                return True
            time.sleep(0.5)
            self._update_pid(process)
        return self.is_running()

    def stop(self, wait=True, wait_other_notice=False, signal_event=signal.SIGTERM, **kwargs):
        """
        Stop the node.
          - wait: if True (the default), wait for the Cassandra process to be
            really dead. Otherwise return after having sent the kill signal.
          - wait_other_notice: return only when the other live nodes of the
            cluster have marked this node has dead.
          - signal_event: Signal event to send to Cassandra; default is to
            let Cassandra clean up and shut down properly (SIGTERM [15])
          - Optional:
             + gently: Let Cassandra clean up and shut down properly; unless
                       false perform a 'kill -9' which shuts down faster.
        """
        if self.is_running():
            if wait_other_notice:
                marks = [(node, node.mark_log()) for node in list(self.cluster.nodes.values()) if node.is_live() and node is not self]

            if common.is_win():
                # Just taskkill the instance, don't bother trying to shut it down gracefully.
                # Node recovery should prevent data loss from hard shutdown.
                # We have recurring issues with nodes not stopping / releasing files in the CI
                # environment so it makes more sense just to murder it hard since there's
                # really little downside.

                # We want the node to flush its data before shutdown as some tests rely on small writes being present.
                # The default Periodic sync at 10 ms may not have flushed data yet, causing tests to fail.
                # This is not a hard requirement, however, so we swallow any exceptions this may throw and kill anyway.
                if signal_event is signal.SIGTERM:
                    try:
                        self.flush()
                    except:
                        common.warning("Failed to flush node: {0} on shutdown.".format(self.name))
                        pass

                os.system("taskkill /F /PID " + str(self.pid))
                if self._find_pid_on_windows():
                    common.warning("Failed to terminate node: {0} with pid: {1}".format(self.name, self.pid))
            else:
                # Determine if the signal event should be updated to keep API compatibility
                if 'gently' in kwargs and kwargs['gently'] is False:
                    signal_event = signal.SIGKILL

                os.kill(self.pid, signal_event)

            if wait_other_notice:
                for node, mark in marks:
                    node.watch_log_for_death(self, from_mark=mark)
            else:
                time.sleep(.1)

            still_running = self.is_running()
            if still_running and wait:
                wait_time_sec = 1
                for i in xrange(0, 7):
                    # we'll double the wait time each try and cassandra should
                    # not take more than 1 minute to shutdown
                    time.sleep(wait_time_sec)
                    if not self.is_running():
                        return True
                    wait_time_sec = wait_time_sec * 2
                raise NodeError("Problem stopping node %s" % self.name)
            else:
                return True
        else:
            return False

    def wait_for_compactions(self, timeout=120):
        """
        Wait for all compactions to finish on this node.
        """
        pattern = re.compile("pending tasks:? +0")
        start = time.time()
        while time.time() - start < timeout:
            output, err, rc = self.nodetool("compactionstats")
            if pattern.search(output):
                return
            time.sleep(1)
        raise TimeoutError.create(start=start, timeout=timeout,
                                  msg="Compactions did not finish in {} seconds".format(timeout),
                                  node=self.name)

    def nodetool_process(self, cmd):
        env = self.get_env()
        nodetool = self.get_tool('nodetool')
        args = [nodetool, '-h', 'localhost', '-p', str(self.jmx_port)]
        args += shlex.split(cmd)

        return subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    def nodetool(self, cmd):
        p = self.nodetool_process(cmd)
        return handle_external_tool_process(p, ['nodetool', '-h', 'localhost', '-p', str(self.jmx_port)] + shlex.split(cmd))

    def dsetool(self, cmd):
        raise common.ArgumentError('Cassandra nodes do not support dsetool')

    def dse(self, dse_options=None):
        raise common.ArgumentError('Cassandra nodes do not support dse')

    def hadoop(self, hadoop_options=None):
        raise common.ArgumentError('Cassandra nodes do not support hadoop')

    def hive(self, hive_options=None):
        raise common.ArgumentError('Cassandra nodes do not support hive')

    def pig(self, pig_options=None):
        raise common.ArgumentError('Cassandra nodes do not support pig')

    def sqoop(self, sqoop_options=None):
        raise common.ArgumentError('Cassandra nodes do not support sqoop')

    def bulkload_process(self, options):
        loader_bin = common.join_bin(self.get_path(), 'bin', 'sstableloader')
        env = self.get_env()
        extension.append_to_client_env(self, env)
        # CASSANDRA-8358 switched from thrift to binary port
        host, port = self.network_interfaces['thrift'] if self.get_cassandra_version() < '2.2' else self.network_interfaces['binary']
        args = ['-d', host, '-p', str(port)]
        return subprocess.Popen([loader_bin] + args + options, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)

    def bulkload(self, options):
        p = self.bulkload_process(options=options)
        return handle_external_tool_process(p, ['sstable bulkload'] + options)

    def scrub_process(self, options):
        scrub_bin = self.get_tool('sstablescrub')
        env = self.get_env()
        return subprocess.Popen([scrub_bin] + options, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)

    def scrub(self, options):
        p = self.scrub_process(options=options)
        return handle_external_tool_process(p, ['sstablescrub'] + options)

    def verify_process(self, options):
        verify_bin = self.get_tool('sstableverify')
        env = self.get_env()
        return subprocess.Popen([verify_bin] + options, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)

    def verify(self, options):
        p = self.verify_process(options=options)
        return handle_external_tool_process(p, ['sstableverify'] + options)

    def enable_aoss(self, thrift_port=10000, web_ui_port=9077):
        pass

    def run_cqlsh_process(self, cmds=None, cqlsh_options=None, terminator=''):
        if cqlsh_options is None:
            cqlsh_options = []
        cqlsh = self.get_tool('cqlsh')
        env = self.get_env()
        extension.append_to_client_env(self, env)
        if self.get_base_cassandra_version() >= 2.1:
            host, port = self.network_interfaces['binary']
        else:
            host, port = self.network_interfaces['thrift']
        args = []
        args += cqlsh_options
        extension.append_to_cqlsh_args(self, env, args)
        args += [host, str(port)]
        sys.stdout.flush()

        if cmds is None:
            if common.is_win():
                subprocess.Popen([cqlsh] + args, env=env, creationflags=subprocess.CREATE_NEW_CONSOLE)
            else:
                os.execve(cqlsh, [common.platform_binary('cqlsh')] + args, env)
        else:
            p = subprocess.Popen([cqlsh] + args, env=env, stdin=subprocess.PIPE, stderr=subprocess.PIPE,
                                 stdout=subprocess.PIPE, universal_newlines=True)

        if cmds is not None:
            for cmd in cmds.split(';'):
                cmd = cmd.strip()
                if cmd:
                    p.stdin.write(cmd + terminator)
            p.stdin.write("quit;\n")

        return p

    def run_cqlsh(self, cmds=None, cqlsh_options=None, terminator=';\n'):
        p = self.run_cqlsh_process(cmds, cqlsh_options, terminator)
        return handle_external_tool_process(p, ['cqlsh', cmds, cqlsh_options])

    def set_log_level(self, new_level, class_name=None):
        known_level = ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'OFF']
        if new_level not in known_level:
            raise common.ArgumentError("Unknown log level %s (use one of %s)" % (new_level, " ".join(known_level)))

        if class_name:
            self.__classes_log_level[class_name] = new_level
        else:
            self.__global_log_level = new_level
        # loggers changed > 2.1
        if self.get_base_cassandra_version() < 2.1:
            self._update_log4j()
        else:
            self.__update_logback()
        return self

    #
    # Update log4j config: copy new log4j-server.properties into
    # ~/.ccm/name-of-cluster/nodeX/conf/log4j-server.properties
    #
    def update_log4j(self, new_log4j_config):
        cassandra_conf_dir = os.path.join(self.get_conf_dir(),
                                          'log4j-server.properties')
        common.copy_file(new_log4j_config, cassandra_conf_dir)

    #
    # Update logback config: copy new logback.xml into
    # ~/.ccm/name-of-cluster/nodeX/conf/logback.xml
    #
    def update_logback(self, new_logback_config):
        cassandra_conf_dir = os.path.join(self.get_conf_dir(),
                                          'logback.xml')
        common.copy_file(new_logback_config, cassandra_conf_dir)

    def update_startup_byteman_script(self, byteman_startup_script):
        """
        Update the byteman startup script, i.e., rule injected before the node starts.

        :param byteman_startup_script: the relative path to the script
        :raise common.LoadError: if the node does not have byteman installed
        """
        if self.byteman_port == '0':
            raise common.LoadError('Byteman is not installed')
        self.byteman_startup_script = byteman_startup_script
        self.import_config_files()

    def clear(self, clear_all=False, only_data=False):
        data_dirs = ['data{0}'.format(x) for x in xrange(0, self.cluster.data_dir_count)]
        data_dirs.append("commitlogs")
        if clear_all:
            data_dirs.extend(['saved_caches', 'logs'])
        for d in data_dirs:
            full_dir = os.path.join(self.get_path(), d)
            if only_data and d != "commitlogs":
                for dir in os.listdir(full_dir):
                    keyspace_dir = os.path.join(full_dir, dir)
                    if os.path.isdir(keyspace_dir) and dir != "system" and dir != "system_cluster_metadata":
                        for f in os.listdir(keyspace_dir):
                            table_dir = os.path.join(keyspace_dir, f)
                            shutil.rmtree(table_dir)
                            os.mkdir(table_dir)
            else:
                common.rmdirs(full_dir)
                os.mkdir(full_dir)

        # Needed for any subdirs stored underneath a data directory.
        # Common for hints post CASSANDRA-6230
        for dir in self._get_directories():
            if not os.path.exists(dir):
                os.mkdir(dir)

    def run_sstable2json(self, out_file=None, keyspace=None, datafiles=None, column_families=None, keys=None, enumerate_keys=False):
        if out_file is None:
            out_file = sys.stdout
        sstable2json = self._find_cmd('sstable2json')
        env = self.get_env()
        sstablefiles = self.__gather_sstables(datafiles, keyspace, column_families)
        print_(sstablefiles)
        for sstablefile in sstablefiles:
            print_("-- {0} -----".format(os.path.basename(sstablefile)))
            args = [sstable2json, sstablefile]
            if enumerate_keys:
                args = args + ["-e"]
            if keys is not None:
                for key in keys:
                    args = args + ["-k", key]
            subprocess.call(args, env=env, stdout=out_file)
            print_("")

    def run_json2sstable(self, in_file, ks, cf, keyspace=None, datafiles=None, column_families=None, enumerate_keys=False):
        json2sstable = self._find_cmd('json2sstable')
        env = self.get_env()
        sstablefiles = self.__gather_sstables(datafiles, keyspace, column_families)

        for sstablefile in sstablefiles:
            in_file_name = os.path.abspath(in_file.name)
            args = [json2sstable, "-s", "-K", ks, "-c", cf, in_file_name, sstablefile]
            subprocess.call(args, env=env)

    def run_sstablesplit_process(self, datafiles=None, size=None, keyspace=None, column_families=None,
                                 no_snapshot=False, debug=False):
        sstablesplit = self._find_cmd('sstablesplit')
        env = self.get_env()
        sstablefiles = self.__gather_sstables(datafiles, keyspace, column_families)

        processes = []

        def do_split(f):
            print_("-- {0}-----".format(os.path.basename(f)))
            cmd = [sstablesplit]
            if size is not None:
                cmd += ['-s', str(size)]
            if no_snapshot:
                cmd.append('--no-snapshot')
            if debug:
                cmd.append('--debug')
            cmd.append(f)
            p = subprocess.Popen(cmd, cwd=os.path.join(self.get_install_dir(), 'bin'),
                                 env=env, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            processes.append(p)

        for sstablefile in sstablefiles:
            do_split(sstablefile)

        return processes

    def run_sstablesplit(self, datafiles=None, size=None, keyspace=None, column_families=None,
                         no_snapshot=False, debug=False):
        processes = self.run_sstablesplit_process(datafiles, size, keyspace, column_families, no_snapshot, debug)
        results = []

        for p in processes:
            results.append(handle_external_tool_process(p, "sstablesplit"))

        return results

    def run_sstablemetadata_process(self, datafiles=None, keyspace=None, column_families=None):
        cdir = self.get_install_dir()
        sstablemetadata = common.join_bin(cdir, os.path.join('tools', 'bin'), 'sstablemetadata')
        env = self.get_env()
        sstablefiles = self.__gather_sstables(datafiles=datafiles, keyspace=keyspace, columnfamilies=column_families)

        cmd = [sstablemetadata]
        cmd.extend(sstablefiles)

        return subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)

    def run_sstablemetadata(self, datafiles=None, keyspace=None, column_families=None):
        p = self.run_sstablemetadata_process(datafiles, keyspace, column_families)
        return handle_external_tool_process(p, "sstablemetadata on keyspace: {}, column_family: {}".format(keyspace, column_families))

    def run_sstabledump_process(self, datafiles=None, keyspace=None, column_families=None, keys=None, enumerate_keys=False, command=False):
        sstabledump = self._find_cmd('sstabledump')

        env = self.get_env()
        sstablefiles = self.__gather_sstables(datafiles=datafiles, keyspace=keyspace, columnfamilies=column_families)
        processes = []

        def do_dump(sstable):
            if command:
                print_("-- {0} -----".format(os.path.basename(sstable)))
            cmd = [sstabledump, sstable]
            if enumerate_keys:
                cmd.append('-e')
            if keys is not None:
                for key in keys:
                    cmd = cmd + ["-k", key]
            p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)
            if command:
                out, err, rc = handle_external_tool_process(p, "sstabledump")
                print_(out)
                print_('\n')
            else:
                processes.append(p)

        for sstable in sstablefiles:
            do_dump(sstable)

        return processes

    def run_sstabledump(self, datafiles=None, keyspace=None, column_families=None, keys=None, enumerate_keys=False, command=False):
        processes = self.run_sstabledump_process(datafiles, keyspace, column_families, keys, enumerate_keys, command)
        results = []

        for p in processes:
            results.append(handle_external_tool_process(p, "sstabledump"))

        return results

    def run_sstableexpiredblockers_process(self, keyspace=None, column_family=None):
        cdir = self.get_install_dir()
        sstableexpiredblockers = common.join_bin(cdir, os.path.join('tools', 'bin'), 'sstableexpiredblockers')
        env = self.get_env()
        cmd = [sstableexpiredblockers, keyspace, column_family]
        return subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)

    def run_sstableexpiredblockers(self, keyspace=None, column_family=None):
        p = self.run_sstableexpiredblockers_process(keyspace=keyspace, column_family=column_family)
        if p is not None:
            return handle_external_tool_process(p, ['sstableexpiredblockers', keyspace, column_family])
        else:
            return None, None, None

    def run_sstableupgrade_process(self, keyspace=None, column_family=None):
        cdir = self.get_install_dir()
        sstableupgrade = self.get_tool('sstableupgrade')
        env = self.get_env()
        cmd = [sstableupgrade, keyspace, column_family]
        p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)
        return p

    def run_sstableupgrade(self, keyspace=None, column_family=None):
        p = self.run_sstableupgrade_process(keyspace, column_family)
        if p is not None:
            return handle_external_tool_process(p, "sstableupgrade on {} : {}".format(keyspace, column_family))
        else:
            return None, None, None

    def get_sstablespath(self, datafiles=None, keyspace=None, tables=None, **kawrgs):
        sstablefiles = self.__gather_sstables(datafiles=datafiles, keyspace=keyspace, columnfamilies=tables)
        return sstablefiles

    def run_sstablerepairedset_process(self, set_repaired=True, datafiles=None, keyspace=None, column_families=None):
        cdir = self.get_install_dir()
        sstablerepairedset = common.join_bin(cdir, os.path.join('tools', 'bin'), 'sstablerepairedset')
        env = self.get_env()
        sstablefiles = self.__gather_sstables(datafiles, keyspace, column_families)
        processes = []

        for sstable in sstablefiles:
            if set_repaired:
                cmd = [sstablerepairedset, "--really-set", "--is-repaired", sstable]
            else:
                cmd = [sstablerepairedset, "--really-set", "--is-unrepaired", sstable]
            p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processes.append(p)

        return processes

    def run_sstablerepairedset(self, set_repaired=True, datafiles=None, keyspace=None, column_families=None):
        processes = self.run_sstablerepairedset_process(set_repaired=set_repaired, datafiles=datafiles, keyspace=keyspace, column_families=column_families)
        results = []
        for p in processes:
            results.append(handle_external_tool_process(p, "sstablerepairedset on {} : {}".format(keyspace, column_families)))
        return results

    def run_sstablelevelreset_process(self, keyspace, cf):
        cdir = self.get_install_dir()
        sstablelevelreset = common.join_bin(cdir, os.path.join('tools', 'bin'), 'sstablelevelreset')
        env = self.get_env()

        cmd = [sstablelevelreset, "--really-reset", keyspace, cf]

        return subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)

    def run_sstablelevelreset(self, keyspace, cf):
        p = self.run_sstablelevelreset_process(keyspace, cf)
        return handle_external_tool_process(p, "sstablelevelreset on {} : {}".format(keyspace, cf))

    def run_sstableofflinerelevel_process(self, keyspace, cf, dry_run=False):
        cdir = self.get_install_dir()
        sstableofflinerelevel = common.join_bin(cdir, os.path.join('tools', 'bin'), 'sstableofflinerelevel')
        env = self.get_env()

        if dry_run:
            cmd = [sstableofflinerelevel, "--dry-run", keyspace, cf]
        else:
            cmd = [sstableofflinerelevel, keyspace, cf]

        return subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)

    def run_sstableofflinerelevel(self, keyspace, cf, dry_run=False):
        p = self.run_sstableofflinerelevel_process(keyspace, cf, dry_run=dry_run)
        return handle_external_tool_process(p, "sstableoflinerelevel on {} : {}".format(keyspace, cf))

    def run_sstableverify_process(self, keyspace, cf, options=None):
        cdir = self.get_install_dir()
        sstableverify = common.join_bin(cdir, 'bin', 'sstableverify')
        env = self.get_env()

        cmd = [sstableverify, keyspace, cf]
        if options is not None:
            cmd[1:1] = options

        return subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)

    def run_sstableverify(self, keyspace, cf, options=None):
        p = self.run_sstableverify_process(keyspace, cf, options=options)
        return handle_external_tool_process(p, "sstableverify on {} : {} with options: {}".format(keyspace, cf, options))

    def _find_cmd(self, cmd):
        """
        Locates command under cassandra root and fixes permissions if needed
        """
        cdir = self.get_install_cassandra_root()
        if self.get_base_cassandra_version() >= 2.1:
            fcmd = common.join_bin(cdir, os.path.join('tools', 'bin'), cmd)
        else:
            fcmd = common.join_bin(cdir, 'bin', cmd)
        try:
            if os.path.exists(fcmd):
                os.chmod(fcmd, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
        except:
            common.warning("Couldn't change permissions to use {0}.".format(cmd))
            common.warning("If it didn't work, you will have to do so manually.")
        return fcmd

    def has_cmd(self, cmd):
        """
        Indicates if specified command can be found under cassandra root
        """
        return os.path.exists(self._find_cmd(cmd))

    def list_keyspaces(self):
        keyspaces = os.listdir(os.path.join(self.get_path(), 'data0'))
        keyspaces.remove('system')
        return keyspaces

    def get_sstables_per_data_directory(self, keyspace, column_family):
        keyspace_dirs = [os.path.join(self.get_path(), "data{0}".format(x), keyspace) for x in xrange(0, self.cluster.data_dir_count)]
        cf_glob = '*'
        if column_family:
            # account for changes in data dir layout from CASSANDRA-5202
            if self.get_base_cassandra_version() < 2.1:
                cf_glob = column_family
            else:
                cf_glob = column_family + '-*'
        for keyspace_dir in keyspace_dirs:
            if not os.path.exists(keyspace_dir):
                raise common.ArgumentError("Unknown keyspace {0}".format(keyspace))

        # data directory layout is changed from 1.1
        if self.get_base_cassandra_version() < 1.1:
            files = [glob.glob(os.path.join(keyspace_dir, "{0}*-Data.db".format(column_family))) for keyspace_dir in keyspace_dirs]
        elif self.get_base_cassandra_version() < 2.2:
            files = [glob.glob(os.path.join(keyspace_dir, cf_glob, "%s-%s*-Data.db" % (keyspace, column_family))) for keyspace_dir in keyspace_dirs]
        else:
            files = [glob.glob(os.path.join(keyspace_dir, cf_glob, "*-Data.db")) for keyspace_dir in keyspace_dirs]

        for d in files:
            for f in d:
                if os.path.exists(f.replace('Data.db', 'Compacted')):
                    files.remove(f)
        return files

    def get_sstables(self, keyspace, column_family):
        return [f for sublist in self.get_sstables_per_data_directory(keyspace, column_family) for f in sublist]

    def get_sstables_via_sstableutil(self, keyspace, table, sstabletype='all', oplogs=False, cleanup=False, match='-Data.db'):
        env = common.make_cassandra_env(self.get_install_cassandra_root(), self.get_node_cassandra_root())
        tool_bin = self.get_tool('sstableutil')

        args = [tool_bin, '--type', sstabletype]

        if oplogs:
            args.extend(['--oplog'])
        if cleanup:
            args.extend(['--cleanup'])

        args.extend([keyspace, table])

        p = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stdout, stderr = p.communicate()

        if p.returncode != 0:
            common.error("""Error invoking sstableutil; returned {code}; args={args}; env={env}
            stdout:
            {stdout}
            stderr:
            {stderr}
            """.format(code=p.returncode, args=args, env=env, stdout=stdout, stderr=stderr))
            raise Exception("Error invoking sstableutil; returned {code}".format(code=p.returncode))

        return sorted(filter(lambda s: match in s, stdout.splitlines()))

    def stress_process(self, stress_options=None, whitelist=False):
        if stress_options is None:
            stress_options = []
        else:
            stress_options = stress_options[:]

        stress = common.get_stress_bin(self.get_install_dir())
        if self.cluster.cassandra_version() <= '2.1':
            stress_options.append('-d')
            stress_options.append(self.address())
        else:
            stress_options.append('-node')
            if whitelist:
                stress_options.append("whitelist")
            stress_options.append(self.address())
            # specify used jmx port if not already set
            if not [opt for opt in stress_options if opt.startswith('jmx=')]:
                stress_options.extend(['-port', 'jmx=' + self.jmx_port])
        args = [stress] + stress_options
        try:
            p = subprocess.Popen(args, cwd=common.parse_path(stress),
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return p
        except KeyboardInterrupt:
            pass

    def stress(self, stress_options=None, whitelist=False):
        p = self.stress_process(stress_options=stress_options, whitelist=whitelist)
        try:
            return handle_external_tool_process(p, ['stress'] + stress_options)
        except KeyboardInterrupt:
            pass

    def shuffle(self, cmd):
        cdir = self.get_install_dir()
        shuffle = common.join_bin(cdir, 'bin', 'cassandra-shuffle')
        host = self.address()
        args = [shuffle, '-h', host, '-p', str(self.jmx_port)] + [cmd]
        try:
            subprocess.call(args)
        except KeyboardInterrupt:
            pass

    def data_size(self, live_data=None):
        """Uses `nodetool info` to get the size of a node's data in KB."""
        if live_data is not None:
            warnings.warn("The 'live_data' keyword argument is deprecated.",
                          DeprecationWarning)
        output = self.nodetool('info')[0]
        return _get_load_from_info_output(output)

    def flush(self, options=None):
        if options is None:
            options = []
        args = ["flush"] + options
        cmd = ' '.join(args)
        self.nodetool(cmd)

    def compact(self, options=None):
        if options is None:
            options = []
        args = ["compact"] + options
        cmd = ' '.join(args)
        self.nodetool(cmd)

    def drain(self, block_on_log=False):
        mark = self.mark_log()
        self.nodetool("drain")
        if block_on_log:
            self.watch_log_for("DRAINED", from_mark=mark)

    def repair(self, options=None):
        if options is None:
            options = []
        args = ["repair"] + options
        cmd = ' '.join(args)
        return self.nodetool(cmd)

    def move(self, new_token):
        self.nodetool("move " + str(new_token))

    def cleanup(self, options=None):
        if options is None:
            options = []
        args = ["cleanup"] + options
        cmd = ' '.join(args)
        self.nodetool(cmd)

    def decommission(self, force=False):
        cmd = 'decommission'
        if force:
            cmd += " --force"
        self.nodetool(cmd)
        self.status = Status.DECOMMISSIONED
        self._update_config()

    def removeToken(self, token):
        self.nodetool("removeToken " + str(token))

    def import_config_files(self):
        self._update_config()
        self.copy_config_files()
        self._update_yaml()
        self._update_topology_file()
        # loggers changed > 2.1
        if self.get_base_cassandra_version() < 2.1:
            self._update_log4j()
        else:
            self.__update_logback()
        self.__update_envfile()

    def import_dse_config_files(self):
        raise common.ArgumentError('Cannot import DSE configuration files on a Cassandra node')

    def copy_config_files(self):
        conf_dir = os.path.join(self.get_install_dir(), 'conf')
        for name in os.listdir(conf_dir):
            filename = os.path.join(conf_dir, name)
            if os.path.isfile(filename):
                shutil.copy(filename, self.get_conf_dir())

    def import_bin_files(self):
        bin_dir = os.path.join(self.get_install_dir(), 'bin')
        for name in os.listdir(bin_dir):
            filename = os.path.join(bin_dir, name)
            if os.path.isfile(filename):
                shutil.copy(filename, self.get_bin_dir())
                common.add_exec_permission(bin_dir, name)

    def __clean_bat(self):
        # While the Windows specific changes to the batch files to get them to run are
        # fairly extensive and thus pretty brittle, all the changes are very unique to
        # the needs of ccm and shouldn't be pushed into the main repo.

        # Change the nodes to separate jmx ports
        bin_dir = os.path.join(self.get_path(), 'bin')
        jmx_port_pattern = "-Dcom.sun.management.jmxremote.port="
        bat_file = os.path.join(bin_dir, "cassandra.bat")
        common.replace_in_file(bat_file, jmx_port_pattern, " " + jmx_port_pattern + self.jmx_port + "^")

        # Split binaries from conf
        home_pattern = "if NOT DEFINED CASSANDRA_HOME set CASSANDRA_HOME=%CD%"
        common.replace_in_file(bat_file, home_pattern, "set CASSANDRA_HOME=" + self.get_install_dir())

        classpath_pattern = "set CLASSPATH=\\\"%CASSANDRA_HOME%\\\\conf\\\""
        common.replace_in_file(bat_file, classpath_pattern, "set CCM_DIR=\"" + self.get_path() + "\"\nset CLASSPATH=\"%CCM_DIR%\\conf\"")

        # escape the double quotes in name of the lib files in the classpath
        jar_file_pattern = "do call :append \"%%i\""
        for_statement = "for %%i in (\"%CASSANDRA_HOME%\lib\*.jar\")"
        common.replace_in_file(bat_file, jar_file_pattern, for_statement + " do call :append \\\"%%i\\\"")

        # escape double quotes in java agent path
        class_dir_pattern = "-javaagent:"
        common.replace_in_file(bat_file, class_dir_pattern, " -javaagent:\\\"%CASSANDRA_HOME%\\lib\\jamm-0.2.5.jar\\\"^")

        # escape the double quotes in name of the class directories
        class_dir_pattern = "set CASSANDRA_CLASSPATH="
        main_classes = "\\\"%CASSANDRA_HOME%\\build\\classes\\main\\\";"
        thrift_classes = "\\\"%CASSANDRA_HOME%\\build\\classes\\thrift\\\""
        common.replace_in_file(bat_file, class_dir_pattern, "set CASSANDRA_CLASSPATH=%CLASSPATH%;" +
                               main_classes + thrift_classes)

        # background the server process and grab the pid
        run_text = "\\\"%JAVA_HOME%\\bin\\java\\\" %JAVA_OPTS% %CASSANDRA_PARAMS% -cp %CASSANDRA_CLASSPATH% \\\"%CASSANDRA_MAIN%\\\""
        run_pattern = ".*-cp.*"
        common.replace_in_file(bat_file, run_pattern, "wmic process call create \"" + run_text + "\" > \"" +
                               self.get_path() + "/dirty_pid.tmp\"\n")

        # On Windows, remove the VerifyPorts check from cassandra.ps1
        if self.cluster.version() >= '2.1':
            common.replace_in_file(os.path.join(self.get_path(), 'bin', 'cassandra.ps1'), '        VerifyPortsAreAvailable', '')

        # Specifically call the .ps1 file in our node's folder
        common.replace_in_file(bat_file, 'powershell /file .*', 'powershell /file "' + os.path.join(self.get_path(), 'bin', 'cassandra.ps1" %*'))

    def _save(self):
        self._update_yaml()
        # loggers changed > 2.1
        if self.get_base_cassandra_version() < 2.1:
            self._update_log4j()
        else:
            self.__update_logback()
        self.__update_envfile()
        self._update_config()

    def _update_config(self):
        dir_name = self.get_path()
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)
            for dir in self._get_directories():
                os.mkdir(dir)

        filename = os.path.join(dir_name, 'node.conf')
        values = {
            'name': self.name,
            'status': self.status,
            'auto_bootstrap': self.auto_bootstrap,
            'interfaces': self.network_interfaces,
            'jmx_port': self.jmx_port,
            'config_options': self.__config_options,
            'dse_config_options': self._dse_config_options,
            'environment_variables': self.__environment_variables,
            'cassandra_version': str(self.get_cassandra_version())
        }
        if self.pid:
            values['pid'] = self.pid
        if self.initial_token:
            values['initial_token'] = self.initial_token
        if self.__install_dir is not None:
            values['install_dir'] = self.__install_dir
        if self.remote_debug_port:
            values['remote_debug_port'] = self.remote_debug_port
        if self.byteman_port:
            values['byteman_port'] = self.byteman_port
        if self.data_center:
            values['data_center'] = self.data_center
        if self.workloads is not None:
            values['workloads'] = self.workloads
        with open(filename, 'w') as f:
            yaml.safe_dump(values, f)

    def _update_yaml(self):
        conf_file = self.get_conf_file()
        with open(conf_file, 'r') as f:
            data = yaml.safe_load(f)

        with open(conf_file, 'r') as f:
            yaml_text = f.read()

        data['cluster_name'] = self.cluster.name
        data['auto_bootstrap'] = self.auto_bootstrap
        data['initial_token'] = self.initial_token
        if not self.cluster.use_vnodes and self.get_base_cassandra_version() >= 1.2:
            data['num_tokens'] = 1
        if 'seeds' in data:
            # cassandra 0.7
            data['seeds'] = self.cluster.get_seeds()
        else:
            # cassandra 0.8
            data['seed_provider'][0]['parameters'][0]['seeds'] = ','.join(self.cluster.get_seeds())
        data['listen_address'], data['storage_port'] = self.network_interfaces['storage']
        if self.network_interfaces['thrift'] is not None and self.get_base_cassandra_version() < 4:
            data['rpc_address'], data['rpc_port'] = self.network_interfaces['thrift']
        if self.network_interfaces['binary'] is not None and self.get_base_cassandra_version() >= 1.2:
            data['rpc_address'], data['native_transport_port'] = self.network_interfaces['binary']

        data['data_file_directories'] = [os.path.join(self.get_path(), 'data{0}'.format(x)) for x in xrange(0, self.cluster.data_dir_count)]
        data['commitlog_directory'] = os.path.join(self.get_path(), 'commitlogs')
        data['saved_caches_directory'] = os.path.join(self.get_path(), 'saved_caches')

        if 'metadata_directory' in data:
            data['metadata_directory'] = os.path.join(self.get_path(), 'metadata')

        if self.get_cassandra_version() > '3.0' and 'hints_directory' in yaml_text:
            data['hints_directory'] = os.path.join(self.get_path(), 'hints')

        if self.get_cassandra_version() >= '3.8':
            data['cdc_raw_directory'] = os.path.join(self.get_path(), 'cdc_raw')

        if self.cluster.partitioner:
            data['partitioner'] = self.cluster.partitioner

        # Get a map of combined cluster and node configuration with the node
        # configuration taking precedence.
        full_options = common.merge_configuration(
            self.cluster._config_options,
            self.__config_options, delete_empty=False)

        # Merge options with original yaml data.
        data = common.merge_configuration(data, full_options)

        conf_dest = os.path.join(self.get_conf_dir(), common.CASSANDRA_CONF)
        with open(conf_dest, 'w') as f:
            yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)

    def _update_log4j(self):
        append_pattern = 'log4j.appender.R.File='
        conf_file = os.path.join(self.get_conf_dir(), common.LOG4J_CONF)
        log_file = os.path.join(self.log_directory(), 'system.log')
        # log4j isn't partial to Windows \.  I can't imagine why not.
        if common.is_win():
            log_file = re.sub("\\\\", "/", log_file)
        common.replace_in_file(conf_file, append_pattern, append_pattern + log_file)

        # Setting the right log level

        # Replace the global log level
        if self.__global_log_level is not None:
            append_pattern = 'log4j.rootLogger='
            common.replace_in_file(conf_file, append_pattern, append_pattern + self.__global_log_level + ',stdout,R')

        # Class specific log levels
        for class_name in self.__classes_log_level:
            logger_pattern = 'log4j.logger'
            full_logger_pattern = logger_pattern + '.' + class_name + '='
            common.replace_or_add_into_file_tail(conf_file, full_logger_pattern, full_logger_pattern + self.__classes_log_level[class_name])

    def __update_logback(self):
        conf_file = os.path.join(self.get_conf_dir(), common.LOGBACK_CONF)

        self.__update_logback_loglevel(conf_file)

        tools_conf_file = os.path.join(self.get_conf_dir(), common.LOGBACK_TOOLS_CONF)
        self.__update_logback_loglevel(tools_conf_file)

    def __update_logback_loglevel(self, conf_file):
        # Setting the right log level - 2.2.2 introduced new debug log
        if self.get_cassandra_version() >= '2.2.2' and self.__global_log_level:
            if self.__global_log_level in ['DEBUG', 'TRACE']:
                root_log_level = self.__global_log_level
                cassandra_log_level = self.__global_log_level
            elif self.__global_log_level == 'INFO':
                root_log_level = self.__global_log_level
                cassandra_log_level = 'DEBUG'
            elif self.__global_log_level in ['WARN', 'ERROR']:
                root_log_level = 'INFO'
                cassandra_log_level = 'DEBUG'
                system_log_filter_pattern = '<level>.*</level>'
                common.replace_in_file(conf_file, system_log_filter_pattern, '      <level>' + self.__global_log_level + '</level>')
            elif self.__global_log_level == 'OFF':
                root_log_level = self.__global_log_level
                cassandra_log_level = self.__global_log_level

            cassandra_append_pattern = '<logger name="org.apache.cassandra" level=".*"/>'
            common.replace_in_file(conf_file, cassandra_append_pattern, '  <logger name="org.apache.cassandra" level="' + cassandra_log_level + '"/>')
        else:
            root_log_level = self.__global_log_level

        # Replace the global log level and org.apache.cassandra log level
        if self.__global_log_level is not None:
            root_append_pattern = '<root level=".*">'
            common.replace_in_file(conf_file, root_append_pattern, '<root level="' + root_log_level + '">')

        # Class specific log levels
        for class_name in self.__classes_log_level:
            logger_pattern = '\t<logger name="'
            full_logger_pattern = logger_pattern + class_name + '" level=".*"/>'
            common.replace_or_add_into_file_tail(conf_file, full_logger_pattern, logger_pattern + class_name + '" level="' + self.__classes_log_level[class_name] + '"/>')

    def __update_envfile(self):
        agentlib_setting = '-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address={}'.format(str(self.remote_debug_port))
        remote_debug_options = agentlib_setting
        # The cassandra-env.ps1 file has been introduced in 2.1
        if common.is_modern_windows_install(self.get_base_cassandra_version()):
            conf_file = os.path.join(self.get_conf_dir(), common.CASSANDRA_WIN_ENV)
            jmx_port_pattern = '^\s+\$JMX_PORT='
            jmx_port_setting = '    $JMX_PORT="' + self.jmx_port + '"'
            if self.get_cassandra_version() < '3.2':
                remote_debug_options = '    $env:JVM_OPTS="$env:JVM_OPTS {}"'.format(agentlib_setting)
        else:
            conf_file = os.path.join(self.get_conf_dir(), common.CASSANDRA_ENV)
            jmx_port_pattern = 'JMX_PORT='
            jmx_port_setting = 'JMX_PORT="' + self.jmx_port + '"'
            if self.get_cassandra_version() < '3.2':
                remote_debug_options = 'JVM_OPTS="$JVM_OPTS {}"'.format(agentlib_setting)

        common.replace_in_file(conf_file, jmx_port_pattern, jmx_port_setting)

        if common.is_modern_windows_install(common.get_version_from_build(node_path=self.get_path())):
            dst = os.path.join(self.get_conf_dir(), common.CASSANDRA_WIN_ENV)
            replacements = [
                ('env:CASSANDRA_HOME =', '        $env:CASSANDRA_HOME="%s"' % self.get_install_dir()),
                ('env:CASSANDRA_CONF =', '    $env:CCM_DIR="' + self.get_path() + '\\conf"\n    $env:CASSANDRA_CONF="$env:CCM_DIR"'),
                ('cp = ".*?env:CASSANDRA_HOME.conf', '    $cp = """$env:CASSANDRA_CONF"""')
            ]
            common.replaces_in_file(dst, replacements)

        if self.remote_debug_port != '0':
            remote_debug_port_pattern = '((-Xrunjdwp:)|(-agentlib:jdwp=))transport=dt_socket,server=y,suspend=n,address='
            if self.get_cassandra_version() < '3.2':
                common.replace_in_file(conf_file, remote_debug_port_pattern, remote_debug_options)
            else:
                for f in glob.glob(os.path.join(self.get_conf_dir(), common.JVM_OPTS_PATTERN)):
                    if os.path.isfile(f):
                        common.replace_in_file(f, remote_debug_port_pattern, remote_debug_options)

        if self.byteman_port != '0':
            byteman_jar = glob.glob(os.path.join(self.get_install_dir(), 'build', 'lib', 'jars', 'byteman-[0-9]*.jar'))[0]
            agent_string = "-javaagent:{}=listener:true,boot:{},port:{}".format(byteman_jar, byteman_jar, str(self.byteman_port))
            if self.byteman_startup_script is not None:
                agent_string = agent_string + ",script:{}".format(self.byteman_startup_script)
            if common.is_modern_windows_install(self.get_base_cassandra_version()):
                with open(conf_file, "r+") as conf_rewrite:
                    conf_lines = conf_rewrite.readlines()
                    # Remove trailing brace, will be replaced
                    conf_lines = conf_lines[:-1]
                    conf_lines.append("    $env:JVM_OPTS=\"$env:JVM_OPTS {}\"\n}}\n".format(agent_string))
                    conf_rewrite.seek(0)
                    conf_rewrite.truncate()
                    conf_rewrite.writelines(conf_lines)
            else:
                common.replaces_or_add_into_file_tail(conf_file, [('.*byteman.*', "JVM_OPTS=\"$JVM_OPTS {}\"".format(agent_string))], add_config_close=False)

        if self.get_cassandra_version() < '2.0.1':
            common.replace_in_file(conf_file, "-Xss", '    JVM_OPTS="$JVM_OPTS -Xss228k"')

        # gc.log was turned on by default in 2.2.5/3.0.3/3.3
        if self.get_cassandra_version() >= '2.2.5':
            gc_log_pattern = "-Xloggc"
            gc_log_path = os.path.join(self.log_directory(), 'gc.log')
            if common.is_win():
                gc_log_setting = '    $env:JVM_OPTS="$env:JVM_OPTS -Xloggc:{}"'.format(gc_log_path)
            else:
                gc_log_setting = 'JVM_OPTS="$JVM_OPTS -Xloggc:{}"'.format(gc_log_path)

            common.replace_in_file(conf_file, gc_log_pattern, gc_log_setting)

            # Java 9
            gc_log_pattern = "-Xlog[:]gc=info"
            if common.is_win():
                gc_log_setting = '    $env:JVM_OPTS="$env:JVM_OPTS -Xlog:gc=info,heap=trace,age=debug,safepoint=info,promotion=trace:file={}:time,uptime,pid,tid,level:filecount=10,filesize=10240"'.format(gc_log_path)
            else:
                gc_log_setting = 'JVM_OPTS="$JVM_OPTS -Xlog:gc=info,heap=trace,age=debug,safepoint=info,promotion=trace:file={}:time,uptime,pid,tid,level:filecount=10,filesize=10240"'.format(gc_log_path)

            common.replace_in_file(conf_file, gc_log_pattern, gc_log_setting)

        for itf in list(self.network_interfaces.values()):
            if itf is not None and common.interface_is_ipv6(itf):
                if self.get_cassandra_version() < '3.2':
                    if common.is_win():
                        common.replace_in_file(conf_file,
                                               '-Djava.net.preferIPv4Stack=true',
                                               '\t$env:JVM_OPTS="$env:JVM_OPTS -Djava.net.preferIPv4Stack=false -Djava.net.preferIPv6Addresses=true"')
                    else:
                        common.replace_in_file(conf_file,
                                               '-Djava.net.preferIPv4Stack=true',
                                               'JVM_OPTS="$JVM_OPTS -Djava.net.preferIPv4Stack=false -Djava.net.preferIPv6Addresses=true"')
                    break
                else:
                    for f in glob.glob(os.path.join(self.get_conf_dir(), common.JVM_OPTS_PATTERN)):
                        if os.path.isfile(f):
                            common.replace_in_file(f, '-Djava.net.preferIPv4Stack=true', '')
                    break

    def update_topology(self, topology):
        self._topology = topology
        self._update_topology_file()

    def _update_topology_file(self):
        content = ""
        for k, v in self._topology:
            content = "%s%s=%s:r1\n" % (content, k, v)

        topology_file = os.path.join(self.get_conf_dir(), 'cassandra-topology.properties')
        with open(topology_file, 'w') as f:
            f.write(content)

    def _is_pid_running(self):
        if self.pid is None:
            return False
        if common.is_win():
            return self._find_pid_on_windows()
        else:
            return self._find_pid_on_unix()

    def _find_pid_on_unix(self):
        try:
            os.kill(self.pid, 0)
            proc = psutil.Process(self.pid)
            if proc.status() == psutil.STATUS_ZOMBIE:
                time.sleep(2)
                raise OSError(errno.ESRCH, "process was zombie, ignoring")
        except OSError as err:
            if err.errno == errno.ESRCH:
                # not running
                return False
            elif err.errno == errno.EPERM:
                # no permission to signal this process
                return False
            else:
                # some other error
                raise
        except psutil.NoSuchProcess as err:
            return False
        else:
            return True

    def __update_status(self):
        if self.pid is None:
            if self.status in [Status.UP, Status.DECOMMISSIONED]:
                self.status = Status.DOWN
            return

        old_status = self.status

        pid_alive = self._is_pid_running()
        if pid_alive:
            if self.status in [Status.DOWN, Status.UNINITIALIZED]:
                self.status = Status.UP
        else:
            if self.status in [Status.UP, Status.DECOMMISSIONED]:
                self.status = Status.DOWN

        if not old_status == self.status:
            if old_status == Status.UP and self.status == Status.DOWN:
                self.pid = None
            self._update_config()

    def _find_pid_on_windows(self):
        found = False
        try:
            import psutil
            found = psutil.pid_exists(self.pid)
        except ImportError:
            common.warning("psutil not installed. Pid tracking functionality will suffer. See README for details.")
            cmd = 'tasklist /fi "PID eq ' + str(self.pid) + '"'
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)

            for line in proc.stdout:
                if re.match("Image", str(line)):
                    found = True
        return found

    def _get_directories(self):
        dirs = []
        for i in ['commitlogs', 'saved_caches', 'logs', 'conf', 'bin', 'hints']:
            dirs.append(os.path.join(self.get_path(), i))
        for x in xrange(0, self.cluster.data_dir_count):
            dirs.append(os.path.join(self.get_path(), 'data{0}'.format(x)))
        return dirs

    def __get_status_string(self):
        if self.status == Status.UNINITIALIZED:
            return "{} ({})".format(Status.DOWN, "Not initialized")
        else:
            return self.status

    def __clean_win_pid(self):
        start = common.now_ms()
        if self.get_base_cassandra_version() >= 2.1:
            # Spin for up to 15s waiting for .bat to write the pid file
            pidfile = self.get_path() + "/cassandra.pid"
            while (not os.path.isfile(pidfile)):
                now = common.now_ms()
                if (now - start > 15000):
                    raise Exception('Timed out waiting for pid file.')
                else:
                    time.sleep(.001)
            # Spin for up to 10s waiting for .bat to fill the pid file
            start = common.now_ms()
            while (os.stat(pidfile).st_size == 0):
                now = common.now_ms()
                if (now - start > 10000):
                    raise Exception('Timed out waiting for pid file to be filled.')
                else:
                    time.sleep(.001)
        else:
            try:
                # Spin for 500ms waiting for .bat to write the dirty_pid file
                while (not os.path.isfile(self.get_path() + "/dirty_pid.tmp")):
                    now = common.now_ms()
                    if (now - start > 500):
                        raise Exception('Timed out waiting for dirty_pid file.')
                    else:
                        time.sleep(.001)

                with open(self.get_path() + "/dirty_pid.tmp", 'r') as f:
                    found = False
                    process_regex = re.compile('ProcessId')

                    readStart = common.now_ms()
                    readEnd = common.now_ms()
                    while (found is False and readEnd - readStart < 500):
                        line = f.read()
                        if (line):
                            m = process_regex.search(line)
                            if (m):
                                found = True
                                linesub = line.split('=')
                                pidchunk = linesub[1].split(';')
                                win_pid = pidchunk[0].lstrip()
                                with open(self.get_path() + "/cassandra.pid", 'w') as pidfile:
                                    found = True
                                    pidfile.write(win_pid)
                        else:
                            time.sleep(.001)
                        readEnd = common.now_ms()
                    if not found:
                        raise Exception('Node: %s  Failed to find pid in ' +
                                        self.get_path() +
                                        '/dirty_pid.tmp. Manually kill it and check logs - ccm will be out of sync.')
            except Exception as e:
                common.error("Problem starting " + self.name + " (" + str(e) + ")")
                raise Exception('Error while parsing <node>/dirty_pid.tmp in path: ' + self.get_path())

    def _delete_old_pid(self):
        pidfile = os.path.join(self.get_path(), 'cassandra.pid')
        if os.path.isfile(pidfile):
            os.remove(pidfile)

    def _update_pid(self, process):
        """
        Reads pid from cassandra.pid file and stores in the self.pid
        After setting up pid updates status (UP, DOWN, etc) and node.conf
        """
        pidfile = os.path.join(self.get_path(), 'cassandra.pid')

        start = time.time()
        while not (os.path.isfile(pidfile) and os.stat(pidfile).st_size > 0):
            if (time.time() - start > 30.0):
                common.error("Timed out waiting for pidfile to be filled (current time is {})".format(datetime.now()))
                break
            else:
                time.sleep(0.1)

        try:
            with open(pidfile, 'rb') as f:
                if common.is_modern_windows_install(self.get_base_cassandra_version()):
                    self.pid = int(f.readline().strip().decode('utf-16').strip())
                else:
                    self.pid = int(f.readline().strip())
        except IOError as e:
            raise NodeError('Problem starting node %s due to %s' % (self.name, e), process)
        self.__update_status()

    def __gather_sstables(self, datafiles=None, keyspace=None, columnfamilies=None):
        files = []
        if keyspace is None:
            for k in self.list_keyspaces():
                files = files + self.get_sstables(k, "")
        elif datafiles is None:
            if columnfamilies is None:
                files = files + self.get_sstables(keyspace, "")
            else:
                for cf in columnfamilies:
                    files = files + self.get_sstables(keyspace, cf)
        else:
            if not columnfamilies or len(columnfamilies) > 1:
                raise common.ArgumentError("Exactly one column family must be specified with datafiles")

            for x in xrange(0, self.cluster.data_dir_count):
                cf_dir = os.path.join(os.path.realpath(self.get_path()), 'data{0}'.format(x), keyspace, columnfamilies[0])

                sstables = set()
                for datafile in datafiles:
                    if not os.path.isabs(datafile):
                        datafile = os.path.join(os.getcwd(), datafile)

                    if not datafile.startswith(cf_dir + '-') and not datafile.startswith(cf_dir + os.sep):
                        raise NodeError("File doesn't appear to belong to the specified keyspace and column familily: " + datafile)

                    sstable = _sstable_regexp.match(os.path.basename(datafile))
                    if not sstable:
                        raise NodeError("File doesn't seem to be a valid sstable filename: " + datafile)

                    sstable = sstable.groupdict()
                    if not sstable['tmp'] and sstable['number'] not in sstables:
                        if not os.path.exists(datafile):
                            raise IOError("File doesn't exist: " + datafile)
                        sstables.add(sstable['number'])
                        files.append(datafile)

        return files

    def _clean_win_jmx(self):
        if self.get_base_cassandra_version() >= 2.1:
            sh_file = os.path.join(common.CASSANDRA_CONF_DIR, common.CASSANDRA_WIN_ENV)
            dst = os.path.join(self.get_path(), sh_file)
            common.replace_in_file(dst, "^\s+\$JMX_PORT=", "    $JMX_PORT=\"" + self.jmx_port + "\"")

            # properly use single and double quotes to count for single quotes in the CASSANDRA_CONF path
            common.replace_in_file(
                dst,
                'CASSANDRA_PARAMS=', '    $env:CASSANDRA_PARAMS=\'-Dcassandra' +                        # -Dcassandra
                ' -Dlogback.configurationFile=/"\' + "$env:CASSANDRA_CONF" + \'/logback.xml"\'' +       # -Dlogback.configurationFile=/"$env:CASSANDRA_CONF/logback.xml"
                ' + \' -Dcassandra.config=file:"\' + "///$env:CASSANDRA_CONF" + \'/cassandra.yaml"\'')  # -Dcassandra.config=file:"///$env:CASSANDRA_CONF/cassandra.yaml"

    def get_conf_option(self, option):
        conf_file = self.get_conf_file()
        with open(conf_file, 'r') as f:
            data = yaml.safe_load(f)

        if option in data:
            return data[option]
        else:
            return None

    def pause(self):
        try:
            import psutil
            p = psutil.Process(self.pid)
            p.suspend()
        except ImportError:
            if common.is_win():
                common.warning("psutil not installed. Pause functionality will not work properly on Windows.")
            else:
                os.kill(self.pid, signal.SIGSTOP)

    def resume(self):
        try:
            import psutil
            p = psutil.Process(self.pid)
            p.resume()
        except ImportError:
            if common.is_win():
                common.warning("psutil not installed. Resume functionality will not work properly on Windows.")
            else:
                os.kill(self.pid, signal.SIGCONT)

    def jstack_process(self, opts=None):
        opts = [] if opts is None else opts
        jstack_location = os.path.abspath(os.path.join(os.environ['JAVA_HOME'],
                                                       'bin',
                                                       'jstack'))
        jstack_cmd = [jstack_location, '-J-d64'] + opts + [str(self.pid)]
        return subprocess.Popen(jstack_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def jstack(self, opts=None):
        p = self.jstack_process(opts=opts)
        return handle_external_tool_process(p, ['jstack'] + opts)

    def byteman_submit_process(self, opts):
        cdir = self.get_install_dir()
        byteman_cmd = []
        byteman_cmd.append(os.path.join(os.environ['JAVA_HOME'],
                                        'bin',
                                        'java'))
        byteman_cmd.append('-cp')
        byteman_cmd.append(glob.glob(os.path.join(cdir, 'build', 'lib', 'jars', 'byteman-submit-[0-9]*.jar'))[0])
        byteman_cmd.append('org.jboss.byteman.agent.submit.Submit')
        byteman_cmd.append('-p')
        byteman_cmd.append(self.byteman_port)
        byteman_cmd += opts
        return subprocess.Popen(byteman_cmd)

    def byteman_submit(self, opts):
        p = self.byteman_submit_process(opts=opts)
        return handle_external_tool_process(p, ['byteman_submit'] + opts)

    def data_directories(self):
        return [os.path.join(self.get_path(), 'data{0}'.format(x)) for x in xrange(0, self.cluster.data_dir_count)]

    def get_sstable_data_files_process(self, ks, table):
        env = self.get_env()
        args = [self.get_tool('sstableutil'), '--type', 'final', ks, table]

        p = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        return p

    def get_sstable_data_files(self, ks, table):
        """
        Read sstable data files by using sstableutil, so we ignore temporary files
        """
        p = self.get_sstable_data_files_process(ks=ks, table=table)

        out, _, _ = handle_external_tool_process(p, ["sstableutil", '--type', 'final', ks, table])

        return sorted(filter(lambda s: s.endswith('-Data.db'), out.splitlines()))


def _get_load_from_info_output(info):
    load_lines = [s for s in info.split('\n')
                  if s.startswith('Load')]
    if not len(load_lines) == 1:
        msg = ('Expected output from `nodetool info` to contain exactly 1 '
               'line starting with "Load". Found:\n') + info
        raise RuntimeError(msg)
    load_line = load_lines[0].split()

    # Don't have access to C* version here, so we need to support both prefix styles
    # See CASSANDRA-9692 on Apache JIRA
    unit_multipliers = {'KiB': 1,
                        'KB': 1,
                        'MiB': 1024,
                        'MB': 1024,
                        'GiB': 1024 * 1024,
                        'GB': 1024 * 1024,
                        'TiB': 1024 * 1024 * 1024,
                        'TB': 1024 * 1024 * 1024}
    load_num, load_units = load_line[2], load_line[3]

    try:
        load_mult = unit_multipliers[load_units]
    except KeyError:
        expected = ', '.join(list(unit_multipliers))
        msg = ('Expected `nodetool info` to report load in one of the '
               'following units:\n'
               '    {expected}\n'
               'Found:\n'
               '    {found}').format(expected=expected, found=load_units)
        raise RuntimeError(msg)

    return float(load_num) * load_mult


def _grep_log_for_errors(log):
    except_re = re.compile(r'[Ee]xception|AssertionError')
    log_cat_re = re.compile(r'(\W|^)(INFO|DEBUG|WARN|ERROR)\W')

    def log_line_category(line):
        match = log_cat_re.search(line)
        return match.group(2) if match else None

    matches = []

    loglines = log.splitlines()

    for line_num, line in enumerate(loglines):
        found_exception = False
        line_category = log_line_category(line)
        if line_category == 'ERROR':
            matches.append([line])
            found_exception = True

        elif line_category == 'WARN':
            match = except_re.search(line)
            if match is not None:
                matches.append([line])
                found_exception = True

        if found_exception:
            for next_line_num in range(line_num + 1, len(loglines)):
                next_line = loglines[next_line_num]
                # if a log line can't be identified, assume continuation of an ERROR/WARN exception
                if log_line_category(next_line) is None:
                    matches[-1].append(next_line)
                else:
                    break

    return matches


def handle_external_tool_process(process, cmd_args):
    out, err = process.communicate()
    if (out is not None) and isinstance(out, bytes):
        out = out.decode()
    if (err is not None) and isinstance(err, bytes):
        err = err.decode()
    rc = process.returncode

    if rc != 0:
        raise ToolError(cmd_args, rc, out, err)

    ret = namedtuple('Subprocess_Return', 'stdout stderr rc')
    return ret(stdout=out, stderr=err, rc=rc)
