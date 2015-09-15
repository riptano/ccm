# ccm node
from __future__ import with_statement

import errno
import glob
import itertools
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
import warnings
from datetime import datetime

from six import iteritems, print_, string_types

import yaml
from ccmlib import common
from ccmlib.cli_session import CliSession
from ccmlib.repository import setup
from six.moves import xrange


class Status():
    UNINITIALIZED = "UNINITIALIZED"
    UP = "UP"
    DOWN = "DOWN"
    DECOMMISIONNED = "DECOMMISIONNED"


class NodeError(Exception):

    def __init__(self, msg, process=None):
        Exception.__init__(self, msg)
        self.process = process


class TimeoutError(Exception):

    def __init__(self, data):
        Exception.__init__(self, str(data))


class NodetoolError(Exception):

    def __init__(self, command, exit_status, stdout=None, stderr=None):
        self.command = command
        self.exit_status = exit_status
        self.stdout = stdout
        self.stderr = stderr

        message = "Nodetool command '%s' failed; exit status: %d" % (command, exit_status)
        if stdout:
            message += "; stdout: "
            message += stdout
        if stderr:
            message += "; stderr: "
            message += stderr

        Exception.__init__(self, message)

# Groups: 1 = cf, 2 = tmp or none, 3 = suffix (Compacted or Data.db)
_sstable_regexp = re.compile('((?P<keyspace>[^\s-]+)-(?P<cf>[^\s-]+)-)?(?P<tmp>tmp(link)?-)?(?P<version>[^\s-]+)-(?P<number>\d+)-(?P<big>big-)?(?P<suffix>[a-zA-Z]+)\.[a-zA-Z0-9]+$')


class Node(object):
    """
    Provides interactions to a Cassandra node.
    """

    def __init__(self, name, cluster, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save=True, binary_interface=None):
        """
        Create a new Node.
          - name: the name for that node
          - cluster: the cluster this node is part of
          - auto_bootstrap: whether or not this node should be set for auto-bootstrap
          - thrift_interface: the (host, port) tuple for thrift
          - storage_interface: the (host, port) tuple for internal cluster communication
          - jmx_port: the port for JMX to bind to
          - remote_debug_port: the port for remote debugging
          - initial_token: the token for this node. If None, use Cassandra token auto-assigment
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
        self.jmx_port = jmx_port
        self.remote_debug_port = remote_debug_port
        self.initial_token = initial_token
        self.pid = None
        self.data_center = None
        self.workload = None
        self.__config_options = {}
        self.__install_dir = None
        self.__global_log_level = None
        self.__classes_log_level = {}
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
            data = yaml.load(f)
        try:
            itf = data['interfaces']
            initial_token = None
            if 'initial_token' in data:
                initial_token = data['initial_token']
            remote_debug_port = 2000
            if 'remote_debug_port' in data:
                remote_debug_port = data['remote_debug_port']
            binary_interface = None
            if 'binary' in itf and itf['binary'] is not None:
                binary_interface = tuple(itf['binary'])
            node = cluster.create_node(data['name'], data['auto_bootstrap'], tuple(itf['thrift']), tuple(itf['storage']), data['jmx_port'], remote_debug_port, initial_token, save=False, binary_interface=binary_interface)
            node.status = data['status']
            if 'pid' in data:
                node.pid = int(data['pid'])
            if 'install_dir' in data:
                node.__install_dir = data['install_dir']
            if 'config_options' in data:
                node.__config_options = data['config_options']
            if 'data_center' in data:
                node.data_center = data['data_center']
            if 'workload' in data:
                node.workload = data['workload']
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
        return common.make_cassandra_env(self.get_install_dir(), self.get_path())

    def get_install_cassandra_root(self):
        return self.get_install_dir()

    def get_node_cassandra_root(self):
        return self.get_path()

    def get_conf_dir(self):
        """
        Returns the path to the directory where Cassandra config are located
        """
        return os.path.join(self.get_path(), 'conf')

    def address(self):
        """
        Returns the IP use by this node for internal communication
        """
        return self.network_interfaces['storage'][0]

    def get_install_dir(self):
        """
        Returns the path to the cassandra source directory used by this node.
        """
        if self.__install_dir is None:
            return self.cluster.get_install_dir()
        else:
            common.validate_install_dir(self.__install_dir)
            return self.__install_dir

    def set_install_dir(self, install_dir=None, version=None, verbose=False):
        """
        Sets the path to the cassandra source directory for use by this node.
        """
        if version is None:
            self.__install_dir = install_dir
            if install_dir is not None:
                common.validate_install_dir(install_dir)
        else:
            dir, v = setup(version, verbose=verbose)
            self.__install_dir = dir
        self.import_config_files()
        self.import_bin_files()
        return self

    def set_workload(self, workload):
        raise common.ArgumentError("Cannot set workload on cassandra node")

    def get_cassandra_version(self):
        try:
            return common.get_version_from_build(self.get_install_dir())
        except common.CCMError:
            return self.cluster.cassandra_version()

    def get_base_cassandra_version(self):
        version = self.get_cassandra_version()
        return float(version[:version.index('.') + 2])

    def set_configuration_options(self, values=None, batch_commitlog=None):
        """
        Set Cassandra configuration options.
        ex:
            node.set_configuration_options(values={
                'hinted_handoff_enabled' : True,
                'concurrent_writes' : 64,
            })
        The batch_commitlog option gives an easier way to switch to batch
        commitlog (since it requires setting 2 options and unsetting one).
        """
        if values is not None:
            for k, v in iteritems(values):
                self.__config_options[k] = v
        if batch_commitlog is not None:
            if batch_commitlog:
                self.__config_options["commitlog_sync"] = "batch"
                self.__config_options["commitlog_sync_batch_window_in_ms"] = 5
                self.__config_options["commitlog_sync_period_in_ms"] = None
            else:
                self.__config_options["commitlog_sync"] = "periodic"
                self.__config_options["commitlog_sync_period_in_ms"] = 10000
                self.__config_options["commitlog_sync_batch_window_in_ms"] = None

        self.import_config_files()

    def show(self, only_status=False, show_cluster=True):
        """
        Print infos on this node configuration.
        """
        self.__update_status()
        indent = ''.join([" " for i in xrange(0, len(self.name) + 2)])
        print_("%s: %s" % (self.name, self.__get_status_string()))
        if not only_status:
            if show_cluster:
                print_("%s%s=%s" % (indent, 'cluster', self.cluster.name))
            print_("%s%s=%s" % (indent, 'auto_bootstrap', self.auto_bootstrap))
            print_("%s%s=%s" % (indent, 'thrift', self.network_interfaces['thrift']))
            if self.network_interfaces['binary'] is not None:
                print_("%s%s=%s" % (indent, 'binary', self.network_interfaces['binary']))
            print_("%s%s=%s" % (indent, 'storage', self.network_interfaces['storage']))
            print_("%s%s=%s" % (indent, 'jmx_port', self.jmx_port))
            print_("%s%s=%s" % (indent, 'remote_debug_port', self.remote_debug_port))
            print_("%s%s=%s" % (indent, 'initial_token', self.initial_token))
            if self.pid:
                print_("%s%s=%s" % (indent, 'pid', self.pid))

    def is_running(self):
        """
        Return true if the node is running
        """
        self.__update_status()
        return self.status == Status.UP or self.status == Status.DECOMMISIONNED

    def is_live(self):
        """
        Return true if the node is live (it's run and is not decommissionned).
        """
        self.__update_status()
        return self.status == Status.UP

    def logfilename(self):
        """
        Return the path to the current Cassandra log of this node.
        """
        return os.path.join(self.get_path(), 'logs', 'system.log')

    def grep_log(self, expr):
        """
        Returns a list of lines matching the regular expression in parameter
        in the Cassandra log of this node
        """
        matchings = []
        pattern = re.compile(expr)
        with open(self.logfilename()) as f:
            for line in f:
                m = pattern.search(line)
                if m:
                    matchings.append((line, m))
        return matchings

    def grep_log_for_errors(self):
        """
        Returns a list of errors with stack traces
        in the Cassandra log of this node
        """
        with open(self.logfilename()) as f:
            if hasattr(self, 'error_mark'):
                f.seek(self.error_mark)
            return _grep_log_for_errors(f.read())

    def mark_log_for_errors(self):
        """
        Ignore errors behind this point when calling
        node.grep_log_for_errors()
        """
        self.error_mark = self.mark_log()

    def mark_log(self):
        """
        Returns "a mark" to the current position of this node Cassandra log.
        This is for use with the from_mark parameter of watch_log_for_* methods,
        allowing to watch the log from the position when this method was called.
        """
        if not os.path.exists(self.logfilename()):
            return 0
        with open(self.logfilename()) as f:
            f.seek(0, os.SEEK_END)
            return f.tell()

    def print_process_output(self, name, proc, verbose=False):
        try:
            [stdout, stderr] = proc.communicate()
        except ValueError:
            [stdout, stderr] = ['', '']
        if len(stderr) > 1:
            print_("[%s ERROR] %s" % (name, stderr.strip()))

    # This will return when exprs are found or it timeouts
    def watch_log_for(self, exprs, from_mark=None, timeout=600, process=None, verbose=False):
        """
        Watch the log until one or more (regular) expression are found.
        This methods when all the expressions have been found or the method
        timeouts (a TimeoutError is then raised). On successful completion,
        a list of pair (line matched, match object) is returned.
        """
        elapsed = 0
        tofind = [exprs] if isinstance(exprs, string_types) else exprs
        tofind = [re.compile(e) for e in tofind]
        matchings = []
        reads = ""
        if len(tofind) == 0:
            return None

        while not os.path.exists(self.logfilename()):
            time.sleep(.5)
            if process:
                process.poll()
                if process.returncode is not None:
                    self.print_process_output(self.name, process, verbose)
                    if process.returncode != 0:
                        raise RuntimeError()  # Shouldn't reuse RuntimeError but I'm lazy

        with open(self.logfilename()) as f:
            if from_mark:
                f.seek(from_mark)

            while True:
                # First, if we have a process to check, then check it.
                # Skip on Windows - stdout/stderr is cassandra.bat
                if not common.is_win():
                    if process:
                        process.poll()
                        if process.returncode is not None:
                            self.print_process_output(self.name, process, verbose)
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
                    # yep, it's ugly
                    time.sleep(1)
                    elapsed = elapsed + 1
                    if elapsed > timeout:
                        raise TimeoutError(time.strftime("%d %b %Y %H:%M:%S", time.gmtime()) + " [" + self.name + "] Missing: " + str([e.pattern for e in tofind]) + ":\n" + reads)

                if process:
                    if common.is_win():
                        if not self.is_running():
                            return None
                    else:
                        process.poll()
                        if process.returncode == 0:
                            return None

    def watch_log_for_death(self, nodes, from_mark=None, timeout=600):
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
        tofind = ["%s is now [dead|DOWN]" % node.address() for node in tofind]
        self.watch_log_for(tofind, from_mark=from_mark, timeout=timeout)

    def watch_log_for_alive(self, nodes, from_mark=None, timeout=120):
        """
        Watch the log of this node until it detects that the provided other
        nodes are marked UP. This method works similarily to watch_log_for_death.
        """
        tofind = nodes if isinstance(nodes, list) else [nodes]
        tofind = ["%s.* now UP" % node.address() for node in tofind]
        self.watch_log_for(tofind, from_mark=from_mark, timeout=timeout)

    def wait_for_binary_interface(self, **kwargs):
        """
        Waits for the Binary CQL interface to be listening.  If > 1.2 will check
        log for 'Starting listening for CQL clients' before checking for the
        interface to be listening.

        Emits a warning if not listening after 10 seconds.
        """
        if self.cluster.version() >= '1.2':
            self.watch_log_for("Starting listening for CQL clients", **kwargs)

        binary_itf = self.network_interfaces['binary']
        if not common.check_socket_listening(binary_itf, timeout=10):
            warnings.warn("Binary interface %s:%s is not listening after 10 seconds, node may have failed to start."
                          % (binary_itf[0], binary_itf[1]))

    def start(self,
              join_ring=True,
              no_wait=False,
              verbose=False,
              update_pid=True,
              wait_other_notice=False,
              replace_token=None,
              replace_address=None,
              jvm_args=[],
              wait_for_binary_proto=False,
              profile_options=None,
              use_jna=False):
        """
        Start the node. Options includes:
          - join_ring: if false, start the node with -Dcassandra.join_ring=False
          - no_wait: by default, this method returns when the node is started and listening to clients.
            If no_wait=True, the method returns sooner.
          - wait_other_notice: if True, this method returns only when all other live node of the cluster
            have marked this node UP.
          - replace_token: start the node with the -Dcassandra.replace_token option.
          - replace_address: start the node with the -Dcassandra.replace_address option.
        """
        # Validate Windows env
        if common.is_win() and not common.is_ps_unrestricted() and self.cluster.version() >= '2.1':
            raise NodeError("PS Execution Policy must be unrestricted when running C* 2.1+")

        if self.is_running():
            raise NodeError("%s is already running" % self.name)

        for itf in list(self.network_interfaces.values()):
            if itf is not None and replace_address is None:
                common.check_socket_available(itf)

        if wait_other_notice:
            marks = [(node, node.mark_log()) for node in list(self.cluster.nodes.values()) if node.is_live()]

        self.mark = self.mark_log()

        cdir = self.get_install_dir()
        launch_bin = common.join_bin(cdir, 'bin', 'cassandra')
        # Copy back the cassandra scripts since profiling may have modified it the previous time
        shutil.copy(launch_bin, self.get_bin_dir())
        launch_bin = common.join_bin(self.get_path(), 'bin', 'cassandra')

        # If Windows, change entries in .bat file to split conf from binaries
        if common.is_win():
            self.__clean_bat()

        if profile_options is not None:
            config = common.get_config()
            if 'yourkit_agent' not in config:
                raise NodeError("Cannot enable profile. You need to set 'yourkit_agent' to the path of your agent in a ~/.ccm/config")
            cmd = '-agentpath:%s' % config['yourkit_agent']
            if 'options' in profile_options:
                cmd = cmd + '=' + profile_options['options']
            print_(cmd)
            # Yes, it's fragile as shit
            pattern = r'cassandra_parms="-Dlog4j.configuration=log4j-server.properties -Dlog4j.defaultInitOverride=true'
            common.replace_in_file(launch_bin, pattern, '    ' + pattern + ' ' + cmd + '"')

        os.chmod(launch_bin, os.stat(launch_bin).st_mode | stat.S_IEXEC)

        env = common.make_cassandra_env(cdir, self.get_path())

        if common.is_win():
            self._clean_win_jmx()

        pidfile = os.path.join(self.get_path(), 'cassandra.pid')
        args = [launch_bin, '-p', pidfile, '-Dcassandra.join_ring=%s' % str(join_ring)]
        if replace_token is not None:
            args.append('-Dcassandra.replace_token=%s' % str(replace_token))
        if replace_address is not None:
            args.append('-Dcassandra.replace_address=%s' % str(replace_address))
        if use_jna is False:
            args.append('-Dcassandra.boot_without_jna=true')
        env['JVM_EXTRA_OPTS'] = env.get('JVM_EXTRA_OPTS', "") + " ".join(jvm_args)

        # In case we are restarting a node
        # we risk reading the old cassandra.pid file
        self._delete_old_pid()

        process = None
        FNULL = open(os.devnull, 'w')
        if common.is_win():
            # clean up any old dirty_pid files from prior runs
            if (os.path.isfile(self.get_path() + "/dirty_pid.tmp")):
                os.remove(self.get_path() + "/dirty_pid.tmp")
            process = subprocess.Popen(args, cwd=self.get_bin_dir(), env=env, stdout=FNULL, stderr=subprocess.PIPE)
        else:
            process = subprocess.Popen(args, env=env, stdout=FNULL, stderr=subprocess.PIPE)
        # Our modified batch file writes a dirty output with more than just the pid - clean it to get in parity
        # with *nix operation here.
        if common.is_win():
            self.__clean_win_pid()
            self._update_pid(process)
            print_("Started: {0} with pid: {1}".format(self.name, self.pid), file=sys.stderr, flush=True)
        elif update_pid:
            self._update_pid(process)

            if not self.is_running():
                raise NodeError("Error starting node %s" % self.name, process)

        if wait_other_notice:
            for node, mark in marks:
                node.watch_log_for_alive(self, from_mark=mark)

        if wait_for_binary_proto:
            self.wait_for_binary_interface(from_mark=self.mark)

        return process

    def stop(self, wait=True, wait_other_notice=False, gently=True):
        """
        Stop the node.
          - wait: if True (the default), wait for the Cassandra process to be
            really dead. Otherwise return after having sent the kill signal.
          - wait_other_notice: return only when the other live nodes of the
            cluster have marked this node has dead.
          - gently: Let Cassandra clean up and shut down properly. Otherwise do
            a 'kill -9' which shuts down faster.
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
                if gently is True:
                    try:
                        self.flush()
                    except:
                        print_("WARN: Failed to flush node: {0} on shutdown.".format(self.name))
                        pass

                os.system("taskkill /F /PID " + str(self.pid))
                if self._find_pid_on_windows():
                    print_("WARN: Failed to terminate node: {0} with pid: {1}".format(self.name, self.pid))
            else:
                if gently:
                    os.kill(self.pid, signal.SIGTERM)
                else:
                    os.kill(self.pid, signal.SIGKILL)

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

    def wait_for_compactions(self):
        """
        Wait for all compactions to finish on this node.
        """
        pattern = re.compile("pending tasks: 0")
        while True:
            output, err = self.nodetool("compactionstats", capture_output=True)
            if pattern.search(output):
                break
            time.sleep(10)

    def nodetool(self, cmd, capture_output=True):
        env = common.make_cassandra_env(self.get_install_cassandra_root(), self.get_node_cassandra_root())
        nodetool = self.get_tool('nodetool')
        args = [nodetool, '-h', 'localhost', '-p', str(self.jmx_port)]
        args += cmd.split()
        if capture_output:
            p = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
        else:
            p = subprocess.Popen(args, env=env)
            stdout, stderr = None, None

        exit_status = p.wait()
        if exit_status != 0:
            raise NodetoolError(" ".join(args), exit_status, stdout, stderr)

        return stdout, stderr

    def dsetool(self, cmd):
        raise common.ArgumentError('Cassandra nodes do not support dsetool')

    def dse(self, dse_options=[]):
        raise common.ArgumentError('Cassandra nodes do not support dse')

    def hadoop(self, hadoop_options=[]):
        raise common.ArgumentError('Cassandra nodes do not support hadoop')

    def hive(self, hive_options=[]):
        raise common.ArgumentError('Cassandra nodes do not support hive')

    def pig(self, pig_options=[]):
        raise common.ArgumentError('Cassandra nodes do not support pig')

    def sqoop(self, sqoop_options=[]):
        raise common.ArgumentError('Cassandra nodes do not support sqoop')

    def bulkload(self, options):
        loader_bin = common.join_bin(self.get_path(), 'bin', 'sstableloader')
        env = common.make_cassandra_env(self.get_install_cassandra_root(), self.get_node_cassandra_root())
        host, port = self.network_interfaces['thrift']
        args = ['-d', host, '-p', str(port)]
        os.execve(loader_bin, [common.platform_binary('sstableloader')] + args + options, env)

    def scrub(self, options):
        scrub_bin = self.get_tool('sstablescrub')
        env = common.make_cassandra_env(self.get_install_cassandra_root(), self.get_node_cassandra_root())
        os.execve(scrub_bin, [common.platform_binary('sstablescrub')] + options, env)

    def verify(self, options):
        verify_bin = self.get_tool('sstableverify')
        env = common.make_cassandra_env(self.get_install_cassandra_root(), self.get_node_cassandra_root())
        os.execve(verify_bin, [common.platform_binary('sstableverify')] + options, env)

    def run_cli(self, cmds=None, show_output=False, cli_options=[]):
        cli = self.get_tool('cassandra-cli')
        env = common.make_cassandra_env(self.get_install_cassandra_root(), self.get_node_cassandra_root())
        host = self.network_interfaces['thrift'][0]
        port = self.network_interfaces['thrift'][1]
        args = ['-h', host, '-p', str(port), '--jmxport', str(self.jmx_port)] + cli_options
        sys.stdout.flush()
        if cmds is None:
            os.execve(cli, [common.platform_binary('cassandra-cli')] + args, env)
        else:
            p = subprocess.Popen([cli] + args, env=env, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            for cmd in cmds.split(';'):
                p.stdin.write(cmd + ';\n')
            p.stdin.write("quit;\n")
            p.wait()
            for err in p.stderr:
                print_("(EE) ", err, end='')
            if show_output:
                i = 0
                for log in p.stdout:
                    # first four lines are not interesting
                    if i >= 4:
                        print_(log, end='')
                    i = i + 1

    def run_cqlsh(self, cmds=None, show_output=False, cqlsh_options=[], return_output=False):
        cqlsh = self.get_tool('cqlsh')
        env = common.make_cassandra_env(self.get_install_cassandra_root(), self.get_node_cassandra_root())
        host = self.network_interfaces['thrift'][0]
        if self.get_base_cassandra_version() >= 2.1:
            port = self.network_interfaces['binary'][1]
        else:
            port = self.network_interfaces['thrift'][1]
        args = cqlsh_options + [host, str(port)]
        sys.stdout.flush()
        if cmds is None:
            if common.is_win():
                subprocess.Popen([cqlsh] + args, env=env, creationflags=subprocess.CREATE_NEW_CONSOLE)
            else:
                os.execve(cqlsh, [common.platform_binary('cqlsh')] + args, env)
        else:
            p = subprocess.Popen([cqlsh] + args, env=env, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
            for cmd in cmds.split(';'):
                cmd = cmd.strip()
                if cmd:
                    p.stdin.write(cmd + ';\n')
            p.stdin.write("quit;\n")
            p.wait()

            output = (p.stdout.read(), p.stderr.read())

            for err in output[1].split('\n'):
                print_("(EE) ", err, end='')

            if show_output:
                print_(output[0], end='')

            if return_output:
                return output

    def cli(self):
        cdir = self.get_install_dir()
        cli = common.join_bin(cdir, 'bin', 'cassandra-cli')
        env = common.make_cassandra_env(cdir, self.get_path())
        host = self.network_interfaces['thrift'][0]
        port = self.network_interfaces['thrift'][1]
        args = ['-h', host, '-p', str(port), '--jmxport', str(self.jmx_port)]
        return CliSession(subprocess.Popen([cli] + args, env=env, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE))

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

    def clear(self, clear_all=False, only_data=False):
        data_dirs = ['data']
        if not only_data:
            data_dirs.append("commitlogs")
            if clear_all:
                data_dirs.extend(['saved_caches', 'logs'])
        for d in data_dirs:
            full_dir = os.path.join(self.get_path(), d)
            if only_data:
                for dir in os.listdir(full_dir):
                    keyspace_dir = os.path.join(full_dir, dir)
                    if os.path.isdir(keyspace_dir) and dir != "system":
                        for f in os.listdir(keyspace_dir):
                            full_path = os.path.join(keyspace_dir, f)
                            if os.path.isfile(full_path):
                                os.remove(full_path)
            else:
                common.rmdirs(full_dir)
                os.mkdir(full_dir)

        # Needed for any subdirs stored underneath a data directory.
        # Common for hints post CASSANDRA-6230
        for dir in self._get_directories():
            if not os.path.exists(dir):
                os.mkdir(dir)

    def run_sstable2json(self, out_file=None, keyspace=None, datafiles=None, column_families=None, enumerate_keys=False):
        print_("running")
        if out_file is None:
            out_file = sys.stdout
        sstable2json = self._find_cmd('sstable2json')
        env = common.make_cassandra_env(self.get_install_cassandra_root(), self.get_node_cassandra_root())
        sstablefiles = self.__gather_sstables(datafiles, keyspace, column_families)
        print_(sstablefiles)
        for sstablefile in sstablefiles:
            print_("-- {0} -----".format(os.path.basename(sstablefile)))
            args = [sstable2json, sstablefile]
            if enumerate_keys:
                args = args + ["-e"]
            subprocess.call(args, env=env, stdout=out_file)
            print_("")

    def run_json2sstable(self, in_file, ks, cf, keyspace=None, datafiles=None, column_families=None, enumerate_keys=False):
        json2sstable = self._find_cmd('json2sstable')
        env = common.make_cassandra_env(self.get_install_cassandra_root(), self.get_node_cassandra_root())
        sstablefiles = self.__gather_sstables(datafiles, keyspace, column_families)

        for sstablefile in sstablefiles:
            in_file_name = os.path.abspath(in_file.name)
            args = [json2sstable, "-s", "-K", ks, "-c", cf, in_file_name, sstablefile]
            subprocess.call(args, env=env)

    def run_sstablesplit(self, datafiles=None, size=None, keyspace=None, column_families=None,
                         no_snapshot=False, debug=False):
        sstablesplit = self._find_cmd('sstablesplit')
        env = common.make_cassandra_env(self.get_install_cassandra_root(), self.get_node_cassandra_root())
        sstablefiles = self.__gather_sstables(datafiles, keyspace, column_families)

        results = []

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
            (out, err) = p.communicate()
            rc = p.returncode
            results.append((out, err, rc))

        for sstablefile in sstablefiles:
            do_split(sstablefile)

        return results

    def run_sstablemetadata(self, output_file=None, datafiles=None, keyspace=None, column_families=None):
        cdir = self.get_install_dir()
        sstablemetadata = common.join_bin(cdir, os.path.join('tools', 'bin'), 'sstablemetadata')
        env = common.make_cassandra_env(cdir, self.get_path())
        sstablefiles = self.__gather_sstables(datafiles=datafiles, keyspace=keyspace, columnfamilies=column_families)
        results = []

        for sstable in sstablefiles:
            cmd = [sstablemetadata, sstable]
            if output_file is None:
                p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)
                (out, err) = p.communicate()
                rc = p.returncode
                results.append((out, err, rc))
            else:
                subprocess.call(cmd, env=env, stdout=output_file)
        if output_file is None:
            return results

    def run_sstableexpiredblockers(self, output_file=None, keyspace=None, column_family=None):
        cdir = self.get_install_dir()
        sstableexpiredblockers = common.join_bin(cdir, os.path.join('tools', 'bin'), 'sstableexpiredblockers')
        env = common.make_cassandra_env(cdir, self.get_path())
        cmd = [sstableexpiredblockers, keyspace, column_family]
        results = []
        if output_file is None:
            p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)
            (out, err) = p.communicate()
            rc = p.returncode
            results.append((out, err, rc))
        else:
            subprocess.call(cmd, env=env, stdout=output_file)
        if output_file is None:
            return results

    def get_sstablespath(self, output_file=None, datafiles=None, keyspace=None, tables=None):
        sstablefiles = self.__gather_sstables(datafiles=datafiles, keyspace=keyspace, columnfamilies=tables)
        return sstablefiles

    def run_sstablerepairedset(self, set_repaired=True, datafiles=None, keyspace=None, column_families=None):
        cdir = self.get_install_dir()
        sstablerepairedset = common.join_bin(cdir, os.path.join('tools', 'bin'), 'sstablerepairedset')
        env = common.make_cassandra_env(cdir, self.get_path())
        sstablefiles = self.__gather_sstables(datafiles, keyspace, column_families)

        for sstable in sstablefiles:
            if set_repaired == True:
                cmd = [sstablerepairedset, "--really-set", "--is-repaired", sstable]
            else:
                cmd = [sstablerepairedset, "--really-set", "--is-unrepaired", sstable]
            subprocess.call(cmd, env=env)

    def run_sstablelevelreset(self, keyspace, cf, output=False):
        cdir = self.get_install_dir()
        sstablelevelreset = common.join_bin(cdir, os.path.join('tools', 'bin'), 'sstablelevelreset')
        env = common.make_cassandra_env(cdir, self.get_path())

        cmd = [sstablelevelreset, "--really-reset", keyspace, cf]

        if output == True:
            p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)
            (stdout, stderr) = p.communicate()
            rc = p.returncode
            return (stdout, stderr, rc)
        else:
            return subprocess.call(cmd, env=env)

    def run_sstableofflinerelevel(self, keyspace, cf, dry_run=False, output=False):
        cdir = self.get_install_dir()
        sstableofflinerelevel = common.join_bin(cdir, os.path.join('tools', 'bin'), 'sstableofflinerelevel')
        env = common.make_cassandra_env(cdir, self.get_path())

        if dry_run == True:
            cmd = [sstableofflinerelevel, "--dry-run", keyspace, cf]
        else:
            cmd = [sstableofflinerelevel, keyspace, cf]

        if output == True:
            p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)
            (stdout, stderr) = p.communicate()
            rc = p.returncode
            return (stdout, stderr, rc)
        else:
            return subprocess.call(cmd, env=env)

    def run_sstableverify(self, keyspace, cf, options=None, output=False):
        cdir = self.get_install_dir()
        sstableverify = common.join_bin(cdir, 'bin', 'sstableverify')
        env = common.make_cassandra_env(cdir, self.get_path())

        cmd = [sstableverify, keyspace, cf]
        if options is not None:
            cmd[1:1] = options

        if output == True:
            p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)
            (stdout, stderr) = p.communicate()
            rc = p.returncode
            return (stdout, stderr, rc)
        else:
            return subprocess.call(cmd, env=env)

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
            os.chmod(fcmd, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
        except:
            print_("WARN: Couldn't change permissions to use {0}.".format(cmd))
            print_("WARN: If it didn't work, you will have to do so manually.")
        return fcmd

    def list_keyspaces(self):
        keyspaces = os.listdir(os.path.join(self.get_path(), 'data'))
        keyspaces.remove('system')
        return keyspaces

    def get_sstables(self, keyspace, column_family):
        keyspace_dir = os.path.join(self.get_path(), 'data', keyspace)
        cf_glob = '*'
        if column_family:
            # account for changes in data dir layout from CASSANDRA-5202
            if self.get_base_cassandra_version() < 2.1:
                cf_glob = column_family
            else:
                cf_glob = column_family + '-*'
        if not os.path.exists(keyspace_dir):
            raise common.ArgumentError("Unknown keyspace {0}".format(keyspace))

        # data directory layout is changed from 1.1
        if self.get_base_cassandra_version() < 1.1:
            files = glob.glob(os.path.join(keyspace_dir, "{0}*-Data.db".format(column_family)))
        elif self.get_base_cassandra_version() < 2.2:
            files = glob.glob(os.path.join(keyspace_dir, cf_glob, "%s-%s*-Data.db" % (keyspace, column_family)))
        else:
            files = glob.glob(os.path.join(keyspace_dir, cf_glob, "*big-Data.db"))
        for f in files:
            if os.path.exists(f.replace('Data.db', 'Compacted')):
                files.remove(f)
        return files

    def stress(self, stress_options=[], capture_output=False, **kwargs):
        stress = common.get_stress_bin(self.get_install_dir())
        if self.cluster.cassandra_version() <= '2.1':
            stress_options.append('-d')
            stress_options.append(self.address())
        else:
            stress_options.append('-node')
            stress_options.append(self.address())
            # specify used jmx port if not already set
            if not [opt for opt in stress_options if opt.startswith('jmx=')]:
                stress_options.extend(['-port', 'jmx=' + self.jmx_port])
        args = [stress] + stress_options
        try:
            if capture_output:
                p = subprocess.Popen(args, cwd=common.parse_path(stress),
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     **kwargs)
                stdout, stderr = p.communicate()
            else:
                p = subprocess.Popen(args, cwd=common.parse_path(stress),
                                     **kwargs)
                stdout, stderr = None, None
            p.wait()
            return stdout, stderr
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
        info = self.nodetool('info', capture_output=True)[0]
        return _get_load_from_info_output(info)

    def flush(self):
        self.nodetool("flush")

    def compact(self):
        self.nodetool("compact")

    def drain(self, block_on_log=False):
        mark = self.mark_log()
        self.nodetool("drain")
        if block_on_log:
            self.watch_log_for("DRAINED", from_mark=mark)

    def repair(self, options=[], **kwargs):
        args = ["repair"] + options
        cmd = ' '.join(args)
        return self.nodetool(cmd, **kwargs)

    def move(self, new_token):
        self.nodetool("move " + str(new_token))

    def cleanup(self):
        self.nodetool("cleanup")

    def version(self):
        self.nodetool("version")

    def decommission(self):
        self.nodetool("decommission")
        self.status = Status.DECOMMISIONNED
        self._update_config()

    def removeToken(self, token):
        self.nodetool("removeToken " + str(token))

    def import_config_files(self):
        self._update_config()
        self.copy_config_files()
        self.__update_yaml()
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
        common.replace_in_file(os.path.join(self.get_path(), 'bin', 'cassandra.ps1'), '        VerifyPortsAreAvailable', '')

        # Specifically call the .ps1 file in our node's folder
        common.replace_in_file(bat_file, 'powershell /file .*', 'powershell /file "' + os.path.join(self.get_path(), 'bin', 'cassandra.ps1" %*'))

    def _save(self):
        self.__update_yaml()
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
                os.mkdir(os.path.join(dir_name, dir))

        filename = os.path.join(dir_name, 'node.conf')
        values = {
            'name': self.name,
            'status': self.status,
            'auto_bootstrap': self.auto_bootstrap,
            'interfaces': self.network_interfaces,
            'jmx_port': self.jmx_port,
            'config_options': self.__config_options,
        }
        if self.pid:
            values['pid'] = self.pid
        if self.initial_token:
            values['initial_token'] = self.initial_token
        if self.__install_dir is not None:
            values['install_dir'] = self.__install_dir
        if self.remote_debug_port:
            values['remote_debug_port'] = self.remote_debug_port
        if self.data_center:
            values['data_center'] = self.data_center
        if self.workload is not None:
            values['workload'] = self.workload
        with open(filename, 'w') as f:
            yaml.safe_dump(values, f)

    def __update_yaml(self):
        conf_file = os.path.join(self.get_conf_dir(), common.CASSANDRA_CONF)
        with open(conf_file, 'r') as f:
            data = yaml.load(f)

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
        data['rpc_address'], data['rpc_port'] = self.network_interfaces['thrift']
        if self.network_interfaces['binary'] is not None and self.get_base_cassandra_version() >= 1.2:
            _, data['native_transport_port'] = self.network_interfaces['binary']

        data['data_file_directories'] = [os.path.join(self.get_path(), 'data')]
        data['commitlog_directory'] = os.path.join(self.get_path(), 'commitlogs')
        data['saved_caches_directory'] = os.path.join(self.get_path(), 'saved_caches')
        if self.cluster.version() > '3.0' and 'hints_directory' in yaml_text:
            data['hints_directory'] = os.path.join(self.get_path(), 'data', 'hints')

        if self.cluster.partitioner:
            data['partitioner'] = self.cluster.partitioner

        full_options = dict(list(self.cluster._config_options.items()) + list(self.__config_options.items()))  # last win and we want node options to win
        for name in full_options:
            value = full_options[name]
            if isinstance(value, str) and (value is None or len(value) == 0):
                try:
                    del data[name]
                except KeyError:
                    # it is fine to remove a key not there
                    pass
            else:
                try:
                    if isinstance(data[name], dict):
                        for option in full_options[name]:
                            data[name][option] = full_options[name][option]
                    else:
                        data[name] = full_options[name]
                except KeyError:
                    data[name] = full_options[name]

        with open(conf_file, 'w') as f:
            yaml.safe_dump(data, f, default_flow_style=False)

    def _update_log4j(self):
        append_pattern = 'log4j.appender.R.File='
        conf_file = os.path.join(self.get_conf_dir(), common.LOG4J_CONF)
        log_file = os.path.join(self.get_path(), 'logs', 'system.log')
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
        append_pattern = '<file>.*</file>'
        conf_file = os.path.join(self.get_conf_dir(), common.LOGBACK_CONF)
        log_file = os.path.join(self.get_path(), 'logs', 'system.log')
        common.replace_in_file(conf_file, append_pattern, '<file>' + log_file + '</file>')

        append_pattern = '<fileNamePattern>.*</fileNamePattern>'
        common.replace_in_file(conf_file, append_pattern, '<fileNamePattern>' + log_file + '.%i.zip</fileNamePattern>')

        self.__update_logback_loglevel(conf_file)

        tools_conf_file = os.path.join(self.get_conf_dir(), common.LOGBACK_TOOLS_CONF)
        self.__update_logback_loglevel(tools_conf_file)

    def __update_logback_loglevel(self, conf_file):
        # Setting the right log level

        # Replace the global log level
        if self.__global_log_level is not None:
            append_pattern = '<root level=".*">'
            common.replace_in_file(conf_file, append_pattern, '<root level="' + self.__global_log_level + '">')

        # Class specific log levels
        for class_name in self.__classes_log_level:
            logger_pattern = '\t<logger name="'
            full_logger_pattern = logger_pattern + class_name + '" level=".*"/>'
            common.replace_or_add_into_file_tail(conf_file, full_logger_pattern, logger_pattern + class_name + '" level="' + self.__classes_log_level[class_name] + '"/>')

    def __update_envfile(self):
        # The cassandra-env.ps1 file has been introduced in 2.1
        if common.is_win() and self.get_base_cassandra_version() >= 2.1:
            conf_file = os.path.join(self.get_conf_dir(), common.CASSANDRA_WIN_ENV)
            jmx_port_pattern = '^\s+\$JMX_PORT='
            jmx_port_setting = '    $JMX_PORT="' + self.jmx_port + '"'
            remote_debug_options = '    $env:JVM_OPTS="$env:JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=' + str(self.remote_debug_port) + '"'
        else:
            conf_file = os.path.join(self.get_conf_dir(), common.CASSANDRA_ENV)
            jmx_port_pattern = 'JMX_PORT='
            jmx_port_setting = 'JMX_PORT="' + self.jmx_port + '"'
            remote_debug_options = 'JVM_OPTS="$JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=' + str(self.remote_debug_port) + '"'

        common.replace_in_file(conf_file, jmx_port_pattern, jmx_port_setting)

        if self.remote_debug_port != '0':
            remote_debug_port_pattern = '((-Xrunjdwp:)|(-agentlib:jdwp=))transport=dt_socket,server=y,suspend=n,address='
            common.replace_in_file(conf_file, remote_debug_port_pattern, remote_debug_options)

        if self.get_cassandra_version() < '2.0.1':
            common.replace_in_file(conf_file, "-Xss", '    JVM_OPTS="$JVM_OPTS -Xss228k"')

        for itf in list(self.network_interfaces.values()):
            if itf is not None and common.interface_is_ipv6(itf):
                if common.is_win():
                    common.replace_in_file(conf_file,
                                           '-Djava.net.preferIPv4Stack=true',
                                           '\t$env:JVM_OPTS="$env:JVM_OPTS -Djava.net.preferIPv4Stack=false -Djava.net.preferIPv6Addresses=true"')
                else:
                    common.replace_in_file(conf_file,
                                           '-Djava.net.preferIPv4Stack=true',
                                           'JVM_OPTS="$JVM_OPTS -Djava.net.preferIPv4Stack=false -Djava.net.preferIPv6Addresses=true"')
                break

    def __update_status(self):
        if self.pid is None:
            if self.status == Status.UP or self.status == Status.DECOMMISIONNED:
                self.status = Status.DOWN
            return

        old_status = self.status

        # os.kill on windows doesn't allow us to ping a process
        if common.is_win():
            self.__update_status_win()
        else:
            try:
                os.kill(self.pid, 0)
            except OSError as err:
                if err.errno == errno.ESRCH:
                    # not running
                    if self.status == Status.UP or self.status == Status.DECOMMISIONNED:
                        self.status = Status.DOWN
                elif err.errno == errno.EPERM:
                    # no permission to signal this process
                    if self.status == Status.UP or self.status == Status.DECOMMISIONNED:
                        self.status = Status.DOWN
                else:
                    # some other error
                    raise err
            else:
                if self.status == Status.DOWN or self.status == Status.UNINITIALIZED:
                    self.status = Status.UP

        if not old_status == self.status:
            if old_status == Status.UP and self.status == Status.DOWN:
                self.pid = None
            self._update_config()

    def __update_status_win(self):
        if self._find_pid_on_windows():
            if self.status == Status.DOWN or self.status == Status.UNINITIALIZED:
                self.status = Status.UP
        else:
            self.status = Status.DOWN

    def _find_pid_on_windows(self):
        found = False
        try:
            import psutil
            found = psutil.pid_exists(self.pid)
        except ImportError:
            print_("WARN: psutil not installed. Pid tracking functionality will suffer. See README for details.")
            cmd = 'tasklist /fi "PID eq ' + str(self.pid) + '"'
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)

            for line in proc.stdout:
                if re.match("Image", line):
                    found = True
        return found

    def _get_directories(self):
        dirs = []
        for i in ['data', 'commitlogs', 'saved_caches', 'logs', 'conf', 'bin', os.path.join('data', 'hints')]:
            dirs.append(os.path.join(self.get_path(), i))
        return dirs

    def __get_status_string(self):
        if self.status == Status.UNINITIALIZED:
            return "%s (%s)" % (Status.DOWN, "Not initialized")
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
                print_("ERROR: Problem starting " + self.name + " (" + str(e) + ")")
                raise Exception('Error while parsing <node>/dirty_pid.tmp in path: ' + self.get_path())

    def _delete_old_pid(self):
        pidfile = os.path.join(self.get_path(), 'cassandra.pid')
        if os.path.isfile(pidfile):
            os.remove(pidfile)

    def _update_pid(self, process):
        pidfile = os.path.join(self.get_path(), 'cassandra.pid')

        start = time.time()
        while not (os.path.isfile(pidfile) and os.stat(pidfile).st_size > 0):
            if (time.time() - start > 30.0):
                print_("Timed out waiting for pidfile to be filled (current time is %s)" % (datetime.now()))
                break
            else:
                time.sleep(0.1)

        try:
            with open(pidfile, 'r') as f:
                if common.is_win() and self.get_base_cassandra_version() >= 2.1:
                    self.pid = int(f.readline().strip().decode('utf-16'))
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

            cf_dir = os.path.join(os.path.realpath(self.get_path()), 'data', keyspace, columnfamilies[0])

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
        conf_file = os.path.join(self.get_conf_dir(), common.CASSANDRA_CONF)
        with open(conf_file, 'r') as f:
            data = yaml.load(f)

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
                print_("WARN: psutil not installed. Pause functionality will not work properly on Windows.")
            else:
                os.kill(self.pid, signal.SIGSTOP)

    def resume(self):
        try:
            import psutil
            p = psutil.Process(self.pid)
            p.resume()
        except ImportError:
            if common.is_win():
                print_("WARN: psutil not installed. Resume functionality will not work properly on Windows.")
            else:
                os.kill(self.pid, signal.SIGCONT)


def _get_load_from_info_output(info):
    load_lines = [s for s in info.split('\n')
                  if s.startswith('Load')]
    if not len(load_lines) == 1:
        msg = ('Expected output from `nodetool info` to contain exactly 1 '
               'line starting with "Load". Found:\n') + info
        raise RuntimeError(msg)
    load_line = load_lines[0].split()

    unit_multipliers = {'KB': 1,
                        'MB': 1024,
                        'GB': 1024 * 1024,
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
    matchings = []
    it = iter(log.splitlines())
    for line in it:
        is_error_line = ('ERROR' in line
                         and 'DEBUG' not in line.split('ERROR')[0])
        if is_error_line:
            matchings.append([line])
            try:
                it, peeker = itertools.tee(it)
                while 'INFO' not in next(peeker):
                    matchings[-1].append(next(it))
            except StopIteration:
                break
    return matchings
