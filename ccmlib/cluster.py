# ccm clusters
from __future__ import absolute_import

import itertools
import os
import random
import re
import shutil
import signal
import subprocess
import threading
import time
from collections import OrderedDict, defaultdict, namedtuple
from distutils.version import LooseVersion #pylint: disable=import-error, no-name-in-module

import yaml
from six import print_

from ccmlib import common, extension, repository
from ccmlib.node import Node, NodeError, TimeoutError
from six.moves import xrange
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


DEFAULT_CLUSTER_WAIT_TIMEOUT_IN_SECS = int(os.environ.get('CCM_CLUSTER_START_DEFAULT_TIMEOUT', 120))

class Cluster(object):

    def __init__(self, path, name, partitioner=None, install_dir=None, create_directory=True, version=None, verbose=False, derived_cassandra_version=None, configuration_yaml=None, **kwargs):
        self.name = name
        self.nodes = {}
        self.seeds = []
        self.partitioner = partitioner
        self._config_options = {}
        self._dse_config_options = {}
        self._misc_config_options = {}
        self._environment_variables = {}
        self.__log_level = "INFO"
        self.__path = path
        self.__version = None
        self.use_vnodes = False
        self.configuration_yaml = configuration_yaml
        # Classes that are to follow the respective logging level
        self._debug = []
        self._trace = []
        self.data_dir_count = 1

        if self.name.lower() == "current":
            raise RuntimeError("Cannot name a cluster 'current'.")

        # This is incredibly important for
        # backwards compatibility.
        if 'cassandra_version' in kwargs:
            version = kwargs['cassandra_version']
        if 'cassandra_dir' in kwargs:
            install_dir = kwargs['cassandra_dir']
        if create_directory:
            # we create the dir before potentially downloading to throw an error sooner if need be
            os.mkdir(self.get_path())

        try:
            if version is None:
                # at this point, install_dir should always not be None, but
                # we keep this for backward compatibility (in loading old cluster)
                if install_dir is not None:
                    if common.is_win():
                        self.__install_dir = install_dir
                    else:
                        self.__install_dir = os.path.abspath(install_dir)
            else:
                repo_dir, v = self.load_from_repository(version, verbose)
                self.__install_dir = repo_dir
                self.__version = v

            if self.__version is None:
                if derived_cassandra_version is not None:
                    self.__version = derived_cassandra_version
                else:
                    self.__version = self.__get_version_from_build()

            if create_directory:
                common.validate_install_dir(self.__install_dir)
                self._update_config()
        except:
            if create_directory:
                common.rmdirs(self.get_path())
            raise

    def load_from_repository(self, version, verbose):
        return repository.setup(version, verbose)

    def set_partitioner(self, partitioner):
        self.partitioner = partitioner
        self._update_config()
        return self

    def set_datadir_count(self, n):
        self.data_dir_count = int(n)
        self._update_config()
        return self

    def set_install_dir(self, install_dir=None, version=None, verbose=False):
        if version is None:
            self.__install_dir = install_dir
            common.validate_install_dir(install_dir)
            self.__version = self.__get_version_from_build()
        else:
            dir, v = repository.setup(version, verbose)
            self.__install_dir = dir
            self.__version = v if v is not None else self.__get_version_from_build()
            if not isinstance(self.__version, LooseVersion):
                self.__version = LooseVersion(self.__version)
        self._update_config()
        for node in list(self.nodes.values()):
            node._cassandra_version = self.__version
            node.import_config_files()

        # if any nodes have a data center, let's update the topology
        if any([node.data_center for node in self.nodes.values()]):
            self.__update_topology_files()

        if self.cassandra_version() >= '4':
            self.set_configuration_options({ 'start_rpc' : None}, delete_empty=True, delete_always=True)
        else:
            self.set_configuration_options(common.CCM_40_YAML_OPTIONS, delete_empty=True, delete_always=True)

        return self

    def actively_watch_logs_for_error(self, on_error_call, interval=1):
        """
        Begins a thread that repeatedly scans system.log for new errors, every interval seconds.
        (The first pass covers the entire log contents written at that point,
        subsequent scans cover newly appended log messages).

        Reports new errors, by calling the provided callback with an OrderedDictionary
        mapping node name to a list of error lines.

        Returns the thread itself, which should be .join()'ed to wrap up execution,
        otherwise will run until the main thread exits.
        """
        class LogWatchingThread(threading.Thread):
            """
            This class is embedded here for now, because it is used only from
            within Cluster, and depends on cluster.nodelist().
            """

            def __init__(self, cluster):
                super(LogWatchingThread, self).__init__()
                self.cluster = cluster
                self.daemon = True  # set so that thread will exit when main thread exits
                self.req_stop_event = threading.Event()
                self.done_event = threading.Event()
                self.log_positions = defaultdict(int)

            def scan(self):
                errordata = OrderedDict()

                try:
                    for node in self.cluster.nodelist():
                        scan_from_mark = self.log_positions[node.name]
                        next_time_scan_from_mark = node.mark_log()
                        if next_time_scan_from_mark == scan_from_mark:
                            # log hasn't advanced, nothing to do for this node
                            continue
                        else:
                            errors = node.grep_log_for_errors_from(seek_start=scan_from_mark)
                        self.log_positions[node.name] = next_time_scan_from_mark
                        if errors:
                            errordata[node.name] = errors
                except IOError as e:
                    if 'No such file or directory' in str(e.strerror):
                        pass  # most likely log file isn't yet written

                    # in the case of unexpected error, report this thread to the callback
                    else:
                        errordata['log_scanner'] = [[str(e)]]

                return errordata

            def scan_and_report(self):
                errordata = self.scan()

                if errordata:
                    on_error_call(errordata)

            def run(self):
                common.debug("Log-watching thread starting.")

                # run until stop gets requested by .join()
                while not self.req_stop_event.is_set():
                    self.scan_and_report()
                    time.sleep(interval)

                try:
                    # do a final scan to make sure we got to the very end of the files
                    self.scan_and_report()
                finally:
                    common.debug("Log-watching thread exiting.")
                    # done_event signals that the scan completed a final pass
                    self.done_event.set()

            def join(self, timeout=None):
                # signals to the main run() loop that a stop is requested
                self.req_stop_event.set()
                # now wait for the main loop to get through a final log scan, and signal that it's done
                self.done_event.wait(timeout=interval * 2)  # need to wait at least interval seconds before expecting thread to finish. 2x for safety.
                super(LogWatchingThread, self).join(timeout)

        log_watcher = LogWatchingThread(self)
        log_watcher.start()
        return log_watcher

    def get_install_dir(self):
        common.validate_install_dir(self.__install_dir)
        return self.__install_dir

    def hasOpscenter(self):
        return False

    def nodelist(self):
        return [self.nodes[name] for name in sorted(self.nodes.keys())]

    def version(self):
        return self.__version

    def cassandra_version(self):
        return self.version()

    def address_regex(self):
        return "/([0-9.]+):7000" if self.cassandra_version() >= '4.0' else "/([0-9.]+)"

    def add(self, node, is_seed, data_center=None):
        if node.name in self.nodes:
            raise common.ArgumentError('Cannot create existing node %s' % node.name)
        self.nodes[node.name] = node
        if is_seed:
            self.seeds.append(node)
        self._update_config()
        node.data_center = data_center
        if data_center is None:
            for existing_node in self.nodelist():
                if existing_node.data_center is not None:
                    raise common.ArgumentError('Please specify the DC this node should be added to')

        node.set_log_level(self.__log_level)

        for debug_class in self._debug:
            node.set_log_level("DEBUG", debug_class)
        for trace_class in self._trace:
            node.set_log_level("TRACE", trace_class)

        if data_center is not None:
            self.__update_topology_files()

        node._save()
        return self

    def populate(self, nodes, debug=False, tokens=None, use_vnodes=None, ipprefix='127.0.0.', ipformat=None, install_byteman=False, use_single_interface=False):
        """Populate a cluster with nodes
        @use_single_interface : Populate the cluster with nodes that all share a single network interface.
        """

        if self.cassandra_version() < '4' and use_single_interface:
            raise common.ArgumentError('use_single_interface is not supported in versions < 4.0')

        node_count = nodes
        dcs = []

        if use_vnodes is None:
            self.use_vnodes = len(tokens or []) > 1 or self._more_than_one_token_configured()
        else:
            self.use_vnodes = use_vnodes

        if isinstance(nodes, list):
            self.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})
            node_count = 0
            i = 0
            for c in nodes:
                i = i + 1
                node_count = node_count + c
                for x in xrange(0, c):
                    dcs.append('dc%d' % i)

        if node_count < 1:
            raise common.ArgumentError('invalid node count %s' % nodes)

        for i in xrange(1, node_count + 1):
            if 'node%s' % i in list(self.nodes.values()):
                raise common.ArgumentError('Cannot create existing node node%s' % i)

        if tokens is None:
            if self.use_vnodes:
                # from 4.0 tokens can be pre-generated via the `allocate_tokens_for_local_replication_factor: 3` strategy
                #  this saves time, as allocating tokens during first start is slow and non-concurrent
                if self.can_generate_tokens() and not 'CASSANDRA_TOKEN_PREGENERATION_DISABLED' in self._environment_variables:
                    if len(dcs) <= 1:
                        for x in xrange(0, node_count):
                            dcs.append('dc1')

                    tokens = self.generated_tokens(dcs)
            else:
                common.debug("using balanced tokens for non-vnode cluster")
                if len(dcs) <= 1:
                    tokens = self.balanced_tokens(node_count)
                else:
                    tokens = self.balanced_tokens_across_dcs(dcs)

        if not ipformat:
            ipformat = ipprefix + "%d"

        for i in xrange(1, node_count + 1):
            tk = None
            if tokens is not None and i - 1 < len(tokens):
                tk = tokens[i - 1]
            dc = dcs[i - 1] if i - 1 < len(dcs) else None

            binary = None
            if self.cassandra_version() >= '1.2':
                if use_single_interface:
                    #Always leave 9042 and 9043 clear, in case someone defaults to adding
                    # a node with those ports
                    binary = (ipformat % 1, 9042 + 2 + (i * 2))
                else:
                    binary = (ipformat % i, 9042)
            thrift = None
            if self.cassandra_version() < '4':
                thrift = (ipformat % i, 9160)

            storage_interface = ((ipformat % i), 7000)
            if use_single_interface:
                #Always leave 7000 and 7001 in case someone defaults to adding
                #with those port numbers
                storage_interface = (ipformat % 1, 7000 + 2 + (i * 2))

            node = self.create_node(name='node%s' % i,
                                    auto_bootstrap=False,
                                    thrift_interface=thrift,
                                    storage_interface=storage_interface,
                                    jmx_port=str(7000 + i * 100),
                                    remote_debug_port=str(2000 + i * 100) if debug else str(0),
                                    byteman_port=str(4000 + i * 100) if install_byteman else str(0),
                                    initial_token=tk,
                                    binary_interface=binary,
                                    environment_variables=self._environment_variables)
            self.add(node, True, dc)
            self._update_config()
        return self

    def create_node(self, name, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save=True, binary_interface=None, byteman_port='0', environment_variables=None, derived_cassandra_version=None):
        return Node(name, self, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save, binary_interface, byteman_port, environment_variables, derived_cassandra_version=derived_cassandra_version)

    def balanced_tokens(self, node_count):
        if self.cassandra_version() >= '1.2' and (not self.partitioner or 'Murmur3' in self.partitioner):
            ptokens = [(i * (2 ** 64 // node_count)) for i in xrange(0, node_count)]
            return [int(t - 2 ** 63) for t in ptokens]
        return [int(i * (2 ** 127 // node_count)) for i in range(0, node_count)]

    def balanced_tokens_across_dcs(self, dcs):
        tokens = []
        current_dc = dcs[0]
        count = 0
        dc_count = 0
        for dc in dcs:
            if dc == current_dc:
                count += 1
            else:
                new_tokens = [tk + (dc_count * 100) for tk in self.balanced_tokens(count)]
                tokens.extend(new_tokens)
                current_dc = dc
                count = 1
                dc_count += 1
        new_tokens = [tk + (dc_count * 100) for tk in self.balanced_tokens(count)]
        tokens.extend(new_tokens)
        return tokens

    def _more_than_one_token_configured(self):
        return int(self._config_options.get('num_tokens', '1')) > 1

    def can_generate_tokens(self):
        return (self.cassandra_version() >= '4'
                    and (self.partitioner is None or ('Murmur3' in self.partitioner or 'Random' in self.partitioner))
                    and self._more_than_one_token_configured
                    and not 'CASSANDRA_TOKEN_PREGENERATION_DISABLED' in self._environment_variables)

    def generated_tokens(self, dcs):
        tokens = []
        # all nodes are in rack1
        current_dc = dcs[0]
        node_count = 0
        for dc in dcs:
            if dc == current_dc:
                node_count += 1
            else:
                self.generate_dc_tokens(node_count, tokens)
                current_dc = dc
                node_count = 1
        self.generate_dc_tokens(node_count, tokens)
        return tokens

    def generate_dc_tokens(self, node_count, tokens):
        if self.cassandra_version() < '4' or (self.partitioner and not ('Murmur3' in self.partitioner or 'Random' in self.partitioner)):
            raise common.ArgumentError("generate-tokens script only for >=4.0 and Murmur3 or Random")
        if not self._more_than_one_token_configured():
            raise common.ArgumentError("Cannot use generate-tokens script without num_tokens > 1")

        partitioner = 'RandomPartitioner' if ( self.partitioner and 'Random' in self.partitioner) else 'Murmur3Partitioner'
        generate_tokens = common.join_bin(self.get_install_dir(), os.path.join('tools', 'bin'), 'generatetokens')
        cmd_list = [generate_tokens, '-n', str(node_count), '-t', str(self._config_options.get("num_tokens")), '--rf', str(min(3,node_count)), '-p', partitioner]
        process = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ.copy())
        # the first line is "Generating tokens for X nodes with" and can be ignored
        process.stdout.readline()

        for n in range(1,node_count+1):
            stdout_output = re.sub(r'^.*?:', '', process.stdout.readline().decode("utf-8"))
            node_tokens = stdout_output.replace('[','').replace(' ','').replace(']','').replace('\n','')
            tokens.append(node_tokens)

        common.debug("pregenerated tokens from cmd_list: {} are {}".format(str(cmd_list),tokens))

    def remove(self, node=None, gently=False):
        if node is not None:
            if node.name not in self.nodes:
                return

            del self.nodes[node.name]
            if node in self.seeds:
                self.seeds.remove(node)
            self._update_config()
            node.stop(gently=gently)
            self.remove_dir_with_retry(node.get_path())
        else:
            self.stop(gently=gently)
            self.remove_dir_with_retry(self.get_path())

    # We can race w/shutdown on Windows and get Access is denied attempting to delete node logs.
    # see CASSANDRA-10075
    def remove_dir_with_retry(self, path):
        tries = 0
        removed = False
        while removed is False:
            try:
                common.rmdirs(path)
                removed = True
            except Exception as e:
                tries = tries + 1
                time.sleep(.1)
                if tries == 5:
                    raise e

    def clear(self):
        self.stop()
        for node in list(self.nodes.values()):
            node.clear()

    def get_path(self):
        return os.path.join(self.__path, self.name)

    def get_seeds(self):
        if self.cassandra_version() >= '4.0':
            #They might be overriding the storage port config now
            storage_port = self._config_options.get("storage_port")
            storage_interfaces = [s.network_interfaces['storage'] for s in self.seeds if isinstance(s, Node)]
            seeds = []

            #Convert node storage interfaces to IP strings and maybe replace the port
            for storage_interface in storage_interfaces:
                port = storage_port if storage_port is not None else str(storage_interface[1])
                if ":" in storage_interface[0]:
                    seeds.append("[" + storage_interface[0] + "]:" + port)
                else:
                    seeds.append(storage_interface[0] + ":" + port)

            #For seeds that are strings need to update the port in the string
            for seed in [string for string in self.seeds if not isinstance(string, Node)]:
                url = urlparse("http://" + seed)
                if storage_port is not None:
                    seeds.append(url.hostname + ":" + str(storage_port))
                else:
                    seeds.append(seed)

            return seeds
        else:
            return [s.network_interfaces['storage'][0] if isinstance(s, Node) else s for s in self.seeds]

    def show(self, verbose):
        msg = "Cluster: '{}'".format(self.name)
        print_(msg)
        print_('-' * len(msg))
        if len(list(self.nodes.values())) == 0:
            print_("No node in this cluster yet")
            return
        for node in list(self.nodes.values()):
            if verbose:
                node.show(show_cluster=False)
                print_("")
            else:
                node.show(only_status=True)

    def start(self, no_wait=False, verbose=False, wait_for_binary_proto=True,
              wait_other_notice=True, jvm_args=None, profile_options=None,
              quiet_start=False, allow_root=False, jvm_version=None, **kwargs):
        if jvm_args is None:
            jvm_args = []

        extension.pre_cluster_start(self)

        # check whether all loopback aliases are available before starting any nodes
        for node in list(self.nodes.values()):
            if not node.is_running():
                for itf in node.network_interfaces.values():
                    if itf is not None:
                        common.assert_socket_available(itf)

        started = []
        for node in list(self.nodes.values()):
            if not node.is_running():
                mark = 0
                if os.path.exists(node.logfilename()):
                    mark = node.mark_log()

                # if the node is going to allocate_strategy_ tokens during start, then wait_for_binary_proto=True
                node_wait_for_binary_proto = (self.can_generate_tokens() and self.use_vnodes and node.initial_token is None)
                p = node.start(update_pid=False, jvm_args=jvm_args, jvm_version=jvm_version,
                               profile_options=profile_options, verbose=verbose, quiet_start=quiet_start,
                               allow_root=allow_root, wait_for_binary_proto=node_wait_for_binary_proto)

                # Prior to JDK8, starting every node at once could lead to a
                # nanotime collision where the RNG that generates a node's tokens
                # gives identical tokens to several nodes. Thus, we stagger
                # the node starts
                if common.get_jdk_version() < '1.8':
                    time.sleep(1)

                started.append((node, p, mark))

        if no_wait:
            time.sleep(2)  # waiting 2 seconds to check for early errors and for the pid to be set
        else:
            for node, p, mark in started:
                if not node._wait_for_running(p, timeout_s=7):
                    raise NodeError("Node {} should be running before waiting for <started listening> log message, "
                                    "but C* process is terminated.".format(node.name))
                try:
                    timeout=kwargs.get('timeout', DEFAULT_CLUSTER_WAIT_TIMEOUT_IN_SECS)
                    timeout=int(os.environ.get('CCM_CLUSTER_START_TIMEOUT_OVERRIDE', timeout))
                    start_message = "Listening for thrift clients..." if self.cassandra_version() < "2.2" else "Starting listening for CQL clients"
                    node.watch_log_for(start_message, timeout=timeout, process=p, verbose=verbose, from_mark=mark,
                                       error_on_pid_terminated=True)
                except RuntimeError:
                    return None

        self.__update_pids(started)

        for node, p, _ in started:
            if not node.is_running():
                raise NodeError("Error starting {0}.".format(node.name), p)

        if not no_wait:
            if wait_other_notice:
                for (node, _, mark), (other_node, _, _) in itertools.permutations(started, 2):
                    node.watch_log_for_alive(other_node, from_mark=mark)

            if wait_for_binary_proto:
                for node, p, mark in started:
                    node.wait_for_binary_interface(process=p, verbose=verbose, from_mark=mark)

        extension.post_cluster_start(self)

        return started

    def stop(self, wait=True, signal_event=signal.SIGTERM, **kwargs):
        not_running = []
        extension.pre_cluster_stop(self)
        for node in list(self.nodes.values()):
            if not node.stop(wait=wait, signal_event=signal_event, **kwargs):
                not_running.append(node)
        extension.post_cluster_stop(self)
        return not_running

    def set_log_level(self, new_level, class_names=None):
        class_names = class_names or []
        known_level = ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'OFF']
        if new_level not in known_level:
            raise common.ArgumentError("Unknown log level %s (use one of %s)" % (new_level, " ".join(known_level)))

        if class_names:
            for class_name in class_names:
                if new_level == 'DEBUG':
                    if class_name in self._trace:
                        raise common.ArgumentError("Class %s already in TRACE" % (class_name))
                    self._debug.append(class_name)
                if new_level == 'TRACE':
                    if class_name in self._debug:
                        raise common.ArgumentError("Class %s already in DEBUG" % (class_name))
                    self._trace.append(class_name)
        else:
            self.__log_level = new_level
            self._update_config()

        for node in self.nodelist():
            for class_name in class_names:
                node.set_log_level(new_level, class_name)

    def wait_for_compactions(self, timeout=600):
        """
        Wait for all compactions to finish on all nodes.
        """
        for node in list(self.nodes.values()):
            if node.is_running():
                node.wait_for_compactions(timeout)
        return self

    def nodetool(self, nodetool_cmd):
        for node in list(self.nodes.values()):
            if node.is_running():
                node.nodetool(nodetool_cmd)
        return self

    def allNativePortsMatch(self):
        current_port = None
        for node in self.nodes.values():
            if current_port is None:
                current_port = node.network_interfaces['binary'][1]
            elif current_port != node.network_interfaces['binary'][1]:
                return False
        return True

    def stress(self, stress_options):
        stress = common.get_stress_bin(self.get_install_dir())
        livenodes = [node.network_interfaces['binary'] for node in list(self.nodes.values()) if node.is_live()]
        if len(livenodes) == 0:
            print_('No live node')
            return

        def live_node_ips_joined():
            return ','.join(n[0] for n in livenodes)

        nodes_options = []
        if self.cassandra_version() <= '2.1':
            if '-d' not in stress_options:
                nodes_options = ['-d', live_node_ips_joined()]
            args = [stress] + nodes_options + stress_options
        elif self.cassandra_version() >= '4.0':
            if '-node' not in stress_options:
                nodes_options = ['-node', ','.join([node[0] + ':' + str(node[1]) for node in livenodes])]
            args = [stress] + stress_options + nodes_options
        else:
            if '-node' not in stress_options:
                nodes_options = ['-node', live_node_ips_joined()]
            args = [stress] + stress_options + nodes_options
        rc = None
        try:
            # need to set working directory for env on Windows
            if common.is_win():
                rc = subprocess.call(args, cwd=common.parse_path(stress))
            else:
                rc = subprocess.call(args)
        except KeyboardInterrupt:
            pass
        return rc

    def set_configuration_options(self, values=None, delete_empty=False, delete_always=False):
        if values is not None:
            self._config_options = common.merge_configuration(self._config_options, values, delete_empty=delete_empty, delete_always=delete_always)

        self._persist_config()
        self.__update_topology_files()
        return self

    def set_configuration_yaml(self, configuration_yaml):
        self.configuration_yaml = configuration_yaml
        self._persist_config()
        self.__update_topology_files()
        return self

    def set_batch_commitlog(self, enabled, use_batch_window=True):
        for node in list(self.nodes.values()):
            node.set_batch_commitlog(enabled=enabled, use_batch_window=use_batch_window)

    def set_dse_configuration_options(self, values=None):
        raise common.ArgumentError('Cannot set DSE configuration options on a Cassandra cluster')

    def set_environment_variable(self, key, value):
        self._environment_variables[key] = value
        for node in list(self.nodes.values()):
            node.set_environment_variable(key, value)
        self._persist_config()

    def _persist_config(self):
        self._update_config()
        for node in list(self.nodes.values()):
            node.import_config_files()

    def flush(self):
        self.nodetool("flush")

    def compact(self):
        self.nodetool("compact")

    def drain(self):
        self.nodetool("drain")

    def repair(self):
        self.nodetool("repair")

    def cleanup(self):
        self.nodetool("cleanup")

    def decommission(self):
        for node in list(self.nodes.values()):
            if node.is_running():
                node.decommission()

    def removeToken(self, token):
        self.nodetool("removeToken " + str(token))

    def bulkload(self, options):
        livenodes = [node for node in self.nodes.values() if node.is_live()]
        if not livenodes:
            raise common.ArgumentError("No live node")
        random.choice(livenodes).bulkload(options)

    def scrub(self, options):
        for node in list(self.nodes.values()):
            node.scrub(options)

    def verify(self, options):
        for node in list(self.nodes.values()):
            node.verify(options)

    def enable_aoss(self):
        common.error("Cannot enable AOSS in C* clusters")
        exit(1)

    def update_log4j(self, new_log4j_config):
        # iterate over all nodes
        for node in self.nodelist():
            node.update_log4j(new_log4j_config)

    def update_logback(self, new_logback_config):
        # iterate over all nodes
        for node in self.nodelist():
            node.update_logback(new_logback_config)

    def __get_version_from_build(self):
        return common.get_version_from_build(self.get_install_dir())

    def _update_config(self):
        node_list = [node.name for node in list(self.nodes.values())]
        seed_list = self.get_seeds()
        filename = os.path.join(self.__path, self.name, 'cluster.conf')
        config_map = {
            'name': self.name,
            'nodes': node_list,
            'seeds': seed_list,
            'partitioner': self.partitioner,
            'install_dir': self.__install_dir,
            'config_options': self._config_options,
            'dse_config_options': self._dse_config_options,
            'misc_config_options' : self._misc_config_options,
            'log_level': self.__log_level,
            'use_vnodes': self.use_vnodes,
            'configuration_yaml': self.configuration_yaml,
            'datadirs': self.data_dir_count,
            'environment_variables': self._environment_variables,
            'cassandra_version': str(self.cassandra_version())
        }
        extension.append_to_cluster_config(self, config_map)
        with open(filename, 'w') as f:
            yaml.safe_dump(config_map, f)

    def __update_pids(self, started):
        for node, p, _ in started:
            node._update_pid(p)

    def __update_topology_files(self):
        dcs = [('default', 'dc1')]
        for node in self.nodelist():
            if node.data_center is not None:
                dcs.append((node.address(), node.data_center))
        for node in self.nodelist():
            node.update_topology(dcs)

    def enable_ssl(self, ssl_path, require_client_auth):
        shutil.copyfile(os.path.join(ssl_path, 'keystore.jks'), os.path.join(self.get_path(), 'keystore.jks'))
        shutil.copyfile(os.path.join(ssl_path, 'cassandra.crt'), os.path.join(self.get_path(), 'cassandra.crt'))
        ssl_options = {'enabled': True,
                       'keystore': os.path.join(self.get_path(), 'keystore.jks'),
                       'keystore_password': 'cassandra'
                      }

        # determine if truststore client encryption options should be enabled
        truststore_file = os.path.join(ssl_path, 'truststore.jks')
        if os.path.isfile(truststore_file):
            shutil.copyfile(truststore_file, os.path.join(self.get_path(), 'truststore.jks'))
            truststore_ssl_options = {'require_client_auth': require_client_auth,
                                      'truststore': os.path.join(self.get_path(), 'truststore.jks'),
                                      'truststore_password': 'cassandra'
                                     }
            ssl_options.update(truststore_ssl_options)

        self._config_options['client_encryption_options'] = ssl_options
        self._update_config()

    def enable_internode_ssl(self, node_ssl_path):
        shutil.copyfile(os.path.join(node_ssl_path, 'keystore.jks'), os.path.join(self.get_path(), 'internode-keystore.jks'))
        shutil.copyfile(os.path.join(node_ssl_path, 'truststore.jks'), os.path.join(self.get_path(), 'internode-truststore.jks'))
        node_ssl_options = {
            'internode_encryption': 'all',
            'keystore': os.path.join(self.get_path(), 'internode-keystore.jks'),
            'keystore_password': 'cassandra',
            'truststore': os.path.join(self.get_path(), 'internode-truststore.jks'),
            'truststore_password': 'cassandra'
        }

        if self.cassandra_version() >= '4.0':
            node_ssl_options['enabled'] = True

        self._config_options['server_encryption_options'] = node_ssl_options
        self._update_config()

    def enable_pwd_auth(self):
        self._config_options['authenticator'] = 'PasswordAuthenticator'
        self._update_config()

    def timed_grep_nodes_for_patterns(self, versions_to_patterns, timeout_seconds, filename="system.log"):
        """
        Searches all nodes in the cluster for a specific regular expression based on the node's version.
        Params:
        @versions_to_patterns : an instance of LogPatternToVersionMap, specifying the different log patterns based on a node's version.
        @version : the earliest version the new pattern was introduced.
        @timeout_seconds : the amount of time to spend searching the logs for.
        @filename : the name of the file to search for the patterns. Defaults to "system.log".

        Returns the first node where the pattern was found, along with the matching lines.
        Raises a TimeoutError if the pattern is not found within the specified timeout period.
        """
        start_time = time.time()
        while True:
            TimeoutError.raise_if_passed(start=start_time, timeout=timeout_seconds,
                                         msg="Unable to find: {x} in any node log within {t} s".format(
                                             x=versions_to_patterns.patterns, t=timeout_seconds))

            for node in self.nodelist():
                pattern = versions_to_patterns(node.get_cassandra_version())
                matchings = node.grep_log(pattern, filename)
                if matchings:
                    ret = namedtuple('Node_Log_Matching', 'node matchings')
                    return ret(node=node, matchings=matchings)
            time.sleep(1)

    def wait_for_any_log(self, pattern, timeout, filename='system.log', marks=None):
        return common.wait_for_any_log(self.nodelist(), pattern, timeout, filename=filename, marks=marks)

    def show_logs(self, selected_nodes_names=None):
        """
        Shows logs of nodes in this cluster, by default, with multitail.
        If you need to alter the command or options, change CCM_MULTITAIL_CMD .
        Params:
        @selected_nodes_names : a list-like object that contains names of nodes to be shown. If empty, this will show all nodes in the cluster.
        """

        if selected_nodes_names is None:
            selected_nodes_names = []

        if len(self.nodes) == 0:
            print_("There are no nodes in this cluster yet.")
            return

        nodes = sorted(list(self.nodes.values()), key=lambda node: node.name)
        nodes_names = [node.name for node in nodes]

        names_logs_dict = {node.name: node.logfilename() for node in nodes}
        if len(selected_nodes_names) == 0: # Parameter selected_nodes_names is empty
            return names_logs_dict.values()
        else:
            if set(selected_nodes_names).issubset(nodes_names):
                return [names_logs_dict[name] for name in selected_nodes_names]
            else:
                raise ValueError("nodes in this cluster are {}. But nodes in argments are {}".format(
                    nodes_names, selected_nodes_names
                ))
