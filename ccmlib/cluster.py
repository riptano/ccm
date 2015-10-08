# ccm clusters

import os
import random
import shutil
import subprocess
import time

from six import iteritems, print_

import yaml
from ccmlib import common, repository
from ccmlib.node import Node, NodeError
from six.moves import xrange


class Cluster(object):

    def __init__(self, path, name, partitioner=None, install_dir=None, create_directory=True, version=None, verbose=False, **kwargs):
        self.name = name
        self.nodes = {}
        self.seeds = []
        self.partitioner = partitioner
        self._config_options = {}
        self._dse_config_options = {}
        self.__log_level = "INFO"
        self.__path = path
        self.__version = None
        self.use_vnodes = False
        # Classes that are to follow the respective logging level
        self._debug = []
        self._trace = []

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
                    self.__version = self.__get_version_from_build()
            else:
                dir, v = self.load_from_repository(version, verbose)
                self.__install_dir = dir
                self.__version = v if v is not None else self.__get_version_from_build()

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

    def set_install_dir(self, install_dir=None, version=None, verbose=False):
        if version is None:
            self.__install_dir = install_dir
            common.validate_install_dir(install_dir)
            self.__version = self.__get_version_from_build()
        else:
            dir, v = repository.setup(version, verbose)
            self.__install_dir = dir
            self.__version = v if v is not None else self.__get_version_from_build()
        self._update_config()
        for node in list(self.nodes.values()):
            node.import_config_files()

        # if any nodes have a data center, let's update the topology
        if any([node.data_center for node in self.nodes.values()]):
            self.__update_topology_files()

        return self

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

    def add(self, node, is_seed, data_center=None):
        if node.name in self.nodes:
            raise common.ArgumentError('Cannot create existing node %s' % node.name)
        self.nodes[node.name] = node
        if is_seed:
            self.seeds.append(node)
        self._update_config()
        node.data_center = data_center
        node.set_log_level(self.__log_level)

        for debug_class in self._debug:
            node.set_log_level("DEBUG", debug_class)
        for trace_class in self._trace:
            node.set_log_level("TRACE", trace_class)

        if data_center is not None:
            self.__update_topology_files()
        node._save()
        return self

    def populate(self, nodes, debug=False, tokens=None, use_vnodes=False, ipprefix='127.0.0.', ipformat=None):
        node_count = nodes
        dcs = []
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

        if tokens is None and not use_vnodes:
            if dcs is None or len(dcs) <= 1:
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
                binary = (ipformat % i, 9042)
            node = self.create_node(name='node%s' % i,
                                    auto_bootstrap=False,
                                    thrift_interface=(ipformat % i, 9160),
                                    storage_interface=(ipformat % i, 7000),
                                    jmx_port=str(7000 + i * 100),
                                    remote_debug_port=str(2000 + i * 100) if debug else str(0),
                                    initial_token=tk,
                                    binary_interface=binary)
            self.add(node, True, dc)
            self._update_config()
        return self

    def create_node(self, name, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save=True, binary_interface=None):
        return Node(name, self, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save, binary_interface)

    def balanced_tokens(self, node_count):
        if self.cassandra_version() >= '1.2' and not self.partitioner:
            ptokens = [(i * (2**64 // node_count)) for i in xrange(0, node_count)]
            return [int(t - 2**63) for t in ptokens]
        return [int(i * (2**127 // node_count)) for i in range(0, node_count)]

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

    def remove(self, node=None):
        if node is not None:
            if node.name not in self.nodes:
                return

            del self.nodes[node.name]
            if node in self.seeds:
                self.seeds.remove(node)
            self._update_config()
            node.stop(gently=False)
            self.remove_dir_with_retry(node.get_path())
        else:
            self.stop(gently=False)
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
        return [s.network_interfaces['storage'][0] for s in self.seeds]

    def show(self, verbose):
        msg = "Cluster: '%s'" % self.name
        print_(msg)
        print_('-' * len(msg))
        if len(list(self.nodes.values())) == 0:
            print_("No node in this cluster yet")
            return
        for node in list(self.nodes.values()):
            if (verbose):
                node.show(show_cluster=False)
                print_("")
            else:
                node.show(only_status=True)

    def start(self, no_wait=False, verbose=False, wait_for_binary_proto=False, wait_other_notice=False, jvm_args=[], profile_options=None):
        if wait_other_notice:
            marks = [(node, node.mark_log()) for node in list(self.nodes.values())]

        started = []
        for node in list(self.nodes.values()):
            if not node.is_running():
                mark = 0
                if os.path.exists(node.logfilename()):
                    mark = node.mark_log()

                p = node.start(update_pid=False, jvm_args=jvm_args, profile_options=profile_options)
                started.append((node, p, mark))

        if no_wait and not verbose:
            time.sleep(2)  # waiting 2 seconds to check for early errors and for the pid to be set
        else:
            for node, p, mark in started:
                try:
                    start_message = "Listening for thrift clients..." if self.cassandra_version() < "2.2" else "Starting listening for CQL clients"
                    node.watch_log_for(start_message, timeout=60, process=p, verbose=verbose, from_mark=mark)
                except RuntimeError:
                    return None

        self.__update_pids(started)

        for node, p, _ in started:
            if not node.is_running():
                raise NodeError("Error starting {0}.".format(node.name), p)

        if not no_wait and self.cassandra_version() >= "0.8":
            # 0.7 gossip messages seems less predictible that from 0.8 onwards and
            # I don't care enough
            for node, _, mark in started:
                for other_node, _, _ in started:
                    if other_node is not node:
                        node.watch_log_for_alive(other_node, from_mark=mark)

        if wait_other_notice:
            for old_node, mark in marks:
                for node, _, _ in started:
                    if old_node is not node:
                        old_node.watch_log_for_alive(node, from_mark=mark)

        if wait_for_binary_proto:
            for node, p, mark in started:
                node.wait_for_binary_interface(process=p, verbose=verbose, from_mark=mark)

        return started

    def stop(self, wait=True, gently=True):
        not_running = []
        for node in list(self.nodes.values()):
            if not node.stop(wait, gently=gently):
                not_running.append(node)
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

    def wait_for_compactions(self):
        """
        Wait for all compactions to finish on all nodes.
        """
        for node in list(self.nodes.values()):
            if node.is_running():
                node.wait_for_compactions()
        return self

    def nodetool(self, nodetool_cmd):
        for node in list(self.nodes.values()):
            if node.is_running():
                node.nodetool(nodetool_cmd)
        return self

    def stress(self, stress_options):
        stress = common.get_stress_bin(self.get_install_dir())
        livenodes = [node.network_interfaces['storage'][0] for node in list(self.nodes.values()) if node.is_live()]
        if len(livenodes) == 0:
            print_("No live node")
            return
        if self.cassandra_version() <= '2.1':
            args = [stress, '-d', ",".join(livenodes)] + stress_options
        else:
            args = [stress] + stress_options + ['-node', ','.join(livenodes)]
        try:
            # need to set working directory for env on Windows
            if common.is_win():
                subprocess.call(args, cwd=common.parse_path(stress))
            else:
                subprocess.call(args)
        except KeyboardInterrupt:
            pass
        return self

    def run_cli(self, cmds=None, show_output=False, cli_options=[]):
        livenodes = [node for node in list(self.nodes.values()) if node.is_live()]
        if len(livenodes) == 0:
            raise common.ArgumentError("No live node")
        livenodes[0].run_cli(cmds, show_output, cli_options)

    def set_configuration_options(self, values=None, batch_commitlog=None):
        if values is not None:
            for k, v in iteritems(values):
                self._config_options[k] = v
        if batch_commitlog is not None:
            if batch_commitlog:
                self._config_options["commitlog_sync"] = "batch"
                self._config_options["commitlog_sync_batch_window_in_ms"] = 5
                self._config_options["commitlog_sync_period_in_ms"] = None
            else:
                self._config_options["commitlog_sync"] = "periodic"
                self._config_options["commitlog_sync_period_in_ms"] = 10000
                self._config_options["commitlog_sync_batch_window_in_ms"] = None

        self._update_config()
        for node in list(self.nodes.values()):
            node.import_config_files()
        self.__update_topology_files()
        return self

    def set_dse_configuration_options(self, values=None):
        raise common.ArgumentError('Cannot set DSE configuration options on a Cassandra cluster')

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
        seed_list = [node.name for node in self.seeds]
        filename = os.path.join(self.__path, self.name, 'cluster.conf')
        with open(filename, 'w') as f:
            yaml.safe_dump({
                'name': self.name,
                'nodes': node_list,
                'seeds': seed_list,
                'partitioner': self.partitioner,
                'install_dir': self.__install_dir,
                'config_options': self._config_options,
                'dse_config_options': self._dse_config_options,
                'log_level': self.__log_level,
                'use_vnodes': self.use_vnodes
            }, f)

    def __update_pids(self, started):
        for node, p, _ in started:
            node._update_pid(p)

    def __update_topology_files(self):
        dcs = [('default', 'dc1')]
        for node in self.nodelist():
            if node.data_center is not None:
                dcs.append((node.address(), node.data_center))

        content = ""
        for k, v in dcs:
            content = "%s%s=%s:r1\n" % (content, k, v)

        for node in self.nodelist():
            topology_file = os.path.join(node.get_conf_dir(), 'cassandra-topology.properties')
            with open(topology_file, 'w') as f:
                f.write(content)

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

        self._config_options['server_encryption_options'] = node_ssl_options
        self._update_config()
