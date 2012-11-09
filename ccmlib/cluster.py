# ccm clusters

import common, yaml, os, subprocess, shutil, repository, time, re
from node import Node, NodeError
from bulkloader import BulkLoader

class Cluster():
    def __init__(self, path, name, partitioner=None, cassandra_dir=None, create_directory=True, cassandra_version=None, verbose=False):
        self.name = name
        self.nodes = {}
        self.seeds = []
        self.partitioner = partitioner
        self._config_options = {}
        self.__log_level = "INFO"
        self.__path = path
        self.__version = None
        if create_directory:
            # we create the dir before potentially downloading to throw an error sooner if need be
            os.mkdir(self.get_path())

        try:
            if cassandra_version is None:
                # at this point, cassandra_dir should always not be None, but
                # we keep this for backward compatibility (in loading old cluster)
                if cassandra_dir is not None:
                    self.__cassandra_dir = os.path.abspath(cassandra_dir)
                    self.__version = self.__get_version_from_build()
            else:
                dir, v = repository.setup(cassandra_version, verbose)
                self.__cassandra_dir = dir
                self.__version = v if v is not None else self.__get_version_from_build()

            if create_directory:
                common.validate_cassandra_dir(self.__cassandra_dir)
                self.__update_config()
        except:
            if create_directory:
                shutil.rmtree(self.get_path())
            raise

    def set_partitioner(self, partitioner):
        self.partitioner = partitioner
        self.__update_config()
        return self

    def set_cassandra_dir(self, cassandra_dir=None, cassandra_version=None, verbose=False):
        if cassandra_version is None:
            self.__cassandra_dir = cassandra_dir
            common.validate_cassandra_dir(cassandra_dir)
            self.__version = self.__get_version_from_build()
        else:
            dir, v = repository.setup(cassandra_version, verbose)
            self.__cassandra_dir = dir
            self.__version = v if v is not None else self.__get_version_from_build()
        self.__update_config()
        for node in self.nodes.values():
            node.import_config_files()
        return self

    def get_cassandra_dir(self):
        common.validate_cassandra_dir(self.__cassandra_dir)
        return self.__cassandra_dir

    def nodelist(self):
        return [ self.nodes[name] for name in sorted(self.nodes.keys()) ]

    def version(self):
        return self.__version

    @staticmethod
    def load(path, name):
        cluster_path = os.path.join(path, name)
        filename = os.path.join(cluster_path, 'cluster.conf')
        with open(filename, 'r') as f:
            data = yaml.load(f)
        try:
            cassandra_dir = None
            if 'cassandra_dir' in data:
                cassandra_dir = data['cassandra_dir']
                repository.validate(cassandra_dir)

            cluster = Cluster(path, data['name'], cassandra_dir=cassandra_dir, create_directory=False)
            node_list = data['nodes']
            seed_list = data['seeds']
            if 'partitioner' in data:
                cluster.partitioner = data['partitioner']
            if 'config_options' in data:
                cluster._config_options = data['config_options']
            if 'log_level' in data:
                cluster.__log_level = data['log_level']
        except KeyError as k:
            raise common.LoadError("Error Loading " + filename + ", missing property:" + k)

        for node_name in node_list:
            cluster.nodes[node_name] = Node.load(cluster_path, node_name, cluster)
        for seed_name in seed_list:
            cluster.seeds.append(cluster.nodes[seed_name])

        return cluster

    def add(self, node, is_seed, data_center=None):
        if node.name in self.nodes:
            raise common.ArgumentError('Cannot create existing node %s' % node.name)
        self.nodes[node.name] = node
        if is_seed:
            self.seeds.append(node)
        self.__update_config()
        node.data_center = data_center
        node.set_log_level(self.__log_level)
        node._save()
        if data_center is not None:
            self.__update_topology_files()
        return self

    def populate(self, nodes, debug=False, tokens=None, use_vnodes=False):
        node_count = nodes
        dcs = []
        if isinstance(nodes, list):
            self.set_configuration_options(values={'endpoint_snitch' : 'org.apache.cassandra.locator.PropertyFileSnitch'})
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
            if 'node%s' % i in self.nodes.values():
                raise common.ArgumentError('Cannot create existing node node%s' % i)

        if tokens is None and not use_vnodes:
            tokens = self.balanced_tokens(node_count)

        for i in xrange(1, node_count + 1):
            tk = None
            if tokens is not None and i-1 < len(tokens):
                tk = tokens[i-1]
            dc = dcs[i-1] if i-1 < len(dcs) else None

            binary = None
            if self.version() >= '1.2':
                binary = ('127.0.0.%s' % i, 9042)
            node = Node('node%s' % i,
                        self,
                        False,
                        ('127.0.0.%s' % i, 9160),
                        ('127.0.0.%s' % i, 7000),
                        str(7000 + i * 100),
                        (str(0),  str(2000 + i * 100))[debug == True],
                        tk,
                        binary_interface=binary)
            self.add(node, True, dc)
            self.__update_config()
        return self

    def balanced_tokens(self, node_count):
        if self.version() >= '1.2' and not self.partitioner:
            ptokens = [(i*(2**64/node_count)) for i in xrange(0, node_count)]
            return [t - 2**63 for t in ptokens]
        return [ (i*(2**127/node_count)) for i in range(0, node_count) ]

    def remove(self, node=None):
        if node is not None:
            if not node.name in self.nodes:
                return

            del self.nodes[node.name]
            if node in self.seeds:
                self.seeds.remove(node)
            self.__update_config()
            node.stop()
            shutil.rmtree(node.get_path())
        else:
            self.stop()
            shutil.rmtree(self.get_path())

    def clear(self):
        self.stop()
        for node in self.nodes.values():
            node.clear()

    def get_path(self):
        return os.path.join(self.__path, self.name)

    def get_seeds(self):
        return [ s.network_interfaces['storage'][0] for s in self.seeds ]

    def show(self, verbose):
        if len(self.nodes.values()) == 0:
            print "No node in this cluster yet"
            return
        for node in self.nodes.values():
            if (verbose):
                node.show(show_cluster=False)
                print ""
            else:
                node.show(only_status=True)

    def start(self, no_wait=False, verbose=False, wait_for_binary_proto=False):
        started = []
        marks = []
        for node in self.nodes.values():
            if not node.is_running():
                p = node.start(update_pid=False)
                started.append((node, p))
                # ugly? indeed!
                while not os.path.exists(node.logfilename()):
                    time.sleep(.01)
                marks.append((node, node.mark_log()))

        if no_wait and not verbose:
            time.sleep(2) # waiting 2 seconds to check for early errors and for the pid to be set
        else:
            for node, p in started:
                for line in p.stdout:
                    if verbose:
                        print "[%s] %s" % (node.name, line.rstrip('\n'))
                for line in p.stderr:
                    if verbose:
                        print "[%s ERROR] %s" % (node.name, line.rstrip('\n'))
                if verbose:
                    print "----"

        self.__update_pids(started)

        for node, p in started:
            if not node.is_running():
                raise NodeError("Error starting {0}.".format(node.name), p)

        if not no_wait and self.version() >= "0.8":
            # 0.7 gossip messages seems less predictible that from 0.8 onwards and
            # I don't care enough
            for node, mark in marks:
                for other_node, _ in marks:
                    if other_node is not node:
                        node.watch_log_for_alive(other_node, from_mark=mark)

        if wait_for_binary_proto:
            for node, _ in started:
                node.watch_log_for("Starting listening for CQL clients")
            time.sleep(0.2)

        return started

    def stop(self, wait=True, gently=False):
        not_running = []
        for node in self.nodes.values():
            if not node.stop(wait, gently=gently):
                not_running.append(node)
        return not_running

    def set_log_level(self, new_level):
        known_level = [ 'TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR' ]
        if new_level not in known_level:
            raise common.ArgumentError("Unknown log level %s (use one of %s)" % (new_level, " ".join(known_level)))

        self.__log_level = new_level
        self.__update_config()

        for node in self.nodelist():
            node.set_log_level(new_level)

    def nodetool(self, nodetool_cmd):
        for node in self.nodes.values():
            if node.is_running():
                node.nodetool(nodetool_cmd)
        return self

    def stress(self, stress_options):
        stress = common.get_stress_bin(self.get_cassandra_dir())
        livenodes = [ node.network_interfaces['storage'][0] for node in self.nodes.values() if node.is_live() ]
        if len(livenodes) == 0:
            print "No live node"
            return
        args = [ stress, '-d', ",".join(livenodes) ] + stress_options
        try:
            subprocess.call(args)
        except KeyboardInterrupt:
            pass
        return self

    def run_cli(self, cmds=None, show_output=False, cli_options=[]):
        livenodes = [ node for node in self.nodes.values() if node.is_live() ]
        if len(livenodes) == 0:
            raise common.ArgumentError("No live node")
        livenodes[0].run_cli(cmds, show_output, cli_options)

    def set_configuration_options(self, values=None, batch_commitlog=None):
        if values is not None:
            for k, v in values.iteritems():
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

        for node in self.nodes.values():
            node.import_config_files()
        return self

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
        for node in self.nodes.values():
            if node.is_running():
                node.decommission()

    def removeToken(self, token):
        self.nodetool("removeToken " + str(token))

    def bulkload(self, options):
        loader = BulkLoader(self)
        loader.load(options)

    def scrub(self, options):
        for node in self.nodes.values():
            node.scrub(options)

    def __get_version_from_build(self):
        cassandra_dir = self.get_cassandra_dir()
        build = os.path.join(cassandra_dir, 'build.xml')
        with open(build) as f:
            for line in f:
                match = re.search('name="base\.version" value="([0-9.]+)[^"]*"', line)
                if match:
                    return match.group(1)
        raise common.CCMError("Cannot find version")

    def __update_config(self):
        node_list = [ node.name for node in self.nodes.values() ]
        seed_list = [ node.name for node in self.seeds ]
        filename = os.path.join(self.__path, self.name, 'cluster.conf')
        with open(filename, 'w') as f:
            yaml.safe_dump({
                'name' : self.name,
                'nodes' : node_list,
                'seeds' : seed_list,
                'partitioner' : self.partitioner,
                'cassandra_dir' : self.__cassandra_dir,
                'config_options' : self._config_options,
                'log_level' : self.__log_level
            }, f)

    def __update_pids(self, started):
        for node, p in started:
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
