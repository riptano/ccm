# ccm clusters

import common, yaml, os, subprocess, shutil, repository, time
from node import Node, NodeError

class Cluster():
    def __init__(self, path, name, partitioner=None, cassandra_dir=None, create_directory=True, cassandra_version=None, verbose=False):
        self.name = name
        self.nodes = {}
        self.seeds = []
        self.partitioner = partitioner
        self._config_options = {}
        self.__path = path
        self.__version = None
        if create_directory:
            # we create the dir before potentially downloading to throw an error sooner if need be
            os.mkdir(self.get_path())

        try:
            if cassandra_version is None:
                self.__cassandra_dir = cassandra_dir
            else:
                self.__cassandra_dir = repository.setup(cassandra_version, verbose)
                self.__version = cassandra_version

            if create_directory:
                common.validate_cassandra_dir(self.__cassandra_dir)
                self.__update_config()
        except:
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
        else:
            self.__cassandra_dir = repository.setup(cassandra_version, verbose=verbose)
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
        if self.__version is None:
            raise common.CCMError("Version is not set")
        return self.__version

    @staticmethod
    def load(path, name):
        cluster_path = os.path.join(path, name)
        filename = os.path.join(cluster_path, 'cluster.conf')
        with open(filename, 'r') as f:
            data = yaml.load(f)
        try:
            cluster = Cluster(path, data['name'], create_directory=False)
            node_list = data['nodes']
            seed_list = data['seeds']
            if 'partitioner' in data:
                cluster.partitioner = data['partitioner']
            if 'cassandra_dir' in data:
                cluster.__cassandra_dir = data['cassandra_dir']
                repository.validate(cluster.__cassandra_dir)
            if 'config_options' in data:
                cluster._config_options = data['config_options']
        except KeyError as k:
            raise common.LoadError("Error Loading " + filename + ", missing property:" + k)

        for node_name in node_list:
            cluster.nodes[node_name] = Node.load(cluster_path, node_name, cluster)
        for seed_name in seed_list:
            cluster.seeds.append(cluster.nodes[seed_name])

        return cluster

    def add(self, node, is_seed):
        if node.name in self.nodes:
            raise common.ArgumentError('Cannot create existing node %s' % node.name)
        self.nodes[node.name] = node
        if is_seed:
            self.seeds.append(node)
        self.__update_config()
        return self

    def populate(self, node_count, tokens=None):
        if node_count < 1 or node_count >= 10:
            raise common.ArgumentError('invalid node count %s' % node_count)

        for i in xrange(1, node_count + 1):
            if 'node%s' % i in self.nodes:
                raise common.ArgumentError('Cannot create existing node node%s' % i)

        for i in xrange(1, node_count + 1):
            tk = None
            if tokens is not None:
                try:
                    tk = tokens[i-1]
                except IndexError:
                    pass
            node = Node('node%s' % i,
                        self,
                        False,
                        ('127.0.0.%s' % i, 9160),
                        ('127.0.0.%s' % i, 7000),
                        str(7000 + i * 100),
                        tk)
            self.add(node, True)
            self.__update_config()
        return self

    @staticmethod
    def balanced_tokens(node_count):
        return [ (i*(2**127/node_count)) for i in range(0, node_count) ]

    def remove(self, node=None):
        if node is not None:
            if not node.name in self.nodes:
                return

            del self.nodes[self.node.name]
            if node in self.seeds:
                self.seeds.remove(self.node)
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

    def start(self, no_wait=False, verbose=False):
        started = []
        marks = []
        for node in self.nodes.values():
            if not node.is_running():
                p = node.start(update_pid=False)
                # ugly? indeed!
                while not os.path.exists(node.logfilename()):
                    time.sleep(.01)
                marks.append((node, node.mark_log()))
                started.append((node, p))

        if no_wait:
            time.sleep(2) # waiting 2 seconds to check for early errors and for the pid to be set
        else:
            for node, p in started:
                for line in p.stdout:
                    if verbose:
                        print "[%s] %s" % (node.name, line.rstrip('\n'))
                if verbose:
                    print "----"

        self.__update_pids(started)

        for node, p in started:
            if not node.is_running():
                raise NodeError("Error starting {0}.".format(node.name), p)

        if not no_wait:
            for node, mark in marks:
                for other_node, _ in marks:
                    if other_node is not node:
                        node.watch_log_for_alive(other_node, from_mark=mark)

        return started

    def stop(self, wait=True):
        not_running = []
        for node in self.nodes.values():
            if not node.stop(wait):
                not_running.append(node)
        return not_running

    def set_log_level(self, new_level):
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
        for node in self.nodes.values():
            node.set_configuration_options(values=values, batch_commitlog=batch_commitlog)
        return self

    def flush(self):
        self.nodetool("flush")

    def compact(self):
        self.nodetool("compact")

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
                'config_options' : self._config_options
            }, f)

    def __update_pids(self, started):
        for node, p in started:
            node.update_pid(p)

