# ccm clusters

import common, yaml, os, subprocess, shutil
from node import Node, NodeError

class Cluster():
    def __init__(self, path, name, partitioner=None, cassandra_dir=None, create_directory=True):
        self.name = name
        self.nodes = {}
        self.seeds = []
        self.path = path
        self.partitioner = partitioner
        self.cassandra_dir = cassandra_dir
        if create_directory:
            os.mkdir(self.get_path())
            self.__save()

    def set_partitioner(self, partitioner):
        self.partitioner = partitioner
        self.__save()
        return self

    def set_cassandra_dir(self, cassandra_dir):
        common.validate_cassandra_dir(cassandra_dir)
        self.cassandra_dir = cassandra_dir
        self.__save()
        return self

    def get_cassandra_dir(self):
        common.validate_cassandra_dir(self.cassandra_dir)
        return self.cassandra_dir

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
        self.__save()
        return self

    def populate(self, node_count):
        if node_count < 1 or node_count >= 10:
            raise common.ArgumentError('invalid node count %s' % node_count)

        for i in xrange(1, node_count + 1):
            if 'node%s' % i in self.nodes:
                raise common.ArgumentError('Cannot create existing node node%s' % i)

        for i in xrange(1, node_count + 1):
            node = Node('node%s' % i,
                        self,
                        False,
                        ('127.0.0.%s' % i, 9160),
                        ('127.0.0.%s' % i, 7000),
                        str(7000 + i * 100),
                        None)
            self.add(node, True)
            node.update_configuration()
        return self

    def remove(self, node=None):
        if node is not None:
            if not node.name in self.nodes:
                return

            del self.cluster.nodes[self.node.name]
            if node in self.cluster.seeds:
                self.cluster.seeds.remove(self.node)
            self.cluster.__save()
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
        return os.path.join(self.path, self.name)

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
        for node in self.nodes.values():
            if not node.is_running():
                p = node.start(update_pid=False)
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

        return started

    def stop(self, wait=True):
        not_running = []
        for node in self.nodes.values():
            if not node.stop(wait):
                not_running.append(node)
        return not_running

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

    def update_configuration(self, hh=True, cl_batch=False, rpc_timeout=None):
        for node in self.nodes.values():
            node.update_configuration(hh=hh, cl_batch=cl_batch, rpc_timeout=rpc_timeout)
        return self

    def set_configuration_option(self, name, value):
        for node in self.nodes.values():
            node.set_configuration_option(name, value)
        return self

    def unset_configuration_option(self, name):
        for node in self.nodes.values():
            node.unset_configuration_option(name)
        return self

    def __save(self):
        node_list = [ node.name for node in self.nodes.values() ]
        seed_list = [ node.name for node in self.seeds ]
        filename = os.path.join(self.path, self.name, 'cluster.conf')
        with open(filename, 'w') as f:
            yaml.dump({
                'name' : self.name,
                'nodes' : node_list,
                'seeds' : seed_list,
                'partitioner' : self.partitioner,
                'cassandra_dir' : self.cassandra_dir }, f)

    def __update_pids(self, started):
        for node, p in started:
            node.update_pid(p)

