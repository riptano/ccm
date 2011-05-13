# ccm clusters

import common, yaml, os, subprocess
from node import Node

class Cluster():
    def __init__(self, path, name):
        self.name = name
        self.nodes = {}
        self.seeds = []
        self.path = path
        self.partitioner = None

    def save(self):
        node_list = [ node.name for node in self.nodes.values() ]
        seed_list = [ node.name for node in self.seeds ]
        filename = os.path.join(self.path, self.name, 'cluster.conf')
        with open(filename, 'w') as f:
            yaml.dump({ 'name' : self.name, 'nodes' : node_list, 'seeds' : seed_list, 'partitioner' : self.partitioner }, f)

    @staticmethod
    def load(path, name):
        cluster_path = os.path.join(path, name)
        filename = os.path.join(cluster_path, 'cluster.conf')
        with open(filename, 'r') as f:
            data = yaml.load(f)
        try:
            cluster = Cluster(path, data['name'])
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
        self.nodes[node.name] = node
        if is_seed:
            self.seeds.append(node)

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

    # update_pids() should be called after this
    def start(self, cassandra_dir):
        started = []
        for node in self.nodes.values():
            if not node.is_running():
                p = node.start(cassandra_dir)
                started.append((node, p))
        return started

    def update_pids(self, started):
        for node, p in started:
            try:
                node.update_pid(p)
            except StartError as e:
                print str(e)

    def stop(self):
        not_running = []
        for node in self.nodes.values():
            if not node.stop():
                not_running.append(node)
        return not_running

    def nodetool(self, cassandra_dir, nodetool_cmd):
        for node in self.nodes.values():
            if node.is_running():
                node.nodetool(cassandra_dir, nodetool_cmd)

    def stress(self, cassandra_dir, stress_options):
        stress = common.get_stress_bin(cassandra_dir)
        livenodes = [ node.network_interfaces['storage'][0] for node in self.nodes.values() if node.is_live() ]
        if len(livenodes) == 0:
            print "No live node"
            return
        args = [ stress, '-d', ",".join(livenodes) ] + stress_options
        try:
            subprocess.call(args)
        except KeyboardInterrupt:
            pass

    def update_configuration(self, cassandra_dir):
        for node in self.nodes.values():
            node.update_configuration(cassandra_dir)

    def set_configuration_option(self, name, value):
        for node in self.nodes.values():
            node.set_configuration_option(name, value)

    def unset_configuration_option(self, name):
        for node in self.nodes.values():
            node.unset_configuration_option(name)
