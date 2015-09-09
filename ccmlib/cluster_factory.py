import os

import yaml
from ccmlib import common, repository
from ccmlib.cluster import Cluster
from ccmlib.dse_cluster import DseCluster
from ccmlib.node import Node


class ClusterFactory():

    @staticmethod
    def load(path, name):
        cluster_path = os.path.join(path, name)
        filename = os.path.join(cluster_path, 'cluster.conf')
        with open(filename, 'r') as f:
            data = yaml.load(f)
        try:
            install_dir = None
            if 'install_dir' in data:
                install_dir = data['install_dir']
                repository.validate(install_dir)
            if install_dir is None and 'cassandra_dir' in data:
                install_dir = data['cassandra_dir']
                repository.validate(install_dir)

            if common.isDse(install_dir):
                cluster = DseCluster(path, data['name'], install_dir=install_dir, create_directory=False)
            else:
                cluster = Cluster(path, data['name'], install_dir=install_dir, create_directory=False)
            node_list = data['nodes']
            seed_list = data['seeds']
            if 'partitioner' in data:
                cluster.partitioner = data['partitioner']
            if 'config_options' in data:
                cluster._config_options = data['config_options']
            if 'log_level' in data:
                cluster.__log_level = data['log_level']
            if 'use_vnodes' in data:
                cluster.use_vnodes = data['use_vnodes']
        except KeyError as k:
            raise common.LoadError("Error Loading " + filename + ", missing property:" + k)

        for node_name in node_list:
            cluster.nodes[node_name] = Node.load(cluster_path, node_name, cluster)
        for seed_name in seed_list:
            cluster.seeds.append(cluster.nodes[seed_name])

        return cluster
