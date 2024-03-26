
from __future__ import absolute_import

import os

import yaml

from ccmlib import common, extension, repository
from ccmlib.cluster import Cluster
from ccmlib.dse_cluster import DseCluster
from ccmlib.node import Node

from distutils.version import LooseVersion  #pylint: disable=import-error, no-name-in-module

class ClusterFactory():

    @staticmethod
    def load(path, name):
        cluster_path = os.path.join(path, name)
        filename = os.path.join(cluster_path, 'cluster.conf')
        with open(filename, 'r') as f:
            data = yaml.safe_load(f)
        try:
            install_dir = None
            if 'install_dir' in data:
                install_dir = data['install_dir']
                repository.validate(install_dir)
            if install_dir is None and 'cassandra_dir' in data:
                install_dir = data['cassandra_dir']
                repository.validate(install_dir)

            cassandra_version = None
            if 'cassandra_version' in data:
                cassandra_version = LooseVersion(data['cassandra_version'])

            if common.isDse(install_dir):
                cluster = DseCluster(path, data['name'], install_dir=install_dir, create_directory=False, derived_cassandra_version=cassandra_version)
            else:
                cluster = Cluster(path, data['name'], install_dir=install_dir, create_directory=False, derived_cassandra_version=cassandra_version)
            node_list = data['nodes']
            seed_list = data['seeds']
            if 'partitioner' in data:
                cluster.partitioner = data['partitioner']
            if 'config_options' in data:
                cluster._config_options = data['config_options']
            if 'dse_config_options' in data:
                cluster._dse_config_options = data['dse_config_options']
            if 'misc_config_options' in data:
                cluster._misc_config_options = data['misc_config_options']
            if 'log_level' in data:
                cluster.__log_level = data['log_level']
            if 'use_vnodes' in data:
                cluster.use_vnodes = data['use_vnodes']
            if 'configuration_yaml' in data:
                cluster.configuration_yaml = data['configuration_yaml']
            if 'datadirs' in data:
                cluster.data_dir_count = int(data['datadirs'])
            extension.load_from_cluster_config(cluster, data)
        except KeyError as k:
            raise common.LoadError("Error Loading " + filename + ", missing property:" + k)

        for node_name in node_list:
            cluster.nodes[node_name] = Node.load(cluster_path, node_name, cluster)
        for seed in seed_list:
            cluster.seeds.append(seed)

        return cluster
