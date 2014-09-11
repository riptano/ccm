# ccm clusters

from ccmlib import repository
from ccmlib.cluster import Cluster
from ccmlib.dse_node import DseNode
from ccmlib import common

class DseCluster(Cluster):
    def __init__(self, path, name, partitioner=None, install_dir=None, create_directory=True, version=None, dse_username=None, dse_password=None, verbose=False):
        self.dse_username = dse_username
        self.dse_password = dse_password
        super(DseCluster, self).__init__(path, name, partitioner, install_dir, create_directory, version, verbose)

    def load_from_repository(self, version, verbose):
        return repository.setup_dse(version, self.dse_username, self.dse_password, verbose)

    def create_node(self, name, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save=True, binary_interface=None):
        return DseNode(name, self, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save, binary_interface)

    def cassandra_version(self):
        return common.get_dse_cassandra_version(self.get_install_dir())
