# ccm clusters
import os
import shutil
import signal
import subprocess
import sys
try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

from six import iteritems, print_

from ccmlib import common, repository
from ccmlib.cluster import Cluster
from ccmlib.dse_node import DseNode


class DseCluster(Cluster):

    def __init__(self, path, name, partitioner=None, install_dir=None, create_directory=True, version=None, dse_username=None, dse_password=None, dse_credentials_file=None, opscenter=None, verbose=False):
        self.dse_username = None
        self.dse_password = None
        self.load_credentials_from_file(dse_credentials_file)
        if dse_username is not None:
            self.dse_username = dse_username
        if dse_password is not None:
            self.dse_password = dse_password

        self.opscenter = opscenter
        super(DseCluster, self).__init__(path, name, partitioner, install_dir, create_directory, version, verbose)

    def load_from_repository(self, version, verbose):
        if self.opscenter is not None:
            odir = repository.setup_opscenter(self.opscenter, verbose)
            target_dir = os.path.join(self.get_path(), 'opscenter')
            shutil.copytree(odir, target_dir)
        return repository.setup_dse(version, self.dse_username, self.dse_password, verbose)

    def load_credentials_from_file(self, dse_credentials_file):
        # Use .dse.ini if it exists in the default .ccm directory.
        if dse_credentials_file is None:
            creds_file = os.path.join(common.get_default_path(), '.dse.ini')
            if os.path.isfile(creds_file):
                dse_credentials_file = creds_file

        if dse_credentials_file is not None:
            parser = ConfigParser.ConfigParser()
            parser.read(dse_credentials_file)
            if parser.has_section('dse_credentials'):
                if parser.has_option('dse_credentials', 'dse_username'):
                    self.dse_username = parser.get('dse_credentials', 'dse_username')
                if parser.has_option('dse_credentials', 'dse_password'):
                    self.dse_password = parser.get('dse_credentials', 'dse_password')
            else:
                print_("Warning: {} does not contain a 'dse_credentials' section.".format(dse_credentials_file), file=sys.stderr)

    def hasOpscenter(self):
        return os.path.exists(os.path.join(self.get_path(), 'opscenter'))

    def create_node(self, name, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save=True, binary_interface=None, byteman_port='0'):
        return DseNode(name, self, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save, binary_interface, byteman_port)

    def start(self, no_wait=False, verbose=False, wait_for_binary_proto=False, wait_other_notice=True, jvm_args=None, profile_options=None, quiet_start=False, allow_root=False):
        if jvm_args is None:
            jvm_args = []
        started = super(DseCluster, self).start(no_wait, verbose, wait_for_binary_proto, wait_other_notice, jvm_args, profile_options, quiet_start=quiet_start, allow_root=allow_root)
        self.start_opscenter()
        return started

    def stop(self, wait=True, gently=True):
        not_running = super(DseCluster, self).stop(wait, gently)
        self.stop_opscenter()
        return not_running

    def cassandra_version(self):
        return common.get_dse_cassandra_version(self.get_install_dir())

    def set_dse_configuration_options(self, values=None):
        if values is not None:
            for k, v in iteritems(values):
                self._dse_config_options[k] = v
        self._update_config()
        for node in list(self.nodes.values()):
            node.import_dse_config_files()
        return self

    def start_opscenter(self):
        if self.hasOpscenter():
            self.write_opscenter_cluster_config()
            args = [os.path.join(self.get_path(), 'opscenter', 'bin', common.platform_binary('opscenter'))]
            subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def stop_opscenter(self):
        pidfile = os.path.join(self.get_path(), 'opscenter', 'twistd.pid')
        if os.path.exists(pidfile):
            with open(pidfile, 'r') as f:
                pid = int(f.readline().strip())
                f.close()
            if pid is not None:
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
            os.remove(pidfile)

    def write_opscenter_cluster_config(self):
        cluster_conf = os.path.join(self.get_path(), 'opscenter', 'conf', 'clusters')
        if not os.path.exists(cluster_conf):
            os.makedirs(cluster_conf)
            if len(self.seeds) > 0:
                seed = self.seeds[0]
                (seed_ip, seed_port) = seed.network_interfaces['thrift']
                seed_jmx = seed.jmx_port
                with open(os.path.join(cluster_conf, self.name + '.conf'), 'w+') as f:
                    f.write('[jmx]\n')
                    f.write('port = %s\n' % seed_jmx)
                    f.write('[cassandra]\n')
                    f.write('seed_hosts = %s\n' % seed_ip)
                    f.write('api_port = %s\n' % seed_port)
                    f.close()
