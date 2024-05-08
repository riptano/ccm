# ccm clusters

from __future__ import absolute_import

import os
import shutil
import signal
import subprocess
import sys

from six import iteritems, print_

from ccmlib import common, repository
from ccmlib.cluster import Cluster
from ccmlib.dse_node import DseNode

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser


class DseCluster(Cluster):

    def __init__(self, path, name, partitioner=None, install_dir=None, create_directory=True, version=None, dse_username=None, dse_password=None, dse_credentials_file=None, opscenter=None, verbose=False, derived_cassandra_version=None, configuration_yaml=None):
        self.dse_username = None
        self.dse_password = None
        self.load_credentials_from_file(dse_credentials_file)
        if dse_username is not None:
            self.dse_username = dse_username
        if dse_password is not None:
            self.dse_password = dse_password

        self.opscenter = opscenter
        self._cassandra_version = None
        if derived_cassandra_version:
            self._cassandra_version = derived_cassandra_version

        super(DseCluster, self).__init__(path, name, partitioner, install_dir, create_directory, version, verbose, configuration_yaml=configuration_yaml)

    def load_from_repository(self, version, verbose):
        if self.opscenter is not None:
            odir = repository.setup_opscenter(self.opscenter, self.dse_username, self.dse_password, verbose)
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
            parser = ConfigParser.RawConfigParser()
            parser.read(dse_credentials_file)
            if parser.has_section('dse_credentials'):
                if parser.has_option('dse_credentials', 'dse_username'):
                    self.dse_username = parser.get('dse_credentials', 'dse_username')
                if parser.has_option('dse_credentials', 'dse_password'):
                    self.dse_password = parser.get('dse_credentials', 'dse_password')
            else:
                common.warning("{} does not contain a 'dse_credentials' section.".format(dse_credentials_file))

    def get_seeds(self):
        return [s.network_interfaces['storage'][0] if isinstance(s, DseNode) else s for s in self.seeds]

    def hasOpscenter(self):
        return os.path.exists(os.path.join(self.get_path(), 'opscenter'))

    def create_node(self, name, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save=True, binary_interface=None, byteman_port='0', environment_variables=None,derived_cassandra_version=None):
        return DseNode(name, self, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save, binary_interface, byteman_port, environment_variables=environment_variables, derived_cassandra_version=derived_cassandra_version)

    def start(self, no_wait=False, verbose=False, wait_for_binary_proto=False, wait_other_notice=True, jvm_args=None, profile_options=None, quiet_start=False, allow_root=False, jvm_version=None):
        if jvm_args is None:
            jvm_args = []
        marks = {}
        for node in self.nodelist():
            marks[node] = node.mark_log()
        started = super(DseCluster, self).start(no_wait, verbose, wait_for_binary_proto, wait_other_notice, jvm_args, profile_options, quiet_start=quiet_start, allow_root=allow_root, timeout=180, jvm_version=jvm_version)
        self.start_opscenter()
        if self._misc_config_options.get('enable_aoss', False):
            self.wait_for_any_log('AlwaysOn SQL started', 600, marks=marks)
        return started

    def stop(self, wait=True, signal_event=signal.SIGTERM, **kwargs):
        not_running = super(DseCluster, self).stop(wait=wait, signal_event=signal.SIGTERM, **kwargs)
        self.stop_opscenter()
        return not_running


    def remove(self, node=None):
        # We _must_ gracefully stop if aoss is enabled, otherwise we will leak the spark workers
        super(DseCluster, self).remove(node=node, gently=self._misc_config_options.get('enable_aoss', False))

    def cassandra_version(self):
        if self._cassandra_version is None:
            self._cassandra_version = common.get_dse_cassandra_version(self.get_install_dir())
        return self._cassandra_version

    def enable_aoss(self):
        if self.version() < '6.0':
            common.error("Cannot enable AOSS in DSE clusters before 6.0")
            exit(1)
        self._misc_config_options['enable_aoss'] = True
        for node in self.nodelist():
            port_offset = int(node.name[4:])
            node.enable_aoss(thrift_port=10000 + port_offset, web_ui_port=9077 + port_offset)
        self._update_config()

    def set_dse_configuration_options(self, values=None):
        if values is not None:
            self._dse_config_options = common.merge_configuration(self._dse_config_options, values)
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
            if len(self.nodes) > 0:
                node = list(self.nodes.values())[0]
                (node_ip, node_port) = node.network_interfaces['thrift']
                node_jmx = node.jmx_port
                with open(os.path.join(cluster_conf, self.name + '.conf'), 'w+') as f:
                    f.write('[jmx]\n')
                    f.write('port = %s\n' % node_jmx)
                    f.write('[cassandra]\n')
                    f.write('seed_hosts = %s\n' % node_ip)
                    f.write('api_port = %s\n' % node_port)
                    f.close()
