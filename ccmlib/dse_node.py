# ccm node
from __future__ import with_statement

from six import print_

import os
import shutil
import stat
import subprocess
import time

from ccmlib.node import Node
from ccmlib import common

class DseNode(Node):
    """
    Provides interactions to a DSE node.
    """

    def __init__(self, name, cluster, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save=True, binary_interface=None):
        super(DseNode, self).__init__(name, cluster, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save, binary_interface)
        self.get_cassandra_version()

    def get_install_cassandra_root(self):
        return os.path.join(self.get_install_dir(), 'resources', 'cassandra')

    def get_node_cassandra_root(self):
        return os.path.join(self.get_path(), 'resources', 'cassandra')

    def get_conf_dir(self):
        """
        Returns the path to the directory where Cassandra config are located
        """
        return os.path.join(self.get_path(), 'resources', 'cassandra', 'conf')

    def get_tool(self, toolname):
        return common.join_bin(os.path.join(self.get_install_dir(), 'resources', 'cassandra'), 'bin', toolname)

    def get_tool_args(self, toolname):
        return [common.join_bin(os.path.join(self.get_install_dir(), 'resources', 'cassandra'), 'bin', 'dse'), toolname]

    def get_env(self):
        return common.make_dse_env(self.get_install_dir(), self.get_path())

    def get_cassandra_version(self):
        return common.get_dse_cassandra_version(self.get_install_dir())

    def set_workload(self, workload):
        self.workload = workload
        self._update_config()

    def start(self,
              join_ring=True,
              no_wait=False,
              verbose=False,
              update_pid=True,
              wait_other_notice=False,
              replace_token=None,
              replace_address=None,
              jvm_args=[],
              wait_for_binary_proto=False,
              profile_options=None,
              use_jna=False):
        """
        Start the node. Options includes:
          - join_ring: if false, start the node with -Dcassandra.join_ring=False
          - no_wait: by default, this method returns when the node is started and listening to clients.
            If no_wait=True, the method returns sooner.
          - wait_other_notice: if True, this method returns only when all other live node of the cluster
            have marked this node UP.
          - replace_token: start the node with the -Dcassandra.replace_token option.
          - replace_address: start the node with the -Dcassandra.replace_address option.
        """

        if self.is_running():
            raise Node.NodeError("%s is already running" % self.name)

        for itf in list(self.network_interfaces.values()):
            if itf is not None and replace_address is None:
                common.check_socket_available(itf)

        if wait_other_notice:
            marks = [ (node, node.mark_log()) for node in list(self.cluster.nodes.values()) if node.is_running() ]


        cdir = self.get_install_dir()
        launch_bin = common.join_bin(cdir, 'bin', 'dse')
        # Copy back the dse scripts since profiling may have modified it the previous time
        shutil.copy(launch_bin, self.get_bin_dir())
        launch_bin = common.join_bin(self.get_path(), 'bin', 'dse')

        # If Windows, change entries in .bat file to split conf from binaries
        if common.is_win():
            self.__clean_bat()

        if profile_options is not None:
            config = common.get_config()
            if not 'yourkit_agent' in config:
                raise Node.NodeError("Cannot enable profile. You need to set 'yourkit_agent' to the path of your agent in a ~/.ccm/config")
            cmd = '-agentpath:%s' % config['yourkit_agent']
            if 'options' in profile_options:
                cmd = cmd + '=' + profile_options['options']
            print_(cmd)
            # Yes, it's fragile as shit
            pattern=r'cassandra_parms="-Dlog4j.configuration=log4j-server.properties -Dlog4j.defaultInitOverride=true'
            common.replace_in_file(launch_bin, pattern, '    ' + pattern + ' ' + cmd + '"')

        os.chmod(launch_bin, os.stat(launch_bin).st_mode | stat.S_IEXEC)

        env = common.make_dse_env(self.get_install_dir(), self.get_path())

        if common.is_win():
            self._clean_win_jmx();

        pidfile = os.path.join(self.get_path(), 'cassandra.pid')
        args = [launch_bin, 'cassandra']

        if self.workload is not None:
            if 'hadoop' in self.workload:
                args.append('-t')
            if 'solr' in self.workload:
                args.append('-s')
            if 'spark' in self.workload:
                args.append('-k')
            if 'cfs' in self.workload:
                args.append('-c')
        args += [ '-p', pidfile, '-Dcassandra.join_ring=%s' % str(join_ring) ]
        if replace_token is not None:
            args.append('-Dcassandra.replace_token=%s' % str(replace_token))
        if replace_address is not None:
            args.append('-Dcassandra.replace_address=%s' % str(replace_address))
        if use_jna is False:
            args.append('-Dcassandra.boot_without_jna=true')
        args = args + jvm_args

        process = None
        if common.is_win():
            # clean up any old dirty_pid files from prior runs
            if (os.path.isfile(self.get_path() + "/dirty_pid.tmp")):
                os.remove(self.get_path() + "/dirty_pid.tmp")
            process = subprocess.Popen(args, cwd=self.get_bin_dir(), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            process = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Our modified batch file writes a dirty output with more than just the pid - clean it to get in parity
        # with *nix operation here.
        if common.is_win():
            self.__clean_win_pid()
            self._update_pid(process)
        elif update_pid:
            if no_wait:
                time.sleep(2) # waiting 2 seconds nevertheless to check for early errors and for the pid to be set
            else:
                for line in process.stdout:
                    if verbose:
                        print_(line.rstrip('\n'))

            self._update_pid(process)

            if not self.is_running():
                raise Node.NodeError("Error starting node %s" % self.name, process)

        if wait_other_notice:
            for node, mark in marks:
                node.watch_log_for_alive(self, from_mark=mark)

        if wait_for_binary_proto:
            self.watch_log_for("Starting listening for CQL clients")
            # we're probably fine at that point but just wait some tiny bit more because
            # the msg is logged just before starting the binary protocol server
            time.sleep(0.2)

        return process

    def dsetool(self, cmd):
        env = common.make_dse_env(self.get_install_dir(), self.get_path())
        host = self.address()
        dsetool = common.join_bin(self.get_install_dir(), 'bin', 'dsetool')
        args = [dsetool, '-h', host, '-j', str(self.jmx_port)]
        args += cmd.split()
        p = subprocess.Popen(args, env=env)
        p.wait()

    def hive(self, show_output=False, hive_options=[]):
        env = common.make_dse_env(self.get_install_dir(), self.get_path())
        dse = common.join_bin(self.get_install_dir(), 'bin', 'dse')
        args = [dse, 'hive']
        args += hive_options
        p = subprocess.Popen(args, env=env)
        p.wait()

    def copy_config_files(self):
        os.makedirs(os.path.join(self.get_path(), 'resources', 'dse', 'conf'))
        os.makedirs(os.path.join(self.get_path(), 'resources', 'cassandra', 'conf'))
        os.makedirs(os.path.join(self.get_path(), 'resources', 'hadoop', 'conf'))
        os.makedirs(os.path.join(self.get_path(), 'resources', 'hive', 'conf'))
        common.copy_directory(os.path.join(self.get_install_dir(), 'resources', 'dse', 'conf'), os.path.join(self.get_path(), 'resources', 'dse', 'conf'))
        common.copy_directory(os.path.join(self.get_install_dir(), 'resources', 'cassandra', 'conf'), os.path.join(self.get_path(), 'resources', 'cassandra', 'conf'))
        common.copy_directory(os.path.join(self.get_install_dir(), 'resources', 'hive', 'conf'), os.path.join(self.get_path(), 'resources', 'hive', 'conf'))

    def import_bin_files(self):
        os.makedirs(os.path.join(self.get_path(), 'resources', 'cassandra', 'bin'))
        common.copy_directory(os.path.join(self.get_install_dir(), 'bin'), self.get_bin_dir())
        common.copy_directory(os.path.join(self.get_install_dir(), 'resources', 'cassandra', 'bin'), os.path.join(self.get_path(), 'resources', 'cassandra', 'bin'))
