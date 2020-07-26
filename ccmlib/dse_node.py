# ccm node
from __future__ import absolute_import, with_statement

import os
import re
import shutil
import signal
import stat
import subprocess
import time

import yaml
from six import iteritems, print_

from ccmlib import common, extension, repository
from ccmlib.node import (Node, NodeError, ToolError,
                         handle_external_tool_process)


class DseNode(Node):

    """
    Provides interactions to a DSE node.
    """

    def __init__(self, name, cluster, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save=True, binary_interface=None, byteman_port='0', environment_variables=None, derived_cassandra_version=None):
        super(DseNode, self).__init__(name, cluster, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save, binary_interface, byteman_port, environment_variables=environment_variables, derived_cassandra_version=derived_cassandra_version)
       
        self._dse_config_options = {}
        if self.cluster.hasOpscenter():
            self._copy_agent()

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
        return common.make_dse_env(self.get_install_dir(), self.get_path(), self.ip_addr)

    def node_setup(self, version, verbose):
        dir, v = repository.setup_dse(version, self.cluster.dse_username, self.cluster.dse_password, verbose=verbose)
        return dir

    def set_workloads(self, workloads):
        self.workloads = workloads
        self._update_config()
        if 'solr' in self.workloads:
            self.__generate_server_xml()
        if 'graph' in self.workloads:
            self.__update_gremlin_config_yaml()
        if 'dsefs' in self.workloads:
            dsefs_options = {'dsefs_options': {'enabled': True,
                                               'work_dir': os.path.join(self.get_path(), 'dsefs'),
                                               'data_directories': [{'dir': os.path.join(self.get_path(), 'dsefs', 'data')}]}}
            self.set_dse_configuration_options(dsefs_options)
        if 'spark' in self.workloads:
            dsefs_enabled = 'dsefs' in self.workloads
            dse_options = {'dsefs_options': {'enabled': dsefs_enabled,
                                             'work_dir': os.path.join(self.get_path(), 'dsefs'),
                                             'data_directories': [{'dir': os.path.join(self.get_path(), 'dsefs', 'data')}]}}
            if self.cluster.version() >= '6.0':
                # Don't overwrite aoss options
                if 'resource_manager_options' not in self._dse_config_options:
                    dse_options['resource_manager_options'] = {'worker_options': {'memory_total': '1g', 'cores_total': 2}}

            self.set_dse_configuration_options(dse_options)
            self._update_spark_env()

    def enable_aoss(self, thrift_port=10000, web_ui_port=9077):
        self._dse_config_options['alwayson_sql_options'] = {'enabled': True, 'thrift_port': thrift_port, 'web_ui_port': web_ui_port}
        self._dse_config_options['resource_manager_options'] = {'worker_options':
                                                                {'cores_total': 2,
                                                                 'memory_total': '2g',
                                                                 'workpools': [{
                                                                     'name': 'alwayson_sql',
                                                                     'cores': 0.5,
                                                                     'memory': 0.5
                                                                 }]}}
        self._update_config()
        self._update_spark_env()
        self._update_yaml()

    def set_dse_configuration_options(self, values=None):
        if values is not None:
            self._dse_config_options = common.merge_configuration(self._dse_config_options, values)
        self.import_dse_config_files()

    def watch_log_for_alive(self, nodes, from_mark=None, timeout=720, filename='system.log'):
        """
        Watch the log of this node until it detects that the provided other
        nodes are marked UP. This method works similarly to watch_log_for_death.

        We want to provide a higher default timeout when this is called on DSE.
        """
        super(DseNode, self).watch_log_for_alive(nodes, from_mark=from_mark, timeout=timeout, filename=filename)

    def get_launch_bin(self):
        cdir = self.get_install_dir()
        launch_bin = common.join_bin(cdir, 'bin', 'dse')
        # Copy back the dse scripts since profiling may have modified it the previous time
        shutil.copy(launch_bin, self.get_bin_dir())
        return common.join_bin(self.get_path(), 'bin', 'dse')

    def add_custom_launch_arguments(self, args):
        args.append('cassandra')
        for workload in self.workloads:
            if 'hadoop' in workload:
                args.append('-t')
            if 'solr' in workload:
                args.append('-s')
            if 'spark' in workload:
                args.append('-k')
            if 'cfs' in workload:
                args.append('-c')
            if 'graph' in workload:
                args.append('-g')

    def start(self,
              join_ring=True,
              no_wait=False,
              verbose=False,
              update_pid=True,
              wait_other_notice=True,
              replace_token=None,
              replace_address=None,
              jvm_args=None,
              wait_for_binary_proto=False,
              profile_options=None,
              use_jna=False,
              quiet_start=False,
              allow_root=False,
              set_migration_task=True,
              jvm_version=None):
        mark = self.mark_log()
        process = super(DseNode, self).start(join_ring, no_wait, verbose, update_pid, wait_other_notice, replace_token,
                                             replace_address, jvm_args, wait_for_binary_proto, profile_options, use_jna,
                                             quiet_start, allow_root, set_migration_task, jvm_version)
        if self.cluster.hasOpscenter():
            self._start_agent()

    def _start_agent(self):
        agent_dir = os.path.join(self.get_path(), 'datastax-agent')
        if os.path.exists(agent_dir):
            self._write_agent_address_yaml(agent_dir)
            self._write_agent_log4j_properties(agent_dir)
            args = [os.path.join(agent_dir, 'bin', common.platform_binary('datastax-agent'))]
            subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def stop(self, wait=True, wait_other_notice=False, signal_event=signal.SIGTERM, **kwargs):
        if self.cluster.hasOpscenter():
            self._stop_agent()
        return super(DseNode, self).stop(wait=wait, wait_other_notice=wait_other_notice, signal_event=signal_event, **kwargs)

    def _stop_agent(self):
        agent_dir = os.path.join(self.get_path(), 'datastax-agent')
        if os.path.exists(agent_dir):
            pidfile = os.path.join(agent_dir, 'datastax-agent.pid')
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

    def nodetool(self, cmd, username=None, password=None, capture_output=True, wait=True):
        if password is not None:
            cmd = '-pw {} '.format(password) + cmd
        if username is not None:
            cmd = '-u {} '.format(username) + cmd

        return super(DseNode, self).nodetool(cmd)

    def dsetool(self, cmd):
        env = self.get_env()
        extension.append_to_client_env(self, env)
        node_ip, binary_port = self.network_interfaces['binary']
        dsetool = common.join_bin(self.get_install_dir(), 'bin', 'dsetool')
        args = [dsetool, '-h', node_ip, '-j', str(self.jmx_port), '-c', str(binary_port)]
        args += cmd.split()
        p = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return handle_external_tool_process(p, args)

    def dse(self, dse_options=None):
        if dse_options is None:
            dse_options = []
        env = self.get_env()
        extension.append_to_client_env(self, env)
        env['JMX_PORT'] = self.jmx_port
        dse = common.join_bin(self.get_install_dir(), 'bin', 'dse')
        args = [dse]
        args += dse_options
        p = subprocess.Popen(args, env=env)  #Don't redirect stdout/stderr, users need to interact with new process
        return handle_external_tool_process(p, args)

    def hadoop(self, hadoop_options=None):
        if hadoop_options is None:
            hadoop_options = []
        env = self.get_env()
        env['JMX_PORT'] = self.jmx_port
        dse = common.join_bin(self.get_install_dir(), 'bin', 'dse')
        args = [dse, 'hadoop']
        args += hadoop_options
        p = subprocess.Popen(args, env=env)  #Don't redirect stdout/stderr, users need to interact with new process
        return handle_external_tool_process(p, args)

    def hive(self, hive_options=None):
        if hive_options is None:
            hive_options = []
        env = self.get_env()
        env['JMX_PORT'] = self.jmx_port
        dse = common.join_bin(self.get_install_dir(), 'bin', 'dse')
        args = [dse, 'hive']
        args += hive_options
        p = subprocess.Popen(args, env=env)  #Don't redirect stdout/stderr, users need to interact with new process
        return handle_external_tool_process(p, args)

    def pig(self, pig_options=None):
        if pig_options is None:
            pig_options = []
        env = self.get_env()
        env['JMX_PORT'] = self.jmx_port
        dse = common.join_bin(self.get_install_dir(), 'bin', 'dse')
        args = [dse, 'pig']
        args += pig_options
        p = subprocess.Popen(args, env=env)  #Don't redirect stdout/stderr, users need to interact with new process
        return handle_external_tool_process(p, args)

    def sqoop(self, sqoop_options=None):
        if sqoop_options is None:
            sqoop_options = []
        env = self.get_env()
        env['JMX_PORT'] = self.jmx_port
        dse = common.join_bin(self.get_install_dir(), 'bin', 'dse')
        args = [dse, 'sqoop']
        args += sqoop_options
        p = subprocess.Popen(args, env=env)  #Don't redirect stdout/stderr, users need to interact with new process
        return handle_external_tool_process(p, args)

    def spark(self, spark_options=None):
        if spark_options is None:
            spark_options = []
        env = self.get_env()
        env['JMX_PORT'] = self.jmx_port
        dse = common.join_bin(self.get_install_dir(), 'bin', 'dse')
        args = [dse, 'spark']
        args += spark_options
        p = subprocess.Popen(args, env=env)  #Don't redirect stdout/stderr, users need to interact with new process
        return handle_external_tool_process(p, args)

    def import_dse_config_files(self):
        self._update_config()
        if not os.path.isdir(os.path.join(self.get_path(), 'resources', 'dse', 'conf')):
            os.makedirs(os.path.join(self.get_path(), 'resources', 'dse', 'conf'))
        common.copy_directory(os.path.join(self.get_install_dir(), 'resources', 'dse', 'conf'), os.path.join(self.get_path(), 'resources', 'dse', 'conf'))
        self._update_yaml()

    def copy_config_files(self):
        for product in ['dse', 'cassandra', 'hadoop', 'hadoop2-client', 'sqoop', 'hive', 'tomcat', 'spark', 'shark', 'mahout', 'pig', 'solr', 'graph']:
            src_conf = os.path.join(self.get_install_dir(), 'resources', product, 'conf')
            dst_conf = os.path.join(self.get_path(), 'resources', product, 'conf')
            if not os.path.isdir(src_conf):
                continue
            if os.path.isdir(dst_conf):
                common.rmdirs(dst_conf)
            shutil.copytree(src_conf, dst_conf)
            if product == 'solr':
                src_web = os.path.join(self.get_install_dir(), 'resources', product, 'web')
                dst_web = os.path.join(self.get_path(), 'resources', product, 'web')
                if os.path.isdir(dst_web):
                    common.rmdirs(dst_web)
                shutil.copytree(src_web, dst_web)
            if product == 'tomcat':
                src_lib = os.path.join(self.get_install_dir(), 'resources', product, 'lib')
                dst_lib = os.path.join(self.get_path(), 'resources', product, 'lib')
                if os.path.isdir(dst_lib):
                    common.rmdirs(dst_lib)
                if os.path.exists(src_lib):
                    shutil.copytree(src_lib, dst_lib)
                src_webapps = os.path.join(self.get_install_dir(), 'resources', product, 'webapps')
                dst_webapps = os.path.join(self.get_path(), 'resources', product, 'webapps')
                if os.path.isdir(dst_webapps):
                    common.rmdirs(dst_webapps)
                shutil.copytree(src_webapps, dst_webapps)
        src_lib = os.path.join(self.get_install_dir(), 'resources', product, 'gremlin-console', 'conf')
        dst_lib = os.path.join(self.get_path(), 'resources', product, 'gremlin-console', 'conf')
        if os.path.isdir(dst_lib):
            common.rmdirs(dst_lib)
        if os.path.exists(src_lib):
            shutil.copytree(src_lib, dst_lib)

    def import_bin_files(self):
        common.copy_directory(os.path.join(self.get_install_dir(), 'bin'), self.get_bin_dir())
        cassandra_bin_dir = os.path.join(self.get_path(), 'resources', 'cassandra', 'bin')
        shutil.rmtree(cassandra_bin_dir, ignore_errors=True)
        os.makedirs(cassandra_bin_dir)
        common.copy_directory(os.path.join(self.get_install_dir(), 'resources', 'cassandra', 'bin'), cassandra_bin_dir)
        if os.path.exists(os.path.join(self.get_install_dir(), 'resources', 'cassandra', 'tools')):
            cassandra_tools_dir = os.path.join(self.get_path(), 'resources', 'cassandra', 'tools')
            shutil.rmtree(cassandra_tools_dir, ignore_errors=True)
            shutil.copytree(os.path.join(self.get_install_dir(), 'resources', 'cassandra', 'tools'), cassandra_tools_dir)
        self.export_dse_home_in_dse_env_sh()

    def export_dse_home_in_dse_env_sh(self):
        '''
        Due to the way CCM lays out files, separating the repository
        from the node(s) confs, the `dse-env.sh` script of each node
        needs to have its DSE_HOME var set and exported. Since DSE
        4.5.x, the stock `dse-env.sh` file includes a commented-out
        place to do exactly this, intended for installers.
        Basically: read in the file, write it back out and add the two
        lines.
        'sstableloader' is an example of a node script that depends on
        this, when used in a CCM-built cluster.
        '''
        with open(self.get_bin_dir() + "/dse-env.sh", "r") as dse_env_sh:
            buf = dse_env_sh.readlines()

        with open(self.get_bin_dir() + "/dse-env.sh", "w") as out_file:
            for line in buf:
                out_file.write(line)
                if line == "# This is here so the installer can force set DSE_HOME\n":
                    out_file.write("DSE_HOME=" + self.get_install_dir() + "\nexport DSE_HOME\n")

    def _update_log4j(self):
        super(DseNode, self)._update_log4j()

        conf_file = os.path.join(self.get_conf_dir(), common.LOG4J_CONF)
        append_pattern = 'log4j.appender.V.File='
        log_file = os.path.join(self.get_path(), 'logs', 'solrvalidation.log')
        if common.is_win():
            log_file = re.sub("\\\\", "/", log_file)
        common.replace_in_file(conf_file, append_pattern, append_pattern + log_file)

        append_pattern = 'log4j.appender.A.File='
        log_file = os.path.join(self.get_path(), 'logs', 'audit.log')
        if common.is_win():
            log_file = re.sub("\\\\", "/", log_file)
        common.replace_in_file(conf_file, append_pattern, append_pattern + log_file)

        append_pattern = 'log4j.appender.B.File='
        log_file = os.path.join(self.get_path(), 'logs', 'audit', 'dropped-events.log')
        if common.is_win():
            log_file = re.sub("\\\\", "/", log_file)
        common.replace_in_file(conf_file, append_pattern, append_pattern + log_file)

    def _update_yaml(self):
        super(DseNode, self)._update_yaml()
        conf_file = os.path.join(self.get_path(), 'resources', 'dse', 'conf', 'dse.yaml')
        with open(conf_file, 'r') as f:
            data = yaml.safe_load(f)

        data['system_key_directory'] = os.path.join(self.get_path(), 'keys')

        # Get a map of combined cluster and node configuration with the node
        # configuration taking precedence.
        full_options = common.merge_configuration(
            self.cluster._dse_config_options,
            self._dse_config_options, delete_empty=False)

        # Merge options with original yaml data.
        data = common.merge_configuration(data, full_options)

        with open(conf_file, 'w') as f:
            yaml.safe_dump(data, f, default_flow_style=False)

    def __generate_server_xml(self):
        server_xml = os.path.join(self.get_path(), 'resources', 'tomcat', 'conf', 'server.xml')
        if os.path.isfile(server_xml):
            os.remove(server_xml)
        with open(server_xml, 'w+') as f:
            f.write('<Server port="8005" shutdown="SHUTDOWN">\n')
            f.write('  <Service name="Solr">\n')
            f.write('    <Connector port="8983" address="%s" protocol="HTTP/1.1" connectionTimeout="20000" maxThreads = "200" URIEncoding="UTF-8"/>\n' % self.ip_addr)
            f.write('    <Engine name="Solr" defaultHost="localhost">\n')
            f.write('      <Host name="localhost"  appBase="../solr/web"\n')
            f.write('            unpackWARs="true" autoDeploy="true"\n')
            f.write('            xmlValidation="false" xmlNamespaceAware="false">\n')
            f.write('      </Host>\n')
            f.write('    </Engine>\n')
            f.write('  </Service>\n')
            f.write('</Server>\n')
            f.close()

    def __update_gremlin_config_yaml(self):
        conf_file = os.path.join(self.get_path(), 'resources', 'graph', 'gremlin-console', 'conf', 'remote.yaml')
        with open(conf_file, 'r') as f:
            data = yaml.safe_load(f)

        data['hosts'] = [self.ip_addr]

        with open(conf_file, 'w') as f:
            yaml.safe_dump(data, f, default_flow_style=False)

    def _get_directories(self):
        dirs = []
        for i in ['data', 'commitlogs', 'saved_caches', 'logs', 'bin', 'keys', 'resources', os.path.join('data', 'hints')]:
            dirs.append(os.path.join(self.get_path(), i))
        return dirs

    def _copy_agent(self):
        agent_source = os.path.join(self.get_install_dir(), 'datastax-agent')
        agent_target = os.path.join(self.get_path(), 'datastax-agent')
        if os.path.exists(agent_source) and not os.path.exists(agent_target):
            shutil.copytree(agent_source, agent_target)

    def _write_agent_address_yaml(self, agent_dir):
        address_yaml = os.path.join(agent_dir, 'conf', 'address.yaml')
        if not os.path.exists(address_yaml):
            with open(address_yaml, 'w+') as f:
                ip = self.ip_addr
                jmx = self.jmx_port
                f.write('stomp_interface: 127.0.0.1\n')
                f.write('local_interface: %s\n' % ip)
                f.write('agent_rpc_interface: %s\n' % ip)
                f.write('agent_rpc_broadcast_address: %s\n' % ip)
                f.write('cassandra_conf: %s\n' % os.path.join(self.get_path(), 'resources', 'cassandra', 'conf', 'cassandra.yaml'))
                f.write('cassandra_install: %s\n' % self.get_path())
                f.write('cassandra_logs: %s\n' % os.path.join(self.get_path(), 'logs'))
                if 'thrift' in self.network_interfaces: 
                    (_, port) = self.network_interfaces['thrift']
                    f.write('thrift_port: %s\n' % port)

                f.write('jmx_port: %s\n' % jmx)
                f.close()

    def _write_agent_log4j_properties(self, agent_dir):
        log4j_properties = os.path.join(agent_dir, 'conf', 'log4j.properties')
        with open(log4j_properties, 'w+') as f:
            f.write('log4j.rootLogger=INFO,R\n')
            f.write('log4j.logger.org.apache.http=OFF\n')
            f.write('log4j.logger.org.eclipse.jetty.util.log=WARN,R\n')
            f.write('log4j.appender.R=org.apache.log4j.RollingFileAppender\n')
            f.write('log4j.appender.R.maxFileSize=20MB\n')
            f.write('log4j.appender.R.maxBackupIndex=5\n')
            f.write('log4j.appender.R.layout=org.apache.log4j.PatternLayout\n')
            f.write('log4j.appender.R.layout.ConversionPattern=%5p [%t] %d{ISO8601} %m%n\n')
            f.write('log4j.appender.R.File=./log/agent.log\n')
            f.close()

    def _update_spark_env(self):
        try:
            node_num = re.search(u'node(\d+)', self.name).group(1)
        except AttributeError:
            node_num = 0
        conf_file = os.path.join(self.get_path(), 'resources', 'spark', 'conf', 'spark-env.sh')
        env = self.get_env()
        content = []
        with open(conf_file, 'r') as f:
            for line in f.readlines():
                for spark_var in env.keys():
                    if line.startswith('export %s=' % spark_var) or line.startswith('export %s=' % spark_var, 2):
                        line = 'export %s=%s\n' % (spark_var, env[spark_var])
                        break
                content.append(line)

        with open(conf_file, 'w') as f:
            f.writelines(content)

        # set unique spark.shuffle.service.port for each node; this is only needed for DSE 5.0.x;
        # starting in 5.1 this setting is no longer needed
        if self.cluster.version() > '5.0' and self.cluster.version() < '5.1':
            defaults_file = os.path.join(self.get_path(), 'resources', 'spark', 'conf', 'spark-defaults.conf')
            with open(defaults_file, 'a') as f:
                port_num = 7737 + int(node_num)
                f.write("\nspark.shuffle.service.port %s\n" % port_num)

        # create Spark working dirs; starting with DSE 5.0.10/5.1.3 these are no longer automatically created
        for e in ["SPARK_WORKER_DIR", "SPARK_LOCAL_DIRS", "SPARK_EXECUTOR_DIRS"]:
            dir = env[e]
            if not os.path.exists(dir):
                os.makedirs(dir)
