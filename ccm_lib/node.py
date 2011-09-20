# ccm node

import common, yaml, os, errno, signal, time, subprocess, shutil, sys, glob

class Status():
    UNINITIALIZED = "UNINITIALIZED"
    UP = "UP"
    DOWN = "DOWN"
    DECOMMISIONNED = "DECOMMISIONNED"

class StartError(Exception):
    def __init__(self, msg, process=None):
        self.msg = msg
        self.process = process

    def __repr__(self):
        return self.msg

class Node():
    def __init__(self, name, cluster, auto_bootstrap, thrift_interface, storage_interface, jmx_port, initial_token):
        self.name = name
        self.cluster = cluster
        self.status = Status.UNINITIALIZED
        self.auto_bootstrap = auto_bootstrap
        self.network_interfaces = { 'thrift' : thrift_interface, 'storage' : storage_interface }
        self.jmx_port = jmx_port
        self.initial_token = initial_token
        self.pid = None
        self.config_options = {}
        self.save()

    def save(self):
        dir_name = self.get_path()
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)
            for dir in self.get_directories():
                os.mkdir(os.path.join(dir_name, dir))

        filename = os.path.join(dir_name, 'node.conf')
        values = {
            'name' : self.name,
            'status' : self.status,
            'auto_bootstrap' : self.auto_bootstrap,
            'interfaces' : self.network_interfaces,
            'jmx_port' : self.jmx_port
        }
        if self.pid:
            values['pid'] = self.pid
        if self.initial_token:
            values['initial_token'] = self.initial_token
        with open(filename, 'w') as f:
            yaml.dump(values, f)

    @staticmethod
    def load(path, name, cluster):
        node_path = os.path.join(path, name)
        filename = os.path.join(node_path, 'node.conf')
        with open(filename, 'r') as f:
            data = yaml.load(f)
        try:
            itf = data['interfaces'];
            initial_token = None
            if 'initial_token' in data:
                initial_token = data['initial_token']
            node = Node(data['name'], cluster, data['auto_bootstrap'], itf['thrift'], itf['storage'], data['jmx_port'], initial_token)
            node.status = data['status']
            if 'pid' in data:
                node.pid = int(data['pid'])
            return node
        except KeyError as k:
            raise common.LoadError("Error Loading " + filename + ", missing property: " + str(k))

    def get_directories(self):
        dirs = {}
        for i in ['data', 'commitlogs', 'saved_caches', 'logs', 'conf', 'bin']:
            dirs[i] = os.path.join(self.cluster.get_path(), self.name, i)
        return dirs

    def get_path(self):
        return os.path.join(self.cluster.get_path(), self.name)

    def get_conf_dir(self):
        return os.path.join(self.get_path(), 'conf')

    def update_configuration(self, cassandra_dir, hh=True, cl_batch=False, rpc_timeout=None):
        self.set_configuration_option("hinted_handoff_enabled", hh, update_yaml=False)
        if cl_batch:
            self.set_configuration_option("commitlog_sync", "batch", update_yaml=False)
            self.set_configuration_option("commitlog_sync_batch_window_in_ms", 5, update_yaml=False)
            self.unset_configuration_option("commitlog_sync_period_in_ms", update_yaml=False)
        if rpc_timeout is not None:
            self.set_configuration_option("rpc_timeout_in_ms", self.options.rpc_timeout, update_yaml=False)

        conf_dir = os.path.join(cassandra_dir, 'conf')
        for name in os.listdir(conf_dir):
            filename = os.path.join(conf_dir, name)
            if os.path.isfile(filename):
                shutil.copy(filename, self.get_conf_dir())

        self.update_yaml()
        self.update_log4j()
        self.update_envfile()

    def update_yaml(self):
        conf_file = os.path.join(self.get_conf_dir(), common.CASSANDRA_CONF)
        with open(conf_file, 'r') as f:
            data = yaml.load(f)

        data['auto_bootstrap'] = self.auto_bootstrap
        data['initial_token'] = self.initial_token
        if 'seeds' in data:
            # cassandra 0.7
            data['seeds'] = self.cluster.get_seeds()
        else:
            # cassandra 0.8
            data['seed_provider'][0]['parameters'][0]['seeds'] = ','.join(self.cluster.get_seeds())
        data['listen_address'], data['storage_port'] = self.network_interfaces['storage']
        data['rpc_address'], data['rpc_port'] = self.network_interfaces['thrift']

        data['data_file_directories'] = [ os.path.join(self.get_path(), 'data') ]
        data['commitlog_directory'] = os.path.join(self.get_path(), 'commitlogs')
        data['saved_caches_directory'] = os.path.join(self.get_path(), 'saved_caches')

        if self.cluster.partitioner:
            data['partitioner'] = self.cluster.partitioner

        for name in self.config_options:
            value = self.config_options[name]
            if value is None:
                del data[name]
            else:
                data[name] = self.config_options[name]

        with open(conf_file, 'w') as f:
            yaml.dump(data, f, default_flow_style=False)

    def set_configuration_option(self, name, value, update_yaml=True):
        self.config_options[name] = value
        if update_yaml:
            self.update_yaml()

    def unset_configuration_option(self, name, update_yaml=True):
        self.config_options[name] = None
        if update_yaml:
            self.update_yaml()

    def update_log4j(self):
        append_pattern='log4j.appender.R.File=';
        conf_file = os.path.join(self.get_conf_dir(), common.LOG4J_CONF)
        log_file = os.path.join(self.get_path(), 'logs', 'system.log')
        common.replace_in_file(conf_file, append_pattern, append_pattern + log_file)

    def update_envfile(self):
        jmx_port_pattern='JMX_PORT=';
        conf_file = os.path.join(self.get_conf_dir(), common.CASSANDRA_ENV)
        common.replace_in_file(conf_file, jmx_port_pattern, jmx_port_pattern + self.jmx_port)

    def get_status_string(self):
        if self.status == Status.UNINITIALIZED:
            return "%s (%s)" % (Status.DOWN, "Not initialized")
        else:
            return self.status

    def show(self, only_status=False, show_cluster=True):
        self.update_status()
        indent = ''.join([ " " for i in xrange(0, len(self.name) + 2) ])
        print "%s: %s" % (self.name, self.get_status_string())
        if not only_status:
          if show_cluster:
              print "%s%s=%s" % (indent, 'cluster', self.cluster.name)
          print "%s%s=%s" % (indent, 'auto_bootstrap', self.auto_bootstrap)
          print "%s%s=%s" % (indent, 'thrift', self.network_interfaces['thrift'])
          print "%s%s=%s" % (indent, 'storage', self.network_interfaces['storage'])
          print "%s%s=%s" % (indent, 'jmx_port', self.jmx_port)
          print "%s%s=%s" % (indent, 'initial_token', self.initial_token)
          if self.pid:
              print "%s%s=%s" % (indent, 'pid', self.pid)

    def is_running(self):
        self.update_status()
        return self.status == Status.UP or self.status == Status.DECOMMISIONNED

    def is_live(self):
        self.update_status()
        return self.status == Status.UP

    def update_status(self):
        if self.pid is None:
            if self.status == Status.UP or self.status == Status.DECOMMISIONNED:
                self.status = Status.DOWN
            return

        old_status = self.status
        try:
            os.kill(self.pid, 0)
        except OSError, err:
            if err.errno == errno.ESRCH:
                # not running
                if self.status == Status.UP or self.status == Status.DECOMMISIONNED:
                    self.status = Status.DOWN
            elif err.errno == errno.EPERM:
                # no permission to signal this process
                if self.status == Status.UP or self.status == Status.DECOMMISIONNED:
                    self.status = Status.DOWN
            else:
                # some other error
                raise err
        else:
            if self.status == Status.DOWN or self.status == Status.UNINITIALIZED:
                self.status = Status.UP
        if not old_status == self.status:
            self.save()

    def start(self, cassandra_dir, join_ring=True, no_wait=False, verbose=False):
        if self.is_running():
            raise StartError("%s is already running" % self.name)

        cass_bin = os.path.join(cassandra_dir, 'bin', 'cassandra')
        env = common.make_cassandra_env(cassandra_dir, self.get_path())
        pidfile = os.path.join(self.get_path(), 'cassandra.pid')
        args = [ cass_bin, '-p', pidfile, '-Dcassandra.join_ring=%s' % str(join_ring) ]
        process = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if no_wait:
            time.sleep(2) # waiting 2 seconds nevertheless to check for early errors and for the pid to be set
        else:
            for line in process.stdout:
                if verbose:
                    print line.rstrip('\n')

        self.update_pid(process)

        if not self.is_running():
            raise StartError("Error starting node %s" % self.name, process)

        return process

    def update_pid(self, process):
        pidfile = os.path.join(self.get_path(), 'cassandra.pid')
        try:
            with open(pidfile, 'r') as f:
                self.pid = int(f.readline().strip())
        except IOError:
            raise StartError('Problem starting node %s' % self.name, process)
        self.update_status()

    def stop(self):
        is_running = False
        if self.is_running():
            is_running = True
            os.kill(self.pid, signal.SIGKILL)
        self.pid = None
        self.save()
        return is_running

    def nodetool(self, cassandra_dir, cmd):
        nodetool = os.path.join(cassandra_dir, 'bin', 'nodetool')
        env = common.make_cassandra_env(cassandra_dir, self.get_path())
        host = self.network_interfaces['storage'][0]
        args = [ nodetool, '-h', host, '-p', str(self.jmx_port), cmd ]
        p = subprocess.Popen(args, env=env)
        p.wait()

    def run_cli(self, cassandra_dir, cmds=None, show_output=False, cli_options=[]):
        cli = os.path.join(cassandra_dir, 'bin', 'cassandra-cli')
        env = common.make_cassandra_env(cassandra_dir, self.get_path())
        host = self.network_interfaces['thrift'][0]
        port = self.network_interfaces['thrift'][1]
        args = [ '-h', host, '-p', str(port) , '--jmxport', str(self.jmx_port) ] + cli_options
        sys.stdout.flush()
        if cmds is None:
            os.execve(cli, [ 'cassandra-cli' ] + args, env)
        else:
            p = subprocess.Popen([ cli ] + args, env=env, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            for cmd in cmds.split(';'):
                p.stdin.write(cmd + ';\n')
            p.stdin.write("quit;\n")
            p.wait()
            for err in p.stderr:
                print "(EE) " + err,
            if show_output:
                i = 0
                for log in p.stdout:
                    # first four lines are not intersting
                    if i >= 4:
                        print log,
                    i = i + 1

    def set_log_level(self, new_level):
        known_level = [ 'TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR' ]
        if new_level not in know_level:
            raise common.ArgumentError("Unknown log level %s (use one of %s)" % (self.level, " ".join(known_level)))

        append_pattern='log4j.rootLogger=';
        conf_file = os.path.join(self.get_conf_dir(), common.LOG4J_CONF)
        l = new_level + ",stdout,R"
        common.replace_in_file(conf_file, append_pattern, append_pattern + l)

    def clear(self, clear_all = False):
        data_dirs = [ 'data', 'commitlogs']
        if clear_all:
            data_dirs = data_dirs + [ 'saved_caches', 'logs']
        for d in data_dirs:
            full_dir = os.path.join(self.get_path(), d)
            shutil.rmtree(full_dir)
            os.mkdir(full_dir)

    def decommission(self):
        self.status = Status.DECOMMISIONNED
        self.save()

    def run_sstable2json(self, cassandra_dir, keyspace, datafile, column_families, enumerate_keys=False):
        sstable2json = os.path.join(cassandra_dir, 'bin', 'sstable2json')
        env = common.make_cassandra_env(cassandra_dir, self.get_path())
        datafiles = []
        if not keyspace:
            for k in self.list_keyspaces():
                datafiles = datafiles + self.get_sstables(k, "")
        elif not datafile:
            if not column_families:
                datafiles = datafiles + self.get_sstables(keyspace, "")
            else:
                for cf in column_families:
                    datafiles = datafiles + self.get_sstables(keyspace, cf)
        else:
            keyspace_dir = os.path.join(self.get_path(), 'data', keyspace)
            datafiles = [ os.path.join(keyspace_dir, datafile) ]
        for file in datafiles:
            print "-- {0} -----".format(os.path.basename(file))
            args = [ sstable2json , file ]
            if enumerate_keys:
                args = args + ["-e"];
            subprocess.call(args, env=env)
            print ""

    def list_keyspaces(self):
        keyspaces = os.listdir(os.path.join(self.get_path(), 'data'))
        keyspaces.remove('system')
        return keyspaces

    def get_sstables(self, keyspace, column_family):
        keyspace_dir = os.path.join(self.get_path(), 'data', keyspace)
        if not os.path.exists(keyspace_dir):
            raise common.ArgumentError("Unknown keyspace {0}".format(keyspace))

        files = glob.glob(os.path.join(keyspace_dir, "{0}*-Data.db".format(column_family)))
        for f in files:
            if os.path.exists(f.replace('Data.db', 'Compacted')):
                files.remove(f)
        return files

    def stress(self, cassandra_dir, stress_options):
        stress = common.get_stress_bin(cassandra_dir)
        args = [ stress ] + stress_options
        try:
            subprocess.call(args)
        except KeyboardInterrupt:
            pass

