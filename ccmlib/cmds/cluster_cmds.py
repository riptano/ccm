import os, sys, shutil
from command import Cmd

from ccmlib import common, repository
from ccmlib.node import Node, NodeError
from ccmlib.cluster import Cluster

def cluster_cmds():
    return [
        "create",
        "add",
        "populate",
        "list",
        "switch",
        "status",
        "remove",
        "clear",
        "liveset",
        "start",
        "stop",
        "flush",
        "compact",
        "stress",
        "updateconf",
        "cli",
        "setdir",
        "bulkload",
        "setlog",
        "scrub",
    ]

def parse_populate_count(v):
    if v is None:
        return None
    tmp = v.split(':')
    if len(tmp) == 1:
        return int(tmp[0])
    else:
        return [ int(t) for t in tmp ]

class ClusterCreateCmd(Cmd):
    def description(self):
        return "Create a new cluster"

    def get_parser(self):
        usage = "usage: ccm create [options] cluster_name"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('--no-switch', action="store_true", dest="no_switch",
            help="Don't switch to the newly created cluster", default=False)
        parser.add_option('-p', '--partitioner', type="string", dest="partitioner",
            help="Set the cluster partitioner class")
        parser.add_option('-v', "--cassandra-version", type="string", dest="cassandra_version",
            help="Download and use provided cassandra version. If version is of the form 'git:<branch name>', then the specified branch will be downloaded from the git repo and compiled. (takes precedence over --cassandra-dir)", default=None)
        parser.add_option("--cassandra-dir", type="string", dest="cassandra_dir",
            help="Path to the cassandra directory to use [default %default]", default="./")
        parser.add_option('-n', '--nodes', type="string", dest="nodes",
            help="Populate the new cluster with that number of nodes (a single int or a colon-separate list of ints for multi-dc setups)")
        parser.add_option('-i', '--ipprefix', type="string", dest="ipprefix", default="127.0.0.",
            help="Ipprefix to use to create the ip of a node while populating")
        parser.add_option('-s', "--start", action="store_true", dest="start_nodes",
            help="Start nodes added through -s", default=False)
        parser.add_option('-d', "--debug", action="store_true", dest="debug",
            help="If -s is used, show the standard output when starting the nodes", default=False)
        parser.add_option('-b', "--binary-protocol", action="store_true", dest="binary_protocol",
            help="Enable the binary protocol", default=False)
        parser.add_option('-D', "--debug-log", action="store_true", dest="debug_log",
            help="With -n, sets debug logging on the new nodes", default=False)
        parser.add_option('-T', "--trace-log", action="store_true", dest="trace_log",
            help="With -n, sets trace logging on the new nodes", default=False)
        parser.add_option("--vnodes", action="store_true", dest="vnodes",
            help="Use vnodes (256 tokens)", default=False)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, cluster_name=True)
        self.nodes = parse_populate_count(options.nodes)

    def run(self):
        try:
            cluster = Cluster(self.path, self.name, cassandra_dir=self.options.cassandra_dir, cassandra_version=self.options.cassandra_version, verbose=True)
        except OSError as e:
            cluster_dir = os.path.join(self.path, self.name)
            import traceback
            print >> sys.stderr, 'Cannot create cluster: %s\n%s' % (str(e), traceback.format_exc())
            exit(1)

        if self.options.partitioner:
            cluster.set_partitioner(self.options.partitioner)

        if cluster.version() >= "1.2" and self.options.binary_protocol:
            cluster.set_configuration_options({ 'start_native_transport' : True })

        if cluster.version() >= "1.2" and self.options.vnodes:
            cluster.set_configuration_options({ 'num_tokens' : 256 })

        if not self.options.no_switch:
            common.switch_cluster(self.path, self.name)
            print 'Current cluster is now: %s' % self.name

        if self.nodes is not None:
            try:
                if self.options.debug_log:
                    cluster.set_log_level("DEBUG")
                if self.options.trace_log:
                    cluster.set_log_level("TRACE")
                cluster.populate(self.nodes, use_vnodes=self.options.vnodes, ipprefix=self.options.ipprefix)
                if self.options.start_nodes:
                    cluster.start(verbose=self.options.debug, wait_for_binary_proto=self.options.binary_protocol)
            except common.ArgumentError as e:
                print >> sys.stderr, str(e)
                exit(1)

class ClusterAddCmd(Cmd):
    def description(self):
        return "Add a new node to the current cluster"

    def get_parser(self):
        usage = "usage: ccm add [options] node_name"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('-b', '--auto-boostrap', action="store_true", dest="boostrap",
            help="Set auto bootstrap for the node", default=False)
        parser.add_option('-s', '--seeds', action="store_true", dest="is_seed",
            help="Configure this node as a seed", default=False)
        parser.add_option('-i', '--itf', type="string", dest="itfs",
            help="Set host and port for thrift, the binary protocol and storage (format: host[:port])")
        parser.add_option('-t', '--thrift-itf', type="string", dest="thrift_itf",
            help="Set the thrift host and port for the node (format: host[:port])")
        parser.add_option('-l', '--storage-itf', type="string", dest="storage_itf",
            help="Set the storage (cassandra internal) host and port for the node (format: host[:port])")
        parser.add_option('--binary-itf', type="string", dest="binary_itf",
            help="Set the binary protocol host and port for the node (format: host[:port]).")
        parser.add_option('-j', '--jmx-port', type="string", dest="jmx_port",
            help="JMX port for the node", default="7199")
        parser.add_option('-r', '--remote-debug-port', type="string", dest="remote_debug_port",
            help="Remote Debugging Port for the node", default="2000")
        parser.add_option('-n', '--token', type="string", dest="initial_token",
            help="Initial token for the node", default=None)
        parser.add_option('-d', '--data-center', type="string", dest="data_center",
            help="Datacenter name this node is part of", default=None)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True, load_node=False)

        if options.itfs is None and (options.thrift_itf is None or options.storage_itf is None or options.binary_itf is None):
            print >> sys.stderr, 'Missing thrift and/or storage and/or binary protocol interfaces or jmx port'
            parser.print_help()
            exit(1)

        if options.thrift_itf is None:
            options.thrift_itf = options.itfs
        if options.storage_itf is None:
            options.storage_itf = options.itfs
        if options.binary_itf is None:
            options.binary_itf = options.itfs

        self.thrift = common.parse_interface(options.thrift_itf, 9160)
        self.storage = common.parse_interface(options.storage_itf, 7000)
        self.binary = common.parse_interface(options.binary_itf, 9042)

        if self.binary[0] != self.thrift[0]:
            print >> sys.stderr, 'Cannot set a binary address different from the thrift one'
            exit(1)


        self.jmx_port = options.jmx_port
        self.remote_debug_port = options.remote_debug_port
        self.initial_token = options.initial_token


    def run(self):
        try:
            node = Node(self.name, self.cluster, self.options.boostrap, self.thrift, self.storage, self.jmx_port, self.remote_debug_port, self.initial_token, binary_interface=self.binary)
            self.cluster.add(node, self.options.is_seed, self.options.data_center)
        except common.ArgumentError as e:
            print >> sys.stderr, str(e)
            exit(1)

class ClusterPopulateCmd(Cmd):
    def description(self):
        return "Add a group of new nodes with default options"

    def get_parser(self):
        usage = "usage: ccm populate -n <node count> {-d}"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('-n', '--nodes', type="string", dest="nodes",
            help="Number of nodes to populate with (a single int or a colon-separate list of ints for multi-dc setups)")
        parser.add_option('-d', '--debug', action="store_true", dest="debug",
            help="Enable remote debugging options", default=False)
        parser.add_option('--vnodes', action="store_true", dest="vnodes",
            help="Populate using vnodes", default=False)
        parser.add_option('-i', '--ipprefix', type="string", dest="ipprefix", default="127.0.0.",
            help="Ipprefix to use to create the ip of a node")
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.nodes = parse_populate_count(options.nodes)

    def run(self):
        try:
            self.cluster.populate(self.nodes, self.options.debug, use_vnodes=self.options.vnodes, ipprefix=self.options.ipprefix)
        except common.ArgumentError as e:
            print >> sys.stderr, str(e)
            exit(1)

class ClusterListCmd(Cmd):
    def description(self):
        return "List existing clusters"

    def get_parser(self):
        usage = "usage: ccm list [options]"
        return self._get_default_parser(usage, self.description())

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args)

    def run(self):
        try:
            current = common.current_cluster_name(self.path)
        except Exception as e:
            current = ''

        for dir in os.listdir(self.path):
            if os.path.exists(os.path.join(self.path, dir, 'cluster.conf')):
                print " %s%s" % ('*' if current == dir else ' ', dir)

class ClusterSwitchCmd(Cmd):
    def description(self):
        return "Switch of current (active) cluster"

    def get_parser(self):
        usage = "usage: ccm switch [options] cluster_name"
        return self._get_default_parser(usage, self.description())

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, cluster_name=True)
        if not os.path.exists(os.path.join(self.path, self.name, 'cluster.conf')):
            print >> sys.stderr, "%s does not appear to be a valid cluster (use ccm cluster list to view valid cluster)" % self.name
            exit(1)

    def run(self):
        common.switch_cluster(self.path, self.name)

class ClusterStatusCmd(Cmd):
    def description(self):
        return "Display status on the current cluster"

    def get_parser(self):
        usage = "usage: ccm status [options]"
        parser =  self._get_default_parser(usage, self.description())
        parser.add_option('-v', '--verbose', action="store_true", dest="verbose",
                help="Print full information on all nodes", default=False)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        self.cluster.show(self.options.verbose)

class ClusterRemoveCmd(Cmd):
    def description(self):
        return "Remove the current cluster (delete all data)"

    def get_parser(self):
        usage = "usage: ccm remove [options]"
        parser =  self._get_default_parser(usage, self.description())
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        self.cluster.remove()
        os.remove(os.path.join(self.path, 'CURRENT'))

class ClusterClearCmd(Cmd):
    def description(self):
        return "Clear the current cluster data (and stop all nodes)"

    def get_parser(self):
        usage = "usage: ccm clear [options]"
        parser =  self._get_default_parser(usage, self.description())
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        self.cluster.clear()

class ClusterLivesetCmd(Cmd):
    def description(self):
        return "Print a comma-separated list of addresses of running nodes (handful in scripts)"

    def get_parser(self):
        usage = "usage: ccm liveset [options]"
        parser =  self._get_default_parser(usage, self.description())
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        l = [ node.network_interfaces['storage'][0] for node in self.cluster.nodes.values() if node.is_live() ]
        print ",".join(l)

class ClusterSetdirCmd(Cmd):
    def description(self):
        return "Set the cassandra directory to use"

    def get_parser(self):
        usage = "usage: ccm setdir [options]"
        parser =  self._get_default_parser(usage, self.description())
        parser.add_option('-v', "--cassandra-version", type="string", dest="cassandra_version",
            help="Download and use provided cassandra version. If version is of the form 'git:<branch name>', then the specified branch will be downloaded from the git repo and compiled. (takes precedence over --cassandra-dir)", default=None)
        parser.add_option("--cassandra-dir", type="string", dest="cassandra_dir",
            help="Path to the cassandra directory to use [default %default]", default="./")
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        try:
            self.cluster.set_cassandra_dir(cassandra_dir=self.options.cassandra_dir, cassandra_version=self.options.cassandra_version, verbose=True)
        except common.ArgumentError as e:
            print >> sys.stderr, str(e)
            exit(1)

class ClusterClearrepoCmd(Cmd):
    def description(self):
        return "Cleanup downloaded cassandra sources"

    def get_parser(self):
        usage = "usage: ccm clearrepo [options]"
        parser =  self._get_default_parser(usage, self.description())
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args)

    def run(self):
        repository.clean_all()

class ClusterStartCmd(Cmd):
    def description(self):
        return "Start all the non started nodes of the current cluster"

    def get_parser(self):
        usage = "usage: ccm cluter start [options]"
        parser =  self._get_default_parser(usage, self.description())
        parser.add_option('-v', '--verbose', action="store_true", dest="verbose",
            help="Print standard output of cassandra process", default=False)
        parser.add_option('--no-wait', action="store_true", dest="no_wait",
            help="Do not wait for cassandra node to be ready", default=False)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        try:
            self.cluster.start(no_wait=self.options.no_wait,
                               verbose=self.options.verbose)
        except NodeError as e:
            print >> sys.stderr, str(e)
            print >> sys.stderr, "Standard error output is:"
            for line in e.process.stderr:
                print >> sys.stderr, line.rstrip('\n')
            exit(1)

class ClusterStopCmd(Cmd):
    def description(self):
        return "Stop all the nodes of the cluster"

    def get_parser(self):
        usage = "usage: ccm cluster stop [options] name"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('-v', '--verbose', action="store_true", dest="verbose",
                help="Print nodes that were not running", default=False)
        parser.add_option('--no-wait', action="store_true", dest="no_wait",
            help="Do not wait for the node to be stopped", default=False)
        parser.add_option('-g', '--gently', action="store_true", dest="gently",
            help="Shut down gently (default)", default=True)
        parser.add_option('--not-gently', action="store_false", dest="gently",
            help="Shut down immediately (kill -9)", default=True)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        try:
            not_running = self.cluster.stop(not self.options.no_wait, gently=self.options.gently)
            if self.options.verbose and len(not_running) > 0:
                sys.out.write("The following nodes were not running: ")
                for node in not_running:
                    sys.out.write(node.name + " ")
                print ""
        except NodeError as e:
            print >> sys.stderr, str(e)
            exit(1)

class _ClusterNodetoolCmd(Cmd):
    def get_parser(self):
        parser = self._get_default_parser(self.usage, self.description())
        return parser

    def description(self):
        return self.descr_text

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        self.cluster.nodetool(self.nodetool_cmd)

class ClusterFlushCmd(_ClusterNodetoolCmd):
    usage = "usage: ccm cluster flush [options] name"
    nodetool_cmd = 'flush'
    descr_text = "Flush all (running) nodes of the cluster"

class ClusterCompactCmd(_ClusterNodetoolCmd):
    usage = "usage: ccm cluster compact [options] name"
    nodetool_cmd = 'compact'
    descr_text = "Compact all (running) node of the cluster"

class ClusterDrainCmd(_ClusterNodetoolCmd):
    usage = "usage: ccm cluster drain [options] name"
    nodetool_cmd = 'drain'
    descr_text = "Drain all (running) node of the cluster"

class ClusterStressCmd(Cmd):
    def description(self):
        return "Run stress using all live nodes"

    def get_parser(self):
        usage = "usage: ccm stress [options] [stress_options]"
        parser = self._get_default_parser(usage, self.description(), ignore_unknown_options=True)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.stress_options = parser.get_ignored() + args

    def run(self):
        try:
            self.cluster.stress(self.stress_options)
        except Exception as e:
            print >> sys.stderr, e

class ClusterUpdateconfCmd(Cmd):
    def description(self):
        return "Update the cassandra config files for all nodes"

    def get_parser(self):
        usage = "usage: ccm updateconf [options] [ new_setting | ...  ], where new_setting should be a string of the form 'compaction_throughput_mb_per_sec: 32'"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('--no-hh', '--no-hinted-handoff', action="store_false",
            dest="hinted_handoff", default=True, help="Disable hinted handoff")
        parser.add_option('--batch-cl', '--batch-commit-log', action="store_true",
            dest="cl_batch", default=False, help="Set commit log to batch mode")
        parser.add_option('--rt', '--rpc-timeout', action="store", type='int',
            dest="rpc_timeout", help="Set rpc timeout")
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        try:
            self.setting = common.parse_settings(args)
        except common.ArgumentError as e:
            print >> sys.stderr, str(e)
            exit(1)

    def run(self):
        self.setting['hinted_handoff_enabled'] = self.options.hinted_handoff
        if self.options.rpc_timeout is not None:
            if self.cluster.version() < "1.2":
                self.setting['rpc_timeout_in_ms'] = self.options.rpc_timeout
            else:
                self.setting['read_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['range_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['write_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['truncate_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['request_timeout_in_ms'] = self.options.rpc_timeout

        self.cluster.set_configuration_options(values=self.setting, batch_commitlog=self.options.cl_batch)

class ClusterCliCmd(Cmd):
    def description(self):
        return "Launch cassandra cli connected to some live node (if any)"

    def get_parser(self):
        usage = "usage: ccm cli [options] [cli_options]"
        parser = self._get_default_parser(usage, self.description(), ignore_unknown_options=True)
        parser.add_option('-x', '--exec', type="string", dest="cmds", default=None,
            help="Execute the specified commands and exit")
        parser.add_option('-v', '--verbose', action="store_true", dest="verbose",
            help="With --exec, show cli output after completion", default=False)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.cli_options = parser.get_ignored() + args[1:]

    def run(self):
        self.cluster.run_cli(self.options.cmds, self.options.verbose, self.cli_options)

class ClusterBulkloadCmd(Cmd):
    def description(self):
        return "Bulkload files into the cluster"

    def get_parser(self):
        usage = "usage: ccm bulkload [options] [sstable_dir]"
        parser = self._get_default_parser(usage, self.description(), ignore_unknown_options=True)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.loader_options = parser.get_ignored() + args

    def run(self):
        self.cluster.bulkload(self.loader_options)

class ClusterScrubCmd(Cmd):
    def description(self):
        return "Scrub files"

    def get_parser(self):
        usage = "usage: ccm scrub [options] <keyspace> <cf>"
        parser = self._get_default_parser(usage, self.description(), ignore_unknown_options=True)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.scrub_options = parser.get_ignored() + args

    def run(self):
        self.cluster.scrub(self.scrub_options)

class ClusterSetlogCmd(Cmd):
    def description(self):
        return "Set log level (INFO, DEBUG, ...) for all node of the cluster - require a node restart"

    def get_parser(self):
        usage = "usage: ccm setlog [options] level"
        return self._get_default_parser(usage, self.description())

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        if len(args) == 0:
            print >> sys.stderr, 'Missing log level'
            parser.print_help()
        self.level = args[0]

    def run(self):
        try:
            self.cluster.set_log_level(self.level)
        except common.ArgumentError as e:
            print >> sys.stderr, str(e)
            exit(1)
