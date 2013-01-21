import os, sys
from command import Cmd

from ccmlib import common
from ccmlib.node import NodeError

def node_cmds():
    return [
        "show",
        "remove",
        "showlog",
        "setlog",
        "start",
        "stop",
        "ring",
        "flush",
        "compact",
        "drain",
        "cleanup",
        "repair",
        "scrub",
        "decommission",
        "json",
        "updateconf",
        "stress",
        "cli",
        "cqlsh",
        "scrub",
        "status",
        "setdir",
    ]

class NodeShowCmd(Cmd):
    def description(self):
        return "Display information on a node"

    def get_parser(self):
        usage = "usage: ccm node_name show [options]"
        return self._get_default_parser(usage, self.description())

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        self.node.show()

class NodeRemoveCmd(Cmd):
    def description(self):
        return "Remove a node (stopping it if necessary and deleting all its data)"

    def get_parser(self):
        usage = "usage: ccm node_name remove [options]"
        return self._get_default_parser(usage, self.description())

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        self.cluster.remove(self.node)

class NodeShowlogCmd(Cmd):
    def description(self):
        return "Show the log of node name (runs your $PAGER on its system.log)"

    def get_parser(self):
        usage = "usage: ccm node_name showlog [options]"
        return self._get_default_parser(usage, self.description())

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        log = self.node.logfilename()
        pager = os.environ.get('PAGER', 'less')
        os.execvp(pager, (pager, log))

class NodeSetlogCmd(Cmd):
    def description(self):
        return "Set node name log level (INFO, DEBUG, ...) - require a node restart"

    def get_parser(self):
        usage = "usage: ccm node_name setlog [options] level"
        return self._get_default_parser(usage, self.description())

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        if len(args) == 1:
            print >> sys.stderr, 'Missing log level'
            parser.print_help()
        self.level = args[1]

    def run(self):
        try:
            self.node.set_log_level(self.level)
        except common.ArgumentError as e:
            print >> sys.stderr, str(e)
            exit(1)

class NodeClearCmd(Cmd):
    def description(self):
        return "Clear the node data & logs (and stop the node)"

    def get_parser(self):
        usage = "usage: ccm node_name_clear [options]"
        parser =  self._get_default_parser(usage, self.description())
        parser.add_option('-a', '--all', action="store_true", dest="all",
                help="Also clear the saved cache and node log files", default=False)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        self.node.stop()
        self.node.clear(self.options.all)

class NodeStartCmd(Cmd):
    def description(self):
        return "Start a node"

    def get_parser(self):
        usage = "usage: ccm node start [options] name"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('-v', '--verbose', action="store_true", dest="verbose",
            help="Print standard output of cassandra process", default=False)
        parser.add_option('--no-wait', action="store_true", dest="no_wait",
            help="Do not wait for cassandra node to be ready", default=False)
        parser.add_option('-j', '--dont-join-ring', action="store_true", dest="no_join_ring",
            help="Launch the instance without joining the ring", default=False)
        parser.add_option('--jvm_arg', action="append", dest="jvm_args",
            help="Specify a JVM argument", default=[])
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        try:
            self.node.start(not self.options.no_join_ring,
                            no_wait=self.options.no_wait,
                            verbose=self.options.verbose,
                            jvm_args=self.options.jvm_args)
        except NodeError as e:
            print >> sys.stderr, str(e)
            print >> sys.stderr, "Standard error output is:"
            for line in e.process.stderr:
                print >> sys.stderr, line.rstrip('\n')
            exit(1)

class NodeStopCmd(Cmd):
    def description(self):
        return "Stop a node"

    def get_parser(self):
        usage = "usage: ccm node stop [options] name"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('--no-wait', action="store_true", dest="no_wait",
            help="Do not wait for the node to be stopped", default=False)
        parser.add_option('-g', '--gently', action="store_true", dest="gently",
            help="Shut down gently (default)", default=True)
        parser.add_option('--not-gently', action="store_false", dest="gently",
            help="Shut down immediately (kill -9)", default=True)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        try:
            if not self.node.stop(not self.options.no_wait, gently=self.options.gently):
                print >> sys.stderr, "%s is not running" % self.name
                exit(1)
        except NodeError as e:
            print >> sys.stderr, str(e)
            exit(1)

class _NodeToolCmd(Cmd):
    def get_parser(self):
        parser = self._get_default_parser(self.usage, self.description())
        return parser

    def description(self):
        return self.descr_text

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        self.node.nodetool(self.nodetool_cmd)

class NodeRingCmd(_NodeToolCmd):
    usage = "usage: ccm node_name ring [options]"
    nodetool_cmd = 'ring'
    descr_text = "Print ring (connecting to node name)"

class NodeStatusCmd(_NodeToolCmd):
    usage = "usage: ccm node_name status [options]"
    nodetool_cmd = 'status'
    descr_text = "Print status (connecting to node name)"

class NodeFlushCmd(_NodeToolCmd):
    usage = "usage: ccm node_name flush [options]"
    nodetool_cmd = 'flush'
    descr_text = "Flush node name"

class NodeCompactCmd(_NodeToolCmd):
    usage = "usage: ccm node_name compact [options]"
    nodetool_cmd = 'compact'
    descr_text = "Compact node name"

class NodeDrainCmd(_NodeToolCmd):
    usage = "usage: ccm node_name drain [options]"
    nodetool_cmd = 'drain'
    descr_text = "Drain node name"

class NodeCleanupCmd(_NodeToolCmd):
    usage = "usage: ccm node_name cleanup [options]"
    nodetool_cmd = 'cleanup'
    descr_text = "Run cleanup on node name"

class NodeRepairCmd(_NodeToolCmd):
    usage = "usage: ccm node_name repair [options]"
    nodetool_cmd = 'repair'
    descr_text = "Run repair on node name"

class NodeDecommissionCmd(_NodeToolCmd):
    usage = "usage: ccm node_name decommission [options]"
    nodetool_cmd = 'decommission'
    descr_text = "Run decommission on node name"

    def run(self):
        _NodeToolCmd.run(self)
        self.node.decommission()

class NodeScrubCmd(_NodeToolCmd):
    usage = "usage: ccm node_name scrub [options]"
    nodetool_cmd = 'scrub'
    descr_text = "Run scrub on node name"

class NodeCliCmd(Cmd):
    def description(self):
        return "Launch a cassandra cli connected to this node"

    def get_parser(self):
        usage = "usage: ccm node_name cli [options] [cli_options]"
        parser = self._get_default_parser(usage, self.description(), ignore_unknown_options=True)
        parser.add_option('-x', '--exec', type="string", dest="cmds", default=None,
            help="Execute the specified commands and exit")
        parser.add_option('-v', '--verbose', action="store_true", dest="verbose",
            help="With --exec, show cli output after completion", default=False)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.cli_options = parser.get_ignored() + args[1:]

    def run(self):
        self.node.run_cli(self.options.cmds, self.options.verbose, self.cli_options)

class NodeCqlshCmd(Cmd):
    def description(self):
        return "Launch a cqlsh session connected to this node"

    def get_parser(self):
        usage = "usage: ccm node_name cqlsh [options] [cli_options]"
        parser = self._get_default_parser(usage, self.description(), ignore_unknown_options=True)
        parser.add_option('-x', '--exec', type="string", dest="cmds", default=None,
            help="Execute the specified commands and exit")
        parser.add_option('-v', '--verbose', action="store_true", dest="verbose",
            help="With --exec, show cli output after completion", default=False)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.cqlsh_options = parser.get_ignored() + args[1:]

    def run(self):
        self.node.run_cqlsh(self.options.cmds, self.options.verbose, self.cqlsh_options)

class NodeScrubCmd(Cmd):
    def description(self):
        return "Scrub files"

    def get_parser(self):
        usage = "usage: ccm node_name scrub [options] <keyspace> <cf>"
        parser = self._get_default_parser(usage, self.description(), ignore_unknown_options=True)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.scrub_options = parser.get_ignored() + args[1:]

    def run(self):
        self.node.scrub(self.scrub_options)

class NodeJsonCmd(Cmd):
    def description(self):
        return "Call sstable2json on the sstables of this node"

    def get_parser(self):
        usage = "usage: ccm node_name json [options] [file]"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('-k', '--keyspace', type="string", dest="keyspace", default=None,
            help="The keyspace to use [use all keyspaces by default]")
        parser.add_option('-c', '--column-families', type="string", dest="cfs", default=None,
            help="Comma separated list of column families to use (requires -k to be set)")
        parser.add_option('-e', '--enumerate-keys', action="store_true", dest="enumerate_keys",
            help="Only enumerate keys (i.e, call sstable2keys)", default=False)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.keyspace = options.keyspace
        self.column_families = None
        self.datafile = None
        if len(args) > 1:
            self.datafile = args[1]
            if self.keyspace is None:
                print >> sys.stderr, "You need a keyspace specified (option -k) if you specify a file"
                exit(1)
        elif options.cfs is not None:
            if self.keyspace is None:
                print >> sys.stderr, "You need a keyspace specified (option -k) if you specify column families"
                exit(1)
            self.column_families = options.cfs.split(',')

    def run(self):
        try:
            self.node.run_sstable2json(self.keyspace, self.datafile, self.column_families, self.options.enumerate_keys)
        except common.ArgumentError as e:
            print >> sys.stderr, e

class NodeUpdateconfCmd(Cmd):
    def description(self):
        return "Update the cassandra config files for this node (useful when updating cassandra)"

    def get_parser(self):
        usage = "usage: ccm node_name updateconf [options] [ new_setting | ...  ], where new_setting should be a string of the form 'compaction_throughput_mb_per_sec: 32'"
        parser = self._get_default_parser(usage, self.description())
        parser.add_option('--no-hh', '--no-hinted-handoff', action="store_false",
            dest="hinted_handoff", default=True, help="Disable hinted handoff")
        parser.add_option('--batch-cl', '--batch-commit-log', action="store_true",
            dest="cl_batch", default=False, help="Set commit log to batch mode")
        parser.add_option('--rt', '--rpc-timeout', action="store", type='int',
            dest="rpc_timeout", help="Set rpc timeout")
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        args = args[1:]
        try:
            self.setting = common.parse_settings(args)
        except common.ArgumentError as e:
            print >> sys.stderr, str(e)
            exit(1)

    def run(self):
        self.setting['hinted_handoff_enabled'] = self.options.hinted_handoff
        if self.options.rpc_timeout is not None:
            if self.node.cluster.version() < "1.2":
                self.setting['rpc_timeout_in_ms'] = self.options.rpc_timeout
            else:
                self.setting['read_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['range_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['write_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['truncate_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['request_timeout_in_ms'] = self.options.rpc_timeout
        self.node.set_configuration_options(values=self.setting, batch_commitlog=self.options.cl_batch)


class NodeStressCmd(Cmd):
    def description(self):
        return "Run stress on a node"

    def get_parser(self):
        usage = "usage: ccm node_name stress [options] [stress_options]"
        parser = self._get_default_parser(usage, self.description(), ignore_unknown_options=True)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.stress_options = parser.get_ignored() + args[1:]

    def run(self):
        try:
            self.node.stress(self.stress_options)
        except OSError:
            print >> sys.stderr, "Could not find stress binary (you may need to build it)"

class NodeSetdirCmd(Cmd):
    def description(self):
        return "Set the cassandra directory to use for the node"

    def get_parser(self):
        usage = "usage: ccm node_name setdir [options]"
        parser =  self._get_default_parser(usage, self.description())
        parser.add_option('-v', "--cassandra-version", type="string", dest="cassandra_version",
            help="Download and use provided cassandra version. If version is of the form 'git:<branch name>', then the specified branch will be downloaded from the git repo and compiled. (takes precedence over --cassandra-dir)", default=None)
        parser.add_option("--cassandra-dir", type="string", dest="cassandra_dir",
            help="Path to the cassandra directory to use [default %default]", default="./")
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        try:
            self.node.set_cassandra_dir(cassandra_dir=self.options.cassandra_dir, cassandra_version=self.options.cassandra_version, verbose=True)
        except common.ArgumentError as e:
            print >> sys.stderr, str(e)
            exit(1)
