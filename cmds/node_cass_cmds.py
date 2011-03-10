import os, sys, time

L = os.path.realpath(__file__).split(os.path.sep)[:-2]
root = os.path.sep.join(L)
sys.path.append(os.path.join(root, 'ccm_lib'))
from command import Cmd
import common
from node import Node, StartError, ArgumentError

class NodeStartCmd(Cmd):
    def description(self):
        return "Start a node"

    def get_parser(self):
        usage = "usage: ccm node start [options] name"
        parser = self._get_default_parser(usage, self.description(), cassandra_dir=True)
        parser.add_option('-v', '--verbose', action="store_true", dest="verbose",
            help="Print standard output of cassandra process", default=False)
        parser.add_option('--no-wait', action="store_true", dest="no_wait",
            help="Do not wait for cassandra node to be ready", default=False)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        if self.node.is_running():
            print "%s is already running" % self.name
            exit(1)

        try:
            process = self.node.start(self.options.cassandra_dir)
        except StartError as e:
            print str(e)
            print "Standard error output is:"
            for line in e.process.stderr:
                print line.rstrip('\n')
            exit(1)

        if self.options.no_wait:
            time.sleep(2) # waiting 2 seconds to check for early errors and
                          # for the pid to be set
        else:
            for line in process.stdout:
                if self.options.verbose:
                    print line.rstrip('\n')

        self.node.update_pid(process)

        if not self.node.is_running():
            print "Error starting node."
            for line in process.stderr:
                print line.rstrip('\n')

class NodeStopCmd(Cmd):
    def description(self):
        return "Stop a node"

    def get_parser(self):
        usage = "usage: ccm node stop [options] name"
        parser = self._get_default_parser(usage, self.description())
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        if not self.node.stop():
            print "%s is not running" % self.name
            exit(1)

class __NodeToolCmd(Cmd):
    def __init__(self, usage, nodetool_cmd):
        self.usage = usage
        self.nodetool_cmd = nodetool_cmd

    def get_parser(self):
        parser = self._get_default_parser(self.usage, self.description(), cassandra_dir=True)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        self.node.nodetool(self.options.cassandra_dir, self.nodetool_cmd)

class NodeRingCmd(__NodeToolCmd):
    def description(self):
        return "Print ring (connecting to node name)"

    def __init__(self):
        usage = "usage: ccm node_name ring [options]"
        super(NodeRingCmd, self).__init__(usage, 'ring')

class NodeFlushCmd(__NodeToolCmd):
    def description(self):
        return "Flush node name"

    def __init__(self):
        usage = "usage: ccm node_name flush [options]"
        super(NodeFlushCmd, self).__init__(usage, 'flush')

class NodeCompactCmd(__NodeToolCmd):
    def description(self):
        return "Compact node name"

    def __init__(self):
        usage = "usage: ccm node_name compact [options]"
        super(NodeCompactCmd, self).__init__(usage, 'compact')

class NodeCleanupCmd(__NodeToolCmd):
    def description(self):
        return "Run cleanup on node name"

    def __init__(self):
        usage = "usage: ccm node_name cleanup [options]"
        super(NodeCleanupCmd, self).__init__(usage, 'cleanup')

class NodeRepairCmd(__NodeToolCmd):
    def description(self):
        return "Run repair on node name"

    def __init__(self):
        usage = "usage: ccm node_name repair [options]"
        super(NodeRepairCmd, self).__init__(usage, 'repair')

class NodeDecommissionCmd(__NodeToolCmd):
    def description(self):
        return "Run decommission on node name"

    def __init__(self):
        usage = "usage: ccm node_name decommission [options]"
        super(NodeDecommissionCmd, self).__init__(usage, 'decommission')

    def run(self):
        super(NodeDecommissionCmd, self).run()
        self.node.decommission()

class NodeCliCmd(Cmd):
    def description(self):
        return "Launch a cassandra cli connected to this node"

    def get_parser(self):
        usage = "usage: ccm node_name cli [options]"
        parser = self._get_default_parser(usage, self.description(), cassandra_dir=True)
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        self.node.run_cli(self.options.cassandra_dir)

class NodeJsonCmd(Cmd):
    def description(self):
        return "Call sstable2json on the sstables of this node"

    def get_parser(self):
        usage = "usage: ccm node_name json [options]"
        parser = self._get_default_parser(usage, self.description(), cassandra_dir=True)
        parser.add_option('-k', '--keyspace', type="string", dest="keyspace",
            help="The keyspace to use [use all keyspaces by default]")
        parser.add_option('-c', '--column-families', type="string", dest="cfs",
            help="Comma separated list of column families to use (requires -k to be set)")
        return parser

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.keyspace = options.keyspace
        self.column_families = None
        if options.cfs:
            if not self.keyspace:
                print "You need a keyspace specified (option -k) if you specify column families"
                exit(1)
            self.column_families = options.cfs.split(',')

    def run(self):
        try:
            self.node.run_sstable2json(self.options.cassandra_dir, self.keyspace, self.column_families)
        except ArgumentError as e:
            print e
