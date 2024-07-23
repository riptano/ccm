
from __future__ import absolute_import

import os
import signal
import subprocess
import sys

from six import print_

from ccmlib import common
from ccmlib.cmds.command import Cmd
from ccmlib.node import NodeError

NODE_CMDS = [
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
    "verify",
    "shuffle",
    "sstablesplit",
    "getsstables",
    "decommission",
    "json",
    "updateconf",
    "updatelog4j",
    "stress",
    "cqlsh",
    "scrub",
    "verify",
    "status",
    "setdir",
    "bulkload",
    "version",
    "nodetool",
    "dsetool",
    "setworkload",
    "dse",
    "hadoop",
    "hive",
    "pig",
    "sqoop",
    "spark",
    "pause",
    "resume",
    "jconsole",
    "versionfrombuild",
    "byteman"
]


def commands():
    return NODE_CMDS


class NodeShowCmd(Cmd):

    descr_text = "Display information on a node"
    usage = "usage: ccm node_name show [options]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        self.node.show()


class NodeRemoveCmd(Cmd):

    descr_text = "Remove a node (stopping it if necessary and deleting all its data)"
    usage = "usage: ccm node_name remove [options]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        self.cluster.remove(self.node)


class NodeShowlogCmd(Cmd):

    descr_text = "Show the log of node name (runs your $PAGER on its system.log)"
    usage = "usage: ccm node_name showlog [options]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        log = self.node.logfilename()
        pager = os.environ.get('PAGER', common.platform_pager())
        os.execvp(pager, (pager, log))


class NodeSetlogCmd(Cmd):

    options_list = [
        (['-c', '--class'], {'type': "string", 'dest': "class_name", 'default': None, 'help': "Optional java class/package. Logging will be set for only this class/package if set"}),
    ]
    descr_text = "Set node name log level (INFO, DEBUG, ...) with/without Java class - require a node restart"
    usage = "usage: ccm node_name setlog [options] level"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        if len(args) == 1:
            print_('Missing log level', file=sys.stderr)
            parser.print_help()
        self.level = args[1]

        try:
            self.class_name = options.class_name
        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)

    def run(self):
        try:
            self.node.set_log_level(self.level, self.class_name)

        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)


class NodeClearCmd(Cmd):

    options_list = [
        (['-a', '--all'], {'action': "store_true", 'dest': "all", 'help': "Also clear the saved cache and node log files", 'default': False}),
    ]
    descr_text = "Clear the node data & logs (and stop the node)"
    usage = "usage: ccm node_name_clear [options]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        self.node.stop()
        self.node.clear(self.options.all)


class NodeStartCmd(Cmd):

    options_list = [
        (['-v', '--verbose'], {'action': "store_true", 'dest': "verbose", 'help': "Print standard output of cassandra process", 'default': False}),
        (['--no-wait'], {'action': "store_true", 'dest': "no_wait", 'help': "Do not wait for cassandra node to be ready", 'default': False}),
        (['--wait-other-notice'], {'action': "store_true", 'dest': "deprecate", 'help': "DEPRECATED/IGNORED: Use '--skip-wait-other-notice' instead. This is now on by default.", 'default': False}),
        (['--skip-wait-other-notice'], {'action': "store_false", 'dest': "wait_other_notice", 'help': "Skip waiting until all live nodes of the cluster have marked the other nodes UP", 'default': True}),
        (['--wait-for-binary-proto'], {'action': "store_true", 'dest': "wait_for_binary_proto", 'help': "Wait for the binary protocol to start", 'default': False}),
        (['-j', '--dont-join-ring'], {'action': "store_true", 'dest': "no_join_ring", 'help': "Launch the instance without joining the ring", 'default': False}),
        (['--replace-address'], {'type': "string", 'dest': "replace_address", 'default': None, 'help': "Replace a node in the ring through the cassandra.replace_address option"}),
        (['--jvm_arg'], {'action': "append", 'dest': "jvm_args", 'help': "Specify a JVM argument", 'default': []}),
        (['--quiet-windows'], {'action': "store_true", 'dest': "quiet_start", 'help': "Pass -q on Windows 2.2.4+ and 3.0+ startup. Ignored on linux.", 'default': False}),
        (['--root'], {'action': "store_true", 'dest': "allow_root", 'help': "Allow CCM to start cassandra as root", 'default': False}),
        (['--jvm-version'], {'type': "int", 'dest': "jvm_version", 'help': "Specify the JVM version to use (e.g. 8 for Java 8)", 'default': None}),
    ]
    descr_text = "Start a node"
    usage = "usage: ccm node start [options] name"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        try:
            self.node.start(not self.options.no_join_ring,
                            no_wait=self.options.no_wait,
                            wait_other_notice=self.options.wait_other_notice,
                            wait_for_binary_proto=self.options.wait_for_binary_proto,
                            verbose=self.options.verbose,
                            replace_address=self.options.replace_address,
                            jvm_args=self.options.jvm_args,
                            quiet_start=self.options.quiet_start,
                            allow_root=self.options.allow_root,
                            jvm_version=self.options.jvm_version)
        except NodeError as e:
            print_(str(e), file=sys.stderr)
            print_("Standard error output is:", file=sys.stderr)
            e.process.stderr_file.seek(0)
            for line in e.process.stderr_file.readlines():
                print_(line.rstrip('\n'), file=sys.stderr)
            exit(1)


class NodeStopCmd(Cmd):

    options_list = [
        (['--no-wait'], {'action': "store_true", 'dest': "no_wait", 'help': "Do not wait for the node to be stopped", 'default': False}),
        (['-g', '--gently'], {'action': "store_const", 'dest': "signal_event", 'help': "Shut down gently (default)", 'const': signal.SIGTERM, 'default': signal.SIGTERM}),
        (['--hang-up'], {'action': "store_const", 'dest': "signal_event", 'help': "Shut down via hang up (kill -1)", 'const': common.get_default_signals()['1']}),
        (['--not-gently'], {'action': "store_const", 'dest': "signal_event", 'help': "Shut down immediately (kill -9)", 'const': common.get_default_signals()['9']}),
    ]
    descr_text = "Stop a node"
    usage = "usage: ccm node stop [options] name"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        try:
            if not self.node.stop(wait=not self.options.no_wait, signal_event=self.options.signal_event):
                print_("%s is not running" % self.name, file=sys.stderr)
                exit(1)
        except NodeError as e:
            print_(str(e), file=sys.stderr)
            exit(1)


class _NodeToolCmd(Cmd):
    usage = "This is a private class, how did you get here?"
    descr_text = "This is a private class, how did you get here?"
    nodetool_cmd = ''

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        stdout, stderr, rc = self.node.nodetool(self.nodetool_cmd + " " + " ".join((self.args[1:])))
        print_(stderr)
        print_(stdout)


class NodeNodetoolCmd(_NodeToolCmd):
    usage = "usage: ccm node_name nodetool [options]"
    descr_text = "Run nodetool (connecting to node name)"

    def run(self):
        stdout, stderr, rc = self.node.nodetool(" ".join(self.args[1:]))
        print_(stderr)
        print_(stdout)


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


class NodeVersionCmd(_NodeToolCmd):
    usage = "usage: ccm node_name version"
    nodetool_cmd = 'version'
    descr_text = "Get the cassandra version of node"


class NodeDecommissionCmd(_NodeToolCmd):
    usage = "usage: ccm node_name decommission [options]"
    options_list = [
        (['--force'], {'action': "store_true", 'dest': "force", 'help': "Force decommission of this node even when it reduces the number of replicas to below configured RF.  Note: This is only relevant for C* 3.12+.", 'default': False}),
    ]
    nodetool_cmd = 'decommission'
    descr_text = "Run decommission on node name"

    def run(self):
        self.node.decommission(force=self.options.force)


class _DseToolCmd(Cmd):
    usage = "This is a private class, how did you get here?"
    descr_text = "This is a private class, how did you get here?"
    dsetool_cmd = ''

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        stdout, stderr, rc = self.node.dsetool(self.dsetool_cmd + " " + " ".join((self.args[1:])))
        print_(stderr)
        print_(stdout)


class NodeDsetoolCmd(_DseToolCmd):
    usage = "usage: ccm node_name dsetool [options]"
    descr_text = "Run dsetool (connecting to node name)"

    def run(self):
        stdout, stderr, rc = self.node.dsetool(" ".join(self.args[1:]))
        print_(stderr)
        print_(stdout)


class NodeCqlshCmd(Cmd):

    options_list = [
        (['-x', '--exec'], {'type': "string", 'dest': "cmds", 'default': None, 'help': "Execute the specified commands and exit"}),
        (['-v', '--verbose'], {'action': "store_true", 'dest': "verbose", 'help': "With --exec, show cli output after completion", 'default': False}),
    ]
    descr_text = "Launch a cqlsh session connected to this node"
    usage = "usage: ccm node_name cqlsh [options] [cli_options]"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.cqlsh_options = parser.get_ignored() + args[1:]

    def run(self):
        self.node.run_cqlsh(self.options.cmds, self.cqlsh_options)


class NodeBulkloadCmd(Cmd):

    descr_text = "Bulkload files into the cluster by connecting to this node"
    usage = "usage: ccm node_name bulkload [options] [sstable_dir]"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.loader_options = parser.get_ignored() + args[1:]

    def run(self):
        self.node.bulkload(self.loader_options)


class NodeScrubCmd(Cmd):

    descr_text = "Scrub files"
    usage = "usage: ccm node_name scrub [options] <keyspace> <cf>"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.scrub_options = parser.get_ignored() + args[1:]

    def run(self):
        self.node.scrub(self.scrub_options)


class NodeVerifyCmd(Cmd):

    descr_text = "Verify files"
    usage = "usage: ccm node_name verify [options] <keyspace> <cf>"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.verify_options = parser.get_ignored() + args[1:]

    def run(self):
        self.node.verify(self.verify_options)


class NodeJsonCmd(Cmd):

    options_list = [
        (['-k', '--keyspace'], {'type': "string", 'dest': "keyspace", 'default': None, 'help': "The keyspace to use [use all keyspaces by default]"}),
        (['-c', '--column-families'], {'type': "string", 'dest': "cfs", 'default': None, 'help': "Comma separated list of column families to use (requires -k to be set)"}),
        (['--key'], {'type': "string", 'action': "append", 'dest': "keys", 'default': None, 'help': "The key to include (you may specify multiple --key)"}),
        (['-e', '--enumerate-keys'], {'action': "store_true", 'dest': "enumerate_keys", 'help': "Only enumerate keys (i.e, call sstable2keys)", 'default': False}),
    ]
    descr_text = "Call sstable2json/sstabledump on the sstables of this node"
    usage = "usage: ccm node_name json [options] [file]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.keyspace = options.keyspace
        if self.keyspace is None:
            print_("You must specify a keyspace.")
            parser.print_help()
            exit(1)
        self.outfile = args[-1] if len(args) >= 2 else None
        self.column_families = options.cfs.split(',') if options.cfs else None

    def run(self):
        try:
            f = sys.stdout
            if self.outfile is not None:
                f = open(self.outfile, 'w')
            if self.node.has_cmd('sstable2json'):
                self.node.run_sstable2json(keyspace=self.keyspace,
                                           out_file=f,
                                           column_families=self.column_families,
                                           keys=self.options.keys,
                                           enumerate_keys=self.options.enumerate_keys)
            elif self.node.has_cmd('sstabledump'):
                self.node.run_sstabledump(keyspace=self.keyspace,
                                          column_families=self.column_families,
                                          keys=self.options.keys,
                                          enumerate_keys=self.options.enumerate_keys,
                                          command=True)
        except common.ArgumentError as e:
            print_(e, file=sys.stderr)


class NodeSstablesplitCmd(Cmd):

    options_list = [
        (['-k', '--keyspace'], {'type': "string", 'dest': "keyspace", 'default': None, 'help': "The keyspace to use [use all keyspaces by default]"}),
        (['-c', '--column-families'], {'type': "string", 'dest': 'cfs', 'default': None, 'help': "Comma separated list of column families to use (requires -k to be set)"}),
        (['-s', '--size'], {'type': 'int', 'dest': "size", 'default': None, 'help': "Maximum size in MB for the output sstables (default: 50 MB)"}),
        (['--no-snapshot'], {'action': 'store_true', 'dest': "no_snapshot", 'default': False, 'help': "Don't snapshot the sstables before splitting"}),
    ]
    descr_text = "Run sstablesplit on the sstables of this node"
    usage = "usage: ccm node_name sstablesplit [options] [file]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.keyspace = options.keyspace
        self.size = options.size
        self.no_snapshot = options.no_snapshot
        self.column_families = None
        self.datafiles = None
        if options.cfs is not None:
            if self.keyspace is None:
                print_("You need a keyspace (option -k) if you specify column families", file=sys.stderr)
                exit(1)
            self.column_families = options.cfs.split(',')

        if len(args) > 1:
            if self.column_families is None:
                print_("You need a column family (option -c) if you specify datafiles", file=sys.stderr)
                exit(1)
            self.datafiles = args[1:]

    def run(self):
        self.node.run_sstablesplit(datafiles=self.datafiles, keyspace=self.keyspace,
                                   column_families=self.column_families, size=self.size,
                                   no_snapshot=self.no_snapshot)


class NodeGetsstablesCmd(Cmd):

    options_list = [
        (['-k', '--keyspace'], {'type': "string", 'dest': "keyspace", 'default': None, 'help': "The keyspace to use [use all keyspaces by default]"}),
        (['-t', '--tables'], {'type': "string", 'dest': 'tables', 'default': None, 'help': "Comma separated list of tables to use (requires -k to be set)"}),
    ]
    descr_text = "Run getsstables to get absolute path of sstables in this node"
    usage = "usage: ccm node_name getsstables [options] [file]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.keyspace = options.keyspace
        self.tables = None
        self.datafiles = None
        if options.tables is not None:
            if self.keyspace is None:
                print_("You need a keyspace (option -k) if you specify tables", file=sys.stderr)
                exit(1)
            self.tables = options.tables.split(',')

        if len(args) > 1:
            if self.tables is None:
                print_("You need a tables (option -t) if you specify datafiles", file=sys.stderr)
                exit(1)
            self.datafiles = args[1:]

    def run(self):
        sstablefiles = self.node.get_sstablespath(datafiles=self.datafiles, keyspace=self.keyspace,
                                                  tables=self.tables)
        print_('\n'.join(sstablefiles))


class NodeUpdateconfCmd(Cmd):

    options_list = [
        (['--no-hh', '--no-hinted-handoff'], {'action': "store_false", 'dest': "hinted_handoff", 'default': True, 'help': "Disable hinted handoff"}),
        (['--batch-cl', '--batch-commit-log'], {'action': "store_true", 'dest': "cl_batch", 'default': None, 'help': "Set commit log to batch mode"}),
        (['--periodic-cl', '--periodic-commit-log'], {'action': "store_true", 'dest': "cl_periodic", 'default': None, 'help': "Set commit log to periodic mode"}),
        (['--rt', '--rpc-timeout'], {'action': "store", 'type': 'int', 'dest': "rpc_timeout", 'help': "Set rpc timeout"}),
        (['-y', '--yaml'], {'action': "store_true", 'dest': "literal_yaml", 'default': False, 'help': "Pass in literal yaml string. Option syntax looks like ccm node_name updateconf -y 'a: [b: [c,d]]'"}),
    ]
    descr_text = "Update the cassandra config files for this node (useful when updating cassandra)"
    usage = "usage: ccm node_name updateconf [options] [ new_setting | ...  ], where new_setting should be a string of the form 'compaction_throughput_mb_per_sec: 32'"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        args = args[1:]
        try:
            self.setting = common.parse_settings(args, literal_yaml=self.options.literal_yaml)
            if self.options.cl_batch and self.options.cl_periodic:
                print_("Can't set commitlog to be both batch and periodic.{}".format(os.linesep))
                parser.print_help()
                exit(1)
        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)

    def run(self):
        self.setting['hinted_handoff_enabled'] = self.options.hinted_handoff
        if self.options.rpc_timeout is not None:
            if self.node.cluster.cassandra_version() < "1.2":
                self.setting['rpc_timeout_in_ms'] = self.options.rpc_timeout
            else:
                self.setting['read_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['range_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['write_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['truncate_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['request_timeout_in_ms'] = self.options.rpc_timeout
        self.node.set_configuration_options(values=self.setting)
        if self.options.cl_batch:
            self.node.set_batch_commitlog(True)
        if self.options.cl_periodic:
            self.node.set_batch_commitlog(False)


class NodeUpdatedseconfCmd(Cmd):

    options_list = [
        (['-y', '--yaml'], {'action': "store_true", 'dest': "literal_yaml", 'default': False, 'help': "Pass in literal yaml string. Option syntax looks like ccm node_name updatedseconf -y 'a: [b: [c,d]]'"}),
    ]
    descr_text = "Update the dse config files for this node"
    usage = "usage: ccm node_name updatedseconf [options] [ new_setting | ...  ], where new setting should be a string of the form 'max_solr_concurrency_per_core: 2'; nested options can be separated with a period like 'cql_slow_log_options.enabled: true'"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        args = args[1:]
        try:
            self.setting = common.parse_settings(args, literal_yaml=self.options.literal_yaml)
        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)

    def run(self):
        self.node.set_dse_configuration_options(values=self.setting)

#
# Class implementens the functionality of updating log4j-server.properties
# on the given node by copying the given config into
# ~/.ccm/name-of-cluster/nodeX/conf/log4j-server.properties
#


class NodeUpdatelog4jCmd(Cmd):

    options_list = [
        (['-p', '--path'], {'type': "string", 'dest': "log4jpath", 'help': "Path to new Cassandra log4j configuration file"}),
    ]
    descr_text = "Update the Cassandra log4j-server.properties configuration file under given node"
    usage = "usage: ccm node_name updatelog4j -p <log4j config>"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        try:
            self.log4jpath = options.log4jpath
            if self.log4jpath is None:
                raise KeyError("[Errno] -p or --path <path of new log4j configuration file> is not provided")
        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)
        except KeyError as e:
            print_(str(e), file=sys.stderr)
            exit(1)

    def run(self):
        try:
            self.node.update_log4j(self.log4jpath)
        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)


class NodeStressCmd(Cmd):

    descr_text = "Run stress on a node"
    usage = "usage: ccm node_name stress [options] [stress_options]"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.stress_options = args[1:] + parser.get_ignored()

    def run(self):
        try:
            self.node.stress(self.stress_options)
        except OSError:
            print_("Could not find stress binary (you may need to build it)", file=sys.stderr)


class NodeShuffleCmd(Cmd):

    descr_text = "Run shuffle on a node"
    usage = "usage: ccm node_name shuffle [options] [shuffle_cmds]"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.shuffle_cmd = args[1]

    def run(self):
        self.node.shuffle(self.shuffle_cmd)


class NodeSetdirCmd(Cmd):

    options_list = [
        (['-v', "--version"], {'type': "string", 'dest': "version", 'help': "Download and use provided cassandra or dse version. If version is of the form 'git:<branch name>', then the specified branch will be downloaded from the git repo and compiled. (takes precedence over --install-dir)", 'default': None}),
        (["--install-dir"], {'type': "string", 'dest': "install_dir", 'help': "Path to the cassandra or dse directory to use [default %default]", 'default': "./"}),
    ]
    descr_text = "Set the cassandra directory to use for the node"
    usage = "usage: ccm node_name setdir [options]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        try:
            self.node.set_install_dir(install_dir=self.options.install_dir, version=self.options.version, verbose=True)
        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)


class NodeSetworkloadCmd(Cmd):

    descr_text = "Sets the workloads for a DSE node"
    usage = "usage: ccm node_name setworkload [cassandra|solr|hadoop|spark|dsefs|cfs|graph],..."

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.workloads = args[1].split(',')
        valid_workloads = ['cassandra', 'solr', 'hadoop', 'spark', 'dsefs', 'cfs', 'graph']
        for workload in self.workloads:
            if workload not in valid_workloads:
                print_(workload, ' is not a valid workload')
                exit(1)

    def run(self):
        try:
            self.node.set_workloads(workloads=self.workloads)
        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)


class NodeDseCmd(Cmd):

    descr_text = "Launch a dse client application connected to this node"
    usage = "usage: ccm node_name dse [dse_options]"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.dse_options = args[1:] + parser.get_ignored()

    def run(self):
        self.node.dse(self.dse_options)


class NodeHadoopCmd(Cmd):

    descr_text = "Launch a hadoop session connected to this node"
    usage = "usage: ccm node_name hadoop [options] [hadoop_options]"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.hadoop_options = args[1:] + parser.get_ignored()

    def run(self):
        self.node.hadoop(self.hadoop_options)


class NodeHiveCmd(Cmd):

    descr_text = "Launch a hive session connected to this node"
    usage = "usage: ccm node_name hive [options] [hive_options]"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.hive_options = args[1:] + parser.get_ignored()

    def run(self):
        self.node.hive(self.hive_options)


class NodePigCmd(Cmd):

    descr_text = "Launch a pig session connected to this node"
    usage = "usage: ccm node_name pig [options] [pig_options]"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.pig_options = parser.get_ignored() + args[1:]

    def run(self):
        self.node.pig(self.pig_options)


class NodeSqoopCmd(Cmd):

    descr_text = "Launch a sqoop session connected to this node"
    usage = "usage: ccm node_name sqoop [options] [sqoop_options]"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.sqoop_options = args[1:] + parser.get_ignored()

    def run(self):
        self.node.sqoop(self.sqoop_options)


class NodeSparkCmd(Cmd):

    descr_text = "Launch a spark session connected to this node"
    usage = "usage: ccm node_name spark [options] [spark_options]"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.spark_options = args[1:] + parser.get_ignored()

    def run(self):
        self.node.spark(self.spark_options)


class NodePauseCmd(Cmd):

    descr_text = "Send a SIGSTOP to this node"
    usage = "usage: ccm node_name pause"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        self.node.pause()


class NodeResumeCmd(Cmd):

    descr_text = "Send a SIGCONT to this node"
    usage = "usage: ccm node_name resume"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        self.node.resume()


class NodeJconsoleCmd(Cmd):

    descr_text = "Opens jconsole client and connect to running node"
    usage = "usage: ccm node_name jconsole"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        cmds = ["jconsole", "localhost:%s" % self.node.jmx_port]
        try:
            subprocess.call(cmds)
        except OSError:
            print_("Could not start jconsole. Please make sure jconsole can be found in your $PATH.")
            exit(1)


class NodeVersionfrombuildCmd(Cmd):

    descr_text = "Print the node's version as grepped from build.xml. Can be used when the node isn't running."
    usage = "usage: ccm node_name versionfrombuild"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)

    def run(self):
        print_(common.get_version_from_build(self.node.get_install_dir()))


class NodeBytemanCmd(Cmd):

    descr_text = "Invoke byteman-submit "
    usage = "usage: ccm node_name byteman-submit"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True)
        self.byteman_options = args[1:] + parser.get_ignored()

    def run(self):
        self.node.byteman_submit(self.byteman_options)
