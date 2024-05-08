
from __future__ import absolute_import

import os
import signal
import subprocess
import sys

from six import print_

from ccmlib import common, repository
from ccmlib.cluster import Cluster
from ccmlib.cluster_factory import ClusterFactory
from ccmlib.cmds.command import Cmd
from ccmlib.common import ArgumentError, get_default_signals
from ccmlib.dse_cluster import DseCluster
from ccmlib.dse_node import DseNode
from ccmlib.node import Node, NodeError

CLUSTER_CMDS = [
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
    "updatedseconf",
    "updatelog4j",
    "setdir",
    "bulkload",
    "setlog",
    "scrub",
    "verify",
    "invalidatecache",
    "checklogerror",
    "showlastlog",
    "jconsole",
    "setworkload",
    "enableaoss",
    "showlogs"
]


def commands():
    return CLUSTER_CMDS


def parse_populate_count(v):
    if v is None:
        return None
    tmp = v.split(':')
    if len(tmp) == 1:
        return int(tmp[0])
    else:
        return [int(t) for t in tmp]


class ClusterCreateCmd(Cmd):

    options_list = [
        (['--no-switch'], {'action': "store_true", 'dest': "no_switch", 'help': "Don't switch to the newly created cluster", 'default': False}),
        (['-p', '--partitioner'], {'type': "string", 'dest': "partitioner", 'help': "Set the cluster partitioner class"}),
        (['-v', "--version"], {'type': "string", 'dest': "version", 'help': "Download and use provided cassandra or dse version. If version is of the form 'git:<branch name>', then the specified cassandra branch will be downloaded from the git repo and compiled. (takes precedence over --install-dir)", 'default': None}),
        (['-o', "--opsc"], {'type': "string", 'dest': "opscenter", 'help': "Download and use provided opscenter version to install with DSE. Will have no effect on cassandra installs)", 'default': None}),
        (["--dse"], {'action': "store_true", 'dest': "dse", 'help': "Use with -v to indicate that the version being loaded is DSE"}),
        (["--dse-username"], {'type': "string", 'dest': "dse_username", 'help': "The username to use to download DSE with", 'default': None}),
        (["--dse-password"], {'type': "string", 'dest': "dse_password", 'help': "The password to use to download DSE with", 'default': None}),
        (["--dse-credentials"], {'type': "string", 'dest': "dse_credentials_file", 'help': "An ini-style config file containing the dse_username and dse_password under a dse_credentials section. [default to {}/.dse.ini if it exists]".format(common.get_default_path_display_name()), 'default': None}),
        (["--install-dir"], {'type': "string", 'dest': "install_dir", 'help': "Path to the cassandra or dse directory to use [default %default]", 'default': "./"}),
        (['-n', '--nodes'], {'type': "string", 'dest': "nodes", 'help': "Populate the new cluster with that number of nodes (a single int or a colon-separate list of ints for multi-dc setups)"}),
        (['-i', '--ipprefix'], {'type': "string", 'dest': "ipprefix", 'help': "Ipprefix to use to create the ip of a node while populating"}),
        (['-I', '--ip-format'], {'type': "string", 'dest': "ipformat", 'help': "Format to use when creating the ip of a node (supports enumerating ipv6-type addresses like fe80::%d%lo0)"}),
        (['-s', "--start"], {'action': "store_true", 'dest': "start_nodes", 'help': "Start nodes added through -s", 'default': False}),
        (['-d', "--debug"], {'action': "store_true", 'dest': "debug", 'help': "If -s is used, show the standard output when starting the nodes", 'default': False}),
        (['-b', "--binary-protocol"], {'action': "store_true", 'dest': "binary_protocol", 'help': "Enable the binary protocol (starting from C* 1.2.5 the binary protocol is started by default and this option is a no-op)", 'default': False}),
        (['-D', "--debug-log"], {'action': "store_true", 'dest': "debug_log", 'help': "With -n, sets debug logging on the new nodes", 'default': False}),
        (['-T', "--trace-log"], {'action': "store_true", 'dest': "trace_log", 'help': "With -n, sets trace logging on the new nodes", 'default': False}),
        (["--vnodes"], {'action': "store_true", 'dest': "vnodes", 'help': "Use vnodes (256 tokens). Must be paired with -n.", 'default': False}),
        (['--configuration-yaml'], {'type': "string", 'action': "store", 'dest': "configuration_yaml", 'help': "Name of the configuration file to use, e.g. cassandra_latest.yaml", 'default': None}),
        (['--jvm_arg'], {'action': "append", 'dest': "jvm_args", 'help': "Specify a JVM argument", 'default': []}),
        (['--profile'], {'action': "store_true", 'dest': "profile", 'help': "Start the nodes with yourkit agent (only valid with -s)", 'default': False}),
        (['--profile-opts'], {'type': "string", 'action': "store", 'dest': "profile_options", 'help': "Yourkit options when profiling", 'default': None}),
        (['--ssl'], {'type': "string", 'dest': "ssl_path", 'help': "Path to keystore.jks and cassandra.crt files (and truststore.jks [not required])", 'default': None}),
        (['--require_client_auth'], {'action': "store_true", 'dest': "require_client_auth", 'help': "Enable client authentication (only vaid with --ssl)", 'default': False}),
        (['--node-ssl'], {'type': "string", 'dest': "node_ssl_path", 'help': "Path to keystore.jks and truststore.jks for internode encryption", 'default': None}),
        (['--pwd-auth'], {'action': "store_true", 'dest': "node_pwd_auth", 'help': "Change authenticator to PasswordAuthenticator (default credentials)", 'default': False}),
        (['--byteman'], {'action': "store_true", 'dest': "install_byteman", 'help': "Start nodes with byteman agent running", 'default': False}),
        (['--root'], {'action': "store_true", 'dest': "allow_root", 'help': "Allow CCM to start cassandra as root", 'default': False}),
        (['--datadirs'], {'type': "int", 'dest': "datadirs", 'help': "Number of data directories to use", 'default': 1}),
        (['--quiet'], {'action': "store_false", 'dest': "verbose", 'help': "Don't show percentage progress output when downloading DSE or C*", 'default': True}),
        (['-S', '--use-single-interface'], { 'action' : "store_true", 'dest' : "use_single_interface", 'default' : False,
                          "help" : "Use multiple ports on a single interface instead of an interface per instance'"}),
    ]
    descr_text = "Create a new cluster"
    usage = "usage: ccm create [options] cluster_name"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, cluster_name=True)
        if options.ipprefix and options.ipformat:
            parser.print_help()
            parser.error("%s and %s may not be used together" % (parser.get_option('-i'), parser.get_option('-I')))
        self.nodes = parse_populate_count(options.nodes)
        if self.options.vnodes and self.nodes is None:
            print_("Can't set --vnodes if not populating cluster in this command.")
            parser.print_help()
            exit(1)
        if not options.version:
            try:
                common.validate_install_dir(options.install_dir)
            except ArgumentError:
                parser.print_help()
                parser.error("%s is not a valid cassandra directory. You must define a cassandra dir or version." % options.install_dir)

            if common.get_dse_version(options.install_dir) is not None:
                common.assert_jdk_valid_for_cassandra_version(common.get_dse_cassandra_version(options.install_dir))
            else:
                common.assert_jdk_valid_for_cassandra_version(common.get_version_from_build(options.install_dir))

        if common.is_win() and os.path.exists('c:\windows\system32\java.exe'):
            print_("""WARN: c:\windows\system32\java.exe exists.
                This may cause registry issues, and jre7 to be used, despite jdk8 being installed.
                """)

    def run(self):
        try:
            if self.options.dse or (not self.options.version and common.isDse(self.options.install_dir)):
                cluster = DseCluster(self.path, self.name, install_dir=self.options.install_dir, version=self.options.version, dse_username=self.options.dse_username, dse_password=self.options.dse_password, dse_credentials_file=self.options.dse_credentials_file, opscenter=self.options.opscenter, verbose=self.options.verbose, configuration_yaml=self.options.configuration_yaml)
            else:
                cluster = Cluster(self.path, self.name, install_dir=self.options.install_dir, version=self.options.version, verbose=self.options.verbose, configuration_yaml=self.options.configuration_yaml)
        except OSError as e:
            import traceback
            print_('Cannot create cluster: %s\n%s' % (str(e), traceback.format_exc()), file=sys.stderr)
            exit(1)

        if self.options.partitioner:
            cluster.set_partitioner(self.options.partitioner)

        if cluster.cassandra_version() >= "1.2.5":
            self.options.binary_protocol = True
        if self.options.binary_protocol:
            cluster.set_configuration_options({'start_native_transport': True})

        if self.options.vnodes:
            if cluster.cassandra_version() >= "4":
                cluster.set_configuration_options({'num_tokens': 16})
            elif cluster.cassandra_version() >= "1.2":
                cluster.set_configuration_options({'num_tokens': 256})

        if not self.options.no_switch:
            common.switch_cluster(self.path, self.name)
            print_('Current cluster is now: %s' % self.name)

        if not (self.options.ipprefix or self.options.ipformat):
            self.options.ipformat = '127.0.0.%d'

        if self.options.ssl_path:
            cluster.enable_ssl(self.options.ssl_path, self.options.require_client_auth)

        if self.options.node_ssl_path:
            cluster.enable_internode_ssl(self.options.node_ssl_path)

        if self.options.node_pwd_auth:
            cluster.enable_pwd_auth()

        if self.options.datadirs:
            cluster.set_datadir_count(self.options.datadirs)

        if self.nodes is not None:
            try:
                if self.options.debug_log:
                    cluster.set_log_level("DEBUG")
                if self.options.trace_log:
                    cluster.set_log_level("TRACE")
                cluster.populate(self.nodes, self.options.debug, use_vnodes=self.options.vnodes, ipprefix=self.options.ipprefix, ipformat=self.options.ipformat, install_byteman=self.options.install_byteman, use_single_interface=self.options.use_single_interface)
                if self.options.start_nodes:
                    profile_options = None
                    if self.options.profile:
                        profile_options = {}
                        if self.options.profile_options:
                            profile_options['options'] = self.options.profile_options
                    if cluster.start(verbose=self.options.debug, wait_for_binary_proto=self.options.binary_protocol, jvm_args=self.options.jvm_args, profile_options=profile_options, allow_root=self.options.allow_root) is None:
                        details = ""
                        if not self.options.debug_log:
                            details = " (you can use --debug for more information)"
                        print_("Error starting nodes, see above for details%s" % details, file=sys.stderr)
            except common.ArgumentError as e:
                print_(str(e), file=sys.stderr)
                exit(1)


class ClusterAddCmd(Cmd):

    options_list = [
        (['-b', '--auto-bootstrap'], {'action': "store_true", 'dest': "bootstrap", 'help': "Set auto bootstrap for the node", 'default': False}),
        (['-s', '--seeds'], {'action': "store_true", 'dest': "is_seed", 'help': "Configure this node as a seed", 'default': False}),
        (['-i', '--itf'], {'type': "string", 'dest': "itfs", 'help': "Set host and port for thrift, the binary protocol and storage (format: host[:port])"}),
        (['-t', '--thrift-itf'], {'type': "string", 'dest': "thrift_itf", 'help': "Set the thrift host and port for the node (format: host[:port])"}),
        (['-l', '--storage-itf'], {'type': "string", 'dest': "storage_itf", 'help': "Set the storage (cassandra internal) host and port for the node (format: host[:port])"}),
        (['--binary-itf'], {'type': "string", 'dest': "binary_itf", 'help': "Set the binary protocol host and port for the node (format: host[:port])."}),
        (['-j', '--jmx-port'], {'type': "string", 'dest': "jmx_port", 'help': "JMX port for the node", 'default': "7199"}),
        (['-r', '--remote-debug-port'], {'type': "string", 'dest': "remote_debug_port", 'help': "Remote Debugging Port for the node", 'default': "2000"}),
        (['-n', '--token'], {'type': "string", 'dest': "initial_token", 'help': "Initial token for the node", 'default': None}),
        (['-d', '--data-center'], {'type': "string", 'dest': "data_center", 'help': "Datacenter name this node is part of", 'default': None}),
        (['--dse'], {'action': "store_true", 'dest': "dse_node", 'help': "Add node to DSE Cluster", 'default': False}),
    ]
    descr_text = "Add a new node to the current cluster"
    usage = "usage: ccm add [options] node_name"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, node_name=True, load_cluster=True, load_node=False)

        if options.itfs is None and (options.thrift_itf is None or options.storage_itf is None or options.binary_itf is None):
            print_('Missing thrift and/or storage and/or binary protocol interfaces or jmx port', file=sys.stderr)
            parser.print_help()
            exit(1)

        if self.name in self.cluster.nodes:
            print_("This name is already in use. Choose another.", file=sys.stderr)
            parser.print_help()
            exit(1)

        used_jmx_ports = [node.jmx_port for node in self.cluster.nodelist()]
        if options.jmx_port in used_jmx_ports:
            print_("This JMX port is already in use. Choose another.", file=sys.stderr)
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
            print_('Cannot set a binary address different from the thrift one', file=sys.stderr)
            exit(1)

        self.jmx_port = options.jmx_port
        self.remote_debug_port = options.remote_debug_port
        self.initial_token = options.initial_token

    def run(self):
        try:
            if self.options.dse_node:
                node = DseNode(self.name, self.cluster, self.options.bootstrap, self.thrift, self.storage, self.jmx_port, self.remote_debug_port, self.initial_token, binary_interface=self.binary)
            else:
                node = Node(self.name, self.cluster, self.options.bootstrap, self.thrift, self.storage, self.jmx_port, self.remote_debug_port, self.initial_token, binary_interface=self.binary)
            self.cluster.add(node, self.options.is_seed, self.options.data_center)
        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)


class ClusterPopulateCmd(Cmd):

    options_list = [
        (['-n', '--nodes'], {'type': "string", 'dest': "nodes", 'help': "Number of nodes to populate with (a single int or a colon-separate list of ints for multi-dc setups)"}),
        (['-d', '--debug'], {'action': "store_true", 'dest': "debug", 'help': "Enable remote debugging options", 'default': False}),
        (['--byteman'], {'action': "store_true", 'dest': "install_byteman", 'help': "Start nodes with byteman agent running", 'default': False}),
        (['--vnodes'], {'action': "store_true", 'dest': "vnodes", 'help': "Populate using vnodes", 'default': False}),
        (['-i', '--ipprefix'], {'type': "string", 'dest': "ipprefix", 'help': "Ipprefix to use to create the ip of a node"}),
        (['-I', '--ip-format'], {'type': "string", 'dest': "ipformat", 'help': "Format to use when creating the ip of a node (supports enumerating ipv6-type addresses like fe80::%d%lo0)"}),
    ]
    descr_text = "Add a group of new nodes with default options"
    usage = "usage: ccm populate -n <node count> {-d}"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        if options.ipprefix and options.ipformat:
            parser.print_help()
            parser.error("%s and %s may not be used together" % (parser.get_option('-i'), parser.get_option('-I')))

        self.nodes = parse_populate_count(options.nodes)
        if self.nodes is None:
            parser.print_help()
            parser.error("Not a valid number of nodes. Did you use -n?")
            exit(1)

    def run(self):
        try:
            if self.options.vnodes:
                if self.cluster.cassandra_version() >= "4":
                    self.cluster.set_configuration_options({'num_tokens': 16})
                elif self.cluster.cassandra_version() >= "1.2":
                    self.cluster.set_configuration_options({'num_tokens': 256})

            if not (self.options.ipprefix or self.options.ipformat):
                self.options.ipformat = '127.0.0.%d'

            self.cluster.populate(self.nodes, self.options.debug, use_vnodes=self.options.vnodes, ipprefix=self.options.ipprefix, ipformat=self.options.ipformat, install_byteman=self.options.install_byteman)
        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)


class ClusterListCmd(Cmd):

    descr_text = "List existing clusters"
    usage = "usage: ccm list [options]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args)

    def run(self):
        try:
            current = common.current_cluster_name(self.path)
        except Exception:
            current = ''

        for dir in os.listdir(self.path):
            if os.path.exists(os.path.join(self.path, dir, 'cluster.conf')):
                print_(" %s%s" % ('*' if current == dir else ' ', dir))


class ClusterSwitchCmd(Cmd):

    descr_text = "Switch of current (active) cluster"
    usage = "usage: ccm switch [options] cluster_name"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, cluster_name=True)
        if not os.path.exists(os.path.join(self.path, self.name, 'cluster.conf')):
            print_("%s does not appear to be a valid cluster (use ccm list to view valid clusters)" % self.name, file=sys.stderr)
            exit(1)

    def run(self):
        common.switch_cluster(self.path, self.name)


class ClusterStatusCmd(Cmd):

    options_list = [
        (['-v', '--verbose'], {'action': "store_true", 'dest': "verbose", 'help': "Print full information on all nodes", 'default': False}),
    ]
    descr_text = "Display status on the current cluster"
    usage = "usage: ccm status [options]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        self.cluster.show(self.options.verbose)


class ClusterShowlogsCmd(Cmd):
    descr_text = "Show logs of nodes in this cluster. If no nodes are specified, logs of all nodes will be shown.\
                 By default multitail is used. If you need to alter the command or options, change CCM_MULTITAIL_CMD."

    usage = "usage: ccm showlogs [node1 node2 ...]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        shower = os.getenv("CCM_MULTITAIL_CMD", 'multitail').split()[0]
        shower_options = os.getenv("CCM_MULTITAIL_CMD", "").split()[1:]
        logs = self.cluster.show_logs(self.args)
        os.execvp(shower, [shower]+shower_options+logs)


class ClusterRemoveCmd(Cmd):

    descr_text = "Remove the current or specified cluster (delete all data)"
    usage = "usage: ccm remove [options] [cluster_name]"

    def validate(self, parser, options, args):
        self.other_cluster = None
        if len(args) > 0:
            # Setup to remove the specified cluster:
            Cmd.validate(self, parser, options, args)
            self.other_cluster = args[0]
            if not os.path.exists(os.path.join(
                    self.path, self.other_cluster, 'cluster.conf')):
                print_("%s does not appear to be a valid cluster"
                       " (use ccm list to view valid clusters)"
                       % self.other_cluster, file=sys.stderr)
                exit(1)
        else:
            # Setup to remove the current cluster:
            Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        if self.other_cluster:
            # Remove the specified cluster:
            cluster = ClusterFactory.load(self.path, self.other_cluster)
            cluster.remove()
            # Remove CURRENT flag if the specified cluster is the current cluster:
            if self.other_cluster == common.current_cluster_name(self.path):
                os.remove(os.path.join(self.path, 'CURRENT'))
        else:
            # Remove the current cluster:
            self.cluster.remove()
            os.remove(os.path.join(self.path, 'CURRENT'))


class ClusterClearCmd(Cmd):

    descr_text = "Clear the current cluster data (and stop all nodes)"
    usage = "usage: ccm clear [options]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        self.cluster.clear()


class ClusterLivesetCmd(Cmd):

    descr_text = "Print a comma-separated list of addresses of running nodes (helpful in scripts)"
    usage = "usage: ccm liveset [options]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        l = [node.network_interfaces['storage'][0] for node in list(self.cluster.nodes.values()) if node.is_live()]
        print_(",".join(l))


class ClusterSetdirCmd(Cmd):

    options_list = [
        (['-v', "--version"], {'type': "string", 'dest': "version", 'help': "Download and use provided cassandra or dse version. If version is of the form 'git:<branch name>', then the specified cassandra branch will be downloaded from the git repo and compiled. (takes precedence over --install-dir)", 'default': None}),
        (["--install-dir"], {'type': "string", 'dest': "install_dir", 'help': "Path to the cassandra or dse directory to use [default %default]", 'default': "./"}),
        (['-n', '--node'], {'type': "string", 'dest': "node", 'help': "Set directory only for the specified node"}),
    ]
    descr_text = "Set the install directory (cassandra or dse) to use"
    usage = "usage: ccm setdir [options]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        try:
            target = self.cluster
            if self.options.node:
                target = self.cluster.nodes.get(self.options.node)
                if not target:
                    print_("Node not found: %s" % self.options.node)
                    return
            target.set_install_dir(install_dir=self.options.install_dir, version=self.options.version, verbose=True)
        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)


class ClusterClearrepoCmd(Cmd):

    descr_text = "Cleanup downloaded cassandra sources"
    usage = "usage: ccm clearrepo [options]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args)

    def run(self):
        repository.clean_all()


class ClusterStartCmd(Cmd):

    options_list = [
        (['-v', '--verbose'], {'action': "store_true", 'dest': "verbose", 'help': "Print standard output of cassandra process", 'default': False}),
        (['--no-wait'], {'action': "store_true", 'dest': "no_wait", 'help': "Do not wait for cassandra node to be ready. Overrides all other wait options.", 'default': False}),
        # This option (wait-other-notice) is now deprecated, as it was never respected
        (['--wait-other-notice'], {'action': "store_true", 'dest': "deprecate", 'help': "DEPRECATED/IGNORED: Use '--skip-wait-other-notice' instead. This is now on by default.", 'default': False}),
        (['--skip-wait-other-notice'], {'action': "store_false", 'dest': "wait_other_notice", 'help': "Skip waiting until all live nodes of the cluster have marked the other nodes UP", 'default': True}),
        (['--wait-for-binary-proto'], {'action': "store_true", 'dest': "wait_for_binary_proto", 'help': "Wait for the binary protocol to start", 'default': False}),
        (['--jvm_arg'], {'action': "append", 'dest': "jvm_args", 'help': "Specify a JVM argument", 'default': []}),
        (['--profile'], {'action': "store_true", 'dest': "profile", 'help': "Start the nodes with yourkit agent (only valid with -s)", 'default': False}),
        (['--profile-opts'], {'type': "string", 'action': "store", 'dest': "profile_options", 'help': "Yourkit options when profiling", 'default': None}),
        (['--quiet-windows'], {'action': "store_true", 'dest': "quiet_start", 'help': "Pass -q on Windows 2.2.4+ and 3.0+ startup. Ignored on linux.", 'default': False}),
        (['--root'], {'action': "store_true", 'dest': "allow_root", 'help': "Allow CCM to start cassandra as root", 'default': False}),
        (['--jvm-version'], {'type': "int", 'dest': "jvm_version", 'help': "Specify the JVM version to use (e.g. 8 for Java 8)", 'default': None}),
    ]
    descr_text = "Start all the non started nodes of the current cluster"
    usage = "usage: ccm cluster start [options]"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        if self.options.deprecate:
            print_("WARN: --wait-other-notice is deprecated. Please see the help text.")
        if self.options.no_wait and (self.options.wait_for_binary_proto or self.options.deprecate):
            print_("ERROR: --no-wait was specified alongside one or more wait options. This is invalid.")
            exit(1)

    def run(self):
        try:
            profile_options = None
            if self.options.profile:
                profile_options = {}
                if self.options.profile_options:
                    profile_options['options'] = self.options.profile_options

            if len(self.cluster.nodes) == 0:
                print_("No node in this cluster yet. Use the populate command before starting.")
                exit(1)

            if self.cluster.start(no_wait=self.options.no_wait,
                                  wait_other_notice=self.options.wait_other_notice,
                                  wait_for_binary_proto=self.options.wait_for_binary_proto,
                                  verbose=self.options.verbose,
                                  jvm_args=self.options.jvm_args,
                                  profile_options=profile_options,
                                  quiet_start=self.options.quiet_start,
                                  allow_root=self.options.allow_root,
                                  jvm_version=self.options.jvm_version) is None:
                details = ""
                if not self.options.verbose:
                    details = " (you can use --verbose for more information)"
                print_("Error starting nodes, see above for details%s" % details, file=sys.stderr)
                exit(1)
        except NodeError as e:
            print_(str(e), file=sys.stderr)
            print_("Standard error output is:", file=sys.stderr)
            for line in e.process.stderr_file.readlines():
                print_(line.rstrip('\n'), file=sys.stderr)
            exit(1)


class ClusterStopCmd(Cmd):

    options_list = [
        (['-v', '--verbose'], {'action': "store_true", 'dest': "verbose", 'help': "Print nodes that were not running", 'default': False}),
        (['--no-wait'], {'action': "store_true", 'dest': "no_wait", 'help': "Do not wait for the node to be stopped", 'default': False}),
        (['-g', '--gently'], {'action': "store_const", 'dest': "signal_event", 'help': "Shut down gently (default)", 'const': signal.SIGTERM, 'default': signal.SIGTERM}),
        (['--hang-up'], {'action': "store_const", 'dest': "signal_event", 'help': "Shut down via hang up (kill -1)", 'const': get_default_signals()['1']}),
        (['--not-gently'], {'action': "store_const", 'dest': "signal_event", 'help': "Shut down immediately (kill -9)", 'const': get_default_signals()['9']}),
    ]
    descr_text = "Stop all the nodes of the cluster"
    usage = "usage: ccm cluster stop [options] name"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        try:
            not_running = self.cluster.stop(wait=not self.options.no_wait, signal_event=self.options.signal_event)
            if self.options.verbose and len(not_running) > 0:
                sys.stdout.write("The following nodes were not running: ")
                for node in not_running:
                    sys.stdout.write(node.name + " ")
                print_("")
        except NodeError as e:
            print_(str(e), file=sys.stderr)
            exit(1)


class _ClusterNodetoolCmd(Cmd):
    usage = "This is a private class, how did you get here?"
    descr_text = "This is a private class, how did you get here?"
    nodetool_cmd = ''

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

    descr_text = "Run stress using all live nodes"
    usage = "usage: ccm stress [options] [stress_options]"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.stress_options = args + parser.get_ignored()

    def run(self):
        try:
            rc = self.cluster.stress(self.stress_options)
            exit(rc)
        except Exception as e:
            print_(e, file=sys.stderr)
            exit(1)


class ClusterUpdateconfCmd(Cmd):

    options_list = [
        (['--no-hh', '--no-hinted-handoff'], {'action': "store_false", 'dest': "hinted_handoff", 'default': True, 'help': "Disable hinted handoff"}),
        (['--batch-cl', '--batch-commit-log'], {'action': "store_true", 'dest': "cl_batch", 'default': None, 'help': "Set commit log to batch mode"}),
        (['--periodic-cl', '--periodic-commit-log'], {'action': "store_true", 'dest': "cl_periodic", 'default': None, 'help': "Set commit log to periodic mode"}),
        (['--rt', '--rpc-timeout'], {'action': "store", 'type': 'int', 'dest': "rpc_timeout", 'help': "Set rpc timeout"}),
        (['-y', '--yaml'], {'action': "store_true", 'dest': "literal_yaml", 'default': False, 'help': "If enabled, treat argument as yaml, not kv pairs. Option syntax looks like ccm updateconf -y 'a: [b: [c,d]]'"}),
    ]
    descr_text = "Update the cassandra config files for all nodes"
    usage = "usage: ccm updateconf [options] [ new_setting | ...  ], where new_setting should be a string of the form 'compaction_throughput_mb_per_sec: 32'; nested options can be separated with a period like 'client_encryption_options.enabled: false'"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
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
            if self.cluster.cassandra_version() < "1.2":
                self.setting['rpc_timeout_in_ms'] = self.options.rpc_timeout
            else:
                self.setting['read_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['range_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['write_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['truncate_request_timeout_in_ms'] = self.options.rpc_timeout
                self.setting['request_timeout_in_ms'] = self.options.rpc_timeout
        self.cluster.set_configuration_options(values=self.setting)
        if self.options.cl_batch:
            self.cluster.set_batch_commitlog(True)
        if self.options.cl_periodic:
            self.cluster.set_batch_commitlog(False)


class ClusterUpdatedseconfCmd(Cmd):

    options_list = [
        (['-y', '--yaml'], {'action': "store_true", 'dest': "literal_yaml", 'default': False, 'help': "Pass in literal yaml string. Option syntax looks like ccm updatedseconf -y 'a: [b: [c,d]]'"}),
    ]
    descr_text = "Update the dse config files for all nodes"
    usage = "usage: ccm updatedseconf [options] [ new_setting | ...  ], where new_setting should be a string of the form 'max_solr_concurrency_per_core: 2'; nested options can be separated with a period like 'cql_slow_log_options.enabled: true'"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        try:
            self.setting = common.parse_settings(args, literal_yaml=self.options.literal_yaml)
        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)

    def run(self):
        self.cluster.set_dse_configuration_options(values=self.setting)

#
# Class implements the functionality of updating log4j-server.properties
# on ALL nodes by copying the given config into
# ~/.ccm/name-of-cluster/nodeX/conf/log4j-server.properties
#


class ClusterUpdatelog4jCmd(Cmd):

    options_list = [
        (['-p', '--path'], {'type': "string", 'dest': "log4jpath", 'help': "Path to new Cassandra log4j configuration file"}),
    ]
    descr_text = "Update the Cassandra log4j-server.properties configuration file on all nodes"
    usage = "usage: ccm updatelog4j -p <log4j config>"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        try:
            self.log4jpath = options.log4jpath
            if self.log4jpath is None:
                raise KeyError("[Errno] -p or --path <path of new log4j congiguration file> is not provided")
        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)
        except KeyError as e:
            print_(str(e), file=sys.stderr)
            exit(1)

    def run(self):
        try:
            self.cluster.update_log4j(self.log4jpath)
        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)


class ClusterBulkloadCmd(Cmd):

    descr_text = "Bulkload files into the cluster by connecting to some live node (if any)"
    usage = "usage: ccm bulkload [options] [sstable_dir]"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.loader_options = parser.get_ignored() + args

    def run(self):
        self.cluster.bulkload(self.loader_options)


class ClusterScrubCmd(Cmd):

    descr_text = "Scrub files"
    usage = "usage: ccm scrub [options] <keyspace> <cf>"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.scrub_options = parser.get_ignored() + args

    def run(self):
        self.cluster.scrub(self.scrub_options)


class ClusterVerifyCmd(Cmd):

    descr_text = "Verify files"
    usage = "usage: ccm verify [options] <keyspace> <cf>"
    ignore_unknown_options = True

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.verify_options = parser.get_ignored() + args

    def run(self):
        self.cluster.verify(self.verify_options)


class ClusterSetlogCmd(Cmd):

    options_list = [
        (['-c', '--class'], {'type': "string", 'dest': "class_name", 'default': None, 'help': "Optional java class/package. Logging will be set for only this class/package if set"}),
    ]
    descr_text = "Set log level (INFO, DEBUG, ...) with/without Java class for all node of the cluster - require a node restart"
    usage = "usage: ccm setlog [options] level"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        if len(args) == 0:
            print_('Missing log level', file=sys.stderr)
            parser.print_help()
            exit(1)
        self.level = args[0]

    def run(self):
        try:
            self.cluster.set_log_level(self.level, self.options.class_name)
        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)


class ClusterInvalidatecacheCmd(Cmd):

    descr_text = "Destroys ccm's local git cache."
    usage = "usage: ccm invalidatecache"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args)

    def run(self):
        try:
            common.invalidate_cache()
        except Exception as e:
            print_(str(e), file=sys.stderr)
            print_("Error while deleting cache. Please attempt manually.")
            exit(1)


class ClusterChecklogerrorCmd(Cmd):

    descr_text = "Check for errors in log file of each node."
    usage = "usage: ccm checklogerror"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        for node in self.cluster.nodelist():
            errors = node.grep_log_for_errors()
            for mylist in errors:
                for line in mylist:
                    print_(line)


class ClusterShowlastlogCmd(Cmd):

    descr_text = "Show the last.log for the most recent build through your $PAGER"
    usage = "usage: ccm showlastlog"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        log = repository.lastlogfilename()
        pager = os.environ.get('PAGER', common.platform_pager())
        os.execvp(pager, (pager, log))


class ClusterJconsoleCmd(Cmd):

    descr_text = "Opens jconsole client and connects to all running nodes"
    usage = "usage: ccm jconsole"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        cmds = ["jconsole"] + ["localhost:%s" % node.jmx_port for node in self.cluster.nodes.values()]
        try:
            subprocess.call(cmds, stderr=sys.stderr)
        except OSError:
            print_("Could not start jconsole. Please make sure jconsole can be found in your $PATH.")
            exit(1)


class ClusterSetworkloadCmd(Cmd):

    descr_text = "Sets the workloads for a DSE cluster"
    usage = "usage: ccm setworkload [cassandra|solr|hadoop|spark|dsefs|cfs|graph],..."

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)
        self.workloads = args[0].split(',')
        valid_workloads = ['cassandra', 'solr', 'hadoop', 'spark', 'dsefs', 'cfs', 'graph']
        for workload in self.workloads:
            if workload not in valid_workloads:
                print_(workload, ' is not a valid workload')
                exit(1)

    def run(self):
        try:
            if len(self.cluster.nodes) == 0:
                print_("No node in this cluster yet. Use the populate command before starting.")
                exit(1)
            for node in list(self.cluster.nodes.values()):
                node.set_workloads(workloads=self.workloads)
        except common.ArgumentError as e:
            print_(str(e), file=sys.stderr)
            exit(1)

class ClusterEnableaossCmd(Cmd):

    descr_text = "Enable DSE's Always On SparkSQL Server"
    usage = "usage: ccm enableaoss"

    def validate(self, parser, options, args):
        Cmd.validate(self, parser, options, args, load_cluster=True)

    def run(self):
        try:
            self.cluster.enable_aoss()
        except Exception as e:
            print_(str(e), file=sys.stderr)
            exit(1)

