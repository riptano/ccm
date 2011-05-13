from optparse import OptionParser, BadOptionError, Option
import os, sys, shutil

L = os.path.realpath(__file__).split(os.path.sep)[:-2]
root = os.path.sep.join(L)
sys.path.append(os.path.join(root, 'ccm_lib'))
import common


# This is fairly fragile, but handy for now
class ForgivingParser(OptionParser):
    def __init__(self, usage=None, option_list=None, option_class=Option, version=None, conflict_handler="error", description=None, formatter=None, add_help_option=True, prog=None, epilog=None):
        OptionParser.__init__(self, usage, option_list, option_class, version, conflict_handler, description, formatter, add_help_option, prog, epilog)
        self.ignored = []

    def _process_short_opts(self, rargs, values):
        opt = rargs[0]
        try:
            OptionParser._process_short_opts(self, rargs, values)
        except BadOptionError as e:
            self.ignored.append(opt)
            self.eat_args(rargs)

    def _process_long_opt(self, rargs, values):
        opt = rargs[0]
        try:
            OptionParser._process_long_opt(self, rargs, values)
        except BadOptionError as e:
            self.ignored.append(opt)
            self.eat_args(rargs)

    def eat_args(self, rargs):
        while len(rargs) > 0 and rargs[0][0] != '-':
            self.ignored.append(rargs.pop(0))

    def get_ignored(self):
        return self.ignored

class Cmd(object):
    def get_parser(self):
        pass

    def validate(self, parser, options, args, cluster_name=False, node_name=False, load_cluster=False):
        self.options = options
        self.args = args
        if options.config_dir is None:
            self.path = common.get_default_path()
        else:
            self.path = options.config_dir

        if cluster_name:
          if len(args) == 0:
              print >> sys.stderr, 'Missing cluster name'
              parser.print_help()
              exit(1)
          self.name = args[0]
        if node_name:
          if len(args) == 0:
              print >> sys.stderr, 'Missing node name'
              parser.print_help()
              exit(1)
          self.name = args[0]

        if load_cluster:
            self.cluster = common.load_current_cluster(self.path)
            if node_name:
                try:
                    self.node = self.cluster.nodes[self.name]
                except KeyError:
                    print >> sys.stderr, 'Unknown node %s in cluster %s' % (self.name, self.cluster.name)
                    exit(1)

        if hasattr(self, 'use_cassandra_dir'):
            d = options.cassandra_dir
            bin_dir = os.path.join(d, common.CASSANDRA_BIN_DIR)
            conf_dir = os.path.join(d, common.CASSANDRA_CONF_DIR)
            cnd = os.path.exists(bin_dir)
            cnd = cnd and os.path.exists(conf_dir)
            cnd = cnd and os.path.exists(os.path.join(conf_dir, common.CASSANDRA_CONF))
            cnd = cnd and os.path.exists(os.path.join(conf_dir, common.LOG4J_CONF))
            if not cnd:
                print >> sys.stderr, '%s does not appear to be a cassandra source directory' % d
                exit(1)

    def run(self):
        pass

    def _get_default_parser(self, usage, description, cassandra_dir=False, ignore_unknown_options=False):
        if ignore_unknown_options:
            parser = ForgivingParser(usage=usage, description=description)
        else:
            parser = OptionParser(usage=usage, description=description)
        parser.add_option('--config-dir', type="string", dest="config_dir",
            help="Directory for the cluster files [default to ~/.ccm]")
        if cassandra_dir:
            parser.add_option("--cassandra-dir", type="string", dest="cassandra_dir",
                    help="Path to the cassandra directory to use [default %default]", default="./")
            self.use_cassandra_dir = True
        return parser

    def description():
        return ""
