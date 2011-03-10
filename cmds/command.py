from optparse import OptionParser
import os, sys, shutil

L = os.path.realpath(__file__).split(os.path.sep)[:-2]
root = os.path.sep.join(L)
sys.path.append(os.path.join(root, 'ccm_lib'))
import common

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
              print 'Missing cluster name'
              parser.print_help()
              exit(1)
          self.name = args[0]
        if node_name:
          if len(args) == 0:
              print 'Missing node name'
              parser.print_help()
              exit(1)
          self.name = args[0]

        if load_cluster:
            self.cluster = common.load_current_cluster(self.path)
            if node_name:
                try:
                    self.node = self.cluster.nodes[self.name]
                except KeyError:
                    print 'Unknown node %s in cluster %s' % (self.name, self.cluster.name)
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
                print '%s does not appear to be a cassandra source directory' % d
                exit(1)

    def run(self):
        pass

    def _get_default_parser(self, usage, description, cassandra_dir=False):
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
