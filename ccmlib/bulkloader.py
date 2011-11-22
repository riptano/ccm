# ccm node
from __future__ import with_statement
import tempfile, os, common, subprocess
from node import Node

# We reuse Node because the bulkloader basically needs all the same files,
# even though this is not a real node. But truth is, this is an afterthough,
# so be careful when using this object. This will need some cleanup someday
class BulkLoader(Node):
    def __init__(self, cluster):
        # A lot of things are wrong in that method. It assumes that the ip
        # 127.0.0.<nbnode> is free and use standard ports without asking.
        # It should problably be fixed, but will be good enough for now.
        addr = '127.0.0.%d' % (len(cluster.nodes) + 1)
        self.path = tempfile.mkdtemp(prefix='bulkloader-')
        Node.__init__(self, 'bulkloader', cluster, False, (addr, 9160), (addr, 7000), str(8000), None)

    def get_path(self):
        return os.path.join(self.path, self.name)

    def load(self, options):
        for itf in self.network_interfaces.values():
            common.check_socket_available(itf)

        cdir = self.get_cassandra_dir()
        loader_bin = os.path.join(cdir, 'bin', 'sstableloader')
        env = common.make_cassandra_env(cdir, self.get_path())
        print "Executing with", options
        os.execve(loader_bin, [ 'sstableloader' ] + options, env)
