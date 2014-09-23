# ccm node
from __future__ import with_statement

import os
import tempfile

from ccmlib import common
from ccmlib.node import Node

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
        Node.__init__(self, 'bulkloader', cluster, False, (addr, 9160), (addr, 7000), str(9042), 2000, None)

    def get_path(self):
        return os.path.join(self.path, self.name)

    def load(self, options):
        for itf in self.network_interfaces.values():
            if itf:
                common.check_socket_available(itf)

        cdir = self.get_install_dir()
        loader_bin = common.join_bin(cdir, 'bin', 'sstableloader')
        env = common.make_cassandra_env(cdir, self.get_path())
        if not "-d" in options:
            l = [ node.network_interfaces['storage'][0] for node in self.cluster.nodes.values() if node.is_live() ]
            options = [ "-d",  ",".join(l) ] + options
        #print "Executing with", options
        os.execve(loader_bin, [ common.platform_binary('sstableloader') ] + options, env)
