from ccmlib.cluster import Cluster
from ccmlib.node import Node
from . import TEST_DIR
from . import ccmtest

CLUSTER_PATH = TEST_DIR


class TestCCMIssues(ccmtest.Tester):

    def issue_150_test(self):
        self.cluster = Cluster(CLUSTER_PATH, "150", cassandra_version='2.0.9')
        self.cluster.populate([1, 2], use_vnodes=True)
        self.cluster.start()
        dcs = [node.data_center for node in self.cluster.nodelist()]
        dcs.append('dc2')

        node4 = Node('node4', self.cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000),
                     '7400', '2000', None)
        self.cluster.add(node4, False, 'dc2')
        node4.start()

        dcs_2 = [node.data_center for node in self.cluster.nodelist()]
        self.assertListEqual(dcs, dcs_2)
        node4.nodetool('status')
