import sys
sys.path = [".."] + sys.path

from . import TEST_DIR
from ccmlib.cluster import Cluster
from ccmtest import Tester

CLUSTER_PATH = TEST_DIR

class TestCCMLib(Tester):

    def simple_test(self, version='2.0.9'):
        self.cluster = Cluster(CLUSTER_PATH, "simple", cassandra_version=version)
        self.cluster.populate(3)
        self.cluster.start()
        node1, node2, node3 = self.cluster.nodelist()

        if version < '2.1':
            node1.stress()
        else:
            node1.stress(['write'])

        self.cluster.flush()

    def simple_test_across_versions(self):
        self.simple_test(version='1.2.18')
        self.cluster.remove()

        self.simple_test(version='2.0.9')
        self.cluster.remove()

        self.simple_test(version='2.1.0-rc5')

    def test1(self):
        self.cluster = Cluster(CLUSTER_PATH, "test1", cassandra_version='2.0.3')
        self.cluster.show(False)
        self.cluster.populate(2)
        self.cluster.set_partitioner("Murmur3")
        self.cluster.start()
        self.cluster.set_configuration_options(None, None)
        self.cluster.set_configuration_options({}, True)
        self.cluster.set_configuration_options({"a": "b"}, False)

        [node1, node2] = self.cluster.nodelist()
        node2.compact()
        self.cluster.flush()
        self.cluster.stop()


    def test2(self):
        self.cluster = Cluster(CLUSTER_PATH, "test2", cassandra_version='2.0.3')
        self.cluster.populate(2)
        self.cluster.start()

        self.cluster.set_log_level("ERROR")

        class FakeNode:
            name = "non-existing node"

        self.cluster.remove(FakeNode())
        [node1, node2] = self.cluster.nodelist()
        self.cluster.remove(node1)
        self.cluster.show(True)
        self.cluster.show(False)

        #self.cluster.stress([])
        self.cluster.compact()
        self.cluster.drain()
        self.cluster.stop()


    def test3(self):
        self.cluster = Cluster(CLUSTER_PATH, "test3", cassandra_version='2.0.3')
        self.cluster.populate(2)
        self.cluster.start()
        self.cluster.cleanup()

        self.cluster.clear()
        self.cluster.stop()
