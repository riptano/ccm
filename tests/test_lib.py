import sys
sys.path = [".."] + sys.path

from . import TEST_DIR
from ccmlib.cluster import Cluster

CLUSTER_PATH = TEST_DIR


def test1():
    cluster = Cluster(CLUSTER_PATH, "test1", cassandra_version='2.0.3')
    cluster.show(False)
    cluster.populate(2)
    cluster.set_partitioner("Murmur3")
    cluster.start()
    cluster.set_configuration_options(None, None)
    cluster.set_configuration_options({}, True)
    cluster.set_configuration_options({"a": "b"}, False)

    [node1, node2] = cluster.nodelist()
    node2.compact()
    cluster.flush()
    cluster.remove()
    cluster.stop()


def test2():
    cluster = Cluster(CLUSTER_PATH, "test2", cassandra_version='2.0.3')
    cluster.populate(2)
    cluster.start()

    cluster.set_log_level("ERROR")

    class FakeNode:
        name = "non-existing node"

    cluster.remove(FakeNode())
    [node1, node2] = cluster.nodelist()
    cluster.remove(node1)
    cluster.show(True)
    cluster.show(False)

    #cluster.stress([])
    cluster.compact()
    cluster.drain()
    cluster.stop()


def test3():
    cluster = Cluster(CLUSTER_PATH, "test3", cassandra_version='2.0.3')
    cluster.populate(2)
    cluster.start()
    cluster.cleanup()

    cluster.clear()
    cluster.stop()
