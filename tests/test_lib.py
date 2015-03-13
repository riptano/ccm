import sys
sys.path = [".."] + sys.path

import decimal

from . import TEST_DIR
from . import ccmtest
from ccmlib.cluster import Cluster
import ccmlib

CLUSTER_PATH = TEST_DIR

class TestCCMLib(ccmtest.Tester):

    def simple_test(self, version='2.0.9'):
        self.cluster = Cluster(CLUSTER_PATH, "simple", cassandra_version=version)
        self.cluster.populate(3)
        self.cluster.start()
        node1, node2, node3 = self.cluster.nodelist()

        if version < '2.1':
            node1.stress()
        else:
            node1.stress(['write', 'n=1000000'])

        self.cluster.flush()

    def simple_test_across_versions(self):
        self.simple_test(version='1.2.18')
        self.cluster.remove()

        self.simple_test(version='2.0.9')
        self.cluster.remove()

        self.simple_test(version='2.1.0-rc5')

    def restart_test(self):
        self.cluster = Cluster(CLUSTER_PATH, "restart", cassandra_version='2.0.9')
        self.cluster.populate(3)
        self.cluster.start()

        self.cluster.stop()
        self.cluster.start()

        self.cluster.show(True)

    def multi_dc_test(self):
        self.cluster = Cluster(CLUSTER_PATH, "multi_dc", cassandra_version='2.0.9')
        self.cluster.populate([1, 2])
        self.cluster.start()
        dcs = [node.data_center for node in self.cluster.nodelist()]
        self.cluster.set_configuration_options(None, None)

        self.cluster.stop()
        self.cluster.start()

        dcs_2 = [node.data_center for node in self.cluster.nodelist()]
        self.assertListEqual(dcs, dcs_2)

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

class TestNodeLoad(ccmtest.Tester):
    def test_rejects_multiple_load_lines(self):
        info = 'Load : 699 KB\nLoad : 35 GB'
        with self.assertRaises(RuntimeError):
            ccmlib.node._get_load_from_info_output(info)

    def test_rejects_unexpected_units(self):
        infos = ['Load : 200 PB', 'Load : 12 Parsecs']

        for info in infos:
            with self.assertRaises(RuntimeError):
                ccmlib.node._get_load_from_info_output(info)

    def test_gets_correct_value(self):
        info_value = [('Load : 328.45 KB', decimal.Decimal('328.45')),
                      ('Load : 295.72 MB', decimal.Decimal('295.72') * 1024),
                      ('Load : 183.79 GB',
                       decimal.Decimal('183.79') * 1024 * 1024),
                      ('Load : 82.333 TB',
                       decimal.Decimal('82.333') * 1024 * 1024 * 1024)]

        for info, value in info_value:
            self.assertEqual(ccmlib.node._get_load_from_info_output(info),
                             value)

    def test_with_full_info_output(self):
        data = ('ID                     : 82800bf3-8c1a-4355-9b72-e19aa61d9fba\n'
                'Gossip active          : true\n'
                'Thrift active          : true\n'
                'Native Transport active: true\n'
                'Load                   : 247.59 MB\n'
                'Generation No          : 1426190195\n'
                'Uptime (seconds)       : 526\n'
                'Heap Memory (MB)       : 222.83 / 495.00\n'
                'Off Heap Memory (MB)   : 1.16\n'
                'Data Center            : dc1\n'
                'Rack                   : r1\n'
                'Exceptions             : 0\n'
                'Key Cache              : entries 41, size 3.16 KB, capacity 24 MB, 19 hits, 59 requests, 0.322 recent hit rate, 14400 save period in seconds\n'
                'Row Cache              : entries 0, size 0 bytes, capacity 0 bytes, 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds\n'
                'Counter Cache          : entries 0, size 0 bytes, capacity 12 MB, 0 hits, 0 requests, NaN recent hit rate, 7200 save period in seconds\n'
                'Token                  : -9223372036854775808\n')
        self.assertEqual(ccmlib.node._get_load_from_info_output(data),
                         decimal.Decimal('247.59') * 1024)
