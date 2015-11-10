import sys
sys.path = [".."] + sys.path

import decimal

from . import TEST_DIR
from . import ccmtest
from ccmlib.cluster import Cluster
import ccmlib
from six import StringIO

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


class TestRunCqlsh(ccmtest.Tester):

    def setUp(self):
        '''Create a cluster for cqlsh tests. Assumes that ccmtest.Tester's
        teardown() method will safely stop and remove self.cluster.'''
        self.cluster = Cluster(CLUSTER_PATH, "run_cqlsh",
                               cassandra_version='git:trunk')
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        [self.node] = self.cluster.nodelist()

    def test_run_cqlsh(self):
        '''run_cqlsh works with a simple example input'''
        self.node.run_cqlsh(
            '''
            CREATE KEYSPACE ks WITH replication = { 'class' :'SimpleStrategy', 'replication_factor': 1};
            USE ks;
            CREATE TABLE test (key int PRIMARY KEY);
            INSERT INTO test (key) VALUES (1);
            ''')
        rv = self.node.run_cqlsh('SELECT * from ks.test', return_output=True)
        for s in ['(1 rows)', 'key', '1']:
            self.assertIn(s, rv[0])
        self.assertEqual(rv[1], '')

    def run_cqlsh_printing(self, return_output, show_output):
        '''Parameterized test. Runs run_cqlsh with options to print the output
        and to return it as a string, or with these options combined, depending
        on the values of the arguments.'''
        # redirect run_cqlsh's stdout to a string buffer
        old_stdout, sys.stdout = sys.stdout, StringIO()

        rv = self.node.run_cqlsh('DESCRIBE keyspaces;',
                                 return_output=return_output,
                                 show_output=show_output)

        # put stdout back where it belongs and get the built string value
        sys.stdout, printed_output = old_stdout, sys.stdout.getvalue()

        if return_output:
            # we should see names of system keyspaces
            self.assertIn('system', rv[0])
            # stderr should be empty
            self.assertEqual('', rv[1])
        else:
            # implicitly-returned None
            self.assertEqual(rv, None)

        if show_output:
            self.assertIn('system', printed_output)
        else:
            # nothing should be printed if (not show_output)
            self.assertEqual(printed_output, '')

        if return_output and show_output:
            self.assertEqual(printed_output, rv[0])

    def test_no_output(self):
        self.run_cqlsh_printing(return_output=False, show_output=False)

    def test_print_output(self):
        self.run_cqlsh_printing(return_output=True, show_output=False)

    def test_return_output(self):
        self.run_cqlsh_printing(return_output=False, show_output=True)

    def test_print_and_return_output(self):
        self.run_cqlsh_printing(return_output=True, show_output=True)


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


class TestErrorLogGrepping(ccmtest.Tester):

    def assertGreppedLog(self, log, grepped_log):
        self.assertEqual(ccmlib.node._grep_log_for_errors(log), grepped_log)

    def test_basic_error_message(self):
        err = 'ERROR: You messed up'
        self.assertGreppedLog(err, [[err]])

    def test_error_message_with_timestamp(self):
        err = '2015-05-12 14:12:12,720 ERROR: You messed up'
        self.assertGreppedLog(err, [[err]])

    def test_filter_debug_lines(self):
        err = 'DEBUG: harmless warning message\n'
        self.assertGreppedLog(err, [])

    def test_ignore_empty_lines(self):
        err = ('\n'
               'ERROR: Node unavailable')
        self.assertGreppedLog(err, [['ERROR: Node unavailable']])

    def test_ignore_debug_lines_containing_error(self):
        err = 'DEBUG: another process raised: ERROR: abandon hope!\n'
        self.assertGreppedLog(err, [])

    def test_coalesces_stack_trace_lines(self):
        err = ('ERROR: You have made a terrible mistake\n'
               '  And here are more details on what you did\n'
               'saints preserve us')
        self.assertGreppedLog(err,
                              [['ERROR: You have made a terrible mistake',
                                '  And here are more details on what you did',
                                'saints preserve us']])

    def test_multiple_errors(self):
        err = ('ERROR: You have made a terrible mistake\n'
               '  And here are more details on what you did\n'
               'INFO: Node joined ring\n'
               'ERROR: not again!')
        self.assertGreppedLog(err,
                              [['ERROR: You have made a terrible mistake',
                                '  And here are more details on what you did'],
                               ['ERROR: not again!']])

    def test_does_not_coalesce_info_lines(self):
        err = ('ERROR: You have made a terrible mistake\n'
               '  2015-05-12 14:12:12,720 INFO: why would you ever do that\n')
        self.assertGreppedLog(err, [['ERROR: You have made a terrible mistake']])
