import sys

from six import StringIO

import ccmlib
from ccmlib.cluster import Cluster

from . import TEST_DIR, ccmtest

sys.path = [".."] + sys.path



CLUSTER_PATH = TEST_DIR


class TestCCMLib(ccmtest.Tester):
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

class TestNodeLoad(ccmtest.Tester):

    def test_rejects_multiple_load_lines(self):
        info = 'Load : 699 KiB\nLoad : 35 GiB'
        with self.assertRaises(RuntimeError):
            ccmlib.node._get_load_from_info_output(info)

        info = 'Load : 699 KB\nLoad : 35 GB'
        with self.assertRaises(RuntimeError):
            ccmlib.node._get_load_from_info_output(info)

    def test_rejects_unexpected_units(self):
        infos = ['Load : 200 PiB', 'Load : 200 PB', 'Load : 12 Parsecs']

        for info in infos:
            with self.assertRaises(RuntimeError):
                ccmlib.node._get_load_from_info_output(info)

    def test_gets_correct_value(self):
        info_value = [('Load : 328.45 KiB', 328.45),
                      ('Load : 328.45 KB', 328.45),
                      ('Load : 295.72 MiB', 295.72 * 1024),
                      ('Load : 295.72 MB', 295.72 * 1024),
                      ('Load : 183.79 GiB', 183.79 * 1024 * 1024),
                      ('Load : 183.79 GB', 183.79 * 1024 * 1024),
                      ('Load : 82.333 TiB', 82.333 * 1024 * 1024 * 1024),
                      ('Load : 82.333 TB', 82.333 * 1024 * 1024 * 1024)]

        for info, value in info_value:
            self.assertEqual(ccmlib.node._get_load_from_info_output(info),
                             value)

    def test_with_full_info_output(self):
        data = ('ID                     : 82800bf3-8c1a-4355-9b72-e19aa61d9fba\n'
                'Gossip active          : true\n'
                'Thrift active          : true\n'
                'Native Transport active: true\n'
                'Load                   : 247.59 MiB\n'
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
                         247.59 * 1024)

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
                         247.59 * 1024)


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

    def test_consecutive_errors(self):
        err = ('ERROR: You have made a terrible mistake\n'
               '  And here are more details on what you did\n'
               'INFO: Node joined ring\n'
               'ERROR: not again!\n'
               '  And, yup, here is some more details\n'
               'ERROR: ugh, and a third one!')
        self.assertGreppedLog(err,
                              [['ERROR: You have made a terrible mistake',
                                '  And here are more details on what you did'],
                               ['ERROR: not again!',
                                '  And, yup, here is some more details'],
                               ['ERROR: ugh, and a third one!']])

    def test_does_not_coalesce_info_lines(self):
        err = ('ERROR: You have made a terrible mistake\n'
               '  2015-05-12 14:12:12,720 INFO: why would you ever do that\n')
        self.assertGreppedLog(err, [['ERROR: You have made a terrible mistake']])

    def test_finds_exceptions_logged_as_warn(self):
        line1 = ('WARN  [ReadStage-2] 2017-03-20 13:29:39,165  AbstractLocalAwareExecutorService.java:167 - '
                 'Uncaught exception on thread Thread[ReadStage-2,5,main]: {} java.lang.AssertionError: Lower bound '
                 '[INCL_START_BOUND(HistLoss, -9223372036854775808, -9223372036854775808) ]is bigger than '
                 'first returned value')

        line2 = ('WARN  [MessagingService-Incoming-/IP] 2017-05-26 19:27:11,523 IncomingTcpConnection.java:101 - '
                 'UnknownColumnFamilyException reading from socket; closing org.apache.cassandra.db.'
                 'UnknownColumnFamilyException: Couldnt find table for cfId 922b7940-3a65-11e7-adf3-a3ff55d9bcf1. '
                 'If a table was just created, this is likely due to the schema not being fully propagated.  '
                 'Please wait for schema agreement on table creation.')

        line3 = 'WARN oh no there was an error, it failed, with a failure'  # dont care for this one

        line4 = 'WARN there was an exception!!!'

        err = '\n'.join([line1, line2, line3, line4])
        self.assertGreppedLog(err, [[line1], [line2], [line4]])
