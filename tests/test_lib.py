import sys
import time

from six import StringIO

import ccmlib
from ccmlib.cluster import Cluster
from ccmlib.common import _update_java_version
from ccmlib.node import NodeError
from . import TEST_DIR, ccmtest
from distutils.version import LooseVersion  # pylint: disable=import-error, no-name-in-module

sys.path = [".."] + sys.path

CLUSTER_PATH = TEST_DIR


class TestUpdateJavaVersion(ccmtest.Tester):
    def test_update_java_version(self):
        # Tests for C*-4.0 (Java 8 or 11 or newer)

        result_env = _update_java_version(current_java_version=11, current_java_home_version=11, jvm_version=None,
                                          install_dir=None, cassandra_version=LooseVersion('4.0'),
                                          env={'JAVA_HOME': '/opt/foo/java_home',
                                               'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_4.0_1',
                                          os_env={'CASSANDRA_USE_JDK11': 'true'})
        self.assertEqual({'JAVA_HOME': '/opt/foo/java_home',
                          'PATH': '/some/bin'},
                         result_env)

        result_env = _update_java_version(current_java_version=11, current_java_home_version=11, jvm_version=None,
                                          install_dir=None, cassandra_version=LooseVersion('4.0'),
                                          env={'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_4.0_2',
                                          os_env={'X': '1'})
        self.assertEqual({'PATH': '/some/bin'},
                         result_env)

        result_env = _update_java_version(current_java_version=8, current_java_home_version=8, jvm_version=None,
                                          install_dir=None, cassandra_version=LooseVersion('4.0'),
                                          env={'JAVA_HOME': '/opt/foo/java_home',
                                               'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_4.0_3',
                                          os_env={'X': '1'})
        self.assertEqual({'JAVA_HOME': '/opt/foo/java_home',
                          'PATH': '/some/bin'},
                         result_env)

        result_env = _update_java_version(current_java_version=8, current_java_home_version=8, jvm_version=None,
                                          install_dir=None, cassandra_version=LooseVersion('4.0'),
                                          env={'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_4.0_4',
                                          os_env={'X': '1'})
        self.assertEqual({'PATH': '/some/bin'},
                         result_env)

        result_env = _update_java_version(current_java_version=8, current_java_home_version=8, jvm_version=None,
                                          install_dir=None, cassandra_version=LooseVersion('4.0'),
                                          env={'JAVA_HOME': '/opt/foo/java_home8',
                                               'JAVA8_HOME': '/opt/foo/java_home8',
                                               'JAVA11_HOME': '/opt/foo/java_home11',
                                               'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_4.0_5',
                                          os_env={'X': '1'})
        self.assertEqual({'JAVA_HOME': '/opt/foo/java_home8',
                          'JAVA8_HOME': '/opt/foo/java_home8',
                          'JAVA11_HOME': '/opt/foo/java_home11',
                          'PATH': '/some/bin'},
                         result_env)

        result_env = _update_java_version(current_java_version=8, current_java_home_version=8, jvm_version=None,
                                          install_dir=None, cassandra_version=LooseVersion('4.0'),
                                          env={'JAVA_HOME': '/opt/foo/java_home8',
                                               'JAVA8_HOME': '/opt/foo/java_home8',
                                               'JAVA11_HOME': '/opt/foo/java_home11',
                                               'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_4.0_6',
                                          os_env={'CASSANDRA_USE_JDK11': 'true'})
        self.assertEqual({'JAVA_HOME': '/opt/foo/java_home11',
                          'PATH': '/opt/foo/java_home11/bin:/some/bin',
                          'JAVA8_HOME': '/opt/foo/java_home8',
                          'JAVA11_HOME': '/opt/foo/java_home11'},
                         result_env)

        result_env = _update_java_version(current_java_version=8, current_java_home_version=8, jvm_version=11,
                                          install_dir=None, cassandra_version=LooseVersion('4.0'),
                                          env={'JAVA_HOME': '/opt/foo/java_home8',
                                               'JAVA8_HOME': '/opt/foo/java_home8',
                                               'JAVA11_HOME': '/opt/foo/java_home11',
                                               'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_4.0_7',
                                          os_env={'X': '1'})
        self.assertEqual({'JAVA_HOME': '/opt/foo/java_home11',
                          'PATH': '/opt/foo/java_home11/bin:/some/bin',
                          'JAVA8_HOME': '/opt/foo/java_home8',
                          'JAVA11_HOME': '/opt/foo/java_home11'},
                         result_env)

        result_env = _update_java_version(current_java_version=11, current_java_home_version=11, jvm_version=None,
                                          install_dir=None, cassandra_version=LooseVersion('4.0'),
                                          env={'JAVA_HOME': '/opt/foo/java_home11',
                                               'JAVA8_HOME': '/opt/foo/java_home8',
                                               'JAVA11_HOME': '/opt/foo/java_home11',
                                               'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_4.0_8',
                                          os_env={'X': '1'})
        self.assertEqual({'JAVA_HOME': '/opt/foo/java_home11',
                          'JAVA8_HOME': '/opt/foo/java_home8',
                          'JAVA11_HOME': '/opt/foo/java_home11',
                          'PATH': '/some/bin'},
                         result_env)

        result_env = _update_java_version(current_java_version=11, current_java_home_version=11, jvm_version=None,
                                          install_dir=None, cassandra_version=LooseVersion('4.0'),
                                          env={'JAVA_HOME': '/opt/foo/java_home11',
                                               'JAVA8_HOME': '/opt/foo/java_home8',
                                               'JAVA11_HOME': '/opt/foo/java_home11',
                                               'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_4.0_9',
                                          os_env={'CASSANDRA_USE_JDK11': 'true'})
        self.assertEqual({'JAVA_HOME': '/opt/foo/java_home11',
                          'PATH': '/some/bin',
                          'JAVA8_HOME': '/opt/foo/java_home8',
                          'JAVA11_HOME': '/opt/foo/java_home11'},
                         result_env)

        result_env = _update_java_version(current_java_version=11, current_java_home_version=11, jvm_version=8,
                                          install_dir=None, cassandra_version=LooseVersion('4.0'),
                                          env={'JAVA_HOME': '/opt/foo/java_home11',
                                               'JAVA8_HOME': '/opt/foo/java_home8',
                                               'JAVA11_HOME': '/opt/foo/java_home11',
                                               'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_4.0_10',
                                          os_env={'X': '1'})
        self.assertEqual({'JAVA_HOME': '/opt/foo/java_home8',
                          'PATH': '/opt/foo/java_home8/bin:/some/bin',
                          'JAVA8_HOME': '/opt/foo/java_home8',
                          'JAVA11_HOME': '/opt/foo/java_home11'},
                         result_env)

        result_env = _update_java_version(current_java_version=11, current_java_home_version=11, jvm_version=11,
                                          install_dir=None, cassandra_version=LooseVersion('4.0'),
                                          env={'JAVA_HOME': '/opt/foo/java_home11',
                                               'JAVA8_HOME': '/opt/foo/java_home8',
                                               'JAVA11_HOME': '/opt/foo/java_home11',
                                               'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_4.0_11',
                                          os_env={'X': '1'})
        self.assertEqual({'JAVA_HOME': '/opt/foo/java_home11',
                          'PATH': '/some/bin',
                          'JAVA8_HOME': '/opt/foo/java_home8',
                          'JAVA11_HOME': '/opt/foo/java_home11'},
                         result_env)

        result_env = _update_java_version(current_java_version=11, current_java_home_version=8, jvm_version=None,
                                          install_dir=None, cassandra_version=LooseVersion('4.0'),
                                          env={'JAVA_HOME': '/opt/foo/java_home8',
                                               'PATH': '/some/bin:/opt/foo/java_home11/bin',
                                               'JAVA8_HOME': '/opt/foo/java_home8',
                                               'JAVA11_HOME': '/opt/foo/java_home11'},
                                          for_build=False, info_message='test_update_java_version_4.0_12',
                                          os_env={'X': '1'})
        self.assertEqual({'JAVA_HOME': '/opt/foo/java_home11',
                          'PATH': '/opt/foo/java_home11/bin:/some/bin:/opt/foo/java_home11/bin',
                          'JAVA8_HOME': '/opt/foo/java_home8',
                          'JAVA11_HOME': '/opt/foo/java_home11'},
                         result_env)

        # Tests for pre-C*-4.0 (Java 8 only)

        result_env = _update_java_version(current_java_version=11, current_java_home_version=11, jvm_version=None,
                                          install_dir=None, cassandra_version=LooseVersion('3.11'),
                                          env={'JAVA_HOME': '/opt/foo/java_home',
                                               'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_3.11_1',
                                          os_env={'CASSANDRA_USE_JDK11': 'true'})
        self.assertEqual({'JAVA_HOME': '/opt/foo/java_home',
                          'PATH': '/some/bin'},
                         result_env)

        result_env = _update_java_version(current_java_version=8, current_java_home_version=8, jvm_version=None,
                                          install_dir=None, cassandra_version=LooseVersion('3.11'),
                                          env={'JAVA_HOME': '/opt/foo/java_home8',
                                               'JAVA8_HOME': '/opt/foo/java_home8',
                                               'JAVA11_HOME': '/opt/foo/java_home11',
                                               'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_3.11_2',
                                          os_env={'X': '1'})
        self.assertEqual({'JAVA_HOME': '/opt/foo/java_home8',
                          'JAVA8_HOME': '/opt/foo/java_home8',
                          'JAVA11_HOME': '/opt/foo/java_home11',
                          'PATH': '/some/bin'},
                         result_env)

        result_env = _update_java_version(current_java_version=8, current_java_home_version=8, jvm_version=None,
                                          install_dir=None, cassandra_version=LooseVersion('3.11'),
                                          env={'JAVA_HOME': '/opt/foo/java_home8',
                                               'JAVA8_HOME': '/opt/foo/java_home8',
                                               'JAVA11_HOME': '/opt/foo/java_home11',
                                               'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_3.11_3',
                                          os_env={'CASSANDRA_USE_JDK11': 'true'})
        self.assertEqual({'JAVA_HOME': '/opt/foo/java_home8',
                          'PATH': '/some/bin',
                          'JAVA8_HOME': '/opt/foo/java_home8',
                          'JAVA11_HOME': '/opt/foo/java_home11'},
                         result_env)

        result_env = _update_java_version(current_java_version=11, current_java_home_version=11, jvm_version=None,
                                          install_dir=None, cassandra_version=LooseVersion('3.11'),
                                          env={'JAVA_HOME': '/opt/foo/java_home11',
                                               'JAVA8_HOME': '/opt/foo/java_home8',
                                               'JAVA11_HOME': '/opt/foo/java_home11',
                                               'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_3.11_4',
                                          os_env={'CASSANDRA_USE_JDK11': 'true'})
        self.assertEqual({'JAVA_HOME': '/opt/foo/java_home8',
                          'PATH': '/opt/foo/java_home8/bin:/some/bin',
                          'JAVA8_HOME': '/opt/foo/java_home8',
                          'JAVA11_HOME': '/opt/foo/java_home11'},
                         result_env)

        result_env = _update_java_version(current_java_version=8, current_java_home_version=8, jvm_version=11,
                                          install_dir=None, cassandra_version=LooseVersion('3.11'),
                                          env={'JAVA_HOME': '/opt/foo/java_home8',
                                               'JAVA8_HOME': '/opt/foo/java_home8',
                                               'JAVA11_HOME': '/opt/foo/java_home11',
                                               'PATH': '/some/bin'},
                                          for_build=True, info_message='test_update_java_version_3.11_5',
                                          os_env={'X': '1'})
        self.assertEqual({'JAVA_HOME': '/opt/foo/java_home11',
                          'PATH': '/opt/foo/java_home11/bin:/some/bin',
                          'JAVA8_HOME': '/opt/foo/java_home8',
                          'JAVA11_HOME': '/opt/foo/java_home11'},
                         result_env)


class TestCCMLib(ccmtest.Tester):
    def test2(self):
        self.cluster = Cluster(CLUSTER_PATH, "test2", version='git:trunk')
        self.cluster.populate(2)
        self.cluster.start()

        self.cluster.set_log_level("ERROR")

        class FakeNode:
            name = "non-existing node"

        self.cluster.remove(FakeNode())
        node1 = self.cluster.nodelist()[0]
        self.cluster.remove(node1)
        self.cluster.show(True)
        self.cluster.show(False)

        self.cluster.compact()
        self.cluster.drain()
        self.cluster.stop()

    def test3(self):
        self.cluster = Cluster(CLUSTER_PATH, "test3", version='git:trunk')
        self.cluster.populate(2)
        self.cluster.start()
        node1 = self.cluster.nodelist()[0]
        self.cluster.stress(['write', 'n=100', 'no-warmup', '-rate', 'threads=4'])
        self.cluster.stress(['write', 'n=100', 'no-warmup', '-rate', 'threads=4', '-node', node1.ip_addr])

        self.cluster.clear()
        self.cluster.stop()

    def test_node_start_with_non_default_timeout(self):
        self.cluster = Cluster(CLUSTER_PATH, "nodestarttimeout", cassandra_version='git:trunk')
        self.cluster.populate(1)
        node = self.cluster.nodelist()[0]

        try:
            node.start(wait_for_binary_proto=0)
            self.fail("timeout expected with 0s startup timeout")
        except ccmlib.node.TimeoutError:
            pass
        finally:
            self.cluster.clear()
            self.cluster.stop()

    def test_fast_error_on_cluster_start_failure(self):
        self.cluster = Cluster(CLUSTER_PATH, 'clusterfaststop', cassandra_version='git:trunk')
        self.cluster.populate(1)
        self.cluster.set_configuration_options({'invalid_option': 0})
        self.check_log_errors = False
        node = self.cluster.nodelist()[0]
        start_time = time.time()
        try:
            self.cluster.start(use_vnodes=True)
            self.assertFalse(node.is_running())
            self.assertLess(time.time() - start_time, 30)
        except NodeError:
            self.assertLess(time.time() - start_time, 30)
        finally:
            self.cluster.clear()
            self.cluster.stop()

    def test_fast_error_on_node_start_failure(self):
        self.cluster = Cluster(CLUSTER_PATH, 'nodefaststop', cassandra_version='git:trunk')
        self.cluster.populate(1)
        self.cluster.set_configuration_options({'invalid_option': 0})
        self.check_log_errors = False
        node = self.cluster.nodelist()[0]
        start_time = time.time()
        try:
            node.start(wait_for_binary_proto=45)
            self.assertFalse(node.is_running())
            self.assertLess(time.time() - start_time, 30)
        except NodeError:
            self.assertLess(time.time() - start_time, 30)
        finally:
            self.cluster.clear()
            self.cluster.stop()

    def test_dc_mandatory_on_multidc(self):
        self.cluster = Cluster(CLUSTER_PATH, "mandatorydc", cassandra_version='git:trunk')
        self.cluster.populate([1, 1])

        node3 = self.cluster.create_node(name='node3',
                                         auto_bootstrap=True,
                                         thrift_interface=('127.0.0.3', 9160),
                                         storage_interface=('127.0.0.3', 7000),
                                         jmx_port='7300',
                                         remote_debug_port='0',
                                         initial_token=None,
                                         binary_interface=('127.0.0.3', 9042))
        with self.assertRaisesRegexp(ccmlib.common.ArgumentError, 'Please specify the DC this node should be added to'):
            self.cluster.add(node3, is_seed=False)

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
