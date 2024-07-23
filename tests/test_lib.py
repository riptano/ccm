import os
import sys
import tempfile
import time
from distutils.version import LooseVersion  # pylint: disable=import-error, no-name-in-module
from pathlib import Path

import pytest
import requests
from six import StringIO

import ccmlib
from ccmlib.cluster import Cluster
from ccmlib.common import _update_java_version, get_supported_jdk_versions_from_dist, get_supported_jdk_versions, get_available_jdk_versions
from ccmlib.node import NodeError
from . import TEST_DIR, ccmtest
from distutils.version import LooseVersion  # pylint: disable=import-error, no-name-in-module

sys.path = [".."] + sys.path

CLUSTER_PATH = TEST_DIR


class TestUpdateJavaVersion(ccmtest.Tester):
    env = dict()
    temp_dir = None
    all_versions = ['2.2', '3.0', '3.1', '3.11', '4.0', '4.1', '5.0', '5.1']

    # prepare for tests
    def setUp(self):
        # create a directory in temp location
        self.temp_dir = tempfile.TemporaryDirectory()

        # create fake java distribution directories for 7, 8, 11, 17, and 21 in temp directory
        for jvm_version in [7, 8, 11, 17, 21]:
            path = self.temp_dir.name + '/java{}'.format(jvm_version)
            Path(path + '/bin').mkdir(parents=True, exist_ok=True)
            # create java executable with a script returning java version
            full_version = '{}.0.{}'.format(jvm_version, jvm_version * 2)
            build_version = jvm_version * 3
            with open(path + '/bin/java', 'w') as f:
                f.write('#!/bin/bash\n')
                # there must be an only parameter '-version' in the command line
                f.write('if [ "$1" != "-version" ]; then\n')
                f.write('  exit 1\n')
                f.write('fi\n')
                f.write('echo \'openjdk version "{}" 2023-04-18\'\n'.format(full_version))
                f.write('echo \'OpenJDK Runtime Environment Temurin-{}+{} (build {}+{})\'\n'.format(full_version, build_version, full_version, build_version))
                f.write('echo \'OpenJDK 64-Bit Server VM Temurin-{}+{} (build {}+{}, mixed mode)\'\n'.format(full_version, build_version, full_version, build_version))
            # make the script executable
            os.chmod(path + '/bin/java', 0o755)
            self.env['JAVA{}_HOME'.format(jvm_version)] = path

    def _make_cassandra_install_dir(self, git_branch, is_source_dist):
        dist_dir = '{}/cassandra/{}'.format(self.temp_dir.name, git_branch)
        Path(dist_dir + "/bin").mkdir(parents=True, exist_ok=True)

        if is_source_dist:
            if not Path(dist_dir + '/build.xml').exists():
                with open(dist_dir + '/build.xml', 'w') as f:
                    r = requests.get('https://raw.githubusercontent.com/apache/cassandra/{}/build.xml'.format(git_branch))
                    f.write(r.text)
        else:
            if Path(dist_dir + '/build.xml').exists():
                os.remove(dist_dir + '/build.xml')

        if not Path(dist_dir + '/bin/cassandra.in.sh').exists():
            with open(dist_dir + '/bin/cassandra.in.sh', 'w') as f:
                r = requests.get('https://raw.githubusercontent.com/apache/cassandra/{}/bin/cassandra.in.sh'.format(git_branch))
                f.write(r.text)

        return dist_dir

    def _make_env(self, java_home_version=None, java_path_version=None, include_homes=None):
        env = dict()
        if include_homes:
            for v in include_homes:
                key = 'JAVA{}_HOME'.format(v)
                env[key] = self.env[key]
        else:
            env = self.env.copy()
        if java_home_version is not None:
            env['JAVA_HOME'] = self.env['JAVA{}_HOME'.format(java_home_version)]
        if java_path_version is not None:
            env['PATH'] = self.env['JAVA{}_HOME'.format(java_path_version)] + "/bin:/some/path"
        return env

    def _check_env(self, result_env, expected_java_version):
        self.assertIn('JAVA_HOME', result_env)
        self.assertIn('PATH', result_env)
        self.assertEqual(self.env['JAVA{}_HOME'.format(expected_java_version)], result_env['JAVA_HOME'])
        self.assertIn('{}/bin'.format(self.env['JAVA{}_HOME'.format(expected_java_version)]), result_env['PATH'])

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_supported_jdk_versions_from_dist(self):
        # we cannot assert anything about trunk except that we can figure out some versions
        self.assertIsNotNone(get_supported_jdk_versions_from_dist(self._make_cassandra_install_dir('trunk', False)))
        self.assertIsNotNone(get_supported_jdk_versions_from_dist(self._make_cassandra_install_dir('trunk', True)))

        # some commit of Cassandra 5.1
        self.assertEquals(get_supported_jdk_versions_from_dist(self._make_cassandra_install_dir('6bae4f76fb043b4c3a3886178b5650b280e9a50b', False)), [11, 17])
        self.assertEquals(get_supported_jdk_versions_from_dist(self._make_cassandra_install_dir('6bae4f76fb043b4c3a3886178b5650b280e9a50b', True)), [11, 17])

        self.assertIsNone(get_supported_jdk_versions_from_dist(self._make_cassandra_install_dir('cassandra-5.0', False)))
        self.assertEquals(get_supported_jdk_versions_from_dist(self._make_cassandra_install_dir('cassandra-5.0', True)), [11, 17])

        self.assertIsNone(get_supported_jdk_versions_from_dist(self._make_cassandra_install_dir('cassandra-4.1', False)))
        self.assertIsNone(get_supported_jdk_versions_from_dist(self._make_cassandra_install_dir('cassandra-4.1', True)))

    def test_supported_jdk_versions(self):
        for cassandra_version in [None, '2.2', '3.0', '3.1', '4.0', '4.1']:
            self.assertIn(8, get_supported_jdk_versions(cassandra_version, None, False, {'key': 'value'}))
            self.assertIn(8, get_supported_jdk_versions(cassandra_version, None, True, {'key': 'value'}))

        for cassandra_version in [None, '2.2', '3.0', '3.1', '4.0', '4.1']:
            self.assertIn(8, get_supported_jdk_versions(cassandra_version, None, False, {'key': 'value', 'CASSANDRA_USE_JDK11': 'false'}))
            self.assertIn(8, get_supported_jdk_versions(cassandra_version, None, True, {'key': 'value', 'CASSANDRA_USE_JDK11': 'false'}))

        for cassandra_version in ['4.0', '4.1', '5.0', '5.1']:
            for usd_jdk_11 in ['true', 'TRUE', 'True', 'on', 'ON', 'On', 'yes', 'YES', 'Yes']:
                self.assertNotIn(8, get_supported_jdk_versions(cassandra_version, None, False, {'CASSANDRA_USE_JDK11': usd_jdk_11}))
                self.assertNotIn(8, get_supported_jdk_versions(cassandra_version, None, True, {'CASSANDRA_USE_JDK11': usd_jdk_11}))

        for cassandra_version in ['4.0', '4.1', '5.0', '5.1']:
            self.assertIn(11, get_supported_jdk_versions(cassandra_version, None, False, {'key': 'value'}))
            self.assertIn(11, get_supported_jdk_versions(cassandra_version, None, True, {'key': 'value'}))

        for cassandra_version in [None, '2.2', '3.0', '3.11']:
            self.assertNotIn(11, get_supported_jdk_versions(cassandra_version, None, False, {'key': 'value'}))
            self.assertNotIn(11, get_supported_jdk_versions(cassandra_version, None, True, {'key': 'value'}))

        for cassandra_version in ['5.0', '5.1']:
            self.assertIn(17, get_supported_jdk_versions(cassandra_version, None, False, {'key': 'value'}))
            self.assertIn(17, get_supported_jdk_versions(cassandra_version, None, True, {'key': 'value'}))

        for cassandra_version in [None, '2.2', '3.0', '3.11', '4.0', '4.1']:
            self.assertNotIn(17, get_supported_jdk_versions(cassandra_version, None, False, {'key': 'value'}))
            self.assertNotIn(17, get_supported_jdk_versions(cassandra_version, None, True, {'key': 'value'}))

    def test_get_available_jdk_versions(self):
        self.assertDictEqual(get_available_jdk_versions(self._make_env()), {7: 'JAVA7_HOME', 8: 'JAVA8_HOME', 11: 'JAVA11_HOME', 17: 'JAVA17_HOME', 21: 'JAVA21_HOME'})
        self.assertDictEqual(get_available_jdk_versions(self._make_env(java_home_version=8, include_homes=[11, 17])), {8: 'JAVA_HOME', 11: 'JAVA11_HOME', 17: 'JAVA17_HOME'})

    def _test_java_selection(self, expected_version, path_version, home_version, explicit_version, cassandra_versions, available_homes=None):
        for cassandra_version in cassandra_versions:
            result_env = _update_java_version(current_java_version=path_version, current_java_home_version=home_version,
                                              jvm_version=explicit_version, install_dir=None, cassandra_version=LooseVersion(cassandra_version),
                                              env=self._make_env(java_home_version=home_version, java_path_version=path_version, include_homes=available_homes),
                                              for_build=True, info_message='test_java_selection_{}'.format(cassandra_version), os_env={'key': 'value'})
            self._check_env(result_env, expected_version)
            if expected_version >= 11 and '4.0' <= cassandra_version < '5.0':
                self.assertEqual(result_env['CASSANDRA_USE_JDK11'], 'true')
            else:
                self.assertNotIn('CASSANDRA_USE_JDK11', result_env)

    def _test_java_selection_fail(self, expected_failure_regexp, path_version, home_version, explicit_version, cassandra_versions, available_homes=None):
        for cassandra_version in cassandra_versions:
            self.assertRaisesRegex(RuntimeError, expected_failure_regexp, _update_java_version,
                                   path_version, home_version, explicit_version, None, LooseVersion(cassandra_version),
                                   self._make_env(java_home_version=home_version, java_path_version=path_version, include_homes=available_homes),
                                   True, 'test_java_selection_fail_{}'.format(cassandra_version), {'key': 'value'})

    def test_update_java_version(self):
        # use the highest supported version if there is no current Java command available
        self._test_java_selection(8, None, None, None, ['2.2', '3.0', '3.11'])
        self._test_java_selection(11, None, None, None, ['4.0', '4.1'])
        self._test_java_selection(17, None, None, None, ['5.0', '5.1'])

        self._test_java_selection(8, None, None, None, ['4.0', '4.1'], available_homes=[8])
        self._test_java_selection(11, None, None, None, ['4.0', '4.1'], available_homes=[8, 11])
        self._test_java_selection(11, None, None, None, ['4.0', '4.1'], available_homes=[8, 11])

        # use the current Java version if it is supported, otherwise use the closest possible Java version
        # current is 8
        self._test_java_selection(8, 8, 8, None, ['2.2', '3.0', '3.11', '4.0', '4.1'])
        self._test_java_selection(11, 8, 8, None, ['5.0', '5.1'])
        # current is 11
        self._test_java_selection(8, 11, 11, None, ['2.2', '3.0', '3.11'])
        self._test_java_selection(11, 11, 11, None, ['4.0', '4.1', '5.0', '5.1'])
        # current is 17
        self._test_java_selection(8, 17, 17, None, ['2.2', '3.0', '3.11'])
        self._test_java_selection(11, 17, 17, None, ['4.0', '4.1'])
        self._test_java_selection(17, 17, 17, None, ['5.0', '5.1'])

        # same as above, but now there is only JAVA_HOME defined and no Java command on the PATH
        self._test_java_selection(8, None, 8, None, ['2.2', '3.0', '3.11', '4.0', '4.1'])
        self._test_java_selection(11, None, 8, None, ['5.0', '5.1'])
        self._test_java_selection(8, None, 11, None, ['2.2', '3.0', '3.11'])
        self._test_java_selection(11, None, 11, None, ['4.0', '4.1', '5.0', '5.1'])
        self._test_java_selection(8, None, 17, None, ['2.2', '3.0', '3.11'])
        self._test_java_selection(11, None, 17, None, ['4.0', '4.1'])
        self._test_java_selection(17, None, 17, None, ['5.0', '5.1'])

        # set explicit version should override everything
        self._test_java_selection(21, None, None, 21, self.all_versions)
        self._test_java_selection(21, 8, 8, 21, self.all_versions)
        self._test_java_selection(21, 11, 11, 21, self.all_versions)
        self._test_java_selection(21, 17, 17, 21, self.all_versions)

        # fail if the required Java version is not available
        self._test_java_selection_fail("Cannot find any Java distribution for the current invocation", None, None, None, self.all_versions, available_homes=[21])
        self._test_java_selection_fail("The explicitly requested Java version 11 is not available in the current env", None, None, 11, self.all_versions, available_homes=[8])

        # fail if home and path are inconsistent
        self._test_java_selection_fail("The version of java available on PATH 8 does not match the Java version of the distribution provided via JAVA_HOME 11", path_version=8, home_version=11, explicit_version=None, cassandra_versions=self.all_versions)
        self._test_java_selection_fail("JAVA_HOME must be defined if java command is available on the PATH", path_version=8, home_version=None, explicit_version=None, cassandra_versions=self.all_versions)

@pytest.mark.skip(reason="this test is starting nodes - it needs to be refactored to use mocks instead")
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

# TODO remove this unused class
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
