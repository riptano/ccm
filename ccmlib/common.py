#
# Cassandra Cluster Management lib
#

from __future__ import absolute_import

import copy
import fnmatch
import logging
import os
import platform
import re
import shutil
import signal
import socket
import stat
import subprocess
import sys
import time
from distutils.version import LooseVersion  #pylint: disable=import-error, no-name-in-module

import six
import yaml
from six import print_

BIN_DIR = "bin"
CASSANDRA_CONF_DIR = "conf"
DSE_CASSANDRA_CONF_DIR = "resources/cassandra/conf"
OPSCENTER_CONF_DIR = "conf"

CASSANDRA_CONF = "cassandra.yaml"
JVM_OPTS_PATTERN = "jvm*.options"
LOG4J_CONF = "log4j-server.properties"
LOG4J_TOOL_CONF = "log4j-tools.properties"
LOGBACK_CONF = "logback.xml"
LOGBACK_TOOLS_CONF = "logback-tools.xml"
CASSANDRA_ENV = "cassandra-env.sh"
CASSANDRA_WIN_ENV = "cassandra-env.ps1"
CASSANDRA_SH = "cassandra.in.sh"

CONFIG_FILE = "config"
CCM_CONFIG_DIR = "CCM_CONFIG_DIR"


def get_options_removal_dict(options):
    dict = {}
    for option in options:
        dict[option] = None
    return dict


# Options introduced in 4.0
CCM_40_YAML_OPTIONS = get_options_removal_dict(['repaired_data_tracking_for_range_reads_enabled',
                                                'corrupted_tombstone_strategy',
                                                'repaired_data_tracking_for_partition_reads_enabled',
                                                'report_unconfirmed_repaired_data_mismatches'])

# Options introduced in CASSANDRA-15234 and their respective matching old names for backward compatibility
# When you change a property name and you want backward compatibility with the old one, you need to add an entry
# in the dictionary below in the form 'old_name': 'new_name' to ensure ccm can also support both names and load
# correct values
CCM_41_YAML_OPTIONS = {'permissions_validity_in_ms': 'permissions_validity',
                       'permissions_update_interval_in_ms': 'permissions_update_interval',
                       'roles_validity_in_ms': 'roles_validity',
                       'roles_update_interval_in_ms': 'roles_update_interval',
                       'credentials_validity_in_ms': 'credentials_validity',
                       'credentials_update_interval_in_ms': 'credentials_update_interval',
                       'max_hint_window_in_ms': 'max_hint_window',
                       'native_transport_idle_timeout_in_ms': 'native_transport_idle_timeout',
                       'request_timeout_in_ms': 'request_timeout',
                       'read_request_timeout_in_ms': 'read_request_timeout',
                       'range_request_timeout_in_ms': 'range_request_timeout',
                       'write_request_timeout_in_ms': 'write_request_timeout',
                       'counter_write_request_timeout_in_ms': 'counter_write_request_timeout',
                       'cas_contention_timeout_in_ms': 'cas_contention_timeout',
                       'truncate_request_timeout_in_ms': 'truncate_request_timeout',
                       'streaming_keep_alive_period_in_secs': 'streaming_keep_alive_period',
                       'cross_node_timeout': 'internode_timeout',
                       'slow_query_log_timeout_in_ms': 'slow_query_log_timeout',
                       'memtable_heap_space_in_mb': 'memtable_heap_space',
                       'memtable_offheap_space_in_mb': 'memtable_offheap_space',
                       'repair_session_space_in_mb': 'repair_session_space',
                       'internode_max_message_size_in_bytes': 'internode_max_message_size',
                       'internode_send_buff_size_in_bytes': 'internode_socket_send_buffer_size',
                       'internode_socket_send_buffer_size_in_bytes': 'internode_socket_send_buffer_size',
                       'internode_socket_receive_buffer_size_in_bytes': 'internode_socket_receive_buffer_size',
                       'internode_recv_buff_size_in_bytes': 'internode_socket_receive_buffer_size',
                       'internode_application_send_queue_capacity_in_bytes': 'internode_application_send_queue_capacity',
                       'internode_application_send_queue_reserve_endpoint_capacity_in_bytes':
                           'internode_application_send_queue_reserve_endpoint_capacity',
                       'internode_application_send_queue_reserve_global_capacity_in_bytes':
                           'internode_application_send_queue_reserve_global_capacity',
                       'internode_application_receive_queue_capacity_in_bytes':
                           'internode_application_receive_queue_capacity',
                       'internode_application_receive_queue_reserve_endpoint_capacity_in_bytes':
                           'internode_application_receive_queue_reserve_endpoint_capacity',
                       'internode_application_receive_queue_reserve_global_capacity_in_bytes':
                           'internode_application_receive_queue_reserve_global_capacity',
                       'internode_tcp_connect_timeout_in_ms': 'internode_tcp_connect_timeout',
                       'internode_tcp_user_timeout_in_ms': 'internode_tcp_user_timeout',
                       'internode_streaming_tcp_user_timeout_in_ms': 'internode_streaming_tcp_user_timeout',
                       'native_transport_max_frame_size_in_mb': 'native_transport_max_frame_size',
                       'max_value_size_in_mb': 'max_value_size',
                       'column_index_size_in_kb': 'column_index_size',
                       'column_index_cache_size_in_kb': 'column_index_cache_size',
                       'batch_size_warn_threshold_in_kb': 'batch_size_warn_threshold',
                       'batch_size_fail_threshold_in_kb': 'batch_size_fail_threshold',
                       'compaction_throughput_mb_per_sec': 'compaction_throughput',
                       'compaction_large_partition_warning_threshold_mb': 'compaction_large_partition_warning_threshold',
                       'min_free_space_per_drive_in_mb': 'min_free_space_per_drive',
                       'stream_throughput_outbound_megabits_per_sec': 'stream_throughput_outbound',
                       'inter_dc_stream_throughput_outbound_megabits_per_sec': 'inter_dc_stream_throughput_outbound',
                       'commitlog_total_space_in_mb': 'commitlog_total_space',
                       'commitlog_sync_group_window_in_ms': 'commitlog_sync_group_window',
                       'commitlog_sync_period_in_ms': 'commitlog_sync_period',
                       'commitlog_segment_size_in_mb': 'commitlog_segment_size',
                       'periodic_commitlog_sync_lag_block_in_ms': 'periodic_commitlog_sync_lag_block',
                       'max_mutation_size_in_kb': 'max_mutation_size',
                       'cdc_total_space_in_mb': 'cdc_total_space',
                       'cdc_free_space_check_interval_ms': 'cdc_free_space_check_interval',
                       'dynamic_snitch_update_interval_in_ms': 'dynamic_snitch_update_interval',
                       'dynamic_snitch_reset_interval_in_ms': 'dynamic_snitch_reset_interval',
                       'hinted_handoff_throttle_in_kb': 'hinted_handoff_throttle',
                       'batchlog_replay_throttle_in_kb': 'batchlog_replay_throttle',
                       'hints_flush_period_in_ms': 'hints_flush_period',
                       'max_hints_file_size_in_mb': 'max_hints_file_size',
                       'trickle_fsync_interval_in_kb': 'trickle_fsync_interval',
                       'sstable_preemptive_open_interval_in_mb': 'sstable_preemptive_open_interval',
                       'key_cache_size_in_mb': 'key_cache_size',
                       'row_cache_size_in_mb': 'row_cache_size',
                       'counter_cache_size_in_mb': 'counter_cache_size',
                       'cache_load_timeout_seconds': 'cache_load_timeout',
                       'networking_cache_size_in_mb': 'networking_cache_size',
                       'file_cache_size_in_mb': 'file_cache_size',
                       'index_summary_capacity_in_mb': 'index_summary_capacity',
                       'index_summary_resize_interval_in_minutes': 'index_summary_resize_interval',
                       'gc_log_threshold_in_ms': 'gc_log_threshold',
                       'gc_warn_threshold_in_ms': 'gc_warn_threshold',
                       'tracetype_query_ttl': 'trace_type_query_ttl',
                       'tracetype_repair_ttl': 'trace_type_repair_ttl',
                       'prepared_statements_cache_size_mb': 'prepared_statements_cache_size',
                       'enable_user_defined_functions': 'user_defined_functions_enabled',
                       'enable_scripted_user_defined_functions': 'scripted_user_defined_functions_enabled',
                       'enable_materialized_views': 'materialized_views_enabled',
                       'enable_transient_replication': 'transient_replication_enabled',
                       'enable_sasi_indexes': 'sasi_indexes_enabled',
                       'enable_drop_compact_storage': 'drop_compact_storage_enabled',
                       'enable_user_defined_functions_threads': 'user_defined_functions_threads_enabled',
                       'enable_legacy_ssl_storage_port': 'legacy_ssl_storage_port_enabled',
                       'native_transport_max_concurrent_requests_in_bytes_per_ip': 'native_transport_max_request_data_in_flight_per_ip',
                       'native_transport_max_concurrent_requests_in_bytes': 'native_transport_max_request_data_in_flight',
                       'user_defined_function_warn_timeout': 'user_defined_functions_warn_timeout',
                       'user_defined_function_fail_timeout': 'user_defined_functions_fail_timeout',
                       'validation_preview_purge_head_start_in_sec': 'validation_preview_purge_head_start',
                       'native_transport_receive_queue_capacity_in_bytes': 'native_transport_receive_queue_capacity'}


class InfoFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno in (logging.DEBUG, logging.INFO)


LOG_FMT = '%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s'
DATE_FMT = '%H:%M:%S'
FORMATTER = logging.Formatter(LOG_FMT, DATE_FMT)

INFO_HANDLER = logging.StreamHandler(sys.stdout)
INFO_HANDLER.setLevel(logging.DEBUG)
INFO_HANDLER.addFilter(InfoFilter())
INFO_HANDLER.setFormatter(FORMATTER)

ERROR_HANDLER = logging.StreamHandler(sys.stderr)
ERROR_HANDLER.setLevel(logging.WARNING)
ERROR_HANDLER.setFormatter(FORMATTER)

LOG = logging.getLogger('ccm')
LOG.setLevel(logging.DEBUG)
LOG.addHandler(INFO_HANDLER)
LOG.addHandler(ERROR_HANDLER)


def error(msg):
    LOG.error(msg)


def warning(msg):
    LOG.warning(msg)


def info(msg):
    LOG.info(msg)


def debug(msg):
    LOG.debug(msg)


class CCMError(Exception):
    pass


class LoadError(CCMError):
    pass


class ArgumentError(CCMError):
    pass


class UnavailableSocketError(CCMError):
    pass


class TimeoutError(Exception):

    def __init__(self, data):
        Exception.__init__(self, str(data))


class LogPatternToVersion(object):

    def __init__(self, versions_to_patterns, default_pattern=None):
        self.versions_to_patterns, self.default_pattern = versions_to_patterns, default_pattern

    def __call__(self, version):
        keys_less_than_version = [k for k in self.versions_to_patterns if k <= version]

        if not keys_less_than_version:
            if self.default_pattern is not None:
                return self.default_pattern
            else:
                raise ValueError("Some kind of default pattern must be specified!")

        return self.versions_to_patterns[
            max(keys_less_than_version, key=lambda v: LooseVersion(v) if not isinstance(v, LooseVersion) else v)]

    def __repr__(self):
        return str(self.__class__) + "(versions_to_patterns={}, default_pattern={})".format(self.versions_to_patterns,
                                                                                            self.default_pattern)

    @property
    def patterns(self):
        patterns = list(self.versions_to_patterns.values())
        if self.default_pattern is not None:
            patterns = patterns + [self.default_pattern]
        return patterns

    @property
    def versions(self):
        return list(self.versions_to_patterns)


def get_default_path():
    if CCM_CONFIG_DIR in os.environ and os.environ[CCM_CONFIG_DIR]:
        default_path = os.environ[CCM_CONFIG_DIR]
    else:
        default_path = os.path.join(get_user_home(), '.ccm')

    if not os.path.exists(default_path):
        os.mkdir(default_path)
    return default_path


def get_default_path_display_name():
    default_path = get_default_path().lower()
    user_home = get_user_home().lower()

    if default_path.startswith(user_home):
        default_path = os.path.join('~', default_path[len(user_home) + 1:])

    return default_path


def get_user_home():
    if is_win():
        if sys.platform == "cygwin":
            # Need the fully qualified directory
            output = subprocess.Popen(["cygpath", "-m", os.path.expanduser('~')], stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT).communicate()[0].rstrip()
            return output
        else:
            return os.environ['USERPROFILE']
    else:
        return os.path.expanduser('~')


def get_config():
    config_path = os.path.join(get_default_path(), CONFIG_FILE)
    if not os.path.exists(config_path):
        return {}

    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def now_ms():
    return int(round(time.time() * 1000))


def parse_interface(itf, default_port):
    i = itf.split(':')
    if len(i) == 1:
        return (i[0].strip(), default_port)
    elif len(i) == 2:
        return (i[0].strip(), int(i[1].strip()))
    else:
        raise ValueError("Invalid interface definition: " + itf)


def current_cluster_name(path):
    try:
        with open(os.path.join(path, 'CURRENT'), 'r') as f:
            return f.readline().strip()
    except IOError:
        return None


def switch_cluster(path, new_name):
    with open(os.path.join(path, 'CURRENT'), 'w') as f:
        f.write(new_name + '\n')


def replace_in_file(file, regexp, replace):
    replaces_in_file(file, [(regexp, replace)])


def replaces_in_file(file, replacement_list):
    rs = [(re.compile(regexp), repl) for (regexp, repl) in replacement_list]
    file_tmp = file + "." + str(os.getpid()) + ".tmp"
    with open(file, 'r') as f:
        with open(file_tmp, 'w') as f_tmp:
            for line in f:
                for r, replace in rs:
                    match = r.search(line)
                    if match:
                        line = replace + "\n"
                f_tmp.write(line)
    shutil.move(file_tmp, file)


def replace_or_add_into_file_tail(file, regexp, replace):
    replaces_or_add_into_file_tail(file, [(regexp, replace)])


def replaces_or_add_into_file_tail(file, replacement_list, add_config_close=True):
    rs = [(re.compile(regexp), repl) for (regexp, repl) in replacement_list]
    is_line_found = False
    file_tmp = file + "." + str(os.getpid()) + ".tmp"
    with open(file, 'r') as f:
        with open(file_tmp, 'w') as f_tmp:
            for line in f:
                for r, replace in rs:
                    match = r.search(line)
                    if match:
                        line = replace + "\n"
                        is_line_found = True
                if "</configuration>" not in line:
                    f_tmp.write(line)
            # In case, entry is not found, and need to be added
            if not is_line_found:
                f_tmp.write('\n' + replace + "\n")
            # We are moving the closing tag to the end of the file.
            # Previously, we were having an issue where new lines we wrote
            # were appearing after the closing tag, and thus being ignored.
            if add_config_close:
                f_tmp.write("</configuration>\n")

    shutil.move(file_tmp, file)


def rmdirs(path):
    if is_win():
        # Handle Windows 255 char limit
        shutil.rmtree(u"\\\\?\\" + path)
    else:
        shutil.rmtree(path)


def make_cassandra_env(install_dir, node_path, update_conf=True):
    if is_win() and get_version_from_build(node_path=node_path) >= '2.1':
        sh_file = os.path.join(CASSANDRA_CONF_DIR, CASSANDRA_WIN_ENV)
    else:
        sh_file = os.path.join(BIN_DIR, CASSANDRA_SH)
    orig = os.path.join(install_dir, sh_file)
    dst = os.path.join(node_path, sh_file)
    if not is_win() or not os.path.exists(dst):
        shutil.copy(orig, dst)

    if update_conf and not (is_win() and get_version_from_build(node_path=node_path) >= '2.1'):
        replacements = [
            ('CASSANDRA_HOME=', '\tCASSANDRA_HOME=%s' % install_dir),
            ('CASSANDRA_CONF=', '\tCASSANDRA_CONF=%s' % os.path.join(node_path, 'conf'))
        ]
        replaces_in_file(dst, replacements)

    # If a cluster-wide cassandra.in.sh file exists in the parent
    # directory, append it to the node specific one:
    cluster_sh_file = os.path.join(node_path, os.path.pardir, 'cassandra.in.sh')
    if os.path.exists(cluster_sh_file):
        append = open(cluster_sh_file).read()
        with open(dst, 'a') as f:
            f.write('\n\n### Start Cluster wide config ###\n')
            f.write(append)
            f.write('\n### End Cluster wide config ###\n\n')

    env = os.environ.copy()
    env['CASSANDRA_INCLUDE'] = os.path.join(dst)
    env['MAX_HEAP_SIZE'] = os.environ.get('CCM_MAX_HEAP_SIZE', '500M')
    env['HEAP_NEWSIZE'] = os.environ.get('CCM_HEAP_NEWSIZE', '50M')
    env['CASSANDRA_HOME'] = install_dir
    env['CASSANDRA_CONF'] = os.path.join(node_path, 'conf')

    return env


def make_dse_env(install_dir, node_path, node_ip):
    version = get_version_from_build(node_path=node_path)
    env = os.environ.copy()
    env['MAX_HEAP_SIZE'] = os.environ.get('CCM_MAX_HEAP_SIZE', '500M')
    env['HEAP_NEWSIZE'] = os.environ.get('CCM_HEAP_NEWSIZE', '50M')
    if version < '6.0':
        env['SPARK_WORKER_MEMORY'] = os.environ.get('SPARK_WORKER_MEMORY', '1024M')
        env['SPARK_WORKER_CORES'] = os.environ.get('SPARK_WORKER_CORES', '2')
    else:
        env['ALWAYSON_SQL_LOG_DIR'] = os.path.join(node_path, 'logs')
    env['DSE_HOME'] = os.path.join(install_dir)
    env['DSE_CONF'] = os.path.join(node_path, 'resources', 'dse', 'conf')
    env['CASSANDRA_HOME'] = os.path.join(install_dir, 'resources', 'cassandra')
    env['CASSANDRA_CONF'] = os.path.join(node_path, 'resources', 'cassandra', 'conf')
    env['HIVE_CONF_DIR'] = os.path.join(node_path, 'resources', 'hive', 'conf')
    env['SQOOP_CONF_DIR'] = os.path.join(node_path, 'resources', 'sqoop', 'conf')
    env['TOMCAT_HOME'] = os.path.join(node_path, 'resources', 'tomcat')
    env['TOMCAT_CONF_DIR'] = os.path.join(node_path, 'resources', 'tomcat', 'conf')
    env['PIG_CONF_DIR'] = os.path.join(node_path, 'resources', 'pig', 'conf')
    env['MAHOUT_CONF_DIR'] = os.path.join(node_path, 'resources', 'mahout', 'conf')
    env['SPARK_CONF_DIR'] = os.path.join(node_path, 'resources', 'spark', 'conf')
    env['SHARK_CONF_DIR'] = os.path.join(node_path, 'resources', 'shark', 'conf')
    env['GREMLIN_CONSOLE_CONF_DIR'] = os.path.join(node_path, 'resources', 'graph', 'gremlin-console', 'conf')
    env['SPARK_WORKER_DIR'] = os.path.join(node_path, 'spark', 'worker')
    env['SPARK_LOCAL_DIRS'] = os.path.join(node_path, 'spark', '.local')
    env['SPARK_EXECUTOR_DIRS'] = os.path.join(node_path, 'spark', 'rdd')
    env['SPARK_WORKER_LOG_DIR'] = os.path.join(node_path, 'logs', 'spark', 'worker')
    env['SPARK_MASTER_LOG_DIR'] = os.path.join(node_path, 'logs', 'spark', 'master')
    env['DSE_LOG_ROOT'] = os.path.join(node_path, 'logs', 'dse')
    env['CASSANDRA_LOG_DIR'] = os.path.join(node_path, 'logs')
    env['SPARK_LOCAL_IP'] = '' + node_ip
    if version >= '5.0':
        env['HADOOP1_CONF_DIR'] = os.path.join(node_path, 'resources', 'hadoop', 'conf')
        env['HADOOP2_CONF_DIR'] = os.path.join(node_path, 'resources', 'hadoop2-client', 'conf')
    else:
        env['HADOOP_CONF_DIR'] = os.path.join(node_path, 'resources', 'hadoop', 'conf')
    return env


def check_win_requirements():
    if is_win():
        # Make sure ant.bat is in the path and executable before continuing
        try:
            subprocess.Popen('ant.bat', stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        except Exception:
            sys.exit(
                "ERROR!  Could not find or execute ant.bat.  Please fix this before attempting to run ccm on Windows.")

        # Confirm matching architectures
        # 32-bit python distributions will launch 32-bit cmd environments, losing PowerShell execution privileges on a 64-bit system
        if sys.maxsize <= 2 ** 32 and platform.machine().endswith('64'):
            sys.exit("ERROR!  64-bit os and 32-bit python distribution found.  ccm requires matching architectures.")


def is_win():
    return sys.platform in ("cygwin", "win32")


def is_modern_windows_install(version):
    """
    The 2.1 release line was when Cassandra received beta windows support.
    Many features are gated based on that added compatibility.

    Handles floats, strings, and LooseVersions by first converting all three types to a string, then to a LooseVersion.
    """
    version = LooseVersion(str(version))
    if is_win() and version >= LooseVersion('2.1'):
        return True
    else:
        return False


def is_ps_unrestricted():
    if not is_win():
        raise CCMError("Can only check PS Execution Policy on Windows")
    else:
        try:
            p = subprocess.Popen(['powershell', 'Get-ExecutionPolicy'], stdout=subprocess.PIPE)
        # pylint: disable=E0602
        except WindowsError:
            print_("ERROR: Could not find powershell. Is it in your path?")
        if "Unrestricted" in str(p.communicate()[0]):
            return True
        else:
            return False


def join_bin(root, dir, executable):
    return os.path.join(root, dir, platform_binary(executable))


def platform_binary(input):
    return input + ".bat" if is_win() else input


def platform_pager():
    return "more" if sys.platform == "win32" else "less"


def add_exec_permission(path, executable):
    # 1) os.chmod on Windows can't add executable permissions
    # 2) chmod from other folders doesn't work in cygwin, so we have to navigate the shell
    # to the folder with the executable with it and then chmod it from there
    if sys.platform == "cygwin":
        cmd = "cd " + path + "; chmod u+x " + executable
        os.system(cmd)


def parse_path(executable):
    sep = os.sep
    if sys.platform == "win32":
        sep = "\\\\"
    tokens = re.split(sep, executable)
    del tokens[-1]
    return os.sep.join(tokens)


def parse_bin(executable):
    tokens = re.split(os.sep, executable)
    return tokens[-1]


def get_stress_bin(install_dir):
    candidates = [
        os.path.join(install_dir, 'contrib', 'stress', 'bin', 'stress'),
        os.path.join(install_dir, 'tools', 'stress', 'bin', 'stress'),
        os.path.join(install_dir, 'tools', 'bin', 'stress'),
        os.path.join(install_dir, 'tools', 'bin', 'cassandra-stress'),
        os.path.join(install_dir, 'resources', 'cassandra', 'tools', 'bin', 'cassandra-stress')
    ]
    candidates = [platform_binary(s) for s in candidates]

    for candidate in candidates:
        if os.path.exists(candidate):
            stress = candidate
            break
    else:
        raise Exception("Cannot find stress binary (maybe it isn't compiled)")

    # make sure it's executable -> win32 doesn't care
    if sys.platform == "cygwin":
        # Yes, we're unwinding the path join from above.
        path = parse_path(stress)
        short_bin = parse_bin(stress)
        add_exec_permission(path, short_bin)
    elif not os.access(stress, os.X_OK):
        try:
            os.chmod(stress, os.stat(stress).st_mode | stat.S_IXUSR)
        except:
            raise Exception("stress binary is not executable: %s" % (stress,))

    return stress


def isDse(install_dir):
    if install_dir is None:
        raise ArgumentError('Undefined installation directory')

    bin_dir = os.path.join(install_dir, BIN_DIR)

    if not os.path.exists(bin_dir):
        raise ArgumentError('Installation directory does not contain a bin directory: %s' % install_dir)

    dse_script = os.path.join(bin_dir, 'dse')
    return os.path.exists(dse_script)


def isOpscenter(install_dir):
    if install_dir is None:
        raise ArgumentError('Undefined installation directory')

    bin_dir = os.path.join(install_dir, BIN_DIR)

    if not os.path.exists(bin_dir):
        raise ArgumentError('Installation directory does not contain a bin directory')

    opscenter_script = os.path.join(bin_dir, 'opscenter')
    return os.path.exists(opscenter_script)


def validate_install_dir(install_dir):
    if install_dir is None:
        raise ArgumentError('Undefined installation directory')

    # Windows requires absolute pathing on installation dir - abort if specified cygwin style
    if is_win():
        if ':' not in install_dir:
            raise ArgumentError(
                '%s does not appear to be a cassandra or dse installation directory.  Please use absolute pathing (e.g. C:/cassandra.' % install_dir)

    bin_dir = os.path.join(install_dir, BIN_DIR)
    if isDse(install_dir):
        conf_dir = os.path.join(install_dir, DSE_CASSANDRA_CONF_DIR)
    elif isOpscenter(install_dir):
        conf_dir = os.path.join(install_dir, OPSCENTER_CONF_DIR)
    else:
        conf_dir = os.path.join(install_dir, CASSANDRA_CONF_DIR)
    cnd = os.path.exists(bin_dir)
    cnd = cnd and os.path.exists(conf_dir)
    if not isOpscenter(install_dir):
        cnd = cnd and os.path.exists(os.path.join(conf_dir, CASSANDRA_CONF))
    if not cnd:
        raise ArgumentError('%s does not appear to be a cassandra or dse installation directory' % install_dir)


def assert_socket_available(itf):
    info = socket.getaddrinfo(itf[0], itf[1], socket.AF_UNSPEC, socket.SOCK_STREAM)
    if not info:
        raise UnavailableSocketError("Failed to get address info for [%s]:%s" % itf)

    (family, socktype, proto, canonname, sockaddr) = info[0]
    s = socket.socket(family, socktype)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        s.bind(sockaddr)
        s.close()
        return True
    except socket.error as msg:
        s.close()
        addr, port = itf
        raise UnavailableSocketError(
            "Inet address %s:%s is not available: %s; a cluster may already be running or you may need to add the loopback alias" % (
            addr, port, msg))


def check_socket_listening(itf, timeout=60):
    end = time.time() + timeout
    while time.time() <= end:
        try:
            sock = socket.socket()
            sock.connect(itf)
            sock.close()
            return True
        except socket.error:
            if sock:
                sock.close()
            # Try again in another 200ms
            time.sleep(.2)

    return False


def interface_is_ipv6(itf):
    info = socket.getaddrinfo(itf[0], itf[1], socket.AF_UNSPEC, socket.SOCK_STREAM)
    if not info:
        raise UnavailableSocketError("Failed to get address info for [%s]:%s" % itf)

    return socket.AF_INET6 == info[0][0]


# note: does not handle collapsing hextets with leading zeros


def normalize_interface(itf):
    if not itf:
        return itf
    ip = itf[0]
    parts = ip.partition('::')
    if '::' in parts:
        missing_hextets = 9 - ip.count(':')
        zeros = '0'.join([':'] * missing_hextets)
        ip = ''.join(['0' if p == '' else zeros if p == '::' else p for p in ip.partition('::')])
    return (ip, itf[1])


def parse_settings(args, literal_yaml=False):
    settings = {}
    if literal_yaml:
        for s in args:
            settings = dict(settings, **yaml.safe_load(s))
    else:
        for s in args:
            if is_win():
                # Allow for absolute path on Windows for value in key/value pair
                splitted = s.split(':', 1)
            else:
                splitted = s.split(':')
            if len(splitted) != 2:
                raise ArgumentError("A new setting should be of the form 'key: value', got " + s)
            key = splitted[0].strip()
            val = splitted[1].strip()
            # ok, that's not super beautiful
            if val.lower() == "true":
                val = True
            elif val.lower() == "false":
                val = False
            else:
                try:
                    val = int(val)
                except ValueError:
                    pass
            splitted = key.split('.')
            split_length = len(splitted)
            if split_length >= 2:
                # Where we are currently at in the dict.
                tree_pos = settings
                # Iterate over each split and build structure as needed.
                for pos in range(split_length):
                    split = splitted[pos]
                    if pos == split_length - 1:
                        # If at the last split, set value.
                        tree_pos[split] = val
                    else:
                        # If not at last split, create a new dict at the current
                        # position for this split if it doesn't already exist
                        # and update the current position.
                        if split not in tree_pos:
                            tree_pos[split] = {}
                        tree_pos = tree_pos[split]
            else:
                settings[key] = val
    return settings


#
# Copy file from source to destination with reasonable error handling
#


def copy_file(src_file, dst_file):
    try:
        shutil.copy2(src_file, dst_file)
    except (IOError, shutil.Error) as e:
        print_(str(e), file=sys.stderr)
        exit(1)


def copy_directory(src_dir, dst_dir):
    for name in os.listdir(src_dir):
        filename = os.path.join(src_dir, name)
        if os.path.isfile(filename):
            shutil.copy(filename, dst_dir)


def get_version_from_build(install_dir=None, node_path=None, cassandra=False):
    if install_dir is None and node_path is not None:
        install_dir = get_install_dir_from_cluster_conf(node_path)
    if install_dir is not None:
        # Binary cassandra installs will have a 0.version.txt file
        version_file = os.path.join(install_dir, '0.version.txt')
        if os.path.exists(version_file):
            with open(version_file) as f:
                return LooseVersion(f.read().strip())
        # For DSE look for a dse*.jar and extract the version number
        dse_version = get_dse_version(install_dir)
        if (dse_version is not None):
            if cassandra:
                return get_dse_cassandra_version(install_dir)
            else:
                return LooseVersion(dse_version)
        # Source cassandra installs we can read from build.xml
        build = os.path.join(install_dir, 'build.xml')
        with open(build) as f:
            for line in f:
                match = re.search('name="base\.version" value="([0-9.]+)[^"]*"', line)
                if match:
                    return LooseVersion(match.group(1))
    raise CCMError("Cannot find version")


def get_dse_version(install_dir):
    for root, dirs, files in os.walk(install_dir):
        for file in files:
            match = re.search('^dse(?:-core)?-([0-9.]+)(?:-.*)?\.jar', file)
            if match:
                return match.group(1)
    return None


def get_dse_cassandra_version(install_dir):
    dse_cmd = os.path.join(install_dir, 'bin', 'dse')
    (output, stderr) = subprocess.Popen([dse_cmd, "cassandra", '-v'], stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE).communicate()
    output = output.rstrip()
    match = re.search('([0-9.]+)(?:-.*)?', str(output))
    if match:
        return LooseVersion(match.group(1))

    raise ArgumentError("Unable to determine Cassandra version in: %s.\n\tstdout: '%s'\n\tstderr: '%s'"
                        % (install_dir, output, stderr))


def get_install_dir_from_cluster_conf(node_path):
    file = os.path.join(os.path.dirname(node_path), "cluster.conf")
    with open(file) as f:
        for line in f:
            match = re.search('install_dir: (.*?)$', line)
            if match:
                return match.group(1)
    return None


def is_dse_cluster(path):
    try:
        with open(os.path.join(path, 'CURRENT'), 'r') as f:
            name = f.readline().strip()
            cluster_path = os.path.join(path, name)
            filename = os.path.join(cluster_path, 'cluster.conf')
            with open(filename, 'r') as f:
                data = yaml.safe_load(f)
            if 'dse_dir' in data:
                return True
    except IOError:
        return False


def invalidate_cache():
    rmdirs(os.path.join(get_default_path(), 'repository'))


def get_jdk_version_int(process='java', env=None):
    jdk_version_str = get_jdk_version(process, env)
    if jdk_version_str is None:
        return None
    jdk_version = float(jdk_version_str)
    # Make it Java 8 instead of 1.8 (or 7 instead of 1.7)
    jdk_version = int(jdk_version if jdk_version >= 2 else 10 * (jdk_version - 1))
    return jdk_version


def get_jdk_version(process='java', env=None):
    """
    Retrieve the Java version as reported in the quoted string returned
    by invoking 'java -version'.
    Works for Java 1.8, Java 9 and newer.
    """
    try:
        version = subprocess.check_output([process, '-version'], stderr=subprocess.STDOUT, env=env)
    except OSError:
        info("Could not find {}.".format(process))
        return None

    return _get_jdk_version(version)


def _get_jdk_version(version):
    ver_pattern = '\"(\d+\.\d+).*\"'
    if re.search(ver_pattern, str(version)):
        return re.search(ver_pattern, str(version)).groups()[0]
    # like the output 'java version "9"' for 'java -version'
    ver_pattern = '\"(\d+).*\"'
    return re.search(ver_pattern, str(version)).groups()[0] + ".0"


def get_supported_jdk_versions_internal(path, pattern):
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                match = re.search(pattern, line)
                if match:
                    versions = match.group(1).split(',')
                    versions = [8 if v == '1.8' else int(v) for v in versions]
                    return versions
    return None


def get_supported_jdk_versions_from_dist(install_dir):
    """
    Return the supported Java versions from build.xml (source distributions)
    or from cassandra.in.sh (binary distributions) if such information is present (5.0+ for source, 5.1+ for binary).
    """

    # source distributions have supported Java versions specified in build.xml since 5.0
    versions = get_supported_jdk_versions_internal(os.path.join(install_dir, 'build.xml'),
                                                   'name="java\\.supported" value="([0-9.,]+)"')
    if versions is None:
        # binary distributions have supported Java versions specified in bin/cassandra.in.sh since 5.1
        versions = get_supported_jdk_versions_internal(os.path.join(install_dir, 'bin', 'cassandra.in.sh'),
                                                       'java_versions_supported=([0-9.,]+)')

    if versions and len(versions) > 0:
        info("Supported Java versions for Cassandra distribution in '{}': {}".format(install_dir, versions))
    else:
        info("Supported Java versions could not be retrieved from Cassandra distribution in '{}'".format(install_dir))

    return versions


def get_supported_jdk_versions(cassandra_version, install_dir, for_build, env):
    build_versions = None
    if install_dir:
        build_versions = get_supported_jdk_versions_from_dist(install_dir)
    run_versions = build_versions

    # If the supported Java versions are not available from the distribution, use the known defaults
    if cassandra_version and not isinstance(cassandra_version, LooseVersion):
        cassandra_version = LooseVersion(cassandra_version)

    isCassandraUseJDK11Set = "CASSANDRA_USE_JDK11" in env and (env["CASSANDRA_USE_JDK11"].lower() in ['true', 'yes', 'on'])
    if build_versions is None:
        if cassandra_version and cassandra_version >= LooseVersion('5.0'):
            build_versions = [11, 17]
            run_versions = [11, 17]
            info("Cassandra 5.0+ detected, using Java 11 and 17")
        elif cassandra_version and cassandra_version >= LooseVersion('4.0'):
            if isCassandraUseJDK11Set:
                build_versions = [11]
                run_versions = [11]
                info("Cassandra 4.0+ detected, using Java 11 (Java 8 is excluded because the deprecated CASSANDRA_USE_JDK11 is set)")
            else:
                build_versions = [8, 11]
                run_versions = [8, 11]
                info("Cassandra 4.0+ detected, using Java 8 and 11")
        else:
            # Cassandra versions 3.x and 2.x
            build_versions = [8]
            run_versions = [8]
            info("Cassandra 3.x or 2.x detected, using Java 8")

    # Java versions supported by the Cassandra distribution
    return build_versions if for_build else run_versions


def get_available_jdk_versions(env):
    return {get_jdk_version_int(os.path.join(env_value, 'bin', 'java')): env_key
            for env_key, env_value in env.items() if re.search("JAVA([0-9]+)?_HOME", env_key)}


def update_java_version(jvm_version=None, install_dir=None, cassandra_version=None, env=None,
                        for_build=False, info_message=None):
    """
    Updates or fixes the environment variables for Java distribution.
    If 'jvm_version' is explicitly set, that one will be used as long as it can be found in the current environment
    via JAVA_HOME and JAVAx_HOME variables.
    Otherwise, the supported Java versions will be guessed from the provided `cassandra_version` (or install dir if None).
    Then, if there is a JAVA_HOME variable set, that Java distribution will be used if it is supported by Cassandra.
    If not, the closest supported Java version will be used from the available Java distributions (or the highest
    supported version if JAVA_HOME is not defined).
    Note that, if there is java command available on the PATH, it must be the same version as the Java distribution
    defined in JAVA_HOME, otherwise an error will be raised as there is no obvious way to decide which one to use.

    :param jvm_version: The Java version to use - must be the major Java version number like 8 or 11.
    :param install_dir: Software installation directory.
    :param cassandra_version: The Cassandra version to consider for choosing the correct Java version.
    :param env: Current OS environment variables.
    :param for_build: whether the code should check for a valid version to build or run bdp. Currently only
    applies to source tree that have a 'build-env.yaml' file.
    :param info_message: String logged with info/error messages
    :return: the maybe updated OS environment variables - the only variables that may get updated are PATH and JAVA_HOME.
    If 'env' was 'None' and JAVA_HOME needs to be set, the result will also contain the current OS environment variables
    from 'os.environ'.
    """

    env = env if env else os.environ

    current_java_version = get_jdk_version_int(env=env)
    current_java_home_version = get_jdk_version_int(
        '{}/bin/java'.format(env['JAVA_HOME'])) if 'JAVA_HOME' in env else current_java_version
    # Code to ensure that we start C* using the correct Java version.
    # This is important especially after CASSANDRA-9608 (Java 11 support) when dtests are run using Java 11
    # but a "manually" configured (set_install_dir()) different version requires Java 8.
    return _update_java_version(current_java_version, current_java_home_version,
                                jvm_version=jvm_version, install_dir=install_dir,
                                cassandra_version=cassandra_version, env=env,
                                for_build=for_build, info_message=info_message)


def update_path_in_env(env, path_entry):
    path = env['PATH'] if 'PATH' in env else ''
    if not path.startswith(path_entry):
        path = '{}:{}'.format(path_entry, path)
    env['PATH'] = path
    return env


def _maybe_set_use_jdk11_env(env, jvm_version, cassandra_version):
    if jvm_version >= 11 and cassandra_version and '4.0' <= cassandra_version < '5.0':
        env['CASSANDRA_USE_JDK11'] = 'true'
    elif 'CASSANDRA_USE_JDK11' in env:
        del env['CASSANDRA_USE_JDK11']
    return env


def _update_java_version(current_java_version, current_java_home_version,
                         jvm_version=None, install_dir=None, cassandra_version=None, env=None,
                         for_build=False, info_message=None, os_env=None):

    # Internal variant accessible for tests

    if env is None:
        raise RuntimeError("env passed to _update_java_version must not be None")

    if current_java_version and current_java_home_version is None:
        raise RuntimeError("JAVA_HOME must be defined if java command is available on the PATH.")
    if current_java_version and current_java_home_version != current_java_version:
        raise RuntimeError("The version of java available on PATH {} does not match the Java version of the distribution provided via JAVA_HOME {}."
                           .format(current_java_version, current_java_home_version))

    if cassandra_version is None and install_dir:
        cassandra_version = get_version_from_build(install_dir)

    # Java versions supported by the Cassandra distribution
    supported_versions = get_supported_jdk_versions(cassandra_version, install_dir, for_build, os_env if os_env else os.environ)

    # Java versions available in the current environment (JAVA_HOME, JAVAn_HOME)
    available_versions = get_available_jdk_versions(env)

    # Intersection of supported and available Java versions
    supported_available_versions = {v for v in supported_versions if v in available_versions}

    # Determine "jvm_version" - exact Java version to use if it was not explicitly provided
    if not jvm_version:
        if current_java_version and current_java_version in supported_versions:
            info('{}: Using the current Java {} available on PATH for the current invocation of Cassandra {}.'
                 .format(info_message, current_java_version, cassandra_version))
            # nothing to change in this case
            return _maybe_set_use_jdk11_env(env, current_java_version, cassandra_version)

        elif current_java_home_version and current_java_home_version in supported_versions:
            info('{}: Using the current Java {} available from JAVA_HOME for the current invocation of Cassandra {}.'
                 .format(info_message, current_java_version, cassandra_version))
            # just need to add JAVA_HOME/bin to the PATH
            return _maybe_set_use_jdk11_env(update_path_in_env(env, env['JAVA_HOME'] + '/bin'), current_java_home_version, cassandra_version)

        elif current_java_home_version and current_java_home_version not in supported_versions:
            warning('{}: The current Java {} is not supported by Cassandra {} (supported versions: {}).'
                    .format(info_message, current_java_version, cassandra_version, supported_versions))
        else:
            warning('{}: JAVA_HOME is not defined; CCM will try to find a suitable Java distribution among JAVAx_HOME env variables.')

        if len(supported_available_versions) > 0:
            info('{}: CCM has found {} Java distributions, the required Java version for Cassandra {} is {}.'.format(
                info_message, str(available_versions), cassandra_version, supported_versions))
            ref_version = current_java_home_version if current_java_home_version else max(supported_available_versions)
            # sort by the absolute difference between the current java version and the supported version
            # to use the closest version if the current version is not supported
            jvm_version = sorted(supported_available_versions, key=lambda v: abs(v - ref_version))[0]

        # No supported Java version available in the current env
        else:
            raise RuntimeError('{}: Cannot find any Java distribution for the current invocation. Available Java distributions: {}, required Java distributions: {}'
                               .format(info_message, available_versions, supported_versions))

    else:
        info('{}: Using explicitly requested Java version {} for the current invocation of Cassandra {}'
             .format(info_message, jvm_version, cassandra_version))
        if jvm_version not in available_versions:
            raise RuntimeError('{}: The explicitly requested Java version {} is not available in the current env.'
                               .format(info_message, jvm_version))
        if jvm_version not in supported_versions:
            warning('{}: The explicitly requested Java version {} is not supported by Cassandra {}.'
                    .format(info_message, jvm_version, cassandra_version))

    env['JAVA_HOME'] = env[available_versions[jvm_version]]
    return _maybe_set_use_jdk11_env(update_path_in_env(env, env['JAVA_HOME'] + '/bin'), jvm_version, cassandra_version)


def assert_jdk_valid_for_cassandra_version(cassandra_version, env=None):
    jdk_version = float(get_jdk_version(env=env))
    if cassandra_version >= '4.2':
        if jdk_version < 11 or 11 < jdk_version < 17 or 17 < jdk_version:
            error('Cassandra {} requires Java 11 or Java 17, found Java {}'.format(cassandra_version, jdk_version))
            exit(1)
    elif '4.0' <= cassandra_version < '4.2':
        if jdk_version < 1.8 or 9 <= jdk_version < 11:
            error('Cassandra {} requires Java 1.8 or Java 11, found Java {}'.format(cassandra_version, jdk_version))
            exit(1)
    elif cassandra_version >= '3.0' and jdk_version != 1.8:
        error('Cassandra {} requires Java 1.8, found Java {}'.format(cassandra_version, jdk_version))
        exit(1)
    elif cassandra_version < '3.0' and (jdk_version < 1.7 or jdk_version > 1.8):
        error('Cassandra {} requires Java 1.7 or 1.8, found Java {}'.format(cassandra_version, jdk_version))
        exit(1)


def merge_configuration(original, changes, delete_empty=True, delete_always=False):
    if not isinstance(original, dict):
        # if original is not a dictionary, assume changes override it.
        new = changes
    else:
        # Copy original so we do not mutate it.
        new = copy.deepcopy(original)
        for k, v in changes.items():
            # If the new value is None or an empty string, delete it if it's in the original data.
            # We also ensure the backward compatibility of old and new names added as part of CASSANDRA-15234
            if delete_empty and (v is None or (isinstance(v, str) and len(v) == 0)):
                if k in new and new[k] is not None:
                    del new[k]
                elif k in CCM_41_YAML_OPTIONS.keys() and \
                        CCM_41_YAML_OPTIONS[k] in new and \
                        new[CCM_41_YAML_OPTIONS[k]] is not None:
                    del new[CCM_41_YAML_OPTIONS[k]]
            elif not delete_always:
                new_value = v
                # If key is in both dicts, update it with new values.
                if k in new:
                    if isinstance(v, dict):
                        new_value = merge_configuration(new[k], v, delete_empty)
                elif k in CCM_41_YAML_OPTIONS.keys() and CCM_41_YAML_OPTIONS[k] in new:
                    if isinstance(v, dict):
                        new_value = merge_configuration(new[k], v, delete_empty)
                    del new[CCM_41_YAML_OPTIONS[k]]
                new[k] = new_value

    return new


def is_int_not_bool(obj):
    return isinstance(obj, six.integer_types) and not isinstance(obj, bool)


def is_intlike(obj):
    return isinstance(obj, six.integer_types)


def wait_for_any_log(nodes, pattern, timeout, filename='system.log', marks=None):
    """
    Look for a pattern in the system.log of any in a given list
    of nodes.
    @param nodes The list of nodes whose logs to scan
    @param pattern The target pattern
    @param timeout How long to wait for the pattern. Note that
                    strictly speaking, timeout is not really a timeout,
                    but a maximum number of attempts. This implies that
                    the all the grepping takes no time at all, so it is
                    somewhat inaccurate, but probably close enough.
    @param marks A dict of nodes to marks in the file. Keys must match the first param list.
    @return The first node in whose log the pattern was found
    """
    if marks is None:
        marks = {}
    for _ in range(timeout):
        for node in nodes:
            found = node.grep_log(pattern, filename=filename, from_mark=marks.get(node, None))
            if found:
                return node
        time.sleep(1)

    raise TimeoutError(time.strftime("%d %b %Y %H:%M:%S", time.gmtime()) +
                       " Unable to find: " + repr(pattern) + " in any node log within " + str(timeout) + "s")


def get_default_signals():
    if is_win():
        # Fill the dictionary with SIGTERM as the cluster is killed forcefully
        # on Windows regardless of assigned signal (TASKKILL is used)
        default_signal_events = {'1': signal.SIGTERM, '9': signal.SIGTERM}
    else:
        default_signal_events = {'1': signal.SIGHUP, '9': signal.SIGKILL}
    return default_signal_events
