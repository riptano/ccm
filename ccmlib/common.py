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
import socket
import stat
import subprocess
import sys
import time
from distutils.version import LooseVersion  # pylint: disable=all

import yaml
from six import print_

BIN_DIR = "bin"
CASSANDRA_CONF_DIR = "conf"
DSE_CASSANDRA_CONF_DIR = "resources/cassandra/conf"
OPSCENTER_CONF_DIR = "conf"

CASSANDRA_CONF = "cassandra.yaml"
JVM_OPTS = "jvm.options"
LOG4J_CONF = "log4j-server.properties"
LOG4J_TOOL_CONF = "log4j-tools.properties"
LOGBACK_CONF = "logback.xml"
LOGBACK_TOOLS_CONF = "logback-tools.xml"
CASSANDRA_ENV = "cassandra-env.sh"
CASSANDRA_WIN_ENV = "cassandra-env.ps1"
CASSANDRA_SH = "cassandra.in.sh"

CONFIG_FILE = "config"
CCM_CONFIG_DIR = "CCM_CONFIG_DIR"

logging.basicConfig(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG
                    )

LOG = logging.getLogger('ccm')


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

        return self.versions_to_patterns[max(keys_less_than_version, key=lambda v: LooseVersion(v) if not isinstance(v, LooseVersion) else v)]

    def __repr__(self):
        return str(self.__class__) + "(versions_to_patterns={}, default_pattern={})".format(self.versions_to_patterns, self.default_pattern)

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
            output = subprocess.Popen(["cygpath", "-m", os.path.expanduser('~')], stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()[0].rstrip()
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
        return yaml.load(f)


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
    file_tmp = file + ".tmp"
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
    file_tmp = file + ".tmp"
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
    if not os.path.exists(dst):
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
    env = os.environ.copy()
    env['MAX_HEAP_SIZE'] = os.environ.get('CCM_MAX_HEAP_SIZE', '500M')
    env['HEAP_NEWSIZE'] = os.environ.get('CCM_HEAP_NEWSIZE', '50M')
    env['SPARK_WORKER_MEMORY'] = os.environ.get('SPARK_WORKER_MEMORY', '1024M')
    env['SPARK_WORKER_CORES'] = os.environ.get('SPARK_WORKER_CORES', '2')
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
    env['SPARK_LOCAL_DIRS'] = os.path.join(node_path, 'spark', 'rdd')
    env['SPARK_WORKER_LOG_DIR'] = os.path.join(node_path, 'logs', 'spark', 'worker')
    env['SPARK_MASTER_LOG_DIR'] = os.path.join(node_path, 'logs', 'spark', 'master')
    env['DSE_LOG_ROOT'] = os.path.join(node_path, 'logs', 'dse')
    env['CASSANDRA_LOG_DIR'] = os.path.join(node_path, 'logs')
    env['SPARK_LOCAL_IP'] = '' + node_ip
    if get_version_from_build(node_path=node_path) >= '5.0':
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
            sys.exit("ERROR!  Could not find or execute ant.bat.  Please fix this before attempting to run ccm on Windows.")

        # Confirm matching architectures
        # 32-bit python distributions will launch 32-bit cmd environments, losing PowerShell execution privileges on a 64-bit system
        if sys.maxsize <= 2 ** 32 and platform.machine().endswith('64'):
            sys.exit("ERROR!  64-bit os and 32-bit python distribution found.  ccm requires matching architectures.")


def is_win():
    return sys.platform in ("cygwin", "win32")


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
            # try to add user execute permissions
            # os.chmod doesn't work on Windows and isn't necessary unless in cygwin...
            if sys.platform == "cygwin":
                add_exec_permission(path, stress)
            else:
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
            raise ArgumentError('%s does not appear to be a cassandra or dse installation directory.  Please use absolute pathing (e.g. C:/cassandra.' % install_dir)

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


def check_socket_available(itf, return_on_error=False):
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
        if return_on_error:
            return False
        addr, port = itf
        raise UnavailableSocketError("Inet address %s:%s is not available: %s" % (addr, port, msg))


def check_socket_listening(itf, timeout=60):
    end = time.time() + timeout
    while time.time() <= end:
        try:
            sock = socket.socket()
            sock.connect(itf)
            sock.close()
            return True
        except socket.error:
            # Try again in another 200ms
            time.sleep(.2)
            continue

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
            settings = dict(settings, **yaml.load(s))
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


def get_version_from_build(install_dir=None, node_path=None):
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
            match = re.search('^dse(?:-core)?-([0-9.]+)(?:-SNAPSHOT)?\.jar', file)
            if match:
                return match.group(1)
    return None


def get_dse_cassandra_version(install_dir):
    clib = os.path.join(install_dir, 'resources', 'cassandra', 'lib')
    for file in os.listdir(clib):
        if fnmatch.fnmatch(file, 'cassandra-all*.jar'):
            match = re.search('cassandra-all-([0-9.]+)(?:-.*)?\.jar', file)
            if match:
                return LooseVersion(match.group(1))
    raise ArgumentError("Unable to determine Cassandra version in: " + install_dir)


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
                data = yaml.load(f)
            if 'dse_dir' in data:
                return True
    except IOError:
        return False


def invalidate_cache():
    rmdirs(os.path.join(get_default_path(), 'repository'))


def get_jdk_version():
    version = subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT)
    ver_pattern = '\"(\d+\.\d+).*\"'
    return re.search(ver_pattern, str(version)).groups()[0]


def assert_jdk_valid_for_cassandra_version(cassandra_version):
    if cassandra_version >= '3.0' and get_jdk_version() < '1.8':
        print_('ERROR: Cassandra 3.0+ requires Java >= 1.8, found Java {}'.format(get_jdk_version()))
        exit(1)


def merge_configuration(original, changes, delete_empty=True):
    if not isinstance(original, dict):
        # if original is not a dictionary, assume changes override it.
        new = changes
    else:
        # Copy original so we do not mutate it.
        new = copy.deepcopy(original)
        for k, v in changes.items():
            # If the new value is None or an empty string, delete it
            # if it's in the original data.
            if delete_empty and k in new and \
                    (v is None or (isinstance(v, str) and len(v) == 0)):
                del new[k]
            else:
                new_value = v
                # If key is in both dicts, update it with new values.
                if k in new:
                    if isinstance(v, dict):
                        new_value = merge_configuration(new[k], v, delete_empty)
                new[k] = new_value
    return new


def is_intlike(obj):
    try:
        int(obj)
        return True
    except TypeError:
        return False
    raise RuntimeError('Reached end of {}; should not be possible'.format(is_intlike.__name__))
