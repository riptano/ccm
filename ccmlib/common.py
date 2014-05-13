#
# Cassandra Cluster Management lib
#

import os
import re
import shutil
import socket
import stat
import subprocess
import sys
from six import print_
import time
import yaml

CASSANDRA_BIN_DIR= "bin"
CASSANDRA_CONF_DIR= "conf"

CASSANDRA_CONF = "cassandra.yaml"
LOG4J_CONF = "log4j-server.properties"
LOG4J_TOOL_CONF = "log4j-tools.properties"
LOGBACK_CONF = "logback.xml"
CASSANDRA_ENV = "cassandra-env.sh"
CASSANDRA_WIN_ENV = "cassandra-env.ps1"
CASSANDRA_SH = "cassandra.in.sh"

CONFIG_FILE = "config"

class CCMError(Exception):
    pass

class LoadError(CCMError):
    pass

class ArgumentError(CCMError):
    pass

class UnavailableSocketError(CCMError):
    pass

def get_default_path():
    default_path = os.path.join(get_user_home(), '.ccm')
    if not os.path.exists(default_path):
        os.mkdir(default_path)
    return default_path

def get_user_home():
    if is_win():
        if sys.platform == "cygwin":
            # Need the fully qualified directory
            output = subprocess.Popen(["cygpath", "-m", os.path.expanduser('~')], stdout = subprocess.PIPE, stderr = subprocess.STDOUT).communicate()[0].rstrip()
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
    rs = [ (re.compile(regexp), repl) for (regexp, repl) in replacement_list]
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

def replaces_or_add_into_file_tail(file, replacement_list):
    rs = [ (re.compile(regexp), repl) for (regexp, repl) in replacement_list]
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
                f_tmp.write(line)
            # In case, entry is not found, and need to be added
            if is_line_found == False:
                f_tmp.write('\n'+ replace + "\n")

    shutil.move(file_tmp, file)

def make_cassandra_env(cassandra_dir, node_path):
    if is_win() and get_version_from_build() >= 2.1:
        sh_file = os.path.join(CASSANDRA_CONF_DIR, CASSANDRA_WIN_ENV)
    else:
        sh_file = os.path.join(CASSANDRA_BIN_DIR, CASSANDRA_SH)
    orig = os.path.join(cassandra_dir, sh_file)
    dst = os.path.join(node_path, sh_file)
    shutil.copy(orig, dst)
    replacements = ""
    if is_win() and get_version_from_build() >= 2.1:
        replacements = [
            ('env:CASSANDRA_HOME =', '        $env:CASSANDRA_HOME="%s"' % cassandra_dir),
            ('env:CASSANDRA_CONF =', '    $env:CASSANDRA_CONF="%s"' % os.path.join(node_path, 'conf'))
        ]
    else:
        replacements = [
            ('CASSANDRA_HOME=', '\tCASSANDRA_HOME=%s' % cassandra_dir),
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
    return env

def check_win_requirements():
    if is_win():
        # Make sure ant.bat is in the path and executable before continuing
        try:
            process = subprocess.Popen('ant.bat', stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        except Exception as e:
            sys.exit("ERROR!  Could not find or execute ant.bat.  Please fix this before attempting to run ccm on Windows.")

def is_win():
    return True if sys.platform == "cygwin" or sys.platform == "win32" else False

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

def get_stress_bin(cassandra_dir):
    candidates = [
        os.path.join(cassandra_dir, 'contrib', 'stress', 'bin', 'stress'),
        os.path.join(cassandra_dir, 'tools', 'stress', 'bin', 'stress'),
        os.path.join(cassandra_dir, 'tools', 'bin', 'stress'),
        os.path.join(cassandra_dir, 'tools', 'bin', 'cassandra-stress')
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

def validate_cassandra_dir(cassandra_dir):
    if cassandra_dir is None:
        raise ArgumentError('Undefined cassandra directory')

    # Windows requires absolute pathing on cassandra dir - abort if specified cygwin style
    if is_win():
        if ':' not in cassandra_dir:
            raise ArgumentError('%s does not appear to be a cassandra source directory.  Please use absolute pathing (e.g. C:/cassandra.' % cassandra_dir)

    bin_dir = os.path.join(cassandra_dir, CASSANDRA_BIN_DIR)
    conf_dir = os.path.join(cassandra_dir, CASSANDRA_CONF_DIR)
    cnd = os.path.exists(bin_dir)
    cnd = cnd and os.path.exists(conf_dir)
    cnd = cnd and os.path.exists(os.path.join(conf_dir, CASSANDRA_CONF))
    if not cnd:
        raise ArgumentError('%s does not appear to be a cassandra source directory' % cassandra_dir)

def check_socket_available(itf):
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(itf)
        s.close()
    except socket.error as msg:
        s.close()
        addr, port = itf
        raise UnavailableSocketError("Inet address %s:%s is not available: %s" % (addr, port, msg))

def parse_settings(args):
    settings = {}
    for s in args:
        splitted = s.split(':')
        if len(splitted) != 2:
            raise ArgumentError("A new setting should be of the form 'key: value', got" + s)
        val = splitted[1].strip()
        # ok, that's not super beautiful
        if val.lower() == "true":
            val = True
        if val.lower() == "false":
            val = True
        try:
            val = int(val)
        except ValueError:
            pass
        settings[splitted[0].strip()] = val
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

def get_version_from_build(cassandra_dir=None):
    if cassandra_dir is None:
        cassandra_dir = os.environ.get('CASSANDRA_HOME')
    if cassandra_dir is not None:
        build = os.path.join(cassandra_dir, 'build.xml')
        with open(build) as f:
            for line in f:
                match = re.search('name="base\.version" value="([0-9.]+)[^"]*"', line)
                if match:
                    return match.group(1)
    raise CCMError("Cannot find version")