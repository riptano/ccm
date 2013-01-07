#
# Cassandra Cluster Management lib
#

import os, common, shutil, re, sys, cluster, node, socket

USER_HOME = os.path.expanduser('~')

CASSANDRA_BIN_DIR= "bin"
CASSANDRA_CONF_DIR= "conf"

CASSANDRA_CONF = "cassandra.yaml"
LOG4J_CONF = "log4j-server.properties"
LOG4J_TOOL_CONF = "log4j-tools.properties"
CASSANDRA_ENV = "cassandra-env.sh"
CASSANDRA_SH = "cassandra.in.sh"

class CCMError(Exception):
    pass

class LoadError(CCMError):
    pass

class ArgumentError(CCMError):
    pass

class UnavailableSocketError(CCMError):
    pass

def get_default_path():
    default_path = os.path.join(USER_HOME, '.ccm')
    if not os.path.exists(default_path):
        os.mkdir(default_path)
    return default_path

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

def load_current_cluster(path):
    name = current_cluster_name(path)
    if name is None:
        print 'No currently active cluster (use ccm cluster switch)'
        exit(1)
    try:
        return cluster.Cluster.load(path, name)
    except common.LoadError as e:
        print str(e)
        exit(1)

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

def make_cassandra_env(cassandra_dir, node_path):
    sh_file = os.path.join(CASSANDRA_BIN_DIR, CASSANDRA_SH)
    orig = os.path.join(cassandra_dir, sh_file)
    dst = os.path.join(node_path, sh_file)
    shutil.copy(orig, dst)
    replacements = [
        ('CASSANDRA_HOME=', '\tCASSANDRA_HOME=%s' % cassandra_dir),
        ('CASSANDRA_CONF=', '\tCASSANDRA_CONF=%s' % os.path.join(node_path, 'conf'))
    ]
    common.replaces_in_file(dst, replacements)

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

def get_stress_bin(cassandra_dir):
    stress = os.path.join(cassandra_dir, 'contrib', 'stress', 'bin', 'stress')
    if os.path.exists(stress):
        return stress

    stress = os.path.join(cassandra_dir, 'tools', 'stress', 'bin', 'stress')
    if os.path.exists(stress):
        return stress

    stress = os.path.join(cassandra_dir, 'tools', 'bin', 'stress')
    if os.path.exists(stress):
        return stress

    stress = os.path.join(cassandra_dir, 'tools', 'bin', 'cassandra-stress')
    if os.path.exists(stress):
        return stress

    raise Exception("Cannot find stress binary (maybe it isn't compiled)")

def validate_cassandra_dir(cassandra_dir):
    if cassandra_dir is None:
        raise ArgumentError('Undefined cassandra directory')

    bin_dir = os.path.join(cassandra_dir, CASSANDRA_BIN_DIR)
    conf_dir = os.path.join(cassandra_dir, CASSANDRA_CONF_DIR)
    cnd = os.path.exists(bin_dir)
    cnd = cnd and os.path.exists(conf_dir)
    cnd = cnd and os.path.exists(os.path.join(conf_dir, CASSANDRA_CONF))
    cnd = cnd and os.path.exists(os.path.join(conf_dir, LOG4J_CONF))
    if not cnd:
        raise ArgumentError('%s does not appear to be a cassandra source directory' % cassandra_dir)

def check_socket_available(itf):
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(itf)
        s.close()
    except socket.error, msg:
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
