# ccm clusters
import os
import shutil
import subprocess
import signal
import stat
import time

from six import iteritems
from ccmlib import repository
from ccmlib.cluster import Cluster
from ccmlib.dse_node import DseNode
from ccmlib import common
from ccmlib import auth_util

class DseCluster(Cluster):
    def __init__(self, path, name, partitioner=None, install_dir=None, create_directory=True, version=None, authn=None, authz=None, dse_username=None, dse_password=None, opscenter=None, verbose=False):
        self.dse_username = dse_username
        self.dse_password = dse_password
        self.opscenter = opscenter
        super(DseCluster, self).__init__(path, name, partitioner, install_dir, create_directory, version, authn, authz, verbose)

    def load_from_repository(self, version, verbose):
        return repository.setup_dse(version, self.dse_username, self.dse_password, verbose)

    def install_extras(self, verbose):
        super(DseCluster, self).install_extras(verbose)
        if self.opscenter is not None:
            odir = repository.setup_opscenter(self.opscenter, verbose)
            target_dir = os.path.join(self.get_path(), 'opscenter')
            shutil.copytree(odir, target_dir)
        if self.authn in ['ldap', 'kerberos']:
            self.install_apacheds(verbose)
            if self.authn == 'ldap':
                self.set_configuration_options(values={'authenticator' : 'com.datastax.bdp.cassandra.auth.LdapAuthenticator'})
                self.set_dse_configuration_options(values={'ldap_options' : {'server_host' : 'localhost',
                                                                             'server_port' : 10389,
                                                                             'search_dn' : 'uid=admin,ou=system',
                                                                             'search_password' : 'secret',
                                                                             'user_search_base' : 'ou=users,dc=example,dc=com',
                                                                             'user_search_filter' : '(uid={0})'}})
            elif self.authn == 'kerberos':
                self.set_configuration_options(values={'authenticator' : 'com.datastax.bdp.cassandra.auth.KerberosAuthenticator'})
        elif self.authn == 'transitional':
            self.set_configuration_options(values={'authenticator' : 'com.datastax.bdp.cassandra.auth.TransitionalAuthenticator'})
        elif self.authn == 'permissive':
            self.set_configuration_options(values={'authenticator' : 'com.datastax.bdp.cassandra.auth.PermissiveTransitionalAuthenticator'})
        if self.authz == 'transitional':
            self.set_configuration_options(values={'authorizer' : 'com.datastax.bdp.cassandra.auth.TransitionalAuthorizer'})

    def hasOpscenter(self):
        return os.path.exists(os.path.join(self.get_path(), 'opscenter'))

    def create_node(self, name, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save=True, binary_interface=None):
        return DseNode(name, self, auto_bootstrap, thrift_interface, storage_interface, jmx_port, remote_debug_port, initial_token, save, binary_interface)

    def start(self, no_wait=False, verbose=False, wait_for_binary_proto=False, wait_other_notice=False, jvm_args=[], profile_options=None):
        self.start_apacheds()
        started = super(DseCluster, self).start(no_wait, verbose, wait_for_binary_proto, wait_other_notice, jvm_args, profile_options)
        self.start_opscenter()

        return started

    def stop(self, wait=True, gently=True):
        not_running = super(DseCluster, self).stop(wait, gently)
        self.stop_opscenter()
        self.stop_apacheds()
        return not_running

    def cassandra_version(self):
        return common.get_dse_cassandra_version(self.get_install_dir())

    def set_dse_configuration_options(self, values=None):
        if values is not None:
            for k, v in iteritems(values):
                self._dse_config_options[k] = v
        self._update_config()
        for node in list(self.nodes.values()):
            node.import_dse_config_files()
        return self

    def add_ldap_user(self, userid, password):
        if not self.authn in ['ldap', 'kerberos']:
            raise common.ArgumentError("You can only add users to the ldap server if LDAP or Kerberos authentication are enabled")
        auth_util.add_user(userid, password)

    def delete_ldap_user(self, userid):
        if not self.authn in ['ldap', 'kerberos']:
            raise common.ArgumentError("You can only delete users from the ldap server if LDAP or Kerberos authentication are enabled")
        auth_util.delete_user(userid)

    def start_opscenter(self):
        if self.hasOpscenter():
            self.write_opscenter_cluster_config()
            args = [os.path.join(self.get_path(), 'opscenter', 'bin', common.platform_binary('opscenter'))]
            subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def start_apacheds(self):
        if self.authn in ['ldap', 'kerberos']:
            args = [os.path.join(self.get_path(), 'apacheds', 'bin', 'apacheds.sh'), 'start']
            subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if not auth_util.do_users_exist():
                self.setup_kerberos()

    def stop_opscenter(self):
        pidfile = os.path.join(self.get_path(), 'opscenter', 'twistd.pid')
        if os.path.exists(pidfile):
            with open(pidfile, 'r') as f:
                pid = int(f.readline().strip())
                f.close()
            if pid is not None:
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
            os.remove(pidfile)

    def stop_apacheds(self):
        if self.authn in ['ldap', 'kerberos']:
            pidfile = os.path.join(self.get_path(), 'apacheds', 'instances', 'default', 'run', 'apacheds.pid')
        if os.path.exists(pidfile):
            with open(pidfile, 'r') as f:
                pid = int(f.readline().strip())
                f.close()
            if pid is not None:
                args = [os.path.join(self.get_path(), 'apacheds', 'bin', 'apacheds.sh'), 'stop']
                subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                while common.check_process(pid):
                    time.sleep(1)

    def write_opscenter_cluster_config(self):
        cluster_conf = os.path.join(self.get_path(), 'opscenter', 'conf', 'clusters')
        if not os.path.exists(cluster_conf):
            os.makedirs(cluster_conf)
            if len(self.seeds) > 0:
                seed = self.seeds[0]
                (seed_ip, seed_port) = seed.network_interfaces['thrift']
                seed_jmx = seed.jmx_port
                with open(os.path.join(cluster_conf, self.name + '.conf'), 'w+') as f:
                    f.write('[jmx]\n')
                    f.write('port = %s\n' % seed_jmx)
                    f.write('[cassandra]\n')
                    f.write('seed_hosts = %s\n' % seed_ip)
                    f.write('api_port = %s\n' % seed_port)
                    f.close()

    def install_apacheds(self, verbose=False):
        adir = repository.setup_apacheds(verbose)
        target_dir = os.path.join(self.get_path(), 'apacheds')
        shutil.copytree(adir, target_dir)
        if self.authn == 'kerberos':
            config_ldif = os.path.join(target_dir, 'instances', 'default', 'conf', 'config.ldif')
            os.rename(config_ldif, config_ldif + '.org')
            with open(config_ldif, "wt") as fout:
                with open(config_ldif + '.org', "rt") as fin:
                    enable = False
                    for line in fin:
                        if enable and line == 'ads-enabled: FALSE\n':
                            line = 'ads-enabled: TRUE\n'
                            enable = False
                        if line == 'ads-serverid: kerberosServer\n' or line == 'dn: ads-interceptorId=keyDerivationInterceptor,ou=interceptors,ads-directoryServiceId=default,ou=config\n':
                            enable = True
                        fout.write(line)
            with open(os.path.join(target_dir, 'instances', 'default', 'conf', 'log4j.properties'), 'a') as fout:
                fout.write('\nlog4j.logger.org.apache.directory.server.KERBEROS_LOG=DEBUG\n')

        apacheds = os.path.join(target_dir, 'bin', 'apacheds.sh')
        os.rename(apacheds, apacheds + '.org')
        with open(apacheds, "wt") as fout:
            with open(apacheds + '.org', "rt") as fin:
                for line in fin:
                    if line == '#!/bin/sh\n':
                        line = '#!/bin/bash\n'
                    fout.write(line)
                fin.close()
            fout.close()
        os.chmod(apacheds, os.stat(apacheds).st_mode | stat.S_IXUSR)

    def setup_kerberos(self):
        auth_util.add_users_ou()
        auth_util.add_user('cassandra', 'secret')
        auth_util.add_ticket_granting_user()
        krb5_conf = os.path.join(self.get_path(), 'krb5.conf')
        with open(krb5_conf, "wt") as f:
            f.write('[libdefaults]\n')
            f.write('    default_realm = EXAMPLE.COM\n')
            f.write('    allow_weak_crypto = true\n')
            f.write('    permitted_enctypes = aes128-cts-hmac-sha1-96\n')
            f.write('    default_tgs_enctypes = aes128-cts-hmac-sha1-96\n')
            f.write('    default_tkt_enctypes = aes128-cts-hmac-sha1-96\n')
            f.write('[realms]\n')
            f.write('    EXAMPLE.COM = {\n')
            f.write('    kdc = 127.0.0.1:60088\n')
            f.write('    admin_server = 127.0.0.1\n')
            f.write('    }\n')
            f.close()
        for node in list(self.nodes.values()):
            auth_util.add_service_principal('dse_%s' % node.name)
            auth_util.add_service_principal('HTTP_%s' % node.name)
            node.set_dse_configuration_options(values={'kerberos_options' : { 'http_principal' : 'HTTP_%s/localhost@EXAMPLE.COM' % node.name,
                                                                              'keytab' : os.path.join(self.get_path(), node.name, 'dse.keytab'),
                                                                              'service_principal' : 'dse_%s/localhost@EXAMPLE.COM' % node.name}})
            auth_util.write_keytab(os.path.join(self.get_path(), node.name, 'dse.keytab'), node.name)



