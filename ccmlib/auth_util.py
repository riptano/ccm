import ldap
import time
import struct

from ldap.cidict import cidict

def make_connection():
    ldapcon = ldap.initialize('ldap://localhost:10389')
    connected = False
    retries = 0
    while not connected:
        try:
            ldapcon.simple_bind_s('uid=admin,ou=system', 'secret')
            connected = True
        except ldap.SERVER_DOWN:
            time.sleep(0.5)
            retries += 1
            if retries == 20:
                raise ldap.SERVER_DOWN

    return ldapcon

def do_users_exist():
    connection = make_connection()
    try:
        search = connection.search_s('ou=users,dc=example,dc=com', ldap.SCOPE_BASE)
    except ldap.NO_SUCH_OBJECT:
        return False
    return len(search) > 0

def add_users_ou():
    record = [
        ('objectclass', ['organizationalUnit']),
        ('ou', ['users'])
    ]
    connection = make_connection()
    connection.add_s('ou=users,dc=example,dc=com', record)

def add_user(userid, password):
    record = [
        ('objectclass', ['person','organizationalperson','inetorgperson', 'krb5principal', 'krb5kdcentry']),
        ('uid', [userid]),
        ('cn', [userid]),
        ('sn', [userid] ),
        ('userpassword', [password]),
        ('krb5PrincipalName', ['%s@EXAMPLE.COM' % userid]),
        ('krb5KeyVersionNumber', ['0'])
    ]
    dn = 'cn=%s,ou=users,dc=example,dc=com' % userid
    connection = make_connection()
    connection.add_s(dn, record)

def add_ticket_granting_user():
    record = [
        ('objectclass', ['person','organizationalperson','inetorgperson', 'krb5principal', 'krb5kdcentry']),
        ('uid', ['krbtgt']),
        ('cn', ['krbtgt']),
        ('sn', ['Service'] ),
        ('userpassword', ['randomKey']),
        ('krb5PrincipalName', ['krbtgt/EXAMPLE.COM@EXAMPLE.COM']),
        ('krb5KeyVersionNumber', ['0'])
    ]
    dn = 'cn=krbtgt,ou=users,dc=example,dc=com'
    connection = make_connection()
    connection.add_s(dn, record)

def add_service_principal(service):
    record = [
        ('objectclass', ['person','organizationalperson','inetorgperson', 'krb5principal', 'krb5kdcentry']),
        ('uid', [service]),
        ('cn', [service]),
        ('sn', ['Service'] ),
        ('userpassword', ['randomKey']),
        ('krb5PrincipalName', ['%s/localhost@EXAMPLE.COM' % service]),
        ('krb5KeyVersionNumber', ['0'])
    ]
    dn = 'cn=%s,ou=users,dc=example,dc=com' % service
    connection = make_connection()
    connection.add_s(dn, record)

def delete_user(userid):
    dn = 'cn=%s,ou=users,dc=example,dc=com' % userid
    connection = make_connection()
    connection.delete_s(dn)

def write_keytab(keytab_file, node):
    connection = make_connection()
    with open(keytab_file, "wb") as f:
        f.write(bytearray([0x05, 0x02]))
        for spn in ['dse_%s' % node, 'HTTP_%s' % node]:
            (_, attrs) = connection.search_s('cn=%s,ou=users,dc=example,dc=com' % spn, ldap.SCOPE_BASE)[0]
            for krbkey in cidict(attrs)['krb5key']:
                key_type = struct.unpack('B', krbkey[6])[0]
                key = krbkey[11:]
                entry_length = len(spn) + len('EXAMPLE.COM') + len('localhost') + len(key) + 21
                f.write(struct.pack('!I', entry_length))
                f.write(struct.pack('!H', 2))
                f.write(struct.pack('!H', len('EXAMPLE.COM')))
                f.write('EXAMPLE.COM')
                f.write(struct.pack('!H', len(spn)))
                f.write(spn)
                f.write(struct.pack('!H', len('localhost')))
                f.write('localhost')
                f.write(struct.pack('!I', 1))
                f.write(struct.pack('!I', int(time.time())))
                f.write(struct.pack('!B', 1))
                f.write(struct.pack('!H', key_type))
                f.write(struct.pack('!H', len(key)))
                f.write(key)
        f.close()
