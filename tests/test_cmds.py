from . import TEST_DIR
from . import ccmtest
from ccmlib.cluster import Cluster
from ccmlib import common
import subprocess, os, yaml
from six import print_

CLUSTER_PATH = TEST_DIR


class TestCCMCmd(ccmtest.Tester):

    def __init__(self, *args, **kwargs):
        ccmtest.Tester.__init__(self, *args, **kwargs)

class TestCCMCreate(TestCCMCmd):

    def tearDown(self):
        p = subprocess.Popen(['ccm', 'remove'])
        p.wait()

    def create_cmd(self, args=None, name='test', version='2.0.10'):
        if args is None:
            args = []
        if version:
            args = ['ccm', 'create', name, '-v', version] + args
        else:
            args = ['ccm', 'create', name] + args
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return p

    def validate_output(self, process):
        stdout, stderr = process.communicate()
        try:
            print_("[OUT] %s" % stdout)
            self.assertEqual(len(stderr), 0)
        except AssertionError:
            print_("[ERROR] %s" % stderr.strip())
            raise

    def cluster_create_version_test(self):
        self.validate_output(self.create_cmd())

    def cluster_create_cassandra_dir_test(self):
        c_dir = common.get_default_path()
        c_dir = os.path.join(c_dir, 'repository')
        c_dir = os.path.join(c_dir, os.listdir(c_dir)[0])
        args = ['--install-dir', c_dir]
        self.validate_output(self.create_cmd(args, version=None))

    def cluster_create_populate_test(self):
        args = ['-n','3']
        self.validate_output(self.create_cmd(args))

    def cluster_create_no_switch_test(self):
        self.create_cmd(args=None, name='not_test')
        args = ['--no-switch']
        self.validate_output(self.create_cmd(args))
        self.assertEqual('not_test', common.current_cluster_name(common.get_default_path()))

        p = subprocess.Popen(['ccm', 'remove'])
        p.wait()
        p = subprocess.Popen(['ccm', 'switch', 'test'])
        p.wait()
        p = subprocess.Popen(['ccm', 'remove'])
        p.wait()

    def cluster_create_start_test(self):
        args = ['-n', '1', '-s']
        self.validate_output(self.create_cmd(args))

        pidfile = os.path.join(common.get_default_path(), 'test', 'node1', 'cassandra.pid')
        with open(pidfile, 'r') as f:
            pid = int(f.readline().strip())
        os.kill(pid, 0)

    def cluster_create_debug_start_test(self):
        args = ['-n', '1', '-s', '-d']
        p = self.create_cmd(args)
        stdout, stderr = p.communicate()

        print_("[OUT] %s" % stdout)
        self.assertGreater(len(stdout), 18000)
        self.assertEqual(len(stderr), 0)

    def cluster_create_vnodes_test(self):
        args = ['-n', '1', '--vnodes']
        self.validate_output(self.create_cmd(args))
        yaml_path = os.path.join(common.get_default_path(), 'test', 'node1', 'conf', 'cassandra.yaml')
        with open(yaml_path, 'r') as f:
            data = yaml.load(f)

        self.assertEqual(256, data['num_tokens'])
