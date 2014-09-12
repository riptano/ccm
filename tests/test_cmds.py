from . import TEST_DIR
from . import ccmtest
from ccmlib.cluster import Cluster
import subprocess
from six import print_

CLUSTER_PATH = TEST_DIR


class TestCCMCmd(ccmtest.Tester):

    def __init__(self, *args, **kwargs):
        ccmtest.Tester.__init__(self, *args, **kwargs)

class TestCCMCreate(TestCCMCmd):

    def tearDown(self):
        p = subprocess.Popen(['ccm', 'remove'])
        p.wait()

    def create_cmd(self, args=None, version='2.0.10'):
        if args is None:
            args = []
        if version:
            args = ['ccm', 'create', 'test', '-v', version] + args
        else:
            args = ['ccm', 'create', 'test'] + args
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

    def cluster_create_populate_test(self):
        args = ['-n','3']
        self.validate_output(self.create_cmd(args))