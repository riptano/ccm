import unittest
from mock import patch

from distutils.version import LooseVersion

from ccmlib import common
from . import ccmtest


class TestCommon(ccmtest.Tester):

    def test_normalize_interface(self):
        normalized = common.normalize_interface(('::1', 9042))
        self.assertEqual(normalized, ('0:0:0:0:0:0:0:1', 9042))

        normalized = common.normalize_interface(('127.0.0.1', 9042))
        self.assertEqual(normalized, ('127.0.0.1', 9042))

        normalized = common.normalize_interface(('fe80::3e15:c2ff:fed3:db74%en0', 9042))
        self.assertEqual(normalized, ('fe80:0:0:0:3e15:c2ff:fed3:db74%en0', 9042))

        normalized = common.normalize_interface(('fe80::1%lo0', 9042))
        self.assertEqual(normalized, ('fe80:0:0:0:0:0:0:1%lo0', 9042))

        normalized = common.normalize_interface(('fd6d:404d:54cb::1', 9042))
        self.assertEqual(normalized, ('fd6d:404d:54cb:0:0:0:0:1', 9042))

    @patch('ccmlib.common.is_win')
    def test_is_modern_windows_install(self, mock_is_win):
        mock_is_win.return_value = True
        self.assertTrue(common.is_modern_windows_install(2.1))
        self.assertTrue(common.is_modern_windows_install('2.1'))
        self.assertTrue(common.is_modern_windows_install(LooseVersion('2.1')))

        self.assertTrue(common.is_modern_windows_install(3.12))
        self.assertTrue(common.is_modern_windows_install('3.12'))
        self.assertTrue(common.is_modern_windows_install(LooseVersion('3.12')))

        self.assertFalse(common.is_modern_windows_install(1.0))
        self.assertFalse(common.is_modern_windows_install('1.0'))
        self.assertFalse(common.is_modern_windows_install(LooseVersion('1.0')))

    def test_merge_configuration(self):
        # test for merging dict val in key, value pair
        dict0 = dict1 = {'key': {'val1': True}}
        dict2 = {'key': {'val2': False}}

        for k, v in dict2.items():
            dict0[k].update(v)

        self.assertEqual(common.merge_configuration(dict1, dict2), dict0)

        # test for merging str val in key, value pair
        dict0 = dict1 = {'key': 'val1'}
        dict2 = {'key': 'val2'}

        for k, v in dict2.items():
            dict0[k] = v

        self.assertEqual(common.merge_configuration(dict1, dict2), dict0)

    def test_get_jdk_version(self):
        v8u152 = """java version "1.8.0_152"
                 Java(TM) SE Runtime Environment (build 1.8.0_152-b16)
                 Java HotSpot(TM) 64-Bit Server VM (build 25.152-b16, mixed mode)
                 """
        # Since Java 9, the version string syntax changed.
        # Most relevant change is that trailing .0's are omitted. I.e. Java "9.0.0"
        # version string is not "9.0.0" but just "9".
        v900 = """java version "9"
               Java(TM) SE Runtime Environment (build 9+1)
               Java HotSpot(TM) 64-Bit Server VM (build 9+1, mixed mode)
               """
        v901 = """java version "9.0.1"
               Java(TM) SE Runtime Environment (build 9.0.1+11)
               Java HotSpot(TM) 64-Bit Server VM (build 9.0.1+11, mixed mode)
               """
        # 10-internal, just to have an internal (local) build in here
        v10_int = """openjdk version "10-internal"
                  OpenJDK Runtime Environment (build 10-internal+0-adhoc.jenkins.openjdk-shenandoah-jdk10-release)
                  OpenJDK 64-Bit Server VM (build 10-internal+0-adhoc.jenkins.openjdk-shenandoah-jdk10-release, mixed mode)
                  """
        v1000 = """java version "10"
                Java(TM) SE Runtime Environment (build 9+1)
                Java HotSpot(TM) 64-Bit Server VM (build 9+1, mixed mode)
                """
        v1001 = """java version "10.0.1"
                Java(TM) SE Runtime Environment (build 10.0.1+11)
                Java HotSpot(TM) 64-Bit Server VM (build 10.0.1+11, mixed mode)
                """

        self.assertEqual(common._get_jdk_version(v8u152), "1.8")
        self.assertEqual(common._get_jdk_version(v900), "9.0")
        self.assertEqual(common._get_jdk_version(v901), "9.0")
        self.assertEqual(common._get_jdk_version(v10_int), "10.0")
        self.assertEqual(common._get_jdk_version(v1000), "10.0")
        self.assertEqual(common._get_jdk_version(v1001), "10.0")

if __name__ == '__main__':
    unittest.main()
