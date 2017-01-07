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

if __name__ == '__main__':
    unittest.main()
