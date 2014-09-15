from . import ccmtest
from ccmlib import common
import yaml

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

    def test_yaml_merge(self):
        primary_filename = "./test_files/primary.yaml"
        secondary_filename = "./test_files/secondary.yaml"

        with open(primary_filename, 'r') as f:
            primary = yaml.load(f)

        with open(secondary_filename, 'r') as f:
            secondary = yaml.load(f)

        merged = common.yaml_merge(primary, secondary)

        expected = {'should_be_one': 1,
            'should_be_one_nested': {'should_still_be_one': 1},
            'not_in_secondary': True,
            'not_in_primary': False
        }

        self.assertItemsEqual(merged, expected)


if __name__ == '__main__':
        unittest.main()
