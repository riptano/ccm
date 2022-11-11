import sys


TEST_DIR = "test-dir"


if sys.version_info < (3, 0):
    FileNotFoundError = OSError
