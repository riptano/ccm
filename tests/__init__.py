import os
import shutil
import sys


TEST_DIR = "test-dir"


if sys.version_info < (3, 0):
    FileNotFoundError = OSError


def setup_package():
    try:
        shutil.rmtree(TEST_DIR)
    except FileNotFoundError:
        pass

    os.makedirs(TEST_DIR)


def teardown_package():
    shutil.rmtree(TEST_DIR)
