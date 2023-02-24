import os
import pytest
import shutil

from tests import TEST_DIR

@pytest.fixture(scope="package", autouse=True)
def setup_and_package():
    try:
        shutil.rmtree(TEST_DIR)
    except FileNotFoundError:
        pass

    os.makedirs(TEST_DIR)
    yield
    shutil.rmtree(TEST_DIR)
