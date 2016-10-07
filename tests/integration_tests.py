from unittest import TestCase
import os
import shutil
from errno import ENOENT

from ccmlib.cluster import Cluster
from ccmlib import common
from ccmlib.cluster_factory import ClusterFactory


def rmtree_if_exists(path):
    try:
        shutil.rmtree(path)
    except OSError as e:
        if e.errno != ENOENT:
            raise


class TestLoadWithNoCacheRepo(TestCase):
    test_dir = '/tmp/no_cache_test/'
    repository_path = os.path.join(test_dir, 'repository')

    def setUp(self):
        rmtree_if_exists(self.test_dir)

        os.makedirs(self.repository_path)
        self.addCleanup(rmtree_if_exists, self.repository_path)

    def test_delete_cache_and_load(self):
        cluster_name = self._testMethodName
        Cluster(
            path=self.test_dir,
            name=cluster_name,
            version='git:trunk',
            create_directory=True,
            verbose=True,
        )
        common.invalidate_cache(self.repository_path)

        loaded_cluster = ClusterFactory.load(
            self.test_dir, cluster_name
        )
