import random
import unittest
from pyspark import SparkContext
import index


class SparkJobTest(unittest.TestCase):
    def setUp(self):
        random.seed(1)
        self.sc = SparkContext(master="local")

    def tearDown(self):
        self.sc.stop()

    def test_user__reputation__to__upvotes_cast(self):
        result = index.run_job(self.sc, 'user__reputation__to__upvotes_cast', 3, ['tests/fixtures/users.xml'])
        data = result.collectAsMap()
        self.assertDictEqual(data, {
            (1.5, 1.5): [
                (1, 1),
                (1, 2),
                (2, 2),
                (2, 1),
            ],
            (1.5, 8.5): [
                (2, 8),
                (2, 8),
                (1, 9),
                (1, 9),
            ],
            (8.5, 1.0): [
                (8, 0),
                (8, 1),
                (9, 1),
                (9, 2),
            ],
        })
