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
            (8.5, 1.0): (4, 0.80901699437494745),
            (1.5, 1.5): (4, 0.70710678118654757),
            (1.5, 8.5): (4, 0.70710678118654757),
        })
