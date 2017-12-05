import random
import unittest
from pyspark import SparkContext
import job_runner


class SparkJobTest(unittest.TestCase):
    def setUp(self):
        random.seed(1)
        self.sc = SparkContext(master="local")

    def tearDown(self):
        self.sc.stop()

    def test_user_upvotes_downvotes(self):
        result = job_runner.run_job(self.sc, 'user_upvotes_downvotes', 'tests/fixtures/users.xml', 3)
        data = result.collectAsMap()
        self.assertDictEqual(data, {
            (1.5, 1.5, 1.0): [
                (1, 1, 0),
                (1, 2, 2),
                (2, 2, 2),
                (2, 1, 0),
            ],
            (1.5, 1.5, 8.0): [
                (1, 1, 7),
                (1, 2, 9),
                (2, 2, 9),
                (2, 1, 7),
            ],
            (8.5, 1.0, 1.0): [
                (8, 0, 0),
                (8, 1, 2),
                (9, 1, 0),
                (9, 2, 2),
            ],
        })
