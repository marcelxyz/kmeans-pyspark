import random
import unittest
from pyspark import SparkContext
import spark_jobs


class SparkJobTest(unittest.TestCase):
    def setUp(self):
        random.seed(1)
        self.sc = SparkContext(master="local")

    def tearDown(self):
        self.sc.stop()

    def test_user__reputation__to__upvotes_cast(self):
        xml_file = self.sc.textFile('fixtures/users.xml')
        result = spark_jobs.user__reputation__to__upvotes_cast(3, xml_file)
        data = result.collectAsMap()
        self.assertDictEqual(data, {
            (0.9375, 0.1111111111111111): [
                (0.875, 0.0),
                (0.875, 0.1111111111111111),
                (1.0, 0.1111111111111111), (1.0, 0.2222222222222222),
            ],
            (0.0625, 0.94444444444444442): [
                (0.125, 0.8888888888888888),
                (0.125, 0.8888888888888888),
                (0.0, 1.0),
                (0.0, 1.0),
            ],
            (0.0625, 0.16666666666666669): [
                (0.0, 0.1111111111111111),
                (0.0, 0.2222222222222222),
                (0.125, 0.2222222222222222),
                (0.125, 0.1111111111111111),
            ]})

    def test_user_rep(self):
        xml_file = self.sc.textFile('fixtures/users.xml')
        result = spark_jobs.user_rep(2, xml_file)
        data = result.collectAsMap()
        self.assertDictEqual(data, {
            (1.0/16,): [
                (0,), (0,),
                (0.125,), (0.125,), (0.125,), (0.125,),
                (0,), (0,),
            ],
            (1.875/2,): [
                (0.875,), (0.875,),
                (1,), (1,),
            ],
        })

    def test_user_questions_asked(self):
        xml_file = self.sc.textFile('fixtures/posts.xml')
        result = spark_jobs.user_questions_asked(1, xml_file)
        data = result.collectAsMap()
        self.assertDictEqual(data, {
            (1.0/3,): [
                (0,),
                (0,),
                (1,),
            ],
        })

    def test_user_questions_answered(self):
        xml_file = self.sc.textFile('fixtures/posts.xml')
        result = spark_jobs.user_questions_answered(1, xml_file)
        data = result.collectAsMap()
        self.assertDictEqual(data, {
            (0.5,): [
                (1,),
                (0.5,),
                (0,),
            ],
        })
