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

    def test_user__upvotes_cast__to__average_post_length__to__profile_views(self):
        users_xml = self.sc.textFile('fixtures/users.xml')
        posts_xml = self.sc.textFile('fixtures/posts.xml')
        result = spark_jobs.user__upvotes_cast__to__average_post_length__to__profile_views(3, users_xml, posts_xml)

    def test_user__membership_time__to__closed_questions(self):
        users_xml = self.sc.textFile('fixtures/users.xml')
        posts_xml = self.sc.textFile('fixtures/posts.xml')
        post_history_xml = self.sc.textFile('fixtures/post-history.xml')
        result = spark_jobs.user__membership_time__to__closed_questions(3, users_xml, posts_xml, post_history_xml)
