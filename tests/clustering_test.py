import unittest
from clustering import KMeans
import random
from pyspark import SparkContext


class ClusteringTest(unittest.TestCase):
    def setUp(self):
        random.seed(1)
        self.sc = SparkContext(master='local')
        self.points = self.sc.parallelize([
            (1, 1, 0),
            (1, 2, 2),
            (2, 2, 2),
            (2, 1, 0),

            (8, 0, 0),
            (8, 1, 2),
            (9, 1, 0),
            (9, 2, 2),

            (1, 1, 7),
            (1, 2, 9),
            (2, 2, 9),
            (2, 1, 7),
        ])
        self.k = 3
        self.kmeans = KMeans(self.k)

    def tearDown(self):
        self.sc.stop()

    def test_find_outer_vertices(self):
        edges = KMeans.find_outer_vertices(self.points)
        self.assertEqual(edges, (1, 9, 0, 2, 0, 9))

    def test_assign_points_to_centroids(self):
        centroids = {
            (1.5, 1.5, 1.0): [],
            (1.5, 1.5, 8.0): [],
            (8.5, 1.0, 1.0): [],
        }
        data = self.kmeans.assign_points_to_centroids(centroids, self.points).collectAsMap()
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

    def test_recalculate_cluster_centroids(self):
        centroids = [
            (
                (10, 11), [(1, 2), (3, 4)]
            ),
            (
                (12, 13), [(2, 3), (4, 5), (6, 7)]
            ),
        ]
        clusters = self.sc.parallelize(centroids).groupByKey().flatMapValues(lambda x: x)

        data = self.kmeans.recalculate_cluster_centroids(clusters).collectAsMap()
        self.assertDictEqual(data, {
            (2, 3): [
                (1, 2),
                (3, 4),
            ],
            (4, 5): [
                (2, 3),
                (4, 5),
                (6, 7),
            ],
        })

    def test_calculate_centroid(self):
        points = [
            (1, 2, 3),
            (3, 4, 5),
        ]
        centroids = KMeans.calculate_centroid(points)
        self.assertEqual(centroids, (2, 3, 4))

    def test_fit(self):
        points = self.sc.parallelize([
            (1, 1), (1, 3), (3, 1), (3, 3),
            (5, 6), (5, 8), (7, 6), (7, 8),
            (10, 2), (10, 4), (12, 2), (12, 4),
        ])
        centroids = self.kmeans.fit(points).collectAsMap()
        self.assertEqual(len(centroids), self.k)
        self.assertDictEqual(centroids, {
            (0.090909090909090912, 0.14285714285714285): [
                (0.0, 0.0),
                (0.0, 0.2857142857142857),
                (0.18181818181818182, 0.0),
                (0.18181818181818182, 0.2857142857142857),
            ],
            (0.45454545454545453, 0.85714285714285721): [
                (0.36363636363636365, 0.7142857142857143),
                (0.36363636363636365, 1.0),
                (0.5454545454545454, 0.7142857142857143),
                (0.5454545454545454, 1.0),
            ],
            (0.9090909090909092, 0.2857142857142857): [
                (0.8181818181818182, 0.14285714285714285),
                (0.8181818181818182, 0.42857142857142855),
                (1.0, 0.14285714285714285),
                (1.0, 0.42857142857142855),
            ],
        })

    def test_calculate_distance(self):
        a = (3, 5, 8, 15)
        b = (2, 3, 4, 5)
        distance = KMeans.calculate_distance(a, b)
        self.assertEqual(distance, 11)

    def test_calculate_average_distance(self):
        centre = (1, 1)
        points = [
            (4, 1),
            (5, 4),
        ]
        self.assertEqual(KMeans.calculate_average_distance(centre, points), 4)

    def test_normalize_data(self):
        points = self.sc.parallelize([
            (10, 2, 50, 11),
            (25, 4, 55, 12),
            (80, 9, 60, 19),
            (100, 10, 65, 21),
        ])
        result = self.kmeans.normalize_data(points, 4).collect()
        self.assertEqual(result, [
            (0, 0, 0, 0),
            (1/6.0, 1/4.0, 1/3.0, 1/10.0),
            (7/9.0, 7/8.0, 2/3.0, 4/5.0),
            (1, 1, 1, 1),
        ])
