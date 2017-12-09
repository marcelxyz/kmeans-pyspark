import unittest
import kmeans
import random
from pyspark import SparkContext


class KmeansTest(unittest.TestCase):
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

    def tearDown(self):
        self.sc.stop()

    def test_find_outer_vertices(self):
        edges = kmeans.find_outer_vertices(self.points)
        self.assertEqual(edges, (1, 9, 0, 2, 0, 9))

    def test_assign_points_to_centroids(self):
        centroids = {
            (1.5, 1.5, 1.0): [],
            (1.5, 1.5, 8.0): [],
            (8.5, 1.0, 1.0): [],
        }
        data = kmeans.assign_points_to_centroids(centroids, self.points).collectAsMap()
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

        data = kmeans.recalculate_cluster_centroids(clusters).collectAsMap()
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
        centroids = kmeans.calculate_centroid(points)
        self.assertEqual(centroids, (2, 3, 4))

    def test_find_cluster_centroids(self):
        centroids = kmeans.fit(self.points, self.k).collectAsMap()
        self.assertEqual(len(centroids), self.k)
        self.assertDictEqual(centroids, {
            (8.5, 1.0, 1.0): (4, 1.3090169943749475),
            (1.5, 1.5, 8.0): (4, 1.2247448713915889),
            (1.5, 1.5, 1.0): (4, 1.2247448713915889),
        })

    def test_calculate_distance(self):
        a = (3, 5, 8, 15)
        b = (2, 3, 4, 5)
        distance = kmeans.calculate_distance(a, b)
        self.assertEqual(distance, 11)

    def test_calculate_average_distance(self):
        centre = (1, 1)
        points = [
            (4, 1),
            (5, 4),
        ]
        self.assertEqual(kmeans.calculate_average_distance(centre, points), 4)
