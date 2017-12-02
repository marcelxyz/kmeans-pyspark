import unittest
import kmeans
import random


class KmeansTest(unittest.TestCase):
    def setUp(self):
        random.seed(1)
        self.points = [
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
        ]
        self.k = 3

    def test_calculate_outer_vertices(self):
        edges = kmeans.find_outer_vertices(self.points)
        self.assertEqual(edges, (1, 9, 0, 2, 0, 9))

    def test_find_cluster_centroids(self):
        centroids = kmeans.find_cluster_centroids(self.points, self.k)
        self.assertEqual(len(centroids), self.k)
        self.assertDictEqual(centroids, {
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

    def test_calculate_distance(self):
        a = (3, 5, 8, 15)
        b = (2, 3, 4, 5)
        distance = kmeans.calculate_distance(a, b)
        self.assertEqual(distance, 11)

    def test_calculate_centroid(self):
        points = [
            (1, 2, 3),
            (3, 4, 5),
        ]
        centroids = kmeans.calculate_centroid(points)
        self.assertEqual(centroids, (2, 3, 4))
