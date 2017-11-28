import unittest
import kmeans
import random


class KmeansTest(unittest.TestCase):
    def setUp(self):
        random.seed(1)
        self.points = [
            (1, 6),
            (1, 8),
            (3, 6),
            (3, 8),

            (4, 1),
            (4, 2),
            (5, 1),
            (5, 2),

            (10, 3),
            (10, 2),
            (13, 4),
            (13, 5),
        ]
        self.num_of_clusters = 3
        self.kmeans = kmeans.KMeans(self.points, self.num_of_clusters)

    def test_calculate_outer_vertices(self):
        edges = self.kmeans.find_outer_vertices()
        self.assertEqual(edges, (1, 13, 1, 8))

    def test_find_cluster_centroids(self):
        means = self.kmeans.find_cluster_centroids()
        means_tuples = list(map(lambda point: tuple(point), means))  # convert list of Point objects to list of tuples
        self.assertEqual(len(means), self.num_of_clusters)
        self.assertTrue((2.0, 7.0) in means_tuples)
        self.assertTrue((4.5, 1.5) in means_tuples)
        self.assertTrue((11.5, 3.5) in means_tuples)

    def test_calculate_distance(self):
        a = (3, 5)
        b = (6, 9)
        distance = kmeans.KMeans.calculate_distance(a, b)
        self.assertEqual(distance, 5)

    def test_calculate_centroid(self):
        points = [
            (1, 2),
            (3, 4),
        ]
        centroids = kmeans.KMeans.calculate_centroid(points)
        self.assertEqual(centroids, (2, 3))
