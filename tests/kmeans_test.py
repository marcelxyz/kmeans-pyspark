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
        self.num_of_clusters = 3
        self.kmeans = kmeans.KMeans(self.points, self.num_of_clusters)

    def test_calculate_outer_vertices(self):
        edges = self.kmeans.find_outer_vertices()
        self.assertEqual(edges, (1, 9, 0, 2, 0, 9))

    def test_find_cluster_centroids(self):
        means = self.kmeans.find_cluster_centroids()
        means_tuples = list(map(lambda point: tuple(point), means))  # convert list of Point objects to list of tuples
        self.assertEqual(len(means), self.num_of_clusters)
        self.assertTrue((1.5, 1.5, 1.0) in means_tuples)
        self.assertTrue((8.5, 1.0, 1.0) in means_tuples)
        self.assertTrue((1.5, 1.5, 8.0) in means_tuples)

    def test_calculate_distance(self):
        a = (3, 5, 8, 15)
        b = (2, 3, 4, 5)
        distance = kmeans.KMeans.calculate_distance(a, b)
        self.assertEqual(distance, 11)

    def test_calculate_centroid(self):
        points = [
            (1, 2, 3),
            (3, 4, 5),
        ]
        centroids = kmeans.KMeans.calculate_centroid(points)
        self.assertEqual(centroids, (2, 3, 4))
