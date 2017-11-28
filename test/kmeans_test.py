import unittest
import kmeans
import random


class KmeansTest(unittest.TestCase):
    def setUp(self):
        random.seed(1)
        self.points = [
            kmeans.Point(1, 6),
            kmeans.Point(1, 8),
            kmeans.Point(3, 6),
            kmeans.Point(3, 8),

            kmeans.Point(4, 1),
            kmeans.Point(4, 2),
            kmeans.Point(5, 1),
            kmeans.Point(5, 2),

            kmeans.Point(10, 3),
            kmeans.Point(10, 2),
            kmeans.Point(13, 4),
            kmeans.Point(13, 5),
        ]
        self.num_of_clusters = 3
        self.kmeans = kmeans.KMeans(self.points, self.num_of_clusters)

    def test_calculate_outer_vertices(self):
        edges = self.kmeans.calculate_outer_vertices()
        self.assertEqual(edges, (1, 13, 1, 8))

    def test_find(self):
        means = self.kmeans.find()
        means_tuples = list(map(lambda point: tuple(point), means))  # convert list of Point objects to list of tuples
        self.assertEqual(len(means), self.num_of_clusters)
        self.assertTrue((2.0, 7.0) in means_tuples)
        self.assertTrue((4.5, 1.5) in means_tuples)
        self.assertTrue((11.5, 3.5) in means_tuples)
