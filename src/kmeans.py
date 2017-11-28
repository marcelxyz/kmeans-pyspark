import math
import random
import numpy


class KMeans:
    def __init__(self, points, clusters):
        """Initialises a KMeans classifier with the list of points and the target number of clusters"""
        self.points = points
        self.k = clusters
        self.outer_vertices = self.find_outer_vertices()

    def find_cluster_centroids(self):
        """Runs an infinite loop which contains an iteration on the self.points to find the nearest mean"""
        centroids = self.generate_random_centroids()

        for i in range(100):
            clusters = {}

            for point in self.points:
                best_distance = float("+inf")
                best_centroid = None

                for centroid in centroids:
                    distance = self.calculate_distance(centroid, point)
                    if distance < best_distance:
                        best_distance = distance
                        best_centroid = centroid

                if best_centroid not in clusters:
                    clusters[best_centroid] = []

                clusters[best_centroid].append(point)

            centroids = [self.calculate_centroid(points) for points in clusters.values()]

        return centroids

    def generate_random_centroids(self):
        """Generates a random point (the centroid) for each cluster"""
        return [self.generate_random_point() for i in range(self.k)]

    @staticmethod
    def calculate_centroid(points):
        x = [p[0] for p in points]
        y = [p[1] for p in points]
        return numpy.mean(x), numpy.mean(y)

    @staticmethod
    def calculate_distance(a, b):
        """Returns the distance between two points"""
        distance_x = abs(a[0] - b[0])
        distance_y = abs(a[1] - b[1])
        return math.sqrt(pow(distance_x, 2) + pow(distance_y, 2))

    def generate_random_point(self):
        """Returns a random point within the edges provided"""
        x = random.uniform(self.outer_vertices[0], self.outer_vertices[1])
        y = random.uniform(self.outer_vertices[2], self.outer_vertices[3])
        return x, y

    def find_outer_vertices(self):
        """Gets the edges of the point set"""
        edges = [
            float("+inf"), float("-inf"), float("+inf"), float("-inf"),
        ]
        for point in self.points:
            edges[0] = min(edges[0], point[0])
            edges[1] = max(edges[1], point[0])
            edges[2] = min(edges[2], point[1])
            edges[3] = max(edges[3], point[1])
        return tuple(edges)
