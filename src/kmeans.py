import math
import random
import numpy


class KMeans:
    def __init__(self, points, clusters):
        """Initialises a KMeans classifier with the list of points and the target number of clusters"""
        self.points = points
        self.k = clusters
        self.n = len(points[0])
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

            new_centroids = [self.calculate_centroid(points) for points in clusters.values()]
            for j in range(0, len(centroid) - len(new_centroids)):
                new_centroids.append(self.generate_random_point())
            centroids = new_centroids
        return centroids

    def generate_random_centroids(self):
        """Generates a random point (the centroid) for each cluster"""
        return [self.generate_random_point() for i in range(self.k)]

    @staticmethod
    def calculate_centroid(points):
        n = len(points[0])
        means = []
        for i in range(0, n):
            means.append(numpy.mean([p[i] for p in points]))
        return tuple(means)

    @staticmethod
    def calculate_distance(a, b):
        """Returns the distance between two points"""
        deltas = map(lambda p: pow(p[0] - p[1], 2), zip(a, b))
        return math.sqrt(sum(deltas))

    def generate_random_point(self):
        """Returns a random point within the edges provided"""
        coordinates = []
        for i in range(self.n):
            coordinates.append(random.uniform(self.outer_vertices[2 * i], self.outer_vertices[2 * i + 1]))
        return tuple(coordinates)

    def find_outer_vertices(self):
        """Gets the edges of the point set"""
        edges = []
        # sets the initial edges to be +inf and -inf for every dimension
        for i in range(0, self.n):
            edges.append(float("+inf"))
            edges.append(float("-inf"))
        for point in self.points:
            for i in range(0, self.n):
                edges[2 * i] = min(edges[2 * i], point[i])
                edges[2 * i + 1] = max(edges[2 * i + 1], point[i])
        return tuple(edges)
