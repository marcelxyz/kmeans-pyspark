import math
import random
import numpy


class KMeans:
    def __init__(self, points, k):
        """Initialises a KMeans classifier with the list of points and the target number of clusters"""
        self.points = points
        self.k = k
        self.dimension_count = len(points[0])
        self.max_iterations = 100
        self.outer_vertices = self.find_outer_vertices()

    def find_cluster_centroids(self):
        """Runs an infinite loop which contains an iteration on the self.points to find the nearest mean"""
        centroids = self.generate_random_centroids()
        for i in xrange(self.max_iterations):
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

            if self.k != len(new_centroids):
                raise RuntimeError('Centroid length differs')

            # for j in xrange(len(centroid) - len(new_centroids)):
            #     new_centroids.append(self.generate_random_point())
            # todo see if we need this

            if centroids == new_centroids:
                break

            centroids = new_centroids
        return centroids

    def generate_random_centroids(self):
        """Generates a random point (the centroid) for each cluster"""
        return [self.generate_random_point() for i in xrange(self.k)]

    def calculate_centroid(self, points):
        means = []
        for i in xrange(self.dimension_count):
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
        for i in xrange(self.dimension_count):
            random_point = random.uniform(self.outer_vertices[2 * i], self.outer_vertices[2 * i + 1])
            coordinates.append(random_point)
        return tuple(coordinates)

    def find_outer_vertices(self):
        """Gets the edges of the point set"""
        edges = []
        # sets the initial edges to be +inf and -inf for every dimension
        for i in xrange(self.dimension_count):
            edges.append(float("+inf"))
            edges.append(float("-inf"))
        for point in self.points:
            for i in xrange(0, self.dimension_count):
                edges[2 * i] = min(edges[2 * i], point[i])
                edges[2 * i + 1] = max(edges[2 * i + 1], point[i])
        return tuple(edges)
