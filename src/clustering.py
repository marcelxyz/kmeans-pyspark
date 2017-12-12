from __future__ import division
import math
import random
import numpy
from pyspark.sql import SQLContext
from pyspark.sql.types import *

class KMeans:
    def __init__(self, k):
        self.k = k

    def fit(self, points):
        """Runs an infinite loop which contains an iteration on the self.points to find the nearest mean.

        :param points: RDD containing the points as tuples
        :param k: the number of clusters to generate
        :return: dict mapping centroid coordinates to the points that belong to it
        """
        points.cache()

        dimension_count = len(points.first())

        points = self.normalize_data(points, dimension_count)

        outer_vertices = self.find_outer_vertices(points)

        random_clusters = [(self.generate_random_point(dimension_count, outer_vertices), []) for i in xrange(self.k)]
        best_clusters = self.parallelize_clusters(points.context, random_clusters)

        # best_clusters is an RDD with the following schema:
        # [
        #   (centroid, point_list),
        # ]
        # where:
        #   - centroid is a tuple containing the centroid coordinates
        #   - point_list is a list of points (stored as tuples) of points contained in that cluster

        while True:
            old_clusters = self.assign_points_to_centroids(best_clusters.keys().collect(), points)

            new_clusters = self.recalculate_cluster_centroids(old_clusters)

            new_cluster_keys = new_clusters.keys().collect()

            if best_clusters.keys().collect() == new_clusters.keys().collect():
                return best_clusters

            # if the points were grouped into a number of centroids that's less than k
            # we need to generate random centroids to have k
            new_clusters = self.add_missing_centroids(self.k, new_clusters, dimension_count, outer_vertices, len(new_cluster_keys))

            best_clusters = new_clusters

    def add_missing_centroids(self, k, new_clusters, dimension_count, outer_vertices, cluster_count):
        if cluster_count == k:
            return new_clusters

        random_clusters = [(self.generate_random_point(dimension_count, outer_vertices), []) for i in xrange(k - cluster_count)]

        return self.parallelize_clusters(new_clusters.context, random_clusters).union(new_clusters)

    def normalize_data(self, points, dimension_count):
        """
        Normalizes all points so they are between 0 and 1 within their column.

        :param points: RDD of all points
        :param dimension_count: number of dimensions in each point
        :return: RDD containing normalized data points
        """
        normalized_columns = map(lambda column: self.normalize_column(points, column), xrange(dimension_count))
        return reduce(lambda a, b: a.zip(b).map(self.flatten_points), normalized_columns)

    @staticmethod
    def flatten_points(point):
        """
        Converts tuples of the form ((a, b), c) to (a, b, c).

        :param point: One or two dimensional tuple
        :return: One dimensional point tuple
        """
        if isinstance(point[0], tuple):
            flat_value = list(point[0])
            flat_value.append(point[1])
            return tuple(flat_value)

        return point

    @staticmethod
    def normalize_column(points, column_index):
        """
        Normalizes a single column of values (one dimension) so all its values are between 0 and 1.

        :param points: RDD of all points
        :param column_index: Column/dimension to normalize
        :return: RDD containing the mapped column
        """
        column = points.map(lambda p: p[column_index])

        minimum = column.min()
        maximum = column.max()

        return column.map(lambda value: (value - minimum) / (maximum - minimum))

    @staticmethod
    def parallelize_clusters(context, clusters):
        return context.parallelize(clusters).groupByKey().flatMapValues(lambda a: a)

    def recalculate_cluster_centroids(self, clusters):
        """

        :param clusters: PipelinedRDD containing the points grouped by their centroid
        :return: PipelinedRDD containing the points grouped by their new centroids
        """
        return clusters.values().map(lambda points: (self.calculate_centroid(list(points)), points))

    def assign_points_to_centroids(self, centroids, points):
        """

        :param centroids: Dict of centroid to point list mappings
        :param points: RDD of all points
        :return: PipelinedRDD containing point lists grouped by centroid
        """
        return points \
            .groupBy(lambda point: self.find_closest_centroid(point, centroids)) \
            .map(lambda cluster: (cluster[0], list(cluster[1])))

    @staticmethod
    def find_closest_centroid(point, centroids):
        best_distance = float("+inf")
        best_centroid = None

        for centroid in centroids:
            distance = KMeans.calculate_distance(centroid, point)
            if distance < best_distance:
                best_distance, best_centroid = distance, centroid

        return best_centroid

    @staticmethod
    def calculate_centroid(points):
        means = []
        for i in xrange(len(points[0])):
            means.append(numpy.mean([p[i] for p in points]))
        return tuple(means)

    @staticmethod
    def calculate_average_distance(centroid, points):
        distances = map(lambda p: KMeans.calculate_distance(centroid, p), points)
        return numpy.mean(distances)

    @staticmethod
    def calculate_distance(a, b):
        """Returns the distance between two points"""
        deltas = map(lambda p: pow(p[0] - p[1], 2), zip(a, b))
        return math.sqrt(sum(deltas))

    @staticmethod
    def generate_random_point(dimension_count, outer_vertices):
        """Returns a random point within the edges provided"""
        coordinates = []
        for i in xrange(dimension_count):
            random_point = random.uniform(outer_vertices[2 * i], outer_vertices[2 * i + 1])
            coordinates.append(random_point)
        return tuple(coordinates)

    @staticmethod
    def find_outer_vertices(points):
        """Gets the edges of the point set"""
        edges = []

        for i in range(len(points.first())):
            edges.append(points.map(lambda p: p[i]).min())
            edges.append(points.map(lambda p: p[i]).max())

        return tuple(edges)
