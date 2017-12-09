import math
import random
import numpy


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
                return self.map_clusters_for_output(best_clusters)

            # if the points were grouped into a number of centroids that's less than k
            # we need to generate random centroids to have k
            new_clusters = self.add_missing_centroids(self.k, new_clusters, dimension_count, outer_vertices, len(new_cluster_keys))

            best_clusters = new_clusters

    def map_clusters_for_output(self, best_clusters):
        """
        Maps the RDD containing the cluster mappings to the following format:

        [
            (centroid, num_of_points, average_distance_to_centroid)
        ]

        :param best_clusters: PipelinedRDD of centroid -> point mappings
        :return: PipelinedRDD
        """
        return best_clusters.map(lambda cluster: (
            cluster[0], (
                len(cluster[1]),
                self.calculate_average_distance(cluster[0], cluster[1])
            )
        ))

    def add_missing_centroids(self, k, new_clusters, dimension_count, outer_vertices, cluster_count):
        if cluster_count == k:
            return new_clusters

        random_clusters = [(self.generate_random_point(dimension_count, outer_vertices), []) for i in xrange(k - cluster_count)]

        return self.parallelize_clusters(new_clusters.context, random_clusters).union(new_clusters)

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
