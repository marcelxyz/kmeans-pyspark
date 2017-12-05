import math
import random
import numpy


def find_cluster_centroids(points, k):
    """Runs an infinite loop which contains an iteration on the self.points to find the nearest mean.

    :param points: RDD containing the points as tuples
    :param k: the number of clusters to generate
    :return: dict mapping centroid coordinates to the points that belong to it
    """

    dimension_count = len(points.first())
    outer_vertices = find_outer_vertices(points)

    random_clusters = [(generate_random_point(dimension_count, outer_vertices), []) for i in xrange(k)]
    # random_clusters[0] = ((9999, 9999, 9999), [])
    best_clusters = points.context.parallelize(random_clusters).groupByKey().flatMapValues(lambda a: a)

    while True:
        old_clusters = assign_points_to_centroids(best_clusters.keys().collect(), points)

        new_clusters = recalculate_cluster_centroids(old_clusters)

        # if the points were grouped into a number of centroids that's less than k
        # we need to generate random centroids to have k
        new_clusters = add_missing_centroids(k, new_clusters, dimension_count, outer_vertices)

        if best_clusters.keys().collect() == new_clusters.keys().collect():
            return best_clusters

        best_clusters = new_clusters


def add_missing_centroids(k, new_clusters, dimension_count, outer_vertices):
    cluster_count = new_clusters.map(lambda cluster: cluster[0]).count()

    if cluster_count == k:
        return new_clusters

    random_centroids = [(generate_random_point(dimension_count, outer_vertices), []) for i in xrange(k - cluster_count)]

    return new_clusters.context.parallelize(random_centroids).groupByKey().flatMapValues(lambda a: a).union(new_clusters)


def recalculate_cluster_centroids(clusters):
    """

    :param clusters: PipelinedRDD containing the points grouped by their centroid
    :return: PipelinedRDD containing the points grouped by their new centroids
    """
    return clusters.values().map(lambda points: (calculate_centroid(list(points)), points))


def assign_points_to_centroids(centroids, points):
    """

    :param centroids: Dict of centroid to point list mappings
    :param points: RDD of all points
    :return: PipelinedRDD containing point lists grouped by centroid
    """
    return points.groupBy(lambda p: find_closest_centroid(p, centroids)).map(lambda c: (c[0], list(c[1])))


def find_closest_centroid(point, centroids):
    best_distance = float("+inf")
    best_centroid = None

    for centroid in centroids:
        distance = calculate_distance(centroid, point)
        if distance < best_distance:
            best_distance, best_centroid = distance, centroid

    return best_centroid


def calculate_centroid(points):
    means = []
    for i in xrange(len(points[0])):
        means.append(numpy.mean([p[i] for p in points]))
    return tuple(means)


def calculate_distance(a, b):
    """Returns the distance between two points"""
    deltas = map(lambda p: pow(p[0] - p[1], 2), zip(a, b))
    return math.sqrt(sum(deltas))


def generate_random_point(dimension_count, outer_vertices):
    """Returns a random point within the edges provided"""
    coordinates = []
    for i in xrange(dimension_count):
        random_point = random.uniform(outer_vertices[2 * i], outer_vertices[2 * i + 1])
        coordinates.append(random_point)
    return tuple(coordinates)


def find_outer_vertices(points):
    """Gets the edges of the point set"""
    edges = []

    for i in range(len(points.first())):
        edges.append(points.map(lambda p: p[i]).min())
        edges.append(points.map(lambda p: p[i]).max())

    return tuple(edges)
