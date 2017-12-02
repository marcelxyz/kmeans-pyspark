import math
import random
import numpy


def find_cluster_centroids(points, k):
    """Runs an infinite loop which contains an iteration on the self.points to find the nearest mean"""

    dimension_count = len(points[0])
    outer_vertices = find_outer_vertices(points)

    best_clusters = {generate_random_point(dimension_count, outer_vertices): [] for i in xrange(k)}

    while True:
        old_clusters = assign_points_to_centroids(best_clusters.keys(), points)

        new_clusters = recalculate_cluster_centroids(old_clusters)

        # if the points were grouped into a number of centroids that's less than k
        # we need to generate random centroids to improve grouping
        add_missing_centroids(k, new_clusters, dimension_count, outer_vertices)

        if best_clusters == new_clusters:
            return best_clusters

        best_clusters = new_clusters


def add_missing_centroids(k, new_clusters, dimension_count, outer_vertices):
    for j in xrange(k - len(new_clusters)):
        random_centroid = generate_random_point(dimension_count, outer_vertices)
        new_clusters[random_centroid] = []


def recalculate_cluster_centroids(clusters):
    new_clusters = {}

    for centroid, point_list in clusters.iteritems():
        new_centroid = calculate_centroid(point_list)
        new_clusters[new_centroid] = point_list

    return new_clusters


def assign_points_to_centroids(centroids, points):
    clusters = {}

    for point in points:
        best_distance = float("+inf")
        best_centroid = None

        for centroid in centroids:
            distance = calculate_distance(centroid, point)
            if distance < best_distance:
                best_distance, best_centroid = distance, centroid

        if best_centroid not in clusters:
            clusters[best_centroid] = []

        clusters[best_centroid].append(point)

    return clusters


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
    dimension_count = len(points[0])
    edges = []
    # sets the initial edges to be +inf and -inf for every dimension
    for i in xrange(dimension_count):
        edges.append(float("+inf"))
        edges.append(float("-inf"))
    for point in points:
        for i in xrange(0, dimension_count):
            edges[2 * i] = min(edges[2 * i], point[i])
            edges[2 * i + 1] = max(edges[2 * i + 1], point[i])
    return tuple(edges)
