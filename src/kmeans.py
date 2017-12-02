import math
import random
import numpy


def find_cluster_centroids(points, k):
    """Runs an infinite loop which contains an iteration on the self.points to find the nearest mean"""

    dimension_count = len(points[0])
    max_iterations = 100
    outer_vertices = find_outer_vertices(points)

    centroids = generate_random_centroids(k, dimension_count, outer_vertices)

    for i in xrange(max_iterations):
        clusters = {}

        for point in points:
            best_distance = float("+inf")
            best_centroid = None

            for centroid in centroids:
                distance = calculate_distance(centroid, point)
                if distance < best_distance:
                    best_distance = distance
                    best_centroid = centroid

            if best_centroid not in clusters:
                clusters[best_centroid] = []

            clusters[best_centroid].append(point)

        new_centroids = [calculate_centroid(p) for p in clusters.values()]

        if len(centroid) > len(new_centroids):
            raise RuntimeError('Centroid length differs')

        # for j in xrange(len(centroid) - len(new_centroids)):
        #     new_centroids.append(generate_random_point())
        # todo see if we need this

        if centroids == new_centroids:
            break

        centroids = new_centroids
    return centroids


def generate_random_centroids(k, dimension_count, outer_vertices):
    """Generates a random point (the centroid) for each cluster"""
    return [generate_random_point(dimension_count, outer_vertices) for i in xrange(k)]


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
