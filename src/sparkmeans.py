from pyspark import SparkConf, SparkContext
import math
import random
import xmltodict
from xml.parsers.expat import ExpatError
import os


def parse_xml_line(line):
    replacements = {
        '&^Cquot': '&quot',
    }

    try:
        line = reduce(lambda x, y: x.replace(y, replacements[y]), replacements, line.strip())
        data = xmltodict.parse(line)
        return data
    except ExpatError as e:
        print(e, line.strip())


def get_value_for_field(xml, element_name, attribute_names):
    field_values = []

    data = parse_xml_line(xml)

    if data is None or element_name not in data:
        return field_values

    for attribute_name in attribute_names:
        attribute_name = "@" + attribute_name
        if attribute_name in data[element_name]:
            field_values.append(data[element_name][attribute_name])

    return field_values


def get_value_for_field_as_int(xml, element_name, attribute_names):
    values = get_value_for_field(xml, element_name, attribute_names)

    try:
        for k, v in enumerate(values):
            values[k] = int(v)
    except:
        values = []

    return tuple(values)

################################################################


def kmeans(points, k, max_iterations):
    outer_vertices = get_outer_vertices(points)
    centroids = get_random_centroids(outer_vertices, k)
    for i in range(0, max_iterations):
        clusters = points.map(lambda point: (get_nearest_centroid(point, centroids), point))
        f = clusters.groupByKey()
        return f


def to_list(x):
    return [x]


def get_centroid(points):
    xs = points.map(lambda point: point[0]).collect()
    ys = points.map(lambda point: point[1]).collect()
    return (avg(xs), avg(ys))


def get_outer_vertices(points):
    xs = points.map(lambda point: point[0]).collect()
    ys = points.map(lambda point: point[1]).collect()
    return (min(xs), max(xs), min(ys), max(ys))


def get_random_centroids(outer_vertices, k):
    centroids = []
    for i in range(0, k):
        centroids.append(get_random_point(outer_vertices))
    return centroids


def get_random_point(outer_vertices):
    return (random.uniform(outer_vertices[0], outer_vertices[1]), random.uniform(outer_vertices[2], outer_vertices[3]))


def get_nearest_centroid(point, centroids):
    best_distance = float("+Inf")
    best_centroid = None
    for centroid in centroids:
        distance = get_distance(point, centroid)
        if distance < best_distance:
            best_distance = distance
            best_centroid = centroid
    return best_centroid


def get_distance(point_a, point_b):
    d_x = abs(point_a[0] - point_b[0])
    d_y = abs(point_a[1] - point_b[1])
    return math.sqrt(pow(d_x, 2) + pow(d_y, 2))

################################################################


conf = SparkConf().setAppName("app_name")
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")

path = os.path.dirname(os.path.realpath(__file__)) + '/../data/users.xml'
user_lines = sc.textFile(path)
upvotes_and_downvotes = user_lines\
    .map(lambda line: get_value_for_field_as_int(line, "row", ["UpVotes", "DownVotes"]))\
    .filter(lambda v: not all(v))

result = kmeans(upvotes_and_downvotes, 4, 100).takeSample(False, 100)
print(result)






