from pyspark import SparkConf, SparkContext
import numpy as np
import math
import random

def get_value_for_field(string, field):
    beg = " " + field + "=\""
    end = "\" "
    beg_i = string.find(beg)
    if (beg_i != -1) :
        substring = string[beg_i+len(beg):len(string)]
        end_i = substring.find(end)
        if(end_i != -1):
            substring = substring[0:end_i]
            return substring
        else:
            return None
    else:
        return None

def get_upvotes_and_downvotes_from_user_row(string):
    try:
        upvotes = int(get_value_for_field(string, "UpVotes"))
        downvotes = int(get_value_for_field(string, "DownVotes"))
        return (upvotes, downvotes)
    except:
        return (None, None)

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
    return centroid

def get_distance(point_a, point_b):
    d_x = abs(point_a[0] - point_b[0])
    d_y = abs(point_a[1] - point_b[1])
    return math.sqrt(pow(d_x, 2) + pow(d_y, 2))

################################################################

conf = SparkConf().setAppName("app_name")
sc = SparkContext(conf = conf)

user_lines = sc.textFile("/home/marcel/Workspace/BigData/data/users.xml")
upvotes_and_downvotes = user_lines.map(lambda line: get_upvotes_and_downvotes_from_user_row(line)).filter(lambda v: v != (None, None))
result = kmeans(upvotes_and_downvotes, 4, 100).takeSample(False, 10)
print(result)






