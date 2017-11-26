import math
import random
import matplotlib.pyplot as plt

class KMeans:

	def __init__(self, points, clusters):
		self.points = points
		self.clusters = clusters

	def update_plot(self, means):
		xs = []
		ys = []
		mxs = []
		mys = []
		for point in self.points:
			xs.append(point.x)
			ys.append(point.y)
		for mean in means:
			mxs.append(mean.x)
			mys.append(mean.y)
		plt.plot(xs, ys, 'ro')
		plt.plot(mxs, mys, 'bo')
		plt.show()

	def find(self):
		edges = self.get_edges()
		means = self.get_random_means(edges)
		#means = [Point(0.5,0.5), Point(0.5,0.5), Point(9,9)]
		j = 0
		while j < 100:
			print("A")
			means_to_points = MeansToPoints(means)
			for point in self.points:
				best_distance = -1
				best_mean = None
				for mean in means:
					d = self.get_distance(mean, point)
					if best_distance == -1 or d < best_distance:
						best_distance = d
						best_mean = mean
				means_to_points.add_point_for_mean(best_mean, point)
			print("B")
			#means_to_points.print_means()
			means = means_to_points.get_centroids()
			for i in range(0, len(means)):
				if means[i] == None:
					means[i] = self.get_random_point(edges)
			j += 1
		self.update_plot(means)

	def get_random_means(self, edges):
		means = []
		for i in range(0, clusters):
			means.append(self.get_random_point(edges))
		return means

	def get_distance(self, point_a, point_b):
		distance_x = abs(point_a.x - point_b.x)
		distance_y = abs(point_a.y - point_b.y)
		return math.sqrt(pow(distance_x,2)+pow(distance_y,2))

	def get_random_point(self, edges):
		x = random.uniform(edges[0], edges[1])
		y = random.uniform(edges[2], edges[3])
		return Point(x, y)

	def get_edges(self):
		edges = [0,0,0,0]
		started = False
		for point in self.points:
			if started == False:
				started = True
				edges[0] = point.x
				edges[1] = point.x
				edges[2] = point.y
				edges[3] = point.y
			else:
				if point.x < edges[0]:
					edges[0] = point.x
				if point.x > edges[1]:
					edges[1] = point.x
				if point.y < edges[2]:
					edges[2] = point.y
				if point.y > edges[3]:
					edges[3] = point.y
		return edges

class Point:

	def __init__(self, x, y):
		self.x = x
		self.y = y

	def __str__(self):
		return "(" + str(self.x) + "," + str(self.y) + ")"

class MeansToPoints:

	def __init__(self, means):
		self.means = means
		self.map = {}
		for mean in self.means:
			self.map[mean] = []

	def add_point_for_mean(self, mean, point):
		self.map.get(mean, None).append(point)

	def get_centroids(self):
		centroids = []
		for key, value in self.map.items():
			centroids.append(self.get_centroid(value))
		return centroids

	def get_centroid(self, points):
		if len(points) == 0:
			return None
		x_sum = 0
		y_sum = 0
		for point in points:
			x_sum += point.x
			y_sum += point.y
		x = x_sum/len(points)
		y = y_sum/len(points)
		return Point(x, y)

	def print_means(self):
		for mean, points in self.map.items():
			t = len(points)
			print(mean)


points = [
	Point(0.5,1), Point(0.5,0.5), Point(1,1), Point(1.5,0.5),
	Point(1,8), Point(0.5,9), Point(0.5,8.5), Point(1,9.5),
	Point(9.5,6), Point(9,7), Point(8.5,6.5), Point(9,7.5),
	Point(6.5,4), Point(4,6), Point(5,6), Point(6,5)
]
clusters = 4
km = KMeans(points, clusters)
km.find()


























