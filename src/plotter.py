import matplotlib
matplotlib.use('Agg')
import numpy as np
import scipy.stats as stats
import pylab as pl
from clustering import KMeans
from matplotlib import pyplot


def generate_distribution_plot(clusters, output_path):
    pyplot.clf()
    
    for centroid, points in clusters.iteritems():
        distances = sorted([KMeans.calculate_distance(centroid, p) for p in points])

        pdf = stats.norm.pdf(distances, np.mean(distances), np.std(distances))

        pl.plot(distances, pdf)

    pl.savefig(output_path + 'distribution.png')


def generate_scatter_plot(clusters, output_path):
    pyplot.clf()

    for point_list in clusters.itervalues():
        data = zip(*point_list)

        pyplot.scatter(data[0], data[1], c=np.random.rand(3,))

        pyplot.savefig(output_path + 'scatter.png')


def generate_bubble_plot(clusters, output_path):
    pyplot.clf()

    for centroid, points in clusters.iteritems():

        pyplot.scatter(centroid[0], centroid[1], len(points), c=np.random.rand(3,))

        pyplot.savefig(output_path + 'bubble.png')


def generate_pie_plot(clusters, output_path):
    pyplot.clf()

    sizes = [len(points) for points in clusters.itervalues()]

    pyplot.pie(sizes)

    pyplot.savefig(output_path + 'pie.png')
