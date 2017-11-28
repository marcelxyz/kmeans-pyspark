import matplotlib.pyplot as plt
from kmeans import KMeans


def plot(points, centroids):
    """Takes the list of means (Point) and plots them along with the self.points"""
    points_x = [p[0] for p in points]
    points_y = [p[1] for p in points]
    centroids_x = [c[0] for c in centroids]
    centroids_y = [c[1] for c in centroids]

    plt.plot(points_x, points_y, 'ro')
    plt.plot(centroids_x, centroids_y, 'bo')

    plt.show()


if __name__ == '__main__':
    points = [
        (0.5, 1), (0.5, 0.5), (1, 1), (1.5, 0.5),
        (1, 8), (0.5, 9), (0.5, 8.5), (1, 9.5),
        (9.5, 6), (9, 7), (8.5, 6.5), (9, 7.5),
        (5.5, 7), (6.5, 4), (4, 6), (5, 6), (6, 5)
    ]

    k = 4

    centroids = KMeans(points, k).find_cluster_centroids()

    plot(points, centroids)
