import kmeans
import parser


def user_upvotes_downvotes(user_lines, k):
    """

    :param user_lines:
    :param k:
    :return: PythonRDD
    """
    result = user_lines\
        .map(lambda line: parser.extract_attributes(line, ['Reputation', 'UpVotes', 'DownVotes'], int))\
        .filter(lambda a: any(a))

    return kmeans.find_cluster_centroids(result, k)
