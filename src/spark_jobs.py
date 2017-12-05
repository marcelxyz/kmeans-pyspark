import kmeans
import parser


def user_upvotes_downvotes(k, user_lines):
    """

    :param k:
    :param user_lines:
    :return: PythonRDD
    """
    result = user_lines\
        .map(lambda line: parser.extract_attributes(line, ['Reputation', 'UpVotes', 'DownVotes'], int))\
        .filter(lambda a: any(a))

    return kmeans.find_cluster_centroids(result, k)
