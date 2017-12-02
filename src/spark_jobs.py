import kmeans
import parser


def user_upvotes_downvotes(user_lines, k):
    result = user_lines\
        .map(lambda line: parser.extract_attributes(line, 'row', ['Reputation', 'UpVotes', 'DownVotes'], int))\
        .filter(lambda a: any(a))

    return kmeans.find_cluster_centroids(result.collect(), k)
