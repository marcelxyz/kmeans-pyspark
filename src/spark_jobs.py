from clustering import KMeans
import xml_parser


def user__reputation__to__upvotes_cast(k, user_lines):
    """
    Classifies users based on their reputation score and number of upvotes they cast.

    :param k: Number of clusters
    :param user_lines: PythonRDD containing the lines in the users XML file
    :return: PythonRDD of results
    """
    result = user_lines\
        .map(lambda line: xml_parser.extract_attributes(line, ['Reputation', 'UpVotes'], int))\
        .filter(lambda a: any(a))

    return KMeans(k).fit(result)
