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


def length__aboutme__to__user_rep(k, user_lines):
    """
    Classifies users based on their about me length and reputation.
    :param k: Number of clusters
    :param user_lines: PythonRDD containing the lines in the users XML file
    :return: PythonRDD of results
    """
    result = user_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ["Reputation", "AboutMe"])) \
        .filter(lambda a: any(a)) \
        .map(lambda a: (int(a[0]), len(a[1])))

    return KMeans(k).fit(result)


def post__edits__average__to__user_rep(k, user_lines, post_history_lines):
    """
    Classifies users based on their average number of post edits and reputation.
    :param k: Number of clusters
    :param user_lines: PythonRDD containing the lines in the users XML file
    :param post_history_lines: PythonRDD containing the lines in the post history XML file
    :return: PythonRDD of results
    """
    reputations = user_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ["Id", "Reputation"], int)) \
        .filter(lambda a: any(a))

    post_edits_average = post_history_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ["UserId", "PostHistoryTypeId"], int)) \
        .filter(lambda a: any(a) and a[1] == 5) \
        .map(lambda a: (int(a[0]), 1)) \
        .reduceByKey(lambda a, b: a + b)

    result = reputations.join(post_edits_average).map(lambda value: value[1])

    return KMeans(k).fit(result)
