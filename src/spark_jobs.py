from __future__ import division
from clustering import KMeans
import xml_parser


def user__reputation__to__upvotes_cast(k, user_lines):
    """
            Classifies users based on their reputation score and number of upvotes they cast.

            :param k: Number of clusters
            :param user_lines: PythonRDD containing the lines in the users XML file
            :return: PythonRDD of results
            """
    result = user_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ['Reputation', 'UpVotes'], int)) \
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

# 7. Attributes: upvotes cast, user's average post length, (rep or rep per day or profile views or date of signup).
# 8. Attributes: post tags, (user rep or date of signup), quality of user's questions (how many were closed or marked as dupe). Context: find if questions with specific tags are of low/high and how that pertains to user rep (so maybe low rep users ask questions about PHP and they are usually low quality).


def user__upvotes_cast__to__average_post_length__to__profile_views(k, users, post_history):
    user_avg_post_length = post_history\
        .map(lambda line: xml_parser.extract_attributes(line, ['UserId', 'Text']))\
        .filter(lambda a: any(a))\
        .map(lambda data: (int(data[0]), len(data[1])))\
        .aggregateByKey((0, 0), lambda a, b: (a[0] + b, a[1] + 1), lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .mapValues(lambda value: value[0] / value[1])

    user_data = users\
        .map(lambda line: xml_parser.extract_attributes(line, ['Id', 'UpVotes', 'Views'], int))\
        .filter(lambda a: any(a))\
        .map(lambda data: (data[0], (data[1], data[2])))

    # join data and map each tuple to the following format:
    # (upvotes_cast, views, average_post_length)
    joined_data = user_data.join(user_avg_post_length)\
        .map(lambda data: (data[1][0][0], data[1][0][1], data[1][1]))

    return KMeans(k).fit(joined_data)
