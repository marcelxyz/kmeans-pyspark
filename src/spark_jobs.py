from __future__ import division
from clustering import KMeans
from datetime import datetime
from operator import add
import time
import xml_parser


def user__reputation__to__upvotes_cast(k, user_lines):
    """
    Classifies users based on the following:
        - user reputation
        - user upvotes cast

    :param k: Number of clusters
    :param user_lines: PythonRDD containing the lines in the users XML file
    :return: RDD of clustered data
    """
    result = user_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ['Reputation', 'UpVotes'], int)) \
        .filter(lambda a: any(a))

    return KMeans(k).fit(result)


def length__aboutme__to__user_rep(k, user_lines):
    """
    Classifies users based on the following:
        - length of about me
        - user reputation

    :param k: Number of clusters
    :param user_lines: RDD with XML file with users
    :return: RDD of clustered data
    """
    result = user_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ['Reputation', 'AboutMe'])) \
        .filter(lambda a: any(a)) \
        .map(lambda a: (int(a[0]), len(a[1])))

    return KMeans(k).fit(result)


def post__edits__average__to__user_rep(k, user_lines, post_history_lines):
    """
    Classifies users based on the following:
        - number of times a user's post has been edited
        - user's reputation

    :param k: Number of clusters
    :param user_lines: RDD with XML file with users
    :param post_history_lines: RDD with XML file with post history
    :return: RDD of clustered data
    """
    reputations = user_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ['Id', 'Reputation'], int)) \
        .filter(lambda a: any(a))

    post_edits_average = post_history_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ['UserId', 'PostHistoryTypeId'], int)) \
        .filter(lambda a: any(a) and a[1] == 5) \
        .map(lambda a: (a[0], 1)) \
        .reduceByKey(add)

    result = reputations.join(post_edits_average).map(lambda value: value[1])

    return KMeans(k).fit(result)


def datetime_to_timestamp(datetime_value):
    return time.mktime(datetime.strptime(datetime_value, '%Y-%m-%dT%H:%M:%S.%f').timetuple())


def user__membership_time__to__closed_questions(k, users, posts, post_history):
    """
    Classifies users based on the following:
        - amount of time a user has been a member
        - number of close or delete votes any of their posts have received

    :param k: Number of clusters
    :param users: RDD with XML file with users
    :param posts: RDD with XML file with posts
    :param post_history: RDD with XML file with post history
    :return: RDD of clustered data
    """
    # (user_id, timestamp)
    user_data = users\
        .map(lambda line: xml_parser.extract_attributes(line, ['Id', 'CreationDate']))\
        .filter(lambda a: any(a))\
        .map(lambda data: (int(data[0]), datetime_to_timestamp(data[1])))\

    # (post_id, author_user_id)
    post_data = posts\
        .map(lambda line: xml_parser.extract_attributes(line, ['Id', 'OwnerUserId'], int))\
        .filter(lambda a: any(a))

    # (post_id, number_of_close_and_delete_votes)
    post_history_data = post_history\
        .map(lambda line: xml_parser.extract_attributes(line, ['PostId', 'PostHistoryTypeId'], int))\
        .filter(lambda a: any(a) and a[1] in [10, 12])\
        .map(lambda a: (a[0], 1))\
        .reduceByKey(add)

    # (user_id, number_of_close_and_delete_votes)
    user_delete_and_close_count = post_data.join(post_history_data)\
        .map(lambda a: a[1])\
        .reduceByKey(add)

    # (timestamp, number_of_close_and_delete_votes)
    data = user_data.join(user_delete_and_close_count).map(lambda a: a[1])

    return KMeans(k).fit(data)


def user__upvotes_cast__to__average_post_length__to__profile_views(k, users, posts):
    """
    Classifies users based on the following:
        - amount of upvotes a user has cast
        - user's average post length (question and answer)
        - user's profile views

    :param k: Number of clusters
    :param users: RDD with XML file with users
    :param posts: RDD with XML file with posts
    :return: RDD of clustered data
    """
    # (user_id, average_post_length)
    user_avg_post_length = posts\
        .map(lambda line: xml_parser.extract_attributes(line, ['OwnerUserId', 'Body']))\
        .filter(lambda a: any(a))\
        .map(lambda data: (int(data[0]), len(data[1])))\
        .aggregateByKey((0, 0), lambda a, b: (a[0] + b, a[1] + 1), lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .mapValues(lambda value: value[0] / value[1])

    # (id, (upvotes_cast, profile_views))
    user_data = users\
        .map(lambda line: xml_parser.extract_attributes(line, ['Id', 'UpVotes', 'Views'], int))\
        .filter(lambda a: any(a))\
        .map(lambda data: (data[0], (data[1], data[2])))

    # (upvotes_cast, views, average_post_length)
    joined_data = user_data.join(user_avg_post_length)\
        .map(lambda data: (data[1][0][0], data[1][0][1], data[1][1]))

    return KMeans(k).fit(joined_data)
