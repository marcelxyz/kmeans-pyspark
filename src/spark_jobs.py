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


def user__badges__to__signup__to__answers_and_questions(k, user_lines, badges_lines, posts_lines):
    # (user_id, signup)
    user_id_signup = user_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ['Id', 'CreationDate'])) \
        .filter(lambda a: any(a)) \
        .map(lambda a: (int(a[0]), datetime_to_timestamp(a[1])))

    # (user_id, number_badges)
    user_id_badges = badges_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ['UserId'], int)) \
        .filter(lambda a: any(a)) \
        .map(lambda a: (a[0], 1)) \
        .reduceByKey(add)

    # (user_id, post_type)
    posts = posts_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ['OwnerUserId', 'PostTypeId'], int)) \
        .filter(lambda a: any(a)) \
 
        # (user_id, n_answers)
    user_id_answers = posts \
        .filter(lambda a: a[1] == 2) \
        .map(lambda a: (a[0], 1)) \
        .reduceByKey(add)

    # (user_id, n_asked)
    user_id_questions = posts \
        .filter(lambda a: a[1] == 1) \
        .map(lambda a: (a[0], 1)) \
        .reduceByKey(add)

    # (n_questions, n_answers, n_badges, signup)
    result = user_id_signup.join(user_id_badges)
    result = result.join(user_id_answers)
    result = result.join(user_id_questions)
    result = result.map(lambda a: (a[1][1], a[1][0][1], a[1][0][0][1], a[1][0][0][0]))

    return KMeans(k).fit(result)


def user__reputation__to__own_questions_answered(k, user_lines, post_lines):
    # (user_id, rep)
    user_id_reputation = user_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ['Id', 'Reputation'], int)) \
        .filter(lambda a: any(a))

    # (user_id, n_questions)
    user_id_questions = post_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ['Id', 'OwnerUserId', 'PostTypeId'], int)) \
        .filter(lambda a: None not in a and len(a) == 3 and a[2] == 1) \
        .map(lambda a: (a[0], a[1]))

    # (user_id, n_asked)
    user_id_answers = post_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ['ParentId', 'OwnerUserId', 'PostTypeId'], int)) \
        .filter(lambda a: None not in a and len(a) == 3 and a[2] == 2) \
        .map(lambda a: (a[0], a[1]))

    # (user_id, n_questions_self_answered)
    user_id_own_answers = user_id_questions.intersection(user_id_answers) \
        .map(lambda a: (a[1], 1)) \
        .reduceByKey(add)

    # (rep, n_questions_self_answered)
    result = user_id_reputation.join(user_id_own_answers).map(lambda a: (a[1][0], a[1][1]))

    return KMeans(k).fit(result)


def user__signup__to__distinct_post_tags(k, user_lines, post_lines):
    # (user_id, signup_as_timestamp)
    creationdate = user_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ['Id', 'CreationDate'], str)) \
        .filter(lambda a: any(a)) \
        .map(lambda a: (int(a[0]), datetime_to_timestamp(a[1])))

    # (user_id, number_distinct_tags)
    user_id_tags = post_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ['OwnerUserId', 'Tags'], str)) \
        .filter(lambda a: (None not in a and len(a) == 2) \
        .map(lambda a: (int(a[0]), a[1].replace(">", "")[1:])) \
        .map(lambda a: (a[0], a[1].split("<"))) \
        .flatMapValues(lambda a: a) \
        .distinct() \
        .map(lambda a: (a[0], 1)) \
        .reduceByKey(sum)

    # (signup_as_timestamp, number_distinct_tags)
    result = creationdate.join(user_id_tags).map(lambda a: (a[1][0], a[1][1]))

    return KMeans(k).fit(result)


def user__reputation__to__distinct_post_tags(k, user_lines, post_lines):
    # (user_id, reputation)
    rep = user_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ['Id', 'Reputation'], int)) \
        .filter(lambda a: any(a))

    # (user_id, number_distinct_tags)
    user_id_tags = post_lines \
        .map(lambda line: xml_parser.extract_attributes(line, ['OwnerUserId', 'Tags'])) \
        .filter(lambda a: (None not in a and len(a) == 2)) \
        .map(lambda a: (int(a[0]), a[1].replace(">", "")[1:])) \
        .map(lambda a: (a[0], a[1].split("<"))) \
        .flatMapValues(lambda a: a) \
        .distinct() \
        .map(lambda a: (a[0], 1)) \
        .reduceByKey(sum)

    # (rep, number_distinct_tags)
    result = rep.join(user_id_tags).map(lambda a: (a[1][0], a[1][1]))

    return KMeans(k).fit(result)
