from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("app_name")
sc = SparkContext(conf=conf)


def is_valid(string, fields):
    for field in fields:
        if Parser.get_value_for_field(string, field) == None : return False
    return True

def get_value_for_field(string, field):
    beg = " " + field + "=\""
    end = "\" "
    beg_i = string.find(beg)
    if (beg_i != -1) :
        substring = string[beg_i+len(beg):len(string)]
        end_i = substring.find(end)
        if(end_i != -1):
            substring = substring[0:end_i]
            return substring


def get_value_for_fields(string, fields):
    values = []
    for field in fields:
        values.append(get_value_for_field(field))
    return values

def get_user_id_and_reputation_from_user_row(string):
    try:
        user_id = int(get_value_for_field(string, "Id"))
        reputation = int(get_value_for_field(string, "Reputation"))
        return (user_id, reputation)
    except:
        return (None, None)

def get_user_id_from_comment_row(string):
    try:
        user_id = int(get_value_for_field(string, "UserId"))
        return (user_id, 1)
    except:
        return (None, None)

user_lines = sc.textFile("/data/stackoverflow/Users")
user_id_reputation = user_lines.map(lambda line: get_user_id_and_reputation_from_user_row(line)).filter(lambda v: v != (None, None))
# (user_id, reputation)

comment_lines = sc.textFile("/data/stackoverflow/Comments")
user_id_comments_count = comment_lines.map(lambda line: get_user_id_from_comment_row(line)).filter(lambda e: e!= (None, None)).reduceByKey(lambda a, b: a + b)
# (user_id, comments_count)

reputation_comments_count = user_id_reputation.join(user_id_comments_count)
# (user_id, (reputation, comments_count))

reputation_comments_count.map(assign_points_to_centroid)

random_centroids = {}
def assign_points_to_centroid(my_tuple):
	global random_centroids
	return random_centroids[1]



a = reputation_comments_count.collect()