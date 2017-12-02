from pyspark import SparkContext
import os.path
import parser
from kmeans import KMeans

current_dir = os.path.dirname(os.path.realpath(__file__)) + '/'

sc = SparkContext()
sc.setLogLevel("ERROR")
sc.addFile(current_dir + 'parser.py')
sc.addFile(current_dir + 'kmeans.py')

user_lines = sc.textFile(current_dir + '../data/users.xml')


def user_rep_upvotes_downvotes(line):
    return parser.extract_attributes(line, 'row', ['Reputation', 'UpVotes', 'DownVotes'], int)


result = user_lines.map(user_rep_upvotes_downvotes).filter(lambda a: any(a))
print(result.takeSample(False, 10))

k = KMeans(result.collect(), 4).find_cluster_centroids()
print(k)
