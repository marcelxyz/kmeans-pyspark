# Import required Spark libraries;
from pyspark import SparkConf, SparkContext

# Set up environment
conf  	= SparkConf().setAppName("app_name")
sc 		= SparkContext(conf = conf)

# Perform operations
lines 	= sc.textFile("/data/stackoverflow/Users")
print(lines.count())