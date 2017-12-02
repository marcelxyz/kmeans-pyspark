from pyspark import SparkContext
import random
import os.path
import spark_jobs
import sys


def src_dir():
    return os.path.dirname(os.path.realpath(__file__)) + '/'


def localise_path(path):
    local_path = src_dir() + '../' + path
    if os.path.exists(local_path):
        return local_path
    return path


def run_job(sc, job_name, file_path, k):
    jobs = {
        'user_upvotes_downvotes': spark_jobs.user_upvotes_downvotes
    }

    if job_name not in jobs:
        raise RuntimeError('Job %s not found' % job_name)

    return jobs[job_name](sc.textFile(localise_path(file_path)), k)


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: spark_job_runner.py <job_name> <input_file_path> <k>")
        sys.exit(0)

    random.seed(1)
    sc = SparkContext()
    sc.setLogLevel("ERROR")
    sc.addFile(src_dir() + 'parser.py')
    sc.addFile(src_dir() + 'kmeans.py')
    sc.addFile(src_dir() + 'spark_jobs.py')
    result = run_job(sc, sys.argv[1], sys.argv[2], int(sys.argv[3]))
    print(result.keys())
