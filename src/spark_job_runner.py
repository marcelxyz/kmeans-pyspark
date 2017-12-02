from pyspark import SparkContext
from glob import glob
import random
import os.path
import spark_jobs
import sys


def src_dir():
    return os.path.dirname(os.path.realpath(__file__)) + '/'


def localise_path(path):
    local_path = src_dir() + '../' + path
    if not os.path.isabs(path) and os.path.exists(local_path):
        return local_path
    return path


def run_job(sc, job_name, file_path, k):
    if not hasattr(spark_jobs, job_name):
        raise RuntimeError('Job "%s" not found in module spark_jobs' % job_name)

    fn = getattr(spark_jobs, job_name)

    return fn(sc.textFile(localise_path(file_path)), k)


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: spark_job_runner.py <job_name> <input_file_path> <k>")
        sys.exit(0)

    random.seed(1)
    files = glob(src_dir() + '*.py')

    sc = SparkContext(pyFiles=files)
    sc.setLogLevel("ERROR")

    result = run_job(sc, sys.argv[1], sys.argv[2], int(sys.argv[3]))
    print(result.keys())
