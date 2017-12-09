from pyspark import SparkContext
from glob import glob
from datetime import datetime
import random
import os
import spark_jobs
import sys
import plotter
import pickle
import json


def current_dir():
    return os.path.dirname(os.path.realpath(__file__)) + '/'


def localise_path(path):
    local_path = current_dir() + '../' + path
    if not os.path.isabs(path) and os.path.exists(local_path):
        return local_path
    return path


def load_files(sc, paths):
    return map(lambda path: sc.textFile(localise_path(path)), paths)


def make_dir(job_name):
    output_path = current_dir() + '../output/'
    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    output_path += job_name
    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    output_path += '/' + str(datetime.now()) + '/'
    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    return output_path


def save_result(job_name, result):
    output_path = make_dir(job_name)
    pickle.dump(result, open(output_path + 'data', 'w+'))
    # plotter.generate_distribution_plot(result, output_path)
    # plotter.generate_scatter_plot(result, output_path)
    # plotter.generate_bubble_plot(result, output_path)
    # plotter.generate_pie_plot(result, output_path)


def run_job(sc, job_name, k, file_paths):
    if not hasattr(spark_jobs, job_name):
        raise RuntimeError('Job "%s" not found in module spark_jobs' % job_name)

    job = getattr(spark_jobs, job_name)

    result = job(k, *load_files(sc, file_paths))

    save_result(job_name, result.collectAsMap())


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Usage: index.py <job_name> <k> <input_file_path> [<input_file_path2> <input_file_path3> ... ]")
        sys.exit(0)

    random.seed(1)
    files = glob(current_dir() + '*.py')

    sc = SparkContext(pyFiles=files)
    sc.setLogLevel("WARN")

    run_job(sc, sys.argv[1], int(sys.argv[2]), sys.argv[3:])

    sc.stop()
