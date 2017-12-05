## Local development and running

### Dependencies

Make sure you have Python 2.7 installed along with pip. Then run:

```
pip install -r requirements.txt
```

### Running jobs

```
python src/job_runner.py <job_name> <input_file_path> <k>
```

With the following params:

* `<job_name>` the name of your job function as defined in `src/spark_jobs.py`
* `<input_file_path>` the file path to the data file (this can either be an absolute path or a local path in the project)
* `<k>` the number of clusters to generate

For example:

```
python src/job_runner.py user_upvotes_downvotes tests/fixtures/users.xml 3
```

### Adding new jobs

All the jobs are dynamically loaded from the `src/spark_jobs.py` file. Add **ONE** function there for each job you require.

If you need more than one function or your function is more than approx. 10 lines, then create a separate module file for it. This will keep the `spark_jobs.py` file fairly small and easy to understand.

### Debugging

If you are using an IDE then you can use a debugger to interactively step through code. Much easier than remote debugging on the cluster.

### Running tests

It is best to use an IDE to get meaningful messages and to be able to debug code.

## Running on the cluster

We don't have the required permissions to install custom modules on the QM servers, so the project must be written using what's already available.

To see a list of modules installed on QM's servers, logon to an ITL machine remotely and do `pip list`. This lists all custom modules that are installed.

Submitting a job on the cluster is done using this command:

```
spark-submit src/job_runner.py <job_name> <input_file_path> <k>
```