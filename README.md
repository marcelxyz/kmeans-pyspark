## Local development and running

### Dependencies

Make sure you have Python 2.7 installed along with pip. Then run:

```
pip install -r requirements.txt
```

### Running jobs

All jobs are ran using the central job runner module - `src/index.py`. You shouldn't need to edit this file at all.

```
python src/index.py <job_name> <k> <file1> <file2>
```

Params:

* `<job_name>` the name of your job function as defined in `src/spark_jobs.py`
* `<k>` the number of clusters to generate
* `<file1>` the file path to the data file (this can either be an absolute path or a local path in the project)

The jobs can take more than one file. Those should just be appended to the command.

For example:

```
python src/index.py user__reputation__to__upvotes_cast 3 tests/fixtures/users.xml
```

### Adding new jobs

All the jobs are dynamically loaded from the `src/spark_jobs.py` file. Add **ONE** function there for each job you require.

If you need more than one function or your function is more than approx. 10 lines, then create a separate module file for it. This will keep the `spark_jobs.py` file fairly small and easy to understand.

The interface for job definitions is as follows:

```python
def function(k, file1 [, file2[, file3, ...]]
```

The function **must** take at least the `k` and `file1` arguments (which map to the values passed via the CLI). It can take more files if necessary (again, those need to be provided via the CLI when called).

For example:

```python
def user_reputation__to__post_length(k, users_rdd, posts_rdd):
    # do something great here
```

And then execute using the command:

```python
python src/index.py user_reputation__to__post_length 5 /data/users.xml /data/posts.xml
```

### Debugging

If you are using an IDE then you can use a debugger to interactively step through code. Much easier than remote debugging on the cluster.

### Running tests

It is best to use an IDE to get meaningful messages and to be able to debug code.

## Running on the cluster

We don't have the required permissions to install custom modules on the QM servers, so the project must be written using what's already available.

To see a list of modules installed on QM's servers, logon to an ITL machine remotely and do `pip list`. This lists all custom modules that are installed.

Submitting a job on the cluster is done using this command:

```
spark-submit src/index.py <job_name> <k> <input_file_path>
```