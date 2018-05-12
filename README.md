The `spark` tool is a convenience wrapper for running spark clusters
on top of Slurm. Note that this script is still experimental. In addition it
will require modification to work on different clusters:
  - change the settings in the 'Configuration' section
  - change the sbatch template script at the end of 'spark' to reflect
    the nodes/partition/memory/cpu configuration of the target slurm
    cluster.

# Walkthrough

The follwing was run in an sinteractive session so that the spark shell (or
pyspark in my case) didn't itself run on the login node:

```
$ module load spark/2.1.1
$ spark
NAME
        spark - administer spark clusters on compute nodes

SYNOPSIS
        spark cmd [options]

COMMANDS
        help   - show this help or help for specific cmd
        start  - start a new spark cluster
        list   - list clusters
        show   - show cluster details
        stop   - shut down a cluster
        clean  - clean up the directory where cluster
                 info is stored (/data/wresch/.spark-on-biowulf)

DESCRIPTION
        This tool is used to start, stop, monitor, and
        use spark clusters running on compute nodes.
```

Start a cluster with 3 nodes and a max runtime of 120 minutes:
```
$ spark start -t 120 3
INFO: Submitted job for cluster RjHxpj
```

List your currently active spark clusters
```
$ spark list
Cluster id  Slurm jobid                state
---------- ------------ --------------------
    RjHxpj     43451296              RUNNING
```

With more detail
```
$ spark list -d
Cluster id  Slurm jobid                state
---------- ------------ --------------------
    RjHxpj     43451296              RUNNING
               nodes: 2
            max_time: 120
              spark: 2.1.1
              job_id: 43451296
               START: 2017-06-19 13:16:03
            nodelist: cn[0443-0444]
              master: spark://cn0443:7077
        master_webui: http://cn0443:8080
        webui tunnel: ssh -L 55555:cn0443:8080 -N x.x.x
```

As you can see there is a ssh command that will set up a tunnel
between your local machine, and the spark cluster if you want
to see the spark master's web ui.


Here is how you would connect to the cluster with pyspark using one executor
per node:

```
$ pyspark --master spark://cn0443:7077 --executor-memory=40g
Welcome to
     ____              __
    / __/__  ___ _____/ /__
   _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.1.1
      /_/

Using Python version 2.7.13 (default, Dec 20 2016 23:09:15)
SparkSession available as 'spark'.
>>> txt = spark.sparkContext.textFile("sqlite3.c")
>>> txt.count()
202918
>>> defines = txt.filter(lambda l: l.startswith('#define'))
>>> defines.count()
2492
>>> defines.first()
u'#define SQLITE_CORE 1'
>>> txt.map(lambda l: len(l)).reduce(lambda a, b: a if (a>b) else b)
260
>>> Ctrl-D
```

Or if you prefere a jupyter notebook you can create a wrapper script
like the following:

```
#! /bin/bash

export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --port=3939 --no-browser"
exec pyspark --executor-memory=40g "$@"
```

spark-submit the pi example with multiple executors per node
```
$ spark-submit \
    --driver-memory=3g \
    --master "spark://cn0443:7077" \
    --deploy-mode client \
    --executor-cores=2 \
    --executor-memory=3g \
    ./pi.py

[...snip...]
/pi.py:36, took 562.603120 s
PI=3.141584232000           
```

Shut down the cluster
```
$ spark stop Rj
INFO: Sending cluster RjHxpj/43451296 shutdown signal
INFO: May take a couple of minutes to shut down cleanly

$ spark list
Cluster id  Slurm jobid                state
---------- ------------ --------------------
```

Include inactive clusters in the output
```
$ spark list -di
Cluster id  Slurm jobid                state
---------- ------------ --------------------
    RjHxpj     43451296            COMPLETED
               nodes: 2
            max_time: 30
              spark: 2.1.1
              job_id: 43451296
               START: 2017-06-19 13:16:03
            nodelist: cn[0443-0444]
              master: spark://cn0443:7077
        master_webui: http://cn0443:8080
        webui tunnel: ssh -L 55555:cn0443:8080 -N biowulf.nih.gov
                DONE: 2017-06-19 13:26:31
```

Clean up the spark directory
```
$ spark clean
```

