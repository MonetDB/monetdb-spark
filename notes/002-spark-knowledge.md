# Spark knowledge

This document is a dumping ground for things I learn about Spark, with
links to where I found it.


## Some links I found

Spark SQL, DataFrames and Datasets Guide<br>
https://spark.apache.org/docs/latest/sql-programming-guide.html<br>
Official documentation for Spark 4.0.0. One of a number of "Programming Guides".
More a high level overview of how to use Spark, but we need that too!

Creating DataFrames Manually<br>
https://best-practice-and-impact.github.io/ons-spark/spark-overview/creating-dataframes.html<br>
Tips on how to create Spark dataframes without loading external data, great for unit tests.


Easy Guide to Create a Custom Write Data Source in Apache Spark 3<br>
https://levelup.gitconnected.com/easy-guide-to-create-a-write-data-source-in-apache-spark-3-f7d1e5a93bdb<br>
Walk through for creating a custom writer. Sounds like exactly what we need.
Hope it's any good.

Apache Spark WTF??? — That Thing You Do! (Part II)<br>
https://medium.com/towards-data-engineering/apache-spark-wtf-that-thing-you-do-part-ii-3f7df738ffdd<br>
General information about Spark partitions, reading and writing them.


The Internals of Spark SQL (Apache Spark 2.4.5)<br>
https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/<br>
Maybe a little outdated but might be useful to understand how to work with all those API's.
Has chapters with titles like "Datasets vs DataFrames vs RDDs".

Pyspark Tutorial: Getting Started with Pyspark<br>
https://www.datacamp.com/tutorial/pyspark-tutorial-getting-started-with-pyspark
Also need to know more about how to do basic things with Spark.


## Running a cluster

Installing a Spark cluster locally<br>
https://medium.com/@mb7/how-to-create-a-standalone-apache-spark-cluster-b8f9068ab944

The Overview also contains installation instructions<br>
https://spark.apache.org/docs/latest/index.html

The chapter on submitting jobs shows how to talk to a cluster<br>
https://spark.apache.org/docs/latest/submitting-applications.html

Sparks are submitted using spark-submit, sometimes implicitly.

You pass a master-url: `spark://HOST:PORT`. When it's `local` or say `local[3]`, spark-submit
starts an embedded Spark cluster.

It talks about deploy-mode: client or cluster. In client mode the driver program runs on the
node where spark-submit is running. This node must then be able to reach the worker nodes.
If you set it to cluster, spark-submit runs the driver on the cluster.

### Attempt 1

Goal: run a standalone spark cluster on my local machine so the unit tests don't have to start
their own. This would be helpful (a) because it avoids the >2s startup delay, and
(b) because the Spark outputs clutter the unit tests output.

1. Download Spark. Which Hadoop version to pick? Does it even matter? Pick the default Hadoop3.4.
   https://www.apache.org/dyn/closer.lua/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz.

2. Unpack the tar file and rename the resulting directory to ~/opt/spark. 
   Arbitrary but that's what I'll use.

3. Look at examples/src/main/python/pi.py. It invokes SparkSession.builder.getOrCreate()
   without setting a master url. That's because it's been designed to be run from spark-submit.
   In the unit tests we set the master url.

   Run it as `./bin/spark-submit examples/src/main/python/pi.py 100`.
   It works.

4. Read https://spark.apache.org/docs/latest/spark-standalone.html

5. In conf/spark-env.sh I put `export SPARK_LOCAL_IP=127.0.0.1`.
   After running sbin/start-master.sh, it appears to be listening on 127.0.1.1.
   This is because it picks up host name totoro.lan and somehow that one gets 127.0.1.1.
   The `export SPARK_WORKER_CORES=5` does seem to work.

6. Running `sbin/start-worker.sh spark://127.0.1.1:70771` works. I can see it in the
   Web UI at http://localhost:8080.

7. Now try to run the Pi application on our cluster by adding `--master spark://totoro.lan:7077`.
   It works. Still pretty verbose though.

   Also: there is a cluster web ui at 8080 and a driver web ui at 4040! Finally know what's
   the difference.

8. How to make it less verbose? TaskSetManager, DAGScheduler, TaskSchedulerImpl.
   Create the conf/log4j2.properties. Set
   `appender.console.filter.threshold.type = ThresholdFilter` 
   and `appender.console.filter.threshold.level = warn`.

9. Now we want to install a 'history server' to reproduce all the pretty graphs when
   the job has finished.  `sbin/start-history-server.sh` starts it.
   It looks in /tmp/spark-events by default, was unable to change that so far.
   Events are only logged when you pass `--conf spark.eventLog.enabled=true`
   to spark-submit.
   Ah! Defaults can be set conf/spark-defaults.conf.

10. Can set test.spark=spark://totoro.lan:7077 in override.properties and it works
    in the sense that it goes to the cluster. However, the cluster doesn't look at
    our CLASSPATH so it doesn't work.

    Can still use it for testing though.

11. For the record:
    `sbin/start-master.sh && echo && sbin/start-history-server.sh && echo && sbin/start-worker.sh spark://totoro.lan:7077`,
    `sbin/stop-worker.sh && echo && sbin/stop-history-server.sh && echo && sbin/stop-master.sh`,

12. Final task: disable most of the spark logging in the unit tests.