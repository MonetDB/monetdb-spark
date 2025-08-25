# MonetDB-Spark

MonetDB-Spark adds some MonetDB-specific functionality to Spark.

1. It provides a [Dialect] that tweaks the mapping between Spark's data
   types and MonetDB's data types. This is mostly used when
   `spark.read.format("jdbc").load()` needs to pick the column types of
   the resulting data frame, and when
   `dataframe.write.mode("overwrite").format("jdbc").save()` needs to
   pick the column types of the table it creates.

   For more information on how the [Dialect] maps the types, see
   [notes/005-dialect.md](notes/005-dialect.md).

2. A custom MonetDB-specific [Spark Data Source]. This data source uses
   MonetDB's [`COPY BINARY INTO` feature][COPY BINARY] to load data into
   MonetDB much faster than the regular JDBC data source can. It cannot
   be used to read data from MonetDB.


## Installing MonetDB-Spark

Place the monetdb-jdbc jar and the monetdb-spark jars on the classpath.
It should work with monetdb-jdbc version 3.2 and later but has only been
tested with 12.0. For example,

```shell
» pyspark --jars $HOME/jars/monetdb-jdbc-12.0.jar,$HOME/jars/monetdb-spark-0.1.1-fat.jar
```

The dialect is picked up automatically when the JDBC Data Source is used
with MonetDB. However, the "driver" option must explicitly be set to
"org.monetdb.jdbc.MonetDriver" because Spark is unable to infer this
from the URL. For example, in pyspark:

```python
from pyspark.sql.functions import col
df = spark.range(5).withColumn('b', col('id') % 2 == 0)
df.write \
   .format("jdbc") \
   .mode("overwrite") \
   .option("driver", "org.monetdb.jdbc.MonetDriver") \
   .option("url", "jdbc:monetdb:///demo") \
   .option("user", "monetdb") \
   .option("password", "monetdb") \
   .option("dbtable", "foo") \
   .save()
```

If the above works, it proves the dialect has been detected.
This is because the default dialect fails when it tries to create
boolean columns.

> **IMPORTANT** It is recommended to not enable [Speculative Execution]
> while writing to MonetDB, both with regular JDBC and with the custom
> data source.
>
> Speculative Execution means that Spark may spawn additional tasks to
> commit the same partition, commiting the one that completes first and
> rolling back the others.
> These rollbacks can cause 'holes' in the internal structure of the table
> which may slow down subsequent queries on that table.


## Using it from a Jupyter notebook

The SparkSession needs to be created with the "spark.jars" property set
to a comma-separated list of jar files to put on the class path.
For example,

```python
from glob import glob
SPARK_JARS = glob('/home/jvr/work/2025/monetdb-spark/jars/*')
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder \
    .appName('sparknotebook') \
    .config('spark.jars', ','.join(SPARK_JARS)) \
    .getOrCreate()
```

See also [notes/009-jupyter-notebook.ipynb](notes/009-jupyter-notebook.ipynb)

## Writing data with the custom Data Source

To use the custom data source to write data, replace `.format("jdbc")`
with `.format("org.monetdb.spark")`.

For example, the example above becomes

```python
from pyspark.sql.functions import col
df = spark.range(5).withColumn('b', col('id') % 2 == 0)
df.write \
   .format("org.monetdb.spark") \
   .mode("append") \
   .option("url", "jdbc:monetdb:///demo") \
   .option("user", "monetdb") \
   .option("password", "monetdb") \
   .option("dbtable", "foo") \
   .save()
```

For the time being, only Append mode is supported so the table must
already have been created. Truncate and Overwrite mode will be
implemented in a later version.

The custom data source does not support all options supported by the
JDBC data source. The following configuration options are supported:

* **url**: JDBC URL to connect to. See the [MonetDB JDBC
  documentation](https://www.monetdb.org/documentation-Mar2025/user-guide/client-interfaces/libraries-drivers/jdbc-driver/#jdbc-connection-url-format)
  for details.

* **dbtable**: Name of the table to write. IMPORTANT NOTE: special
  characters are not escaped automatically, they must be escaped by the
  user. The reason they are not automatically escaped is that there is
  no separate "schema" parameter, so if monetdb-spark sees `foo*.b@r` it
  cannot know whether this should be escaped as table `"b@r"` in the
  `"foo*"` schema or as table `"foo*.b@r"` in the default schema. Why is
  there no separate "schema" parameter? Because the jdbc data source
  does not have one either.

* **user**: user name to log in with. Can also be specified as part
  of the URL. There is no default.

* **password**: password to log in with. Can also be specified as part
  of the URL. There is no default.

* **batchsize**: monetdb-spark will issue one COPY BINARY INTO
  statement per **batchsize** rows. Large batch sizes are generally more
  efficient but require more memory in the Spark nodes to accumulate
  the data. For now the default is to issue one COPY BINARY per
  partition but this may change in the future.


## Building

To build, simply run `make`. This will create `monetdb-spark-X.Y.Z-fat.jar`
in the directory `monetdb-spark/target`.

This fat jar contains all needed dependencies except the MonetDB JDBC
driver.

To test, run `make test`. The tests assume they can connect to and
freely modify the database `jdbc:monetdb:///localhost/testspark`. This
database needs to be created beforehand, or a different database must be
configured as explained below.

The following settings are available:

* **test.db**. JDBC url to connect to. If **test.db** contains no
  slashes and colons, it's assumed to be just a database name and
  `jdbc:monetdb://localhost/` is prepended. If no `user=` and
  `password=` parameters are detected, they are automatically appended
  and set to 'monetdb'.

* **test.spark**. Spark cluster to connect to. Defaults to `local[4]`.
  Non-local sparks are unlikely to work because of classpath issues but
  this setting can still be used to tweak the number of workers.

* **test.partitions**. Some tests (only SOME tests!) use this setting to
  control the number of partitions in the their test data. During
  debugging it's occasionally useful to force all data into a single
  partition.

* **test.verbose**. Some tests write more information to stdout if this
  is set to 'true'.

The most convenient way to change these settings is by creating a
properties file `override.properties` in the monetdb-spark subdirectory.
It is also possible to pass them as `-D` flags to Maven. In particular,
if 'make' is invoked as `make test TESTDB=demo`, 'make' will run
`./mvnw test -Dtest.db=demo`.


[Dialect]: https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/jdbc/JdbcDialect.html

[Spark Data Source]: https://spark.apache.org/docs/latest/sql-data-sources.html

[COPY BINARY]: https://www.monetdb.org/documentation/user-guide/sql-manual/data-loading/binary-loading/

[Speculative Execution]: https://kb.databricks.com/scala/understanding-speculative-execution