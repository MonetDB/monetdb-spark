# MonetDB-Spark

MonetDB-Spark adds some MonetDB-specific functionality to Spark. In
particular,

1. It provides a [Dialect] that tweaks the mapping between Spark's data
   types and MonetDB's data types. This is mostly used when
   `spark.read.format("jdbc").load()` needs to pick the column types of
   the resulting data frame, and when
   `dataframe.write.mode("overwrite").format("jdbc").save()` needs to
   pick the column types of the table it creates.

2. A custom MonetDB-specific [Spark Data Source]. This data source
   cannot read data, you should use the JDBC data source for that.
   However, it can write data to MonetDB much faster than the JDBC data
   source can.

For more information on how the [Dialect] maps the types, see
[notes/005-dialect.md](notes/005-dialect.md).


## Installation and use

Place the monetdb-jdbc jar and the monetdb-spark jars on the classpath.
It should work with monetdb-jdbc version 3.2 and later but has only been
tested with 12.0.

For example,

```shell
» pyspark --jars $HOME/jars/monetdb-jdbc-12.0.jar,$HOME/jars/monetdb-spark-0.1.1-fat.jar
```

The dialect should automatically be picked up. Note that the JDBC Data
Source needs the "driver" option, otherwise it won't be able to load the
JDBC driver. For example, in pyspark:

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
If the above works, this is proof the dialect has been detected. We know
this because the default dialect doesn't create BOOLEAN columns in the
right way.

To use the custom data source to write data, replace `.format("jdbc")`
with `.format("org.monetdb.spark")`. The custom data source does not
support all options supported by the JDBC data source.

At this moment, the MonetDB data source only supports appending data to
an existing table (mode Append). We will implement truncate and overwrite
mode later on.

For example,

```python
from pyspark.sql.functions import col
df = spark.range(5).withColumn('b', col('id') % 2 == 0)
df.write \
   .format("org.monetdb.spark") \
   .mode("append") \
   .option("url", "jdbc:monetdb://localhost:44001/demo") \
   .option("user", "monetdb") \
   .option("password", "monetdb") \
   .option("dbtable", "foo") \
   .save()
```


## Building

To build, simply run `make`. This will create `monetdb-spark-X.Y.Z-fat.jar`
in the directory `monetdb-spark/target`.

This fat jar contains all needed dependencies but not the MonetDB JDBC
driver.

To test, run `make test`. The tests assume they can connect to and
freely modify a database `monetdb:///localhost/testspark`. This database
needs to be created beforehand, or a different database must be configured
as explained below.

The following settings are available:

* **test.db**. JDBC url to connect to. If **test.db** contains no
  slashes and colons, it's assumed to be just a database name and
  `jdbc:monetdb://localhost/` is prepended. If no `user=` and
  `password=` parameters are detected, they are automatically appended
  and set to 'monetdb'.

* **test.spark**. Spark cluster to connect to. Defaults to `local[4]`.
  Non-local sparks are unlikely to work but this setting could be used
  to tweak the number of workers.

* **test.partitions**. Some tests (only SOME tests!) use this setting to
  control the number of partitions in the their test data. This can be
  useful while debugging.

The most convenient way to change these settings is by creating a
properties file `override.properties` in the monetdb-spark subdirectory.
It is also possible to pass them as `-D` flags to Maven. In particular,
if 'make' is invoked as `make test TESTDB=demo`, 'make' will run
`./mvnw test -Dtest.db=demo`.


[Dialect]: https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/jdbc/JdbcDialect.html

[Spark Data Source]: https://spark.apache.org/docs/latest/sql-data-sources.html