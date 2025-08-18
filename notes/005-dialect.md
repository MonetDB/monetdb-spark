# Dialect loading issues

Initially it seemed there were some issues with loading the MonetDB JDBC driver
and Spark dialect from the classpath.

One of the issues was that the Dialect was not always used.
This seems to have gone away.

The other issue was that the JDBC driver could not be loaded based on
the jdbc url. This seems to be a known issue with Spark.
As long as you explicitly pass `.option("driver", "org.monetdb.jdbc.MonetDriver")`,
it works and as far as I know there is no way to make it work without that.

The rest of this document is notes I made while looking into this.
Keeping them in this file because it might be useful in the future.


## First attempt

MonetDB-Spark provides `org.monetdb.spark.MonetDialect`,
a custom subclass of `org.apache.spark.sql.jdbc.JdbcDialect`.
Without it, the built-in JDBC writer can't create tables with BOOLEAN
columns.

Consider the following script, 'dialect.scala':

```scala
val spine = spark.range(5)
val df = spine.withColumn("b", spine("id") % 2 === 0)
df.write
	.mode("overwrite")
	.format("jdbc")
	.option("driver", "org.monetdb.jdbc.MonetDriver")
	.option("url", "jdbc:monetdb://localhost/testspark?user=monetdb&password=monetdb")
	.option("dbtable", "mydata")
	.save()
```

This creates a dataframe with an integer 'id' column and a
boolean 'b' column. Then it connects to MonetDB and writes the
data to a newly created or replaced table 'mydata'.

If we run this with the spark-shell we get an error message:
```
» spark-shell <dialect.scala
...
scala> java.lang.ClassNotFoundException: org.monetdb.jdbc.MonetDriver
```

If we help it find the JDBC driver, it fails because it uses BIT rather
than BOOLEAN in the CREATE TABLE statement:
```
» spark-shell --jars ./jars/monetdb-jdbc-12.0.jar <dialect.scala
...
scala> java.sql.SQLException: Type (bit) unknown in: "create table mydata ("id" bigint not null, "b" bit(1)"
```

Finally, when we also include monetdb-spark, it works
```
25/07/15 14:03:17 WARN JdbcUtils: Requested isolation level 1 is not supported; falling back to default isolation level 8
25/07/15 14:03:17 WARN JdbcUtils: Requested isolation level 1 is not supported; falling back to default isolation level 8
25/07/15 14:03:17 WARN JdbcUtils: Requested isolation level 1 is not supported; falling back to default isolation level 8
25/07/15 14:03:17 WARN JdbcUtils: Requested isolation level 1 is not supported; falling back to default isolation level 8
25/07/15 14:03:17 WARN JdbcUtils: Requested isolation level 1 is not supported; falling back to default isolation level 8

scala> 
scala> :quit
```

### Why do we need to include the driver class name?

Why do we need to include the "driver" option? Why can't it find it based on the url alone?
The [Driver Manager] uses the [Service Loader] mechanism to automatically load all
drivers on the class path.

When I try this in a JUnit test [TestSpark], the "driver" option is indeed not needed,
but spark-shell doesn't work without it. Does spark-shell somehow break the service loader?

Pyspark also requires the driver.

[Driver Manager]: https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html
[Service Loader]: https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html
[TestSpark]: ../monetdb-spark/src/test/java/org/monetdb/spark/TestSpark.java


## Second attempt

Trying to reproduce and fix the Dialect loading issues described in Note 005-dialect.
Unfortunately I don't quite understand what I was trying to say there...

Note: at the time of writing, monetdb-jdbc 12.0 is included in the fat jar.
When note 005 was written, it probably wasn't.

Stating our expectation: monetdb-jdbc includes a
`META-INF/services/java.sql.Driver`. Our code includes
`META-INF/services/org.apache.spark.sql.jdbc.JdbcDialect`.
These two files should allow Spark to automatically find the MonetDB jdbc driver
and dialect. Setting `.option("driver", "org.monetdb.jdbc.MonetDriver")`
should not be necessary.

What we saw was that spark-shell worked byt required the jdbc driver class to be
loaded explicitly, and pyspark didn't seem to load it at all?

### Trying it in a unit test

The following test succeeds.

```java
@Test
public void testJdbcDriverAndDialectFound() {
    // Test that we don't need to explicitly load the dialect
    // and the driver, they are picked up automatically.

    startSpark();

    // Test data containing a boolean
    Dataset<Long> spine = spark.range(5); // implicit column name "id"
    Dataset<Row> dataFrame = spine.withColumn("b", col("id").mod(lit("2")).equalTo(lit(0)));
    // dataFrame.show();
    // +---+-----+
    // | id|    b|
    // +---+-----+
    // |  0| true|
    // |  1|false|
    // |  2| true|
    // |  3|false|
    // |  4| true|
    // +---+-----+

    // If the following works, the jdbc driver must have been found
    // automatically because the "driver" option has been commented out,
    // and the dialect must have been found because the default dialect
    // tries to store booleans in a BIT column which MonetDB doesn't
    // support
    dataFrame
            .write()
            .mode("overwrite")
            .format("jdbc")
			//.option("driver", "org.monetdb.jdbc.MonetDriver")
            .option("url", Config.databaseUrl())
            .option("dbtable", "foo")
            .save();

}
```

### Trying it in spark-shell

We first build the fat jar and install pyspark in a venv:
```
» make
» python3 -m venv venv
» . ./venv/bin/activate.fish
(venv) » pip install pyspark
```
Pyspark includes spark-shell. We run it as follows:
```
spark-shell --jars $PWD/monetdb-spark/target/monetdb-spark-0.1.1-SNAPSHOT-fat.jar
```

We run the following script:
```scala
val spine = spark.range(5)
val df = spine.withColumn("b", spine("id") % 2 === 0)
df.write
	.mode("overwrite")
	.format("jdbc")
	//.option("driver", "org.monetdb.jdbc.MonetDriver")
	.option("url", "jdbc:monetdb://localhost:44002/testspark?user=monetdb&password=monetdb")
	.option("dbtable", "foo")
	.save()
```
We get a SQLException:
```
java.sql.SQLException: No suitable driver
  at java.sql/java.sql.DriverManager.getDriver(DriverManager.java:299)
  at org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.$anonfun$driverClass$2(JDBCOptions.scala:118)
```

If we include de "driver" option, it works. So it can find the dialect but not the jdbc driver.
Strange..

### Trying it in pyspark..

```
» pyspark --jars $PWD/monetdb-spark/target/monetdb-spark-0.1.1-SNAPSHOT-fat.jar
```
```python
spine = spark.range(5)
df = spine.withColumn('b', spine.id % 2 == 0)
#df.show()
df.write \
	.mode('overwrite') \
    .format('jdbc') \
    .option('driver', 'org.monetdb.jdbc.MonetDriver') \
    .option('url', 'jdbc:monetdb://localhost:44002/testspark?user=monetdb&password=monetdb') \
	.option('dbtable', 'foo') \
	.save()
````

Again, a 'No suitable driver' exception. Adding the 'driver' option makes it work.
Not so bad, but still strange.

### Checking the class path

Again, in spark-shell

```
scala> new org.monetdb.jdbc.MonetDriver()
val res0: org.monetdb.jdbc.MonetDriver = org.monetdb.jdbc.MonetDriver@7538033f

scala> spark.getClass().getClassLoader().getResources("META-INF/services/java.sql.Driver")
val res1: java.util.Enumeration[java.net.URL] = java.lang.CompoundEnumeration@7d7a0e06

scala> res1.nextElement()
val res2: java.net.URL = jar:file:/home/jvr/work/2025/monetdb-spark/venv/lib/python3.11/site-packages/pyspark/jars/hive-jdbc-2.3.10.jar!/META-INF/services/java.sql.Driver

scala> res1.nextElement()
val res3: java.net.URL = jar:file:/home/jvr/work/2025/monetdb-spark/venv/lib/python3.11/site-packages/pyspark/jars/derby-10.16.1.1.jar!/META-INF/services/java.sql.Driver

scala> res1.nextElement()
java.util.NoSuchElementException
  at java.base/java.lang.CompoundEnumeration.nextElement(ClassLoader.java:2744)
  ... 42 elided

scala> res0.getClass().getClassLoader().getResource("org/monetdb/jdbc/MonetDriver.class")
val res5: java.net.URL = jar:file:/home/jvr/work/2025/monetdb-spark/monetdb-spark/target/monetdb-spark-0.1.1-SNAPSHOT-fat.jar!/org/monetdb/jdbc/MonetDriver.class
```

So, monetdb-spark-0.1.1-SNAPSHOT-fat.jar is on the path but it doesn't see the
services declaration.

## Trying with jdbc external.

Before, when we passed the "driver" option, it loaded the driver from our jar and then
magically, our dialect in that jar was suddenly also available.
So, we expect that this effect goes away if jdbc is external. Then it would
read the driver from monetdb-jdbc.jar instead of ours and our dialect would not be found.
Let's try it.


Let's check this. If we build the fat jar without monetdb-jdbc included, we have
to pass the jdbc driver explicitly through ---jars` and then the dialect will not
be loaded.

```
        <dependency>
            <groupId>monetdb</groupId>
            <artifactId>monetdb-jdbc</artifactId>
            <version>12.0</version>
            <scope>provided</scope>
        </dependency>
```

Interesting, it still can't find the Driver but it can find the Dialect.

However, bottom line is that it seems to work as long as the user manually specifies
the jdbc driver class name.


### Let's try an external cluster.

We have Spark installed somewhere (~/opt/spark)

```shell
» sbin/start-master.sh && echo && sbin/start-history-server.sh && echo && sbin/start-worker.sh spark://totoro.lan:7077
```

Cluster UI at localhost:8080, history at localhost:18080.


