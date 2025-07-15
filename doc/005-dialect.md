# Dialect

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

## Why do we need to include the driver class name?

Why do we need to include the "driver" option? Why can't it find it based on the url alone?
The [Driver Manager] uses the [Service Loader] mechanism to automatically load all
drivers on the class path.

When I try this in a JUnit test [TestSpark], the "driver" option is indeed not needed,
but spark-shell doesn't work without it. Does spark-shell somehow break the service loader?

Pyspark also requires the driver.

[Driver Manager]: https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html
[Service Loader]: https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html
[TestSpark]: ../monetdb-spark/src/test/java/org/monetdb/spark/TestSpark.java