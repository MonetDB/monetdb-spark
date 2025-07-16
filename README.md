# MonetDB-Spark

MonetDB-Spark adds some MonetDB-specific functionality to Spark.
In particular,

1. `org.monetdb.spark.MonetDialect`<br>
   A [JdbcDialect] that teaches Spark's JDBC bindings about MonetDB's
   syntax and data types. So far, the only difference with the default
   is that boolean columns are created with type 'BOOLEAN' instead
   of 'BIT'.

2. `org.monetdb.spark.MonetSource`<br>
   A write-only [Spark Data Source] that allows writing to MonetDB much
   faster than Spark's built-in JDBC writer can.


## The Dialect

Spark should automatically pick up the dialect as long as it's on the
class path. We're having some problems with that, however.
Currently, it works with native Java applications, with spark-shell
as long as the `"driver"` option is passed, and we can't get it to
work with PySpark.

For more details, see [005-dialect](notes/005-dialect.md).


## The Writer

The Writer can be activated by replacing `.format("jdbc')`
with `.format("org.monetdb.spark")`.
The new writer does not support all options and modes that the
full JDBC writer supports. In particular, it only supports writing
to existing tables (`.mode("append")`) and only the options `"url"`,
`"user"`, `"password"` and "dbtable".

This is how it can be used:

## Building

Run `make` in monetdb-spark/.

Or `make test`, this assumes there is a database `testspark` to connect to.
Use `make test TESTDB=bla` to target another database.
The following are all equivalent:

* `TESTDB=jdbc:monetdbs://somehost:12345/demo?user=monetdb&password=monetdb`, 
  a full-fledged JDBC URL
* `TESTDB=jdbc:monetdb://localhost/demo`, you can omit user and password.
* `TESTDB=demo`, you can also omit `jdbc://localhost/`.






[JdbcDialect]: https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/jdbc/JdbcDialect.html

[Spark Data Source]: https://spark.apache.org/docs/latest/sql-data-sources.html