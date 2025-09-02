# Unreleased

First real release. It provides a Dialect that fixes several issues with
how Spark's generic JDBC data source interacts with MonetDB, and also a
MonetDB-specific data source that can upload data to MonetDB much faster
than the JDBC data source can.

The dialect is automatically activated when monetdb-spark-0.2.0-fat.jar
is on the classpath and the JDBC data source is invoked, for example
`df.write.format("jdbc")` or `df.write.jdbc(...)` or
`spark.read.format("jdbc")` or `spark.read.jdbc(...)`

The data source can be activated by using format `"org.monetdb.spark"`
instead of format `"jdbc"`.

## Dialect

Note: the MonetDB JDBC driver and Spark JDBC Dialect are only
activated when the option driver=org.monetdb.jdbc.MonetDriver
is passed, for example
`df.write.format("jdbc").option("driver", "org.monetdb.jdbc.MonetDriver")`.

Notable type mappings:

- Spark BooleanType is mapped to SQL BOOLEAN by default, not BIT.

- Spark ByteType is mapped to SQL SMALLINT by default, not BYTE. We
  cannot use TINYINT by default because the range of ByteType is
  -128..127 and the range of TINYINT is -127..127.

- Spark ShortType is mapped to SQL INTEGER by default. Again, this
  widening is necessary because the ranges of ShortType and SMALLINT
  do not overlap completely.

- Spark IntegerType is mapped to SQL BIGINT by default, again because of the
  ranges.

- Spark LongType is also mapped to SQL BIGINT even though the ranges do not
  entirely overlap. This is because the next larger SQL type, HUGEINT,
  is otherwise not really supported by Spark.

- Spark FloatType is mapped to SQL REAL by default, not SQL DOUBLE.

- Spark TimestampNTZType is mapped to SQL TIMESTAMP and
  Spark TimestampType is mapped to SQL TIMESTAMP WITH TIMEZONE.
  The timezone conversions have been fixed to work correctly.

The dialect does not provide a mapping from Spark DayTimeIntervalType
and Spark YearMonthIntervalType to the corresponding SQL INTERVAL types.
The JDBC data source does not seem to support this either.

- SQL INTERVAL types are mapped to the corresponding
  Spark DayTimeIntervalType and YearMonthIntervalType by default.

- SQL REAL is mapped to Spark FloatType by default, not DoubleType.

- SQL TINYINT is mapped to Spark ByteTYpe by default, not IntegerType.

- SQL SMALLINT is mapped to Spark ShortType by default, not IntegerType

- SQL TIMESTAMP is mapped to Spark TimestampNTZType by default, not TimestampType.

- SQL TIMESTAMP WITH TIMEZONE is mapped to Spark TimestampType by default.

## org.monetdb.spark Writer

The "org.monetdb.spark" data source uses COPY BINARY INTO to insert data
into MonetDB rather than separate INSERT statements. This is about 20
times faster.

In this release, the data source cannot yet create tables, only append
data to existing tables. In other words, it support `.mode('append')`
but not `.mode('overwrite')`.

The column types in the destination table do not have to exactly match
the column types of the source dataframe. For example, any integer-like
type can be written to any integer-like column. If the source range
does not fit the destination range, a range check is performed.
By default, any overflow aborts the upload but if option
**allowoverflow** is set, overflowing values are replaced with NULLs.

The data source does not support reading from MonetDB.


# v0.1.0 - 2025-07-16

This was a prototype release.
