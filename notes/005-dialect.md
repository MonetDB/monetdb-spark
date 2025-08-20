# The MonetDB Dialect

The [MonetDialect](../monetdb-spark/src/main/java/org/monetdb/spark/MonetDialect.java)
tweaks the mapping between Spark data types and MonetDB data types.


## SQL -> Spark

The mapping from SQL types to Spark types is used to determine the type
of the dataframe returned by `spark.read.format("jdbc").load()`.

Spark allows you to override these types by setting the "customSchema"
option.

The mapping is mostly straightforward, however,

* SQL TINYINT becomes Spark ByteType as opposed to the IntegerType that
  would be chosen by the default dialect. We don't really see the point
  in going to a larger type.

* Likewise, SQL SMALLINT becomes Spark ShortType.

* SQL INTERVAL [YEAR/MONTH TO] YEAR/MONTH goes to Spark
  YearMonthIntervalType. You can override this with "customSchema" to
  make it an INTEGER. The INTEGER counts the number of months, so
  INTERVAL '1' YEAR becomes the number 12. (XXX try this)

* SQL INTERVAL [DAY/HOUR/MINUTE/SECOND TO ] DAY/HOUR/MINUTE/SECOND
  becomes Spark DayTimeIntervalType. You can override it with "customSchema"
  to make it a Decimal(x,3). This gives you the length of the interval in
  seconds, with milliseconds precision.


## Spark -> SQL

The mapping from Spark types to SQL types is used by
`dataframe.write.mode("overwrite").format("jdbc").save()` because in
overwrite mode, Spark must create the table and decide the column types.
In append- and truncate mode the existing table is used. 

Spark allows you to override these types by setting the
"createTableColumnTypes" option.

Differences between the Monet dialect and the default dialect.

* Spark BooleanType becomes SQL BOOLEAN (was BIT). Because BIT is not a
  MonetDB type.

* Spark ByteType becomes SQL SMALLINT (was BYTE). Note that SMALLINT is
  16 bits, we don't pick TINYINT because the range of TINYINT is
  -127..+127 and Sparks ByteType also includes the value -128.

* Spark MonetType becomes SQL INTEGER. The default dialect also does
  this. We don't use SMALLINT because the value -32768 does not fit its
  range. We don't know why the default dialect maps to INTEGER rather
  than SMALLINT.

* Spark IntegerType becomes SQL BIGINT (was INTEGER). We can't use
  INTEGER because its range -2147483647..+2147483647 does not include
  the value -2147483648. If you really want INTEGER, use
  "createTableColumnTypes" as mentioned above.

* Spark LongType becomes SQL BIGINT as well. This is the default. Note
  that -9223372036854775808 does not fit and causes an exception. We did
  not pick HUGEINT here because Spark otherwise doesn't support huge 128
  bits integers at all. Instead we hope the value -9223372036854775808
  simply won't occur and throw an exception if it does (XXX try this).

  Let us know if you feel this behavior should be changed.

* Spark TimestampType becomes SQL TIMESTAMP WITH TIMEZONE.
  The default dialect uses TIMESTAMP but that doesn't make sense
  to us because Spark TimestampType is documented to be time zone
  aware.

* Spark TimestampNTZType becomes SQL TIMESTAMP.

