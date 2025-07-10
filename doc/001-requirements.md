# MonetDB Spark Writer - Requirements

The goal is to be able to efficiently upload data from Spark to MonetDB
with a command similar to

```python
df.write.format("org.monetdb.spark.MonetDBSource") \
    .option("url", "jdbc:monetdb://localhost/demo") \
    .option("dbtable", "mytable") \
    .option("user", "monetdb") \
    .option("password", "monetdb") \
    .save()
```

It SHOULD be very similar to the `.format("jdbc")` but much faster. The
problem with the JDBC writer is that it inserts each row as a separate
INSERT INTO, and even with prepared statements that doesn't scale.

It is NOT a requirement to be option-compatible with the JDBC writer.
However, our writer SHOULD use the same option names where possible.

**TODO** We need to decide on a package name. org.monetdb or
com.monetdbsolutions? In other words, are we going to open source this?

The MonetDBSource SHOULD use something like COPY INTO, COPY BINARY INTO
or maybe even something like Parquet/ORC file uploads to achieve this
goal.

It SHOULD use ON CLIENT to avoid having to ssh into the server.

We MAY have to look into MAPI level compression (lz4?) to speed up data
transfer across the network.

We SHOULD provide the writer as a single jar containing all dependencies
except MAYBE the JDBC jar. We MAY also provide other deployments if that
is convenient for us.


## Design notes

Even though Spark seems to store data mostly column-wise, the writer API
exposed by Spark seems to be row-wise. This is unfortunate. Ideally, we
would just directly upload the Parquet files Spark seems to use
internally using `INSERT INTO table SELECT * FROM 'dummy.parquet' ON
CLIENT`. However, it's not clear if Sparks data is always in Parquet
form, how to access it, how to deal with type differences between the
Spark data and the MonetDB table definition, etc. Also, MonetDB doesn't
currently support Parquet files ON CLIENT.

So for the time being we aim for the low hanging fruit of creating an
efficient `dataframe.write` implementation for MonetDB.
