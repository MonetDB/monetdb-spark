# Rough design

Very rough.


## Package structure

Namespace *org.monetdb.spark*. Overal name space of this project.

Namespace *org.monetdb.spark.schema*. Deals with matching Spark types
with SQL types. Can the given dataframe be uploaded to this table at
all? Do we need conversions first? Is COPY BINARY supported?

Class *org.monetdb.spark.bulk.Target*. Holds JDBC URL, owns a connection
and can render the target table name in various forms. For example,
sometimes we need schema and table name separately, sometimes joined
together and quoted. Also exposes TableMetaData that can be passed to
the *org.monetdb.spark.schema* helpers.

Abstract class *org.monetdb.spark.bulk.Loader*. Constructed out of a
*Target* and a Spark *StructType*. Is repeatedly passed a Spark
*InternalRow* (or its parent class *SpecializedGetters*) to append a
row. Has start- and end methods but does not commit or roll back the
transaction.

Exception *org.monetdb.spark.bulk.SchemaMismatchException*. Can be
thrown when a Loader

Namespace *org.monetdb.spark.copyinto*. Implements a *Loader* that uses
regular `COPY INTO table FROM 'dummy' ON CLIENT`.

Namespace *org.monetdb.spark.copybinary*. Implements a *Loader* that
uses `COPY BINARY INTO table FROM 'dummy0', 'dummy1', ... ON CLIENT`.

So, to bulk load some data, you first need to have the data's Spark
*StructType* ready. Then you connect to the database by creating a
Target object. This validates the connection and checks that the table
exists. Then you can try to construct one of the Loaders out of the
target, the struct type and possibly some other loader specific
parameters. If that succeeds you can start the loader, pass it some
rows, finish it and tell the Target to commit the transaction.

Namespace *org.monetdb.spark* also contains a *MonetDBSource* class.
This is not in a subpackage because the end user will refers to it.
However, the bulk of its implementation should be in a subpackage, for
example *org.monetdb.spark.writer*. The *MonetDBSource* knows the
details about the Loaders we provide, we're not going to build an
abstraction layer between Source and Loaders.


## Code layout

We will have a Maven project containing all the above as a subdirectory
*monetdb-spark*. It will not live in the root directory because in the
future we might want to have secondary Maven projects (or otherwise).

The Maven project will build an additional jar that contains all
dependencies. For simplicity that will include the JDBC driver though
in the future we may want to keep that separate.

The Maven project's tests will assume the existence of a MonetDB
database *jdbc:monetdb://localhost/sparktests* that can be clobbered. A
property can be set to change this:
`-Dtest.db=jdbc:monetdb://localhost/demo`.


## Getting started

1. Create the Maven project.

2. Create a unit test which runs Spark locally, creates a dataframe with
   some test data, creates a table to insert it into, uploads the data
   into the table using the regular Spark jdbc support and verifies the
   result.

   We will use that to learn more about Spark and as a baseline to test
   against.