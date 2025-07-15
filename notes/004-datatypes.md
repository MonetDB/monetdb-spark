# Supported data types

In the future we plan to be more flexible but
for the time being we support the following conversions
from `org.apache.spark.sql.types.*` to
`java.sql.JDBCType`.

| Spark Type  | JDBC Type     |
|-------------|---------------|
| BooleanType | BOOLEAN       |
| IntegerType | INTEGER       |
| DoubleType  | DOUBLE        |
| StringType  | VARCHAR, CLOB |
