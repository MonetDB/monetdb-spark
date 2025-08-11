# Temporal types

We need to determine the precise intersection between the temporal types
offered by Spark and MonetDB. As far as I know COPY BINARY INTO supports
all of MonetDB's temporal types so let's start there.

[MonetDB Temporal Types](https://www.monetdb.org/documentation-Mar2025/user-guide/sql-manual/data-types/temporal-types/)

[MonetDB COPY BINARY INTO](https://www.monetdb.org/documentation-Jun2023/user-guide/sql-manual/data-loading/binary-loading/)

[Spark Data Types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)

We also need to check whether the MonetDB dialect needs any changes to 
map the Spark temporals to the correct MonetDB type.


## Type mapping

| Spark                             | MonetDB                   | Binary                | Remark                   |
|-----------------------------------|---------------------------|-----------------------|--------------------------|
| DateType                          | DATE                      | copy_binary_date      | Never with timezone      |
| TimeStampType                     | TIMESTAMPTZ [precision]   | copy_binary_timestamp | Spark precision always 0 |
| TimestampNTZType                  | TIMESTAMP [precision]     | copy_binary_timestamp | Spark precision always 0 |
| -                                 | TIME                      | copy_binary_time      | Spark has not TIME type  |
| -                                 | TIME                      | copy_binary_time      | Spark has not TIME type  |
| YearMonthInterval(YEAR, [YEAR])   | INTERVAL YEAR             | int32                 | Always nr of months      |
| YearMonthInterval(YEAR, MONTH)    | INTERVAL YEAR TO MONTH    | int32                 | Always nr of months      |
| YearMonthInterval(MONTH, MONTH)   | INTERVAL MONTH            | int32                 | Always nr of months      |
| DayTimeInterval(DAY, [DAY])       | INTERVAL DAY              | int64                 | Always milliseconds      |
| DayTimeInterval(DAY, HOUR)        | INTERVAL DAY TO HOUR      | int64                 | Always milliseconds      |
| DayTimeInterval(DAY, MINUTE)      | INTERVAL DAY TO MINUTE    | int64                 | Always milliseconds      |
| DayTimeInterval(DAY, SECOND)      | INTERVAL DAY TO SECOND    | int64                 | Always milliseconds      |
| DayTimeInterval(HOUR, [HOUR])     | INTERVAL HOUR             | int64                 | Always milliseconds      |
| DayTimeInterval(HOUR, MINUTE)     | INTERVAL HOUR TO MINUTE   | int64                 | Always milliseconds      |
| DayTimeInterval(HOUR, SECOND)     | INTERVAL HOUR TO SECOND   | int64                 | Always milliseconds      |
| DayTimeInterval(MINUTE, [MINUTE]) | INTERVAL MINUTE           | int64                 | Always milliseconds      |
| DayTimeInterval(MINUTE, SECOND)   | INTERVAL MINUTE TO SECOND | int64                 | Always milliseconds      |
| DayTimeInterval(SECOND, [SECOND)  | INTERVAL SECOND           | int64                 | Always milliseconds      |


## Constructing the Spark values

Needed for testing.
