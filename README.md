# Spark Data Source (V2) for Kx Systems kdb+ Database

For Spark 2.4.4 and upwards

## Task List

- [ ] Move examples to test area (and rename tests)
- [ ] Complete write support
- [ ] Remove Kx platform support (include in comments and readme)
- [ ] Complete README
- [ ] Deal with TODOs and //!


## Datatype support

kdb+ | Code | Spark | Alias | Note
-- | -- | -- | -- | --
boolean | b | BooleanType | boolean
guid | g | StringType | string | xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
byte | x | ByteType | byte, tinyint
short | h | ShortType | short, smallint
int | i | IntegerType | int, integer
long | j | LongType | long, bigint
real | e | FloatType | float | 4-byte floating point
float | f | DoubleType | double | 8-byte floating point
char | c | StringType | string
symbol | s | StringType | string
timestamp | p | TimestampType | timestamp | ns are truncated
month | m | DateType | date 
date | d | DataType | date
datetime | z | TimestampType | timestamp
time | t | TimestampType | timestamp
timespan | n | TimestampType | timestamp
minute | u | IntegerType | int | range between 0 and 1439
second | v | IntegerType | int | range between 0 and 86399

## Simple example

Start q to listen on port 5000:
```
$ q -p 5000
KDB+ 3.6 2018.11.09 Copyright (C) 1993-2018 Kx Systems
q)
```

Start the Spark shell, and 
```
$ ./bin/spark-shell --jars kdbds.jar
scala> val df = spark.read.format("kdb").
   | option("host", "localhost").option("port", 5000).
   | schema("id long").option("qexpr", "([] id:til 5)").load
df: org.apache.spark.sql.DataFrame = [id: bigint]   
scala> df.show(false)
+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
+---+
```
