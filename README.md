# kdbds_2_3_1  

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

# Simple example

Start q to listen on port 5000:
```
$ q -p 5000
KDB+ 3.6 2018.11.09 Copyright (C) 1993-2018 Kx Systems
q)
```

Start the Spark shell, and 
```
$ ./bin/spark-shell --jars kdbds_2_3_1.jar
scala> val df = spark.read.format("KdbDataSource").
   | option("host", "localhost").option("port", 5000).
   | schema("id long").option("q", "([] id:til 5)").load
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
