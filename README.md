# Spark Data Source (V2) for Kx Systems kdb+ Database

For Spark 2.4.4 and upwards

## Task List

- [ ] Timezone support for writing dates and timestamps
- [ ] Complete README
- [ ] Deal with TODOs and //!

## Datatype support

https://code.kx.com/v2/ref/#datatypes
https://spark.apache.org/docs/latest/sql-reference.html#data-types

### Simple Types

kdb+ | Code | Spark | Alias | Note
:-- | :-: | :-- | :-- | :--
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

### List Types

kdb+ | Code | Spark | Alias | Note
:-- | :-: | :-- | :-- | :--
char[] | C | StringType | string
byte[] | X | CreateArrayType(ByteType, false) | array<char>???
short[] | H | CreateArrayType(ShortType, false) | array<short>
int[] | I | CreateArrayType(IntegerType, false) | array<int>
long[] | J | CreateArrayType(LongType, false) | array<long>
real[] | E | CreateArrayType(FloatType, false) | array<float>
float[] | F | CreateArrayType(DoubleType, false) | array<double>
timestamp[] | P | CreateArrayType(TimestampType, false) | array<timestamp>
date[] | D | CreateArrayType(DateType, false) | array<date>

Note that kdb+ uses the names `real` and `float` for 4-byte and 8-byte floating point respectively, which is contrary to how Spark names tham.

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

## Options

The following options are used by the kdb+ data source. All other options are ignored unles the `function` option is specified.

Option | Description | Notes
:-- | :--- | :-- 
host | Name or IP address of kdb+ process | localhost
port | Port on which kdb+ process is listening | 5000
userpass | user:password if kdb+ process requires authentication | (empty)
timeout | Query timeout in seconds | 60
usetls | Whether to use TLS | false
numpartitions | Number of read partitions to create | 1
loglevel | Log4J logging level (debug, trace, warn, error) | inherited from Spark
qexpr | A q expression that returns a unkeyed table | Schema must be provided
table | A kdb+ table name | kdbds prepends an unkey ("0!") to name
function | A kdb+ function that supports the kdbds call interface | See section
pushfilters | Whether kdb+ function supports push-down filters | true
batchsize | Number for rows to be written per round trip | 10000

## Kdb+ Read Function

Describe here...

## Null Values

General discussion of kdb+ nulls
Include a table of constants used by kdb+. 
Discuss ways of enabling null support

## Kdb+ Write Function

## Platform support

Discuss opportunity to hook in calls via some sort of proxy (often called a query 
router or dispatcher in kdb+)

## Logging

loglevel
logging in the datasource
logging in the kdb+ code (use spark.q script and .sp namespace)
assertion in kdb+ (using Signal (') operator )
log4j entries

## Building and Testing


