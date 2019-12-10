# kdbspark: Spark Data Source (V2) for Kx Systems kdb+ Database

For Spark 2.4.4 and upwards

## Task List

- [ ] Complete README
- [ ] Deal with TODOs and //!
- [ ] Tighten up test suites

## Reading Data From kdb+

kdbspark offers three options to get table data from kdb+. The caller can provide a table name, a q-language expression, or a q function that tightly interacts with Spark.

In all cases, kdbspark needs to ascertain the schema of the table before fetching its data.


### Table Name: option ("table", "...")

This is the simplest way to retrieve tables, in their entirety, into Spark.

```
q)mytbl:([] id:1000 2000; ts:2020.01.02D03:04:05.555 2020.02.03D06:07:08.999; val:3.1415 2.7182)
```
```
scala> val df = spark.read.format("kdb").
     | option("table", "mytbl").
     | load
df: org.apache.spark.sql.DataFrame = [id: bigint, ts: timestamp ... 1 more field]
scala> df.show(false)
+----+-----------------------+------+
|id  |ts                     |val   |
+----+-----------------------+------+
|1000|2020-01-02 03:04:05.555|3.1415|
|2000|2020-02-03 06:07:08.999|2.7182|
+----+-----------------------+------+
```

kdbspark makes two calls to kdb+. The first call gets the table's schema (using the kdb+ meta function), and then maps each kdb+ type to a Spark type (refer to the section on Datatype Support for more information). The second call gets all the table's data and brings it into Spark memory.

Note that this retrieval option assumes that column data do not have nulls. If any null-value does exist in the kdb+ column, it will render in Spark as the placeholder value that kdb+ uses.

### Q-Language Expression: option("qexpr", "...")

Providing a q-language expression that returns a table offers some more flexibility, however the Spark user must provide the schema of the result when defining the DataFrame. The schema can be in the form of a DDL string or a StructType value. Examples are provided below.

```
q)mytbl2:([] id:1000 2000 0Nj; ts:2020.01.02D03:04:05.555 2020.02.03D06:07:08.999,.z.p; val:3.1415 2.7182 6.02210)
q)mytbl2
id   ts                            val   
-----------------------------------------
1000 2020.01.02D03:04:05.555000000 3.1415
2000 2020.02.03D06:07:08.999000000 2.7182
     2019.11.29D22:16:36.887509000 6.0221
```

mytbl2 has a null value in the id column.

```
scala> val df = spark.read.format("kdb").
     | option("qexpr", "select id,val from mytbl2").
     | schema("id long,val double").
     | load
df: org.apache.spark.sql.DataFrame = [id: bigint, val: double]

scala> df.show
+----+------+
|  id|   val|
+----+------+
|1000|3.1415|
|2000|2.7182|
|null|6.0221|
+----+------+
```

Note that specifying the schema using the DDL string assumes that each column may have nulls. For finer control of specifying nullability for each column, use StructType.

```scala
scala> val s = StructType(List(StructField("id", LongType, false),
     | StructField("val", TimestampType, false)))
s: StructType = StructType(StructField(id,LongType,false), StructField(val,DoubleType,false))

scala> val df = spark.read.format("kdb").
     | option("qexpr", "select id,val from mytbl2").
     | schema(s).
     | load
df: org.apache.spark.sql.DataFrame = [id: bigint, val: double]

scala> df.show
+--------------------+------+
|                  id|   val|
+--------------------+------+
|                1000|3.1415|
|                2000|2.7182|
|-9223372036854775808|6.0221|
+--------------------+------+
```

Specifying false (not null) for the nullable field of StructField will result in better performance as kdbspark does not need to examine each value before passing it to Spark, although you will have to consider how to handle the null placeholder value. A table of placeholder valules can be found in the Null Values **** section.

### Function Name: option("function", "...")

This is the most flexible retrieval method -- kdbspark calls a cooperating kdb+ function that can be written to support custom query parameters, push-down filters, and multi-partition (Spark) processing.

The kdb+ function takes a dictionary of options as a parameter and returns unkeyed tables. It is called twice: the first time to return schema information, and the second to retrieve the actual data. Let's walk through the minimum use case and then build on this.

```scala
scala> val df = spark.read.format("kdb").
     | option("function", "sampleQuery").
     | load
```

```q
sampleQuery:{[opt]
  if[opt[`partitionid]=-1 // If asked for schema,
    :0!meta sampleTable // return unkeyed meta of table
    ];

  :sampleTable // Return table
  }
```

Spark partitions its processing to be spread across multiple nodes in a cluster. When Spark calls kdbspark, it can provide the number of partitions (default is 1) on which the query will be performed. Each partition is given an integer ID starting at zero. 

When kdbspark requests the query schema, it directs the request to only one partition, but When the query function is called with a partition ID of -1, it

A kdb+ script file is included in the project resources folder to use a guide.

### Pushdown Filters

Kdbspark supports pushdown filters, which are essentially a subset of predicates and conjunctios that comprise some or all of the where-clause in Spark SQL or the argument to a DataFrame's filter function.

The filters are converted into a nest array format that is send to the kdb+ read function, whose responsibility is to convert the array to a kdb+ where clause or to the constraint parameter in kdb's functional select.

For example, if a user enters the following filter expression:

```
spark> df.filter("cj>100 and cp<=to_timestamp('2020-01-02')")...show
```

Spark converts this to an Array[Filter], and sends it to Kdbspark, which in turn converts it into a mixed array, places it in the options dictionary (key: filters).

```
((`le;`cp;2020.01.02D00:00:00.000000000);(`gt;`cj;100))
```

It is up to the kdb+ developer to convert this expression for execution. A parsing utility (.sp.pruneAndFilter) in spark.q which the kdb+ developer may find useful as it applies the filter to mapped table. This array can be nested with and/or conjunctions and unary not.

The following table provides a summary of the filters used by Spark, their arguments, and how Kdbspark converts each filter to a kdb+ array.

Filter | Args | Kdb+ Conversion
:-- | :-- | :--
EqualTo | colname, value | (`eq;colname;value)
EqualNullSafe | colname, value | (`eq;colname;value)
GreaterThan | colname, value | (`gt;colname;value)
GreaterThanOrEqual | colname, value | (`ge;colname;value)
LessThan | colname, value | (`lt;colname;value)
LessThanOrEqual | colname, value | (`le;colname;value)
IsNull | colname | (`isnull;colname)
IsNotNull | colname | (`isnotnull;colname)
In| colname, values | (`in;colname;values)
Not | child | (`not;child)
Or | left, right | (`or;left;right)
And | left, right | (`and;left;right)
StringStartsWith | colname, value | (`ssw;colname;value)
StringEndsWith | colname, value | (`sew;colname;value)
StringContains | colname, value | (`sc;colname;value)

Note that left, right, and child are Filter expressions themselves, so the resulting kdb+ conversion will have nested arrays.

If the kdb+ developer does not want to support filters, they can set `options("pushFilters", false")`. Note that this will result in all the kdb+ data to be sent to Spark.

### Column Pruning

Spark users can specify a subset of columns that they want to process, as in the following:

spark> df.select("col1", "col2")...show




## Datatype Support

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
$ ./bin/spark-shell --jars kdbspark.jar
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

The following options are used by the kdb+ data source. All other options are ignored unless the `function` option is specified.

Option | Description | Notes
:-- | :--- | :-- 
host | Name or IP address of kdb+ process | localhost
port | Port on which kdb+ process is listening | 5000
userpass | user:password if kdb+ process requires authentication | (empty)
timeout | Query timeout in milliseconds | 60
usetls | Whether to use TLS | false
numpartitions | Number of read partitions to create | 1
loglevel | Log4J logging level (debug, trace, warn, error) | inherited from Spark
qexpr | A q expression that returns a unkeyed table | Schema must be provided
table | A kdb+ table name | KdbSpark prepends an unkey ("0!") to name
function | A kdb+ function that supports the KdbSpark call interface | See section
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

## Performance

```
scala> spark.time(spark.sql("select max(ci),max(ch) from pgtbl").show)
+--------+-------+                                                              
| max(ci)|max(ch)|
+--------+-------+
|19999999|  32766|
+--------+-------+

Time taken: 10486 ms

scala> spark.time(spark.sql("select max(ci),max(ch) from kdbtbl").show)
+--------+-------+
| max(ci)|max(ch)|
+--------+-------+
|19999999|  32766|
+--------+-------+

Time taken: 325 ms
```
```
hhyndman=# select max(ci),max(ch) from pgtbl;
   max    |  max  
----------+-------
 19999999 | 32766
(1 row)

Time: 1140.174 ms (00:01.216)


q)select max ci, max ch from kdbtbl
ci       ch   
--------------
19999999 32766
q)\t select max ci, max ch from kdbtbl
8
```


Spark Object Creation:
```
Object                  | JDBC     | kdbspark
:---------------------- | -------: | -------:
byte[]                  | 40029022 | 27320
char[]                  | 20053641 | 45338
java.lang.String        | 20024793 | 20976
byte[][]                | 20000004 | 2    
scala.Some              | 13784    | 12730
java.lang.Object[]      | 11203    | 9731 
java.lang.StringBuilder | 6423     | 4775 
java.util.HashMap$Node  | 4159     | 2514 
java.lang.Long          | 3151     | 1576 
java.util.ArrayList     | 2319     | 198  
```
```



```



