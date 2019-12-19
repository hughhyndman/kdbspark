# KdbSpark: Spark Data Source (V2) for kdb+ Database

The KdbSpark data source provides a high-performance read and write interface between Apache Spark (2.4) and Kx Systems' kdb+ database. The following sample illustrates a sample Spark shell session that fetches some specific columns from a kdb+ table.

```
spark-2.4.4$ ./bin/spark-shell --master local[4] --jars kdbspark.jar
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.4
      /_/

Using Scala version 2.11.12 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_91)

scala> val df = spark.read.format("kdb").
   | option("expr", "select ci,cp,cf from sampleTable").
   | load
scala> df.show(3, false)
+---+-----------------------+-----------------+
|ci |cp                     |cf               |
+---+-----------------------+-----------------+
|75 |2020-01-19 12:06:54.468|921.537384390831 |
|282|2020-01-02 03:45:55.59 |829.9179940950125|
|536|2020-01-18 13:30:49.836|472.9025000706315|
+---+-----------------------+-----------------+
only showing top 3 rows
```

## Outstanding Work List
- [] Complete README 
- [] Renumber tests 
- [] Sample code for write support
- [] Document Spark streaming to kdb+
- [] Test with Yarn, Mesos, and Kubernetes
- [] Include executor IP in options passed to func if Log.DEBUG

## Reading Data From kdb+

The basic structure for reading data is as follows.

```scala
> val df = spark.read.format("kdb").
    | schema(...).
    | option("name", "value").
    | load
```

`option` allows you to set key-value configurations to set parameters on how you want to read data. A table of all options are provided further in this document. `schema` is optional, and is used if you do not want to interrogate kdb+ for the column names and types of the read result, but it is important to note that Spark requires a schema ahead of reading the data in order for it to initialize various data structures.

Kdbspark offers two methods to get data from kdb+. The caller can provide either a q-language expression (`expr`), or the name of a q function (`func`) that must to adhere to a particular calling sequence.

### Expression: option("expr", "...")

Providing a q-language expression that returns a table (unkeyed) is the simplest method. Examples are provided below.

```
q)mytbl:([] 
  id:1000 2000 0Nj; 
  ts:2020.01.02D03:04:05.555 2020.02.03D06:07:08.999,.z.p; 
  val:3.1415 2.7182 6.02210)

scala> spark.read.format("kdb").
   | option("expr", "select from mytbl where not null(id)").
   | load.show(false)

+----+-----------------------+------+
|id  |ts                     |val   |
+----+-----------------------+------+
|1000|2020-01-02 03:04:05.555|3.1415|
|2000|2020-02-03 06:07:08.999|2.7182|
+----+-----------------------+------+
```

KdbSpark makes two calls to kdb+. The first call gets the table's schema (by prepending "0!meta " to the q-expression), and then maps each kdb+ type to a Spark type (refer to the section on Datatype Support for more information). The second call gets all the table's data and brings it into Spark memory. If you want to avoid the first calls that gets the schema, in particular if your expression is compute or IO intensive, you can provide the schema ahead of time, as follows.

```scala
scala> spark.read.format("kdb").
   | schema("id long, ts timestamp, val double").
   | option("expr", "mytbl").
   | load.show(false)

+----+-----------------------+------+
|id  |ts                     |val   |
+----+-----------------------+------+
|1000|2020-01-02 03:04:05.555|3.1415|
|2000|2020-02-03 06:07:08.999|2.7182|
|null|2019-12-19 16:03:46.888|6.0221|
+----+-----------------------+------+
```

When providing a schema DDL, nulls are assumed. If you want to specify which columns are nullable, you will have to populate an instance of the StructType class. Please refer to the section entitled "Datatype Support" for a description of the mapping between kdb+ and Spark datatypes.

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

Specifying false (not null) for the nullable field of StructField will result in better performance as KdbSpark does not need to examine each value before passing it to Spark, although you will have to consider how to handle the null placeholder value.

### Function Name: option("func", "...")

This is the most flexible retrieval method -- KdbSpark calls a cooperating kdb+ function that can be written to support custom query parameters, push-down filters, and multi-partition (Spark) processing.

The kdb+ function takes a dictionary of options as a parameter and returns tables. If the schema option is not used, this function is called twice: the first time to return schema information, and the second to retrieve the actual data. Let's walk through the minimum use case and then build on this.

```scala
scala> val df = spark.read.format("kdb").
     | option("func", "sampleQuery").
     | load
```

```q
sampleQuery:{[parms]
  $[parms[`partitionid]=-1; // If asked for schema,
    0!meta sampleTable // then return unkeyed meta of table
    sampleTable // else return table
    ]
  }
```

Spark can partition its processing to be spread across multiple nodes in a cluster. When Spark calls KdbSpark, it can provide the number of partitions (default is 1) from which the query will be performed. Each partition is given an integer ID starting at zero. When KdbSpark has to request kdb+ for the query schema, it directs the request to a single partition, and provides a partition ID of -1 in parameters provided to the the query function.

Kdb+ script files are included in the project resources folder to use as a guide. One file, [spark.q](https://github.com/hughhyndman/kdbspark/blob/master/src/test/resources/spark.q), provides helper functions to help implement logging, pushdown filters, etc. The other, [sample.q](https://github.com/hughhyndman/kdbspark/blob/master/src/test/resources/sample.q), is a fully-commented working query.

### Pushdown Filters

KdbSpark supports pushdown filters, which are essentially a set of predicates and conjunctions that comprise some or all of the where-clause in Spark SQL (or the argument to a DataFrame's filter function). The filters are converted into kdb+ nested array format and are included in the function's parameter, whose responsibility is to convert the array to a kdb+ where clause or to the constraint parameter in kdb+'s functional select.

For example, if a user enters the following filter expression:

```scala
spark> df.filter("cj>100 and cp<=to_timestamp('2020-01-02')")...show
```

Spark converts this to an Array[Filter], and sends it to Kdbspark, which in turn converts it into a mixed array, places it in the options dictionary (key: filters).

```
((`le;`cp;2020.01.02D00:00:00.000000000);(`gt;`cj;100))
```

It is up to the kdb+ developer to convert this expression for execution. You may find the parsing utility (.sp.pruneAndFilter) in [spark.q](https://github.com/hughhyndman/kdbspark/blob/master/src/test/resources/spark.q) useful as it applies the filter to a mapped table.

The following table provides a summary of the filters used by Spark, their arguments, and how Kdbspark converts each filter to a kdb+ array.

Filter | Args | Kdb+ Conversion
:-- | :-- | :--
EqualTo | colname, value | (\`eq;colname;value)
EqualNullSafe | colname, value | (\`eq;colname;value)
GreaterThan | colname, value | (\`gt;colname;value)
GreaterThanOrEqual | colname, value | (\`ge;colname;value)
LessThan | colname, value | (\`lt;colname;value)
LessThanOrEqual | colname, value | (\`le;colname;value)
IsNull | colname | (\`isnull;colname)
IsNotNull | colname | (\`isnotnull;colname)
In| colname, values | (\`in;colname;values)
Not | child | (\`not;child)
Or | left, right | (\`or;left;right)
And | left, right | (\`and;left;right)
StringStartsWith | colname, value | (\`ssw;colname;value)
StringEndsWith | colname, value | (\`sew;colname;value)
StringContains | colname, value | (\`sc;colname;value)

Note that left, right, and child are Filter expressions themselves, so the resulting kdb+ conversion will have nested arrays.

If you do not want to support filters, you can set `options("pushFilters", "false")`. Note that this will result in all the kdb+ data to be sent to Spark for filtering.

### Column Pruning

Spark users can specify a subset of columns that they want to process, as in the following:

```spark
spark> df.select("col1", "col2")...show
```

KdbSpark will provide a 

KdbSpark includes a `columns` entry in the parameters with a list of column names to include in the result.

## Datatype Support

TBD

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
expr | A q expression that returns a unkeyed table | Schema can be provided
func | A kdb+ function that supports the KdbSpark call interface | 
pushfilters | Whether kdb+ function supports push-down filters | true
batchsize | Number for rows to be written per round trip | 10000

## Kdb+ Write Function

TBD

## Reading in Parallel

TBD: Discuss:

* options("numpartitions", ...)
* semicolon-separated list of hosts options("host", "a;b;c")
* ditto for ports
* each kdb+ `func` execution receives a different partitionid
* using that partitionid to index other options that might contain various parameters

```
scala> val df = spark.read.format("kdb").
   | option("numpartitions", 3).
   | option("func", "multi").
   | option("host", "host1;host2;host3").
   | option("port", "5000;5000;5001").
   | option("multiparm", "parm1;parm2;parm3").
   | load
```

## Platform support

TBD 

Discuss opportunity to hook in calls via some sort of proxy (often called a query 
router or dispatcher in kdb+). Include Brendan's snippet for Kx.

## Logging

TBD:
* loglevel
* logging in the datasource
* logging in the kdb+ code (use spark.q script and .sp namespace)
* assertion in kdb+ (using Signal (') operator )
* log4j entries

## Performance Considerations

TBD

## ETL Samples

TBD

Show ETL to and from kdb+, Parquet, BigQuery, Postgres, and a Spark streaming example
