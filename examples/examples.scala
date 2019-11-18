val HOST = "localhost"
val PORT = 5000

//
// Call the sample query function, which details the general framework
// of a kdb+ function that supports calls from Spark. 
//
val dfSample = spark.read.
	format("kdb").
	option("host", HOST).
	option("port", PORT).
	option("function", "sampleQuery").
	load
	
dfSample.show


//
// Invoke a Q-language expression that returns an unkeyed table
//
val dfBasic = spark.read.
	format("kdb").
	option("host", HOST).
	option("port", PORT).
	schema("id long"). // Must provide a schema for q expressions
	option("qexpr", "([] id:til 10)"). // Expression must return an unkeyed table
	load

dfBasic.show

//
// Get the contents of a kdb+ table
//
val dfTable = spark.read.format("kdb").
	option("host", HOST).
	option("port", PORT).
	option("table", "sampleTable").
	load

dfTable.show


//
// The host and port are provided. A kdb+ function is called, and since the schema
// is provided ahead of time, the kdb+ function need only return the table result.
// The pushFilters option value of false indicates to Spark that the kdb+ cannot
// support filter expressions.
//
// Note that Spark assumes that all kdb+ columns are nullable when a schema is
// provided as a SQL declaration. Use the StructType approach to specify nullability.
//
val dfSimple1 = spark.read.
	format("kdb").
	option("host", HOST).
	option("port", PORT).
	schema("jcolumn long, pcolumn timestamp, clcolumn string"). // Defaults to nullable
	option("function", "exampleSimple").
	option("pushFilters", false). // Function cannot support push-down filters
	load

dfSimple1.show(5, false)

//
// A variation of above using StructType schemas
//
import org.apache.spark.sql.types._

val kdbSchema = StructType(List(
	StructField("jcolumn", LongType, false),
	StructField("pcolumn", TimestampType, false),
	StructField("clcolumn", StringType, false)
))

val dfSimple2 = spark.read.
	format("kdb").
	option("host", HOST).
	option("port", PORT).
	schema(kdbSchema). // Better control of nullability
	option("function", "exampleSimple").
	option("pushFilters", false). 
	load

dfSimple2.show(5, false)	

//
// Almost all kdb+ datatypes are supported. Here, the kdb+ function is able to
// support returning the schema that describes the query result, so there is 
// no need to provide the schema directly to Spark. 
//
val dfSchema = spark.read.
	format("kdb").
	option("host", HOST).
	option("port", PORT).
	option("function", "exampleSchema").
	option("pushFilters", false).
	load	

dfSchema.show(5, false)

//
// This kdb+ function can support column pruning and push-down filters. Detailed
// logging is turned on (Log4J) to aid in debugging. The log level is sent to the
// kdb+ function so it can also emit log messages
//	
val dfFilters = spark.read.
	format("kdb").
	option("host", HOST).
	option("port", PORT).
	option("function", "exampleFilters").
	option("pushFilters", true). // This is the default and can be elided
	load

dfFilters.
	filter("jcolumn>100 and pcolumn<=to_timestamp('2020-01-02')").
	select("pcolumn","jcolumn").
	count

//
// This example demonstrates a number of features:
//
// - The dataframe is divided into 4 partitions, which creates 4 read tasks that
//   are run across the executors.
// - The host name can be a list of hosts, separated by semicolumns. The same for ports
// - Arbitrary options can be provided that are ignored by the kdb+ datasource, but
//   send to the kdb+ function. These can be used to help kdb+ accommodate partitioning.
//
// If the number of hosts (or ports) does not match the number of partitions, then
// the software will loop through the entries.
//
val dfMulti = spark.read.
	format("kdb").
	option("numPartitions", 4). // Creates 4 read tasks
	option("host", "localhost;127.0.0.1;localhost;127.0.0.1"). // List is optional
	option("port", s"$PORT;$PORT;$PORT;$PORT").
	option("function", "exampleMulti").
	option("ex5parms", "2;4;6;7"). // Whole string sent to each kdb+ function
	option("ex5maxrows", 3). // Sent as string to each kdb+ function
	load

dfMulti.show(false)

//
// Nulls are supported. This example calls a kdb+ function that returns a table
// with some column values containing nulls. The custom option nullsupport option
// allows the caller to either convert kdb+ nulls to Spark nulls, or pass the 
// kdb+ null placeholder value without change.
//
val dfNulls = spark.read.
	format("kdb").
	option("function", "exampleNulls").
	option("nullsupport", true). // Map kdb+ null placeholders to Spark nulls
	load

dfNulls.show(false)

//
// With no null support
//
val dfNoNulls = spark.read.
	format("kdb").
	option("host", HOST).
	option("port", PORT).
	option("function", "exampleNulls").
	option("nullsupport", false). // Pass through kdb+ null placeholders
	load

dfNoNulls.show(false)

//
// Returns a table of the most common column types used by kdb+ developers:
//    kdb+ types: xbhijefscmdzt and C (char-list=string)
//
val dfCommonTypes = spark.read.
	format("kdb").
	option("host", HOST).
	option("port", PORT).
	option("function", "exampleCommonTypes").
	option("numrows", 10). // Custom option passed down to kdb+ function
	load

dfCommonTypes.show(false)


//
// Returns a table of array (list) types, including kdb+ types: XCHIJEFPD
//
val dfArrayTypes = spark.read.
	format("kdb").
	option("host", HOST).
	option("port", PORT).
	option("function", "exampleArrayTypes").
	option("numrows", 10).
	load

dfArrayTypes.show(false)


//
// Large table
//
val dfLarge = spark.read.
	format("kdb").
	option("host", HOST).
	option("port", PORT).
	option("function", "exampleLarge").
	load

dfLarge.filter("icolumn>1000 and icolumn<1050").show(false)

dfLarge.createOrReplaceTempView("tblLarge")
spark.sql("select * from tblLarge where icolumn>1000 and icolumn<1050").show


//TODO: Show more write examples (with commit and abort)
dfCommonTypes.write.
	format("kdb").
	option("host", HOST).
	option("port", PORT).
	option("batchsize", 4).
	option("function", "testWrite").
	option("writeaction", "append").
	save
