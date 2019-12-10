import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import java.sql.{Timestamp => JTimestamp, Date => JDate}

class KdbTestBase extends FunSuite with BeforeAndAfterAllConfigMap {
  var spark: SparkSession = _

  /* Set global options */
  var gopts = new mutable.HashMap[String, String]
  gopts.put("host", "localhost")
  gopts.put("port", "5000")
  gopts.put("loglevel", "debug")

  override def beforeAll(cm: ConfigMap) :Unit = {
    val conf = new SparkConf()
      .setAppName("kdbspark")
      .setMaster("local")
      .set("spark.default.parallelism", "1")

    /* Place test configuration options in global options for data source reader */
    for ((k,v) <- cm) {
      gopts.put(k, v.toString)
    }

    spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
  }

  override def afterAll(cm: ConfigMap) :Unit  = {
    spark.close
  }  
}

class KdbTestRead extends KdbTestBase {

  test("KDBSPARK-01: Simple end-to-end test") {
    val df = spark.read.format("kdb").options(gopts)
      .schema("cj long")
      .option("qexpr", "([] cj:til 10)")
      .load
    df.show(3, false)
  }

  test("KDBSPARK-02: Reading a complete table") {
    val df = spark.read.format("kdb").options(gopts)
      .option("table", "test02table")
      .load
    df.show(3, false)
  }

  test("KDBSPARK-03: Simple result with schema provided") {
    val df = spark.read.format("kdb").options(gopts)
      .schema("cj long, cp timestamp, ccc string") // Defaults to nullable
      .option("function", "test03")
      .option("pushFilters", false) // Function does not support push-down filters
      .load
    df.show(5, false)
  }

  test("KDBSPARK-04: Simple result with schema provided (StructType)") {
    val s = StructType(List(
      StructField("cj", LongType, false),
      StructField("cp", TimestampType, false),
      StructField("ccc", StringType, false)
    ))

    val df = spark.read.format("kdb").options(gopts)
      .schema(s) // Better control of nullability
      .option("function", "test04")
      .option("pushFilters", false)
      .load

    df.show(5, false)
  }

  test("KDBSPARK-05: Schema inquiry") {
    val df = spark.read.format("kdb").options(gopts)
      .option("function", "test05")
      .load

    df.show(5, false)
  }

  test("KDBSPARK-06: Column pruning and pushdown filters") {
    val df = spark.read.format("kdb").options(gopts)
      .option("function", "test06")
      .option("pushFilters", true)
      .load

    df.filter("cj>100 and cp<=to_timestamp('2020-01-02')").select("cp", "cj").show(5, false)
  }

  test("KDBSPARK-07: Atomic data types") {
    val df = spark.read.format("kdb").options(gopts)
      .option("function", "test07")
      .load

    df.show(5, false)
  }

  test("KDBSPARK-08: Array data types") {
    val df = spark.read.format("kdb").options(gopts)
      .option("function", "test08")
      .load

      df.show(5, false)
  }

  test("KDBSPARK-09: Null support") {
    val df = spark.read.format("kdb").options(gopts)
      .option("function", "test09")
      .load

    df.show(5, false)
  }

  test("KDBSPARK-10: Missing schema exception") {
    val e = intercept[Exception] {
      spark.read.format("kdb").options(gopts)
        .option("qexpr", "([] til 10)")
        .load
    }.getMessage
    assert(e.contains("Schema"))
  }

  test("KDBSPARK-11: Unsupported datatype") {
    val e = intercept[Exception] {
      val s = StructType(List(StructField("cj", DecimalType(20,2), false)))
      val df = spark.read.format("kdb").options(gopts)
        .schema(s)
        .option("qexpr", "([] cj:til 10)")
        .load
      df.show(false)
    }.getMessage
    assert(e.contains("Unsupported"))
  }

  test("KDBSPARK-12: Column name mismatch") {
    val e = intercept[Exception] {
      val df = spark.read.format("kdb").options(gopts)
        .schema("ci long")
        .option("qexpr", "([] cj:1 2)")
        .load

      df.show(false)
    }.getMessage
    assert(e.contains("Missing"))
  }

  test("KDBSPARK-13: Integer datatype mismatch") {
    val e = intercept[Exception] {
      val df = spark.read.format("kdb").options(gopts)
        .schema("cj int")
        .option("qexpr", "([] cj:1 2)")
        .load

      df.show(false)
    }.getMessage
    assert(e.contains("Expecting"))
  }

  test("KDBSPARK-14: Long datatype mismatch") {
    val e = intercept[Exception] {
      val df = spark.read.format("kdb").options(gopts)
        .schema("cj long")
        .option("qexpr", "([] cj:1 2i)")
        .load

      df.show(false)
    }.getMessage
    assert(e.contains("Expecting"))
  }

  test("KDBSPARK-15: Date and Timestamp retrieval") {
    val df = spark.read.format("kdb").options(gopts)
      .option("function", "test15")
      .load

      df.show(false)
  }

  test("KDBSPARK-16: Short datatype mismatch") {
    val e = intercept[Exception] {
      val df = spark.read.format("kdb").options(gopts)
        .schema("cj short")
        .option("qexpr", "([] cj:1 2i)")
        .load

      df.show(false)
    }.getMessage
    assert(e.contains("Expecting"))
  }

//TODO: Data type mismatches here
}

class KdbTestWrite extends KdbTestBase {
  test("KDBSPARK-50: Test numeric datatypes") {
    val sc = spark.sparkContext

    val row = Row.apply(
      true,
      123.toByte,
      123.toShort,
      123.toInt,
      1234.toLong,
      3.5.toFloat,
      3.5.toDouble)

    val schema =
      StructType(
        StructField("cb", BooleanType) ::
        StructField("cx", ByteType) ::
        StructField("ch", ShortType) ::
        StructField("ci", IntegerType) ::
        StructField("cj", LongType) ::
        StructField("ce", FloatType) ::
        StructField("cf", DoubleType) ::
        Nil)
    val df = spark.createDataFrame(sc.parallelize(Array[Row](row)), schema)

    df.write.format("kdb").options(gopts)
      .option("batchsize", 2)
      .option("function", "test50")
      .option("writeaction", "append")
      .save
  }

  test("KDBSPARK-51: Test date datatypes") {
    val sc = spark.sparkContext

    val row = Row.apply(JTimestamp.valueOf("2020-01-01 00:00:00.0"),
      JDate.valueOf("2020-01-01")
      )

    val schema =
      StructType(
        StructField("cp", TimestampType) ::
        StructField("cd", DateType) ::
        Nil)

    val df = spark.createDataFrame(sc.parallelize(Array[Row](row)), schema)
    df.show(false)

    df.write.format("kdb").options(gopts)
      .option("batchsize", 2)
      .option("function", "test51")
      .option("writeaction", "append")
      .save
  }

  test("KDBSPARK-52: Array datatypes") {
    val df = spark.read.format("kdb").options(gopts)
      .option("table", "test08table")
      .load

    df.select("cxx", "cbb", "chh", "cii", "cjj", "cee", "cff", "cpp", "cdd")
      .write.format("kdb").options(gopts)
      .option("batchsize", 1)
      .option("function", "test52")
      .option("writeaction", "append")
      .save
  }

  test("KDBSPARK-53: Date and Timestamp loopback") {
    val df = spark.read.format("kdb").options(gopts)
      .option("table", "test53table")
      .load
    df.show(false)

    df.write.format("kdb").options(gopts)
      .option("function", "test53")
      .save

    spark.read.format("kdb").options(gopts).option("table", "T53").load.show(false)
  }
}


