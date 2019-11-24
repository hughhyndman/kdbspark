import org.scalatest._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.{HashMap}

class KdbTestSuite extends FunSuite with BeforeAndAfterAllConfigMap {
  var spark: SparkSession = _

  /* Set global options */
  var gopts = new HashMap[String, String]
  gopts.put("host", "localhost")
  gopts.put("port", "5000")


  override def beforeAll(cm: ConfigMap) = {
    val conf = new SparkConf()
      .setAppName("kdbspark")
      .setMaster("local")
      .set("spark.default.parallelism", "1")
      .set("spark.jars", "/users/hhyndman/dev/spark-2.4.4/kdbspark.jar")

    /* Place test configuration options in global optons for data source reader */
    for ((k,v) <- cm) {
      gopts.put(k, v.toString)
    }

    spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
  }

  override def afterAll(cm: ConfigMap)  = {
    spark.close
  }

  test("KDBSPARK-01: Simple end-to-end test") {
    val df = spark.read.format("kdb")
      .options(gopts)
      .schema("id long")
      .option("qexpr", "([] id:til 10)")
      .load
    df.show(3, false)
  }

  test("KDBSPARK-02: Reading a complete table") {
    val df = spark.read.format("kdb")
      .options(gopts)
      .option("table", "test02table")
      .load
    df.show(3, false)
  }

  test("KDBSPARK-03: Simple result with schema provided") {
    val df = spark.read
      .format("kdb")
      .options(gopts)
      .schema("j long, p timestamp, cc string") // Defaults to nullable
      .option("function", "test03")
      .option("pushFilters", false) // Function does not support push-down filters
      .load
    df.show(5, false)
  }

  test("KDBSPARK-04: Simple result with schema provided (StructType)") {
    import org.apache.spark.sql.types._

    val s = StructType(List(
      StructField("j", LongType, false),
      StructField("p", TimestampType, false),
      StructField("cc", StringType, false)
    ))

    val df = spark.read
      .format("kdb")
      .options(gopts)
      .schema(s) // Better control of nullability
      .option("function", "test04")
      .option("pushFilters", false)
      .load

    df.show(5, false)
  }

  test("KDBSPARK-05: Schema inquiry") {
    val df = spark.read
      .format("kdb")
      .options(gopts)
      .option("function", "test05")
      .load

    df.show(5, false)
  }

  test("KDBSPARK-06: Column pruning and pushdown filters") {
    val df = spark.read
      .format("kdb")
      .options(gopts)
      .option("function", "test06")
      .option("pushFilters", true)
      .load

    df.filter("j>100 and p<=to_timestamp('2020-01-02')").select("p", "j").show(5, false)
  }

  test("KDBSPARK-07: Atomic data types") {
    val df = spark.read
      .format("kdb")
      .options(gopts)
      .option("function", "test07")
      .load

    df.show(5, false)
  }

  test("KDBSPARK-08: Array data types") {
    val df = spark.read
      .format("kdb")
      .option("function", "test08")
      .options(gopts)
      .load

      df.show(5, false)
  }

  test("KDBSPARK-09: Null support") {
    val df = spark.read
      .format("kdb")
      .options(gopts)
      .option("function", "test09")
      .option("nullsupport", true)
      .load

    df.show(5, false)
  }

  test("KDBSPARK-10: Not null support") {
    val df = spark.read
      .format("kdb")
      .options(gopts)
      .option("function", "test10")
      .option("nullsupport", false)
      .load

    df.show(5, false)
  }

}

