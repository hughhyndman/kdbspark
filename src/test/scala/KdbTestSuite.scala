import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

class KdbTestSuite extends FunSuite with BeforeAndAfterAll {
  var spark: SparkSession = _

  override def beforeAll = {
    val conf = new SparkConf()
      .setAppName("kdbspark")
      .setMaster("local")
      .set("spark.default.parallelism", "1")
      .set("spark.jars", "/users/hhyndman/dev/spark-2.4.4/kdbspark.jar")

    spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
  }

  override def afterAll  = {
    spark.stop
  }

  test("Test01 - Simple end-to-end test") {
    val df = spark.read.format("kdb")
      .option("host", "localhost")
      .option("port", 5000)
      .schema("id long")
      .option("qexpr", "([] id:til 10)")
      .load
    df.show(3)
  }

  test("Test02 - What happened here?") {
    assert(true);
  }
}

