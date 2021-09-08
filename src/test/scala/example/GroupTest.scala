import org.apache.spark.sql.SparkSession
import scala.util.Random
import java.util.UUID
import org.apache.spark.rdd.RDD
import java.nio.file.Files

case class Place(id: String, country: String, payload: Array[Byte])
object Place {
  def payload: Array[Byte] = Array.fill(1024)((Random.nextInt(256) - 128).toByte)
  def random(cc: String): Place =
    Place(UUID.randomUUID.toString, cc, payload)
}

class GroupTest extends munit.FunSuite {
  val sparkFixture = new Fixture[SparkSession]("spark") {
    private var spark: SparkSession = null
    def apply() = spark

    override def beforeAll(): Unit = {
      spark = SparkSession
        .builder()
        .config("spark.ui.enabled", false)
        .master("local[*]")
        .appName("shapes_spark_tests")
        .getOrCreate()
    }

    override def afterAll(): Unit = {
      spark.close()
    }
  }
  override def munitFixtures = List(sparkFixture)

  def randPlaces: Seq[Place] = {
    (1 to 100).map(i => Place.random("ae")) ++ (1 to 1000).map(i => Place.random("us"))
  }

  def debug(dir: String): Unit = {
    import sys.process._
    println(s"tree $dir" !!)

    Seq("/bin/sh", "-c", s"du -sh $dir/ds-parquet-partitioned-country/*").!
  }

  test("output partitioning strategies") {
    val spark = sparkFixture()

    val places: RDD[Place] = spark.sparkContext.parallelize(randPlaces, 10)
    assertEquals(1100l, places.count)

    import spark.implicits._

    val base = Files.createTempDirectory("spark-tests").toFile
    println(base)
    places.toDS.write.parquet(base.getAbsolutePath + "/ds-parquet")
    places.toDS.write.partitionBy("country").parquet(base.getAbsolutePath + "/ds-parquet-partitioned-country")

    debug(base.getAbsolutePath)
  }
}
