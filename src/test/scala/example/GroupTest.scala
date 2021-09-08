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
    (1 to 100).map(i => Place.random("ae")) ++
      (1 to 1000).map(i => Place.random("gb")) ++
      (1 to 20000).map(i => Place.random("ie")) ++
      (1 to 50000).map(i => Place.random("us"))
  }

  def debug(dir: String): Unit = {
    import sys.process._
    println(s"tree --du -h $dir" !!)

    Seq("/bin/sh", "-c", s"du -sh $dir/ds-parquet-partitioned-country/*").!
  }


  // /tmp/spark-tests843519477656602314
  // ├── ds-parquet
  // │   ├── part-00000-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
  // │   ├── part-00001-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
  // │   ├── part-00002-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
  // │   ├── part-00003-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
  // │   ├── part-00004-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
  // │   ├── part-00005-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
  // │   ├── part-00006-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
  // │   ├── part-00007-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
  // │   ├── part-00008-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
  // │   ├── part-00009-4af7d6c4-babb-4c03-9d8f-b812dcfe88a7-c000.snappy.parquet
  // │   └── _SUCCESS
  // └── ds-parquet-partitioned-country
  //     ├── country=ae
  //     │   └── part-00000-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
  //     ├── country=gb
  //     │   └── part-00000-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
  //     ├── country=ie
  //     │   ├── part-00000-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
  //     │   ├── part-00001-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
  //     │   └── part-00002-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
  //     ├── country=us
  //     │   ├── part-00002-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
  //     │   ├── part-00003-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
  //     │   ├── part-00004-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
  //     │   ├── part-00005-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
  //     │   ├── part-00006-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
  //     │   ├── part-00007-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
  //     │   ├── part-00008-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
  //     │   └── part-00009-f5f1f756-5206-49c6-a77b-e61521f8698a.c000.snappy.parquet
  //     └── _SUCCESS
  // Sizes:
  // 0       /tmp/spark-tests843519477656602314/ds-parquet-partitioned-country/_SUCCESS
  // 120K    /tmp/spark-tests843519477656602314/ds-parquet-partitioned-country/country=ae
  // 1.1M    /tmp/spark-tests843519477656602314/ds-parquet-partitioned-country/country=gb
  // 21M     /tmp/spark-tests843519477656602314/ds-parquet-partitioned-country/country=ie
  // 52M     /tmp/spark-tests843519477656602314/ds-parquet-partitioned-country/country=us

  // Num Records:
  // partition: ae -- 100
  // partition: gb -- 1000
  // partition: ie -- 20000
  // partition: us -- 50000
  test("output partitioning strategies") {
    val spark = sparkFixture()

    val places: RDD[Place] = spark.sparkContext.parallelize(randPlaces, 5)
    // assertEquals(11100l, places.count)

    import spark.implicits._

    val base = Files.createTempDirectory("spark-tests").toFile
    println(base)
    places.toDS.write.parquet(base.getAbsolutePath + "/ds-parquet")
    places.toDS.write.partitionBy("country").parquet(base.getAbsolutePath + "/ds-parquet-partitioned-country")


    println(s"RDD Partitioner: ${places.partitioner}")

    places.groupBy(_.country).foreachPartition { parts =>
      parts.foreach { case (cc, places) =>
        println(s"partition: ${cc} -- ${places.size}")
      }
    }

    places
      .sortBy(_ => Random.nextDouble())
      .toDS.write.partitionBy("country")
      .parquet(base.getAbsolutePath + "/partitioned-random-ordered")

    debug(base.getAbsolutePath)
  }
}
