package tools

import java.time.Instant
import java.util.Date

import experiments.LocationMatcher.{LocationRecord, PointOfInterest}
import org.apache.spark._
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SparkSession}

import scala.util.Random
/**
  * Created by lenovo on 6/2/2018.
  */
object DataSeeder {

  def randomLatLon: (Double, Double) = {
    val max = 180
    val min = -180

    val lat = min + (max - min) * Random.nextDouble
    val lon = min + (max - min) * Random.nextDouble

    (lat, lon)
  }

  def randomUTCStamp(daysBack: Int = 30): Long = (System.currentTimeMillis() / 1000) - (Random.nextInt(daysBack) * 86400)

  def locationRecord(i: Int, ids: Set[String]) = {
    val (lat, lon) = randomLatLon
    val advertiserId = ids.toSeq(Random.nextInt(ids.size))
    // given today generate random timestamp up to 30 days old
    val timestamp = randomUTCStamp()
    LocationRecord(advertiserId, timestamp, lat, lon)
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("ParquetSeeder")
      .getOrCreate()

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val numberItems = args(0).toInt
    val parquetOutputLocation = args(1)
    val csvOutputLocation = args(2)

    val ids = Set(
      "CocaCola", "Pepsi", "Google", "Facebook", "NSA"
    )

   spark.sparkContext.parallelize(0 to numberItems)
      .map(locationRecord(_, ids))
      .toDF()
      .write
      .parquet(parquetOutputLocation)

    println("Created {} parquet entries", sqlContext.read.parquet(parquetOutputLocation).count())

    spark.sparkContext.parallelize(0 to 100)
      .map(i => {
        val (lat, lon) = randomLatLon
        PointOfInterest(s"Point-$i", lat, lon, Random.nextDouble() * 500)
      })
        .toDF()
      .write
      .csv(csvOutputLocation)

    println("Created {} csv entries", sqlContext.read.csv(csvOutputLocation).count())
  }
}
