package experiments

import Math._
import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
/**
  * Goal: Find all records in a set that are within some time range and within 50m of a set of 100 points of interest
  *
  * Given:
  *   100 points of interest in csv form:
  *     name, lat, lon, radius
  *   Set of location records in parquet format:
  *     advertiser_id: String, location_at: Timestamp(UTC), lat, lon
  *
  *
  */
object LocationMatcher {
  case class Coordinate(lat: Double, lon: Double)
  case class LocationRecord(advertise_id: String, location_at: Long, lat: Double, lon: Double)
  case class PointOfInterest(name: String, lat: Double, lon: Double, radius: Double)
  case class Query(timeStart: Long, timeStop: Long, maxDistance: Double)

  def haversine(lat0: Double, lon0: Double, lat1: Double, lon1: Double): Double = {
    val dLat=(lat1 - lat0).toRadians
    val dLon=(lon1 - lon0).toRadians

    val x = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat0.toRadians) * cos(lat1.toRadians)
    val c = 2 * asin(sqrt(x))
    6372000.8 * c
  }

  def isInRadius(lat0: Double, lon0: Double, lat1: Double, lon1: Double, r: Double): Boolean = {
    haversine(lat0, lon0, lat1, lon1) <= r
  }

  def locationIsNear(location: LocationRecord, pointOfInterest: PointOfInterest, tolerableRadius: Double) = {
    isInRadius(location.lat, location.lon, pointOfInterest.lat, pointOfInterest.lon, tolerableRadius)
  }

  def naiveImplementation(spark: SparkSession, pointsOfInterest: DataFrame, locationData: DataFrame, t0: Long, t1: Long, d: Double): RDD[(String, Array[LocationRecord])] = {
    import spark.implicits._
    // first filter all location data that is outside of range
    val filteredLocations = locationData.filter($"location_at" > t0 && $"location_at" < t1).as[LocationRecord].collect()
    val poi = spark.sparkContext.parallelize(pointsOfInterest.map(r => {
      PointOfInterest(r.getString(0), r.getString(1).toDouble, r.getString(2).toDouble, r.getString(3).toDouble)
    }).collect())

    poi
      .map(x => {
        val n = x.name
        val l = filteredLocations.map(l => {
          val isNear = locationIsNear(l, x, d)
          (l, isNear)
        })
        (n, l)
      })
      .map({
        case (x, l) => (x, l.filter(_._2).map(_._1))
      })
  }

  def f(sparkSession: SparkSession, pointsOfInterest: Dataset[PointOfInterest], locations: Dataset[LocationRecord], query: Query): Dataset[(PointOfInterest, Dataset[LocationRecord])] = {
    import sparkSession.implicits._
    val timeFilteredLocations = locations.filter(l => l.location_at > query.timeStart && l.location_at < query.timeStop)
    pointsOfInterest.map(poi => {
      (poi, timeFilteredLocations.filter(l => locationIsNear(l, poi, query.maxDistance)))
    })
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  def main(args: Array[String]): Unit = {
    val poiLocation = args(0)
    val parquetLocation = args(1)
    val r = args(2).toInt
    val output = args(3)

    deleteRecursively(new File(output))

    val spark: SparkSession = SparkSession.builder()
      .appName("ParquetSeeder")
      .getOrCreate()

    import spark.implicits._
//
//    implicit val poiEncoder = org.apache.spark.sql.Encoders.kryo[PointOfInterest]
//    implicit val locationEncoder = org.apache.spark.sql.Encoders.kryo[LocationRecord]
    val pointsOfInterest: Array[PointOfInterest] = spark.sqlContext.read.csv(poiLocation).map(r => {
      PointOfInterest(r.getString(0), r.getString(1).toDouble, r.getString(2).toDouble, r.getString(3).toDouble)
    }).collect()
    val locationData: Dataset[LocationRecord] = spark.sqlContext.read.parquet(parquetLocation).as[LocationRecord]

    val t0 = 1525442564
    val t1 = 1528120964
    val query = Query(timeStart = t0, timeStop = t1, maxDistance = r)
//
//    println(s"${pointsOfInterest.count()} points of interest")
//    println(s"${locationData.count()} locations")
//    val result: Dataset[(PointOfInterest, Dataset[LocationRecord])] = f(spark, pointsOfInterest, locationData, query)
    val timeFilteredLocations = locationData.filter(l => l.location_at > query.timeStart && l.location_at < query.timeStop)
//    val result = pointsOfInterest.map(poi => {
//      import spark.implicits._
//      (poi, timeFilteredLocations.filter(l => locationIsNear(l, poi, query.maxDistance)))
//    })

    val result = timeFilteredLocations.flatMap(location => {
      pointsOfInterest
        .filter(poi => locationIsNear(location, poi, query.maxDistance))
        .map(poi => (poi, location))
    })
    result
      //.map(t => (t._1, t._2.collect()))
      .write
      .json(output)
//    val result: RDD[(String, Array[LocationRecord])] = naiveImplementation(spark, pointsOfInterest, locationData, t0, t1, r)
//
//    val c = result
//      .flatMap({
//        case (poiName: String, locations: Array[LocationRecord]) =>
//          println(s"$poiName as ${locations.length} locations")
//          locations.map(l => (poiName, l.toString))
//      })
//
//    import spark.implicits._
//    c.toDF()
//      .write
//      .json(output)

//    c.saveAsTextFil??e(output)
  }
}
