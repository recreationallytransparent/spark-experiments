package experiments

import Math._
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

//
//  def distance(p1: (Double, Double), p2: (Double, Double)): Double = {
//    Math.sqrt(Math.pow(p2._1 - p1._1, 2) + Math.pow(p2._2 - p1._2, 2))
//  }

  /**
    * Return distance between two points in m
    * @param a
    * @param b
    * @return m
    */
  def haversine(a: Coordinate, b: Coordinate): Double = {
    val dLat=(b.lat - a.lat).toRadians
    val dLon=(b.lon - a.lon).toRadians

    val x = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(a.lat.toRadians) * cos(b.lat.toRadians)
    val c = 2 * asin(sqrt(x))
    6372000.8 * c
  }

  /**
    * Compute if point a is with point b's radius
    * @param a
    * @param b
    * @param r radius in m
    * @return Boolean
    */
  def isInRadius(a: Coordinate, b: Coordinate, r: Double): Boolean = {
    haversine(a, b) <= r
  }


  def main(args: Array[String]): Unit = {
    val whitehouse = Coordinate(38.8977, 77.0365)
    val empire = Coordinate(40.7484, 73.9857)
    println(haversine(whitehouse, empire))
  }
}
