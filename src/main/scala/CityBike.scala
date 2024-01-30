import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD

import scala.math._

object CityBike extends App{
  case class Station(stationId: String, name: String, long: Double, lat: Double)
  case class Trip(startStationName: String, endStationName: String, dateStartMillis: Long, dateEndMillis: Long)

  def getDistKilometers(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val earthRadiusKm = 6371.0

    val dLat = toRadians(lat2 - lat1)
    val dLon = toRadians(lon2 - lon1)

    val a = sin(dLat / 2) * sin(dLat / 2) +
      cos(toRadians(lat1)) * cos(toRadians(lat2)) *
        sin(dLon / 2) * sin(dLon / 2)

    val c = 2 * atan2(sqrt(a), sqrt(1 - a))
    earthRadiusKm * c
  }

  // Fonction pour convertir une ligne en Trip
  def parseLine(line: String): Option[Trip] = {
    val row = line.split(",")
    if (row.length >= 4) {
      try {
        val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // Corrected date format
        val dateStartMillis = format.parse(row(2)).getTime()
        val dateEndMillis = format.parse(row(3)).getTime()
        Some(Trip(row(4), row(6), dateStartMillis, dateEndMillis))
      } catch {
        case _: Throwable => None
      }
    } else {
      None
    }
  }

  /**
   * 1.1 Préparation des données
   */

  //Read file
  val sparkConf = new SparkConf().setAppName("graphXTP").setMaster("local[1]")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("WARN")
  val rddFromFile = sc.textFile("data.csv")
  val tripRDD: RDD[Trip] = rddFromFile.flatMap(parseLine)

  /**
   * 1.2 - Création du premier graphes
   */

  // Construire l'RDD de nœuds (stations) et l'RDD d'arêtes (trajets)
  val stationsRDD: RDD[(VertexId, Station)] = tripRDD.flatMap(trip => Seq(
    (hash(trip.startStationName), Station(trip.sta, trip.startStationName, 0, 0)),
    (hash(trip.endStationName), Station(trip.endStationName, trip.endStationName, 0, 0))
  ))

  val tripsRDD: RDD[Edge[Trip]] = tripRDD.map(trip =>
    Edge(hash(trip.startStationName), hash(trip.endStationName), trip)
  )

  // Construire le graphe
  val bikeGraph: Graph[Station, Trip] = Graph(stationsRDD, tripsRDD)

  var hashtable = scala.collection.mutable.Map[Long, String ]()

  // Définir la fonction de hachage pour les nœuds
  def hash(name: String): Long = {
    hashtable.put(name.hashCode.toLong & 0xFFFFFFFFL, name)
    name.hashCode.toLong & 0xFFFFFFFFL
  }

  /**
   * 1.3 - Extraction of Subgraph and Analysis
   */

  // Function to check if a date falls within the specified range
  def isInDateRange(startMillis: Long, endMillis: Long): Boolean = {
    val startDate = new java.text.SimpleDateFormat("yyyy-MM-dd").parse("2021-12-05").getTime
    val endDate = new java.text.SimpleDateFormat("yyyy-MM-dd").parse("2021-12-25").getTime

    startMillis >= startDate && endMillis <= endDate
  }

  //print the station if the stationID is JC JC013
  println(hash("JC013"))
  for (vert <- bikeGraph.vertices) {
      println(vert._2.stationId + " " + vert._1 + " unhashed" + hashtable.get(vert._1))
  }

  /*

  // Extract subgraph based on the time interval
  val subGraph = bikeGraph.subgraph(epred = edge => isInDateRange(edge.attr.dateStartMillis, edge.attr.dateEndMillis))

  // Define a message class for aggregateMessages
  case class TripCount(inCount: Int, outCount: Int)

  // Aggregate messages to compute total incoming and outgoing trips for each station
  val tripCounts = subGraph.aggregateMessages[TripCount](
    triplet => {
      triplet.sendToSrc(TripCount(0, 1)) // Outgoing trip from source
      triplet.sendToDst(TripCount(1, 0)) // Incoming trip to destination
    },
    (a, b) => TripCount(a.inCount + b.inCount, a.outCount + b.outCount) // Reduce function to sum up counts
  )


  // Get the top 10 stations with most outgoing and incoming trips
  val mostOutgoing = tripCounts.sortBy(_._2.outCount, ascending = false).take(10)
  val mostIncoming = tripCounts.sortBy(_._2.inCount, ascending = false).take(10)

  println("Stations with Most Outgoing Trips:")
  // for all, print the station name and the number of outgoing trips
  for (tripCount <- mostOutgoing) {
    println(hashtable(tripCount._1) + " : " + tripCount._2.outCount)
  }

  println("Stations with Most Incoming Trips:")
  for (tripCount <- mostIncoming) {
    println(hashtable(tripCount._1) + " : " + tripCount._2.outCount)
  }*/

}
