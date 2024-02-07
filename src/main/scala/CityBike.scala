import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD
import scala.math._

object CityBike extends App{
  case class Station(stationId: String, name: String, long: Double, lat: Double)

  case class StationWithDistance(station: Station, distance: Double)
  case class Trip(startStationName: String, endStationName: String, dateStartMillis: Long, dateEndMillis: Long)

  case class Dataset(
                      rideId: String,
                      rideableType: String,
                      startedAt: Long,
                      endedAt: Long,
                      startStationName: String,
                      startStationId: String,
                      endStationName: String,
                      endStationId: String,
                      startLat: Double,
                      startLng: Double,
                      endLat: Double,
                      endLng: Double,
                      memberCasual: String
                    )

  // Méthode pour parser une ligne en un objet Dataset
  def fromCsvLine(line: String): Option[Dataset] = {
    val parts = line.split(",")
    if (parts.length >= 4) {
      try {
        val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        Some(
          Dataset(
            rideId = parts(0),
            rideableType = parts(1),
            startedAt = format.parse(parts(2)).getTime(),
            endedAt = format.parse(parts(3)).getTime,
            startStationName = parts(4),
            startStationId = parts(5),
            endStationName = parts(6),
            endStationId = parts(7),
            startLat = parts(8).toDouble,
            startLng = parts(9).toDouble,
            endLat = parts(10).toDouble,
            endLng = parts(11).toDouble,
            memberCasual = parts(12)
          ))
        } catch {
          case _: Throwable => None
        }
      } else
      {
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
  val DataFromFile = sc.textFile("data.csv")
  val DatasetRDD: RDD[Dataset] = DataFromFile.flatMap(fromCsvLine)

  DatasetRDD.take(10).foreach(println)

  /**
   * 1.2 - Création du premier graphes
   */

  // Construire l'RDD de nœuds (stations) et l'RDD d'arêtes (trajets)
  val stationsRDD: RDD[(VertexId, Station)] = DatasetRDD.flatMap(dataset => Seq(
    (hash(dataset.startStationId), Station(dataset.endStationName, dataset.startStationName, dataset.startLng , dataset.startLat)),
    (hash(dataset.endStationId), Station(dataset.startStationName, dataset.endStationName, dataset.endLng, dataset.endLat))
  ))

  val tripsRDD: RDD[Edge[Trip]] = DatasetRDD.map(dataset =>
    Edge(hash(dataset.startStationId), hash(dataset.endStationId), Trip(dataset.startStationName, dataset.endStationName, dataset.startedAt, dataset.endedAt))
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
  }


  // 1.3 - Station proximity
  // Question 1.3.1
  // Trouvez et affichez la station la plus proche de la station JC013 tel que la distance du trajet (relation)
  // entre les deux stations soit minimale. (aggregateMessage)
  // (Pour le calcul de distance, utilisez la fonction getDistKilometers).


  val jc013Id = hash("JC013")
  val helper = new HelpfulFunctions()

  // Assuming jc013Id is correctly defined as the hashed value of "JC013"

  // Define a message class for minimum distance, excluding self-comparisons
  case class MinDistance(minDist: Double = Double.MaxValue, stationId: VertexId = -1,stationName: String)

  // Aggregate messages to find the nearest station to JC013 by minimal distance, excluding JC013 itself
  val nearestInDistanceToJC013 = bikeGraph.aggregateMessages[MinDistance](
    triplet => {
      // Calculate distance if the current edge does not involve JC013 talking to itself
      if ((triplet.srcId != jc013Id || triplet.dstId != jc013Id) && (triplet.srcId == jc013Id || triplet.dstId == jc013Id)) {
        val dist = helper.getDistKilometers(triplet.srcAttr.lat, triplet.srcAttr.long, triplet.dstAttr.lat, triplet.dstAttr.long)
        if (triplet.srcId == jc013Id) {
          triplet.sendToDst(MinDistance(dist, triplet.dstId,triplet.dstAttr.name)) // Send distance to destination only if it's not JC013
        }
        if (triplet.dstId == jc013Id) {
          triplet.sendToSrc(MinDistance(dist, triplet.srcId,triplet.srcAttr.name)) // Send distance to source only if it's not JC013
        }
      }
    },
    (a, b) => if (a.minDist < b.minDist) a else b // Choose the message with the minimum distance
  )

  // Find and print the nearest station excluding JC013 itself
  if (nearestInDistanceToJC013.isEmpty()) {
    println("No stations found or no distances calculated.")
  } else {
    val nearestStation = nearestInDistanceToJC013.filter(_._1 != jc013Id).sortBy(_._2.minDist, ascending = true).first()
    println(s"The nearest in distance station to JC013 is ${nearestStation._2.stationName} (ID : ${hashtable(nearestStation._2.stationId)}) with a distance of ${nearestStation._2.minDist} kilometers.")
  }



  case class MinDuration(minDur: Double = Double.MaxValue, stationId: VertexId = -1,stationName: String)

  val nearestInDurationToJC013 = bikeGraph.aggregateMessages[MinDuration](
    triplet => {
      // Calculate distance if the current edge does not involve JC013 talking to itself
      if ((triplet.srcId != jc013Id || triplet.dstId != jc013Id) && (triplet.srcId == jc013Id || triplet.dstId == jc013Id)) {
        val timeDiff = helper.getTimeDifference(triplet.attr.dateStartMillis, triplet.attr.dateEndMillis)
        if (triplet.srcId == jc013Id) {
          triplet.sendToDst(MinDuration(timeDiff, triplet.dstId,triplet.dstAttr.name)) // Send distance to destination only if it's not JC013
        }
        if (triplet.dstId == jc013Id) {
          triplet.sendToSrc(MinDuration(timeDiff, triplet.srcId,triplet.srcAttr.name)) // Send distance to source only if it's not JC013
        }
      }
    },
    (a, b) => if (a.minDur < b.minDur) a else b // Choose the message with the minimum distance
  )

  if (nearestInDurationToJC013.isEmpty()) {
    println("No stations found or no distances calculated.")
  } else {
    val nearestStation = nearestInDurationToJC013.filter(_._1 != jc013Id).sortBy(_._2.minDur, ascending = true).first()
    println(s"The nearest in time station to JC013 is ${nearestStation._2.stationName} (ID : ${hashtable(nearestStation._2.stationId)}) with a duration of ${nearestStation._2.minDur} seconds.")
  }



}
