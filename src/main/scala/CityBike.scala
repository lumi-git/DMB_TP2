import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD
import scala.math._

object CityBike extends App{
  case class Station(stationId: String, name: String, long: Double, lat: Double)
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

  println(" ============== Exercice 1.1 ============== ")

  //Read file
  val sparkConf = new SparkConf().setAppName("graphXTP").setMaster("local[1]")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("WARN")
  val DataFromFile = sc.textFile("data.csv")
  val DatasetRDD: RDD[Dataset] = DataFromFile.flatMap(fromCsvLine)

  println(" ======= Exercice 1.1 - show dataset  ======= ")
  DatasetRDD.take(10).foreach(println)

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


  println(" ============== Exercice 1.2 ============== ")
  def isInDateRange(startMillis: Long, endMillis: Long): Boolean = {
    val startDate = new java.text.SimpleDateFormat("yyyy-MM-dd").parse("2021-12-05").getTime
    val endDate = new java.text.SimpleDateFormat("yyyy-MM-dd").parse("2021-12-25").getTime

    startMillis >= startDate && endMillis <= endDate
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


  println(" ============== Exercice 1.3 ============== ")
  val jc013Id = hash("JC013")
  val helper = new HelpfulFunctions()
  case class MinDistance(minDist: Double = Double.MaxValue, stationId: VertexId = -1,stationName: String)

  val nearestInDistanceToJC013 = bikeGraph.aggregateMessages[MinDistance](
    triplet => {
      if ((triplet.srcId != jc013Id || triplet.dstId != jc013Id) && (triplet.srcId == jc013Id || triplet.dstId == jc013Id)) {
        val dist = helper.getDistKilometers(triplet.srcAttr.lat, triplet.srcAttr.long, triplet.dstAttr.lat, triplet.dstAttr.long)
        if (triplet.srcId == jc013Id) {
          triplet.sendToDst(MinDistance(dist, triplet.dstId,triplet.dstAttr.name))
        }
        if (triplet.dstId == jc013Id) {
          triplet.sendToSrc(MinDistance(dist, triplet.srcId,triplet.srcAttr.name))
        }
      }
    },
    (a, b) => if (a.minDist < b.minDist) a else b
  )

  if (nearestInDistanceToJC013.isEmpty()) {
    println("No stations found or no distances calculated.")
  } else {
    val nearestStation = nearestInDistanceToJC013.filter(_._1 != jc013Id).sortBy(_._2.minDist, ascending = true).first()
    println(s"The nearest in distance station to JC013 is ${nearestStation._2.stationName} (ID : ${hashtable(nearestStation._2.stationId)}) with a distance of ${nearestStation._2.minDist} kilometers.")
  }



  case class MinDuration(minDur: Double = Double.MaxValue, stationId: VertexId = -1,stationName: String)

  val nearestInDurationToJC013 = bikeGraph.aggregateMessages[MinDuration](
    triplet => {
      if ((triplet.srcId != jc013Id || triplet.dstId != jc013Id) && (triplet.srcId == jc013Id || triplet.dstId == jc013Id)) {
        val timeDiff = helper.getTimeDifference(triplet.attr.dateStartMillis, triplet.attr.dateEndMillis)
        if (triplet.srcId == jc013Id) {
          triplet.sendToDst(MinDuration(timeDiff, triplet.dstId,triplet.dstAttr.name))
        }
        if (triplet.dstId == jc013Id) {
          triplet.sendToSrc(MinDuration(timeDiff, triplet.srcId,triplet.srcAttr.name))
        }
      }
    },
    (a, b) => if (a.minDur < b.minDur) a else b
  )

  if (nearestInDurationToJC013.isEmpty()) {
    println("No stations found or no distances calculated.")
  } else {
    val nearestStation = nearestInDurationToJC013.filter(_._1 != jc013Id).sortBy(_._2.minDur, ascending = true).first()
    println(s"The nearest in time station to JC013 is ${nearestStation._2.stationName} (ID : ${hashtable(nearestStation._2.stationId)}) with a duration of ${nearestStation._2.minDur/1000} seconds.")
  }


  println(" ============== Exercice 1.4 ============== ")
  println(" ======= Exercice 1.4 - Partie 1 ======= ")
  case class StationWithDistance(station: Station, distance: Double)

  val initialGraph = bikeGraph.mapVertices((id, station) =>
    if (id == jc013Id) StationWithDistance(station, 0)
    else StationWithDistance(station, Double.MaxValue)
  )

  val pregelGraph = initialGraph.pregel(Double.MaxValue)(
    (id, state, newDist) => StationWithDistance(state.station, math.min(state.distance, newDist)),
    triplet => {
      if (triplet.srcAttr.station != null && triplet.dstAttr.station != null) {
        val dist = helper.getDistKilometers(
          triplet.srcAttr.station.lat, triplet.srcAttr.station.long,
          triplet.dstAttr.station.lat, triplet.dstAttr.station.long
        )
        if (triplet.srcAttr.distance + dist < triplet.dstAttr.distance) {
          Iterator((triplet.dstId, triplet.srcAttr.distance + dist))
        } else {
          Iterator.empty
        }
      } else {
        Iterator.empty
      }
    },
    (a, b) => math.min(a, b)
  )

  val distancesFromJC013 = pregelGraph.vertices
    .filter { case (id, StationWithDistance(station, distance)) => id != jc013Id && distance < Double.MaxValue }
    .sortBy { case (id, StationWithDistance(station, distance)) => distance }
    .take(10)

  println("Top 10 des stations les plus proches de JC013 :")
  distancesFromJC013.foreach {
    case (id, StationWithDistance(station, distance)) =>
      println(s"${station.name} à une distance de ${distance.formatted("%.2f")} km")
  }

  println(" ======= Exercice 1.4 - Partie 2 ======= ")
  case class StationWithPath(station: Station, distance: Double, path: List[Long])

  val initialGraphWithPath = bikeGraph.mapVertices { (id, station) =>
    if (id == jc013Id) StationWithPath(station, 0, List(jc013Id))
    else StationWithPath(station, Double.MaxValue, List())
  }

  val pregelGraphWithPath = initialGraphWithPath.pregel((Double.MaxValue, List[Long]()))(
    (id, oldValue, message) => {
      if (message._1 < oldValue.distance) {
        StationWithPath(oldValue.station, message._1, message._2)
      } else {
        oldValue
      }
    },
    triplet => {
      val newDist = triplet.srcAttr.distance + helper.getDistKilometers(
        triplet.srcAttr.station.lat, triplet.srcAttr.station.long,
        triplet.dstAttr.station.lat, triplet.dstAttr.station.long)
      if (newDist < triplet.dstAttr.distance) {
        Iterator((triplet.dstId, (newDist, triplet.srcAttr.path :+ triplet.dstId)))
      } else {
        Iterator.empty
      }
    },
    (a, b) => if (a._1 < b._1) a else b
  )

  val distancesFromJC013WithPath = pregelGraphWithPath.vertices
    .filter { case (id, StationWithPath(station, distance, path)) => id != jc013Id && distance < Double.MaxValue }
    .sortBy { case (id, StationWithPath(station, distance, path)) => distance }
    .take(10)

  println("Top 10 des stations les plus proches de JC013 :")
  distancesFromJC013WithPath.foreach {
    case (id, StationWithPath(station, distance, path)) =>
      val pathStations = path.map(hashtable.getOrElse(_, "Unknown Station")).mkString(" -> ")
      println(s"${station.name} est à une distance de ${distance.formatted("%.2f")} km via le chemin $pathStations")
  }


}
