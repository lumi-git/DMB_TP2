import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object CityBike extends App{
  case class Station(stationId: String, name: String, long: Double, lat: Double)
  case class Trip(startStationName: String, endStationName: String, dateStartMillis: Long, dateEndMillis: Long)

  // Fonction pour convertir une ligne en Trip
  def parseLine(line: String): Option[Trip] = {
    val row = line.split(",")
    if (row.length >= 4) {
      try {
        val format = new java.text.SimpleDateFormat("yyyy-mm-dd HH:mm:ss")
        val dateStartMillis = format.parse(row(2)).getTime()
        val dateEndMillis = format.parse(row(3)).getTime()
        val timeTrip = dateEndMillis - dateStartMillis
        val timeTripMinutes = timeTrip / (60 * 1000)
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

  val DATASET_PATH = "/Volumes/ExternalSSD/ESIR3/DMB/TP2DMB/"
  val rddFromFile = sc.textFile(DATASET_PATH +"data.csv")
  val tripRDD: RDD[Trip] = rddFromFile.flatMap(parseLine)

  /**
   * 1.2 - Création du premier graphes
   */

  // Construire l'RDD de nœuds (stations) et l'RDD d'arêtes (trajets)
  val stationsRDD: RDD[(VertexId, Station)] = tripRDD.flatMap(trip => Seq(
    (hash(trip.startStationName), Station(trip.startStationName, trip.startStationName, 0, 0)),
    (hash(trip.endStationName), Station(trip.endStationName, trip.endStationName, 0, 0))
  ))

  val tripsRDD: RDD[Edge[Trip]] = tripRDD.map(trip =>
    Edge(hash(trip.startStationName), hash(trip.endStationName), trip)
  )

  // Construire le graphe
  val bikeGraph: Graph[Station, Trip] = Graph(stationsRDD, tripsRDD)

  // Définir la fonction de hachage pour les nœuds
  def hash(name: String): Long = {
    name.hashCode.toLong & 0xFFFFFFFFL
  }

  println("1.2 : création du graphe")
  // Afficher les nœuds du graphe
  bikeGraph.vertices.collect().foreach(println)
  // Afficher les arêtes du graphe
  bikeGraph.edges.collect().foreach(println)
  println("------------------------------------------------------------")


}