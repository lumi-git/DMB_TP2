/*import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.Console.println

object template extends App {
  val sparkConf = new SparkConf().setAppName("graphXTP").setMaster("local[1]")
  val sc = new SparkContext(sparkConf)
  val DATASET_PATH = "/Volumes/ExternalSSD/ESIR3/DMB/TP2DMB/Spark-GraphX/toyGraphDataset/"
  /*  Graph creation  */
  val users = sc.textFile(DATASET_PATH +"users.txt")
  val followers = sc.textFile(DATASET_PATH +"followers.txt")


  /*  Création de graphe */

  /*

  Les étapes suivantes consistent à convertir L'RDD users en VertexRDD[VD] et l'RDD followers en EdgeRDD[ED, VD],
  puis à créer un graphe avec ces deux RDDs.

   */

  case class User(name: String, nickname: String)

  //Convertissez chaque élément de l'RDD users en objet de classe user.

  val usersRDD: RDD[(VertexId, User)] = users.map{line =>
    val row = line split ','
    if (row.length > 2){
      (row(0).toLong, User(row(1), row(2)))
    }else {
      (row(0).toLong, User(row(1), "NOT DEFINED"))
    }
  }

  //Transformez chaque élément de l 'RDD followers en Edge[Int]

  val followersRDD: RDD[Edge[Int]] = followers map {line =>
    val row = line split ' '
    (Edge(row(0).toInt, row(1).toInt, 1))
  }

  //Contruisez le graphe à partir de usersRDD et followersRDD

  val toyGraph : Graph[User, Int] =
    Graph(usersRDD, followersRDD)
  toyGraph.vertices.collect()
  toyGraph.edges.collect()

  /*  Extraction de sous-graphe  */

  /*

  Affichez l'entourage des noeuds Lady gaga et Barack Obama avec une solution qui fait appel
  à la fonction \textit{subgraph}.

   */

  val desiredUsers = List("ladygaga", "BarackObama")

  toyGraph.subgraph(edgeTriplet => desiredUsers contains edgeTriplet.srcAttr.name )
    .triplets.foreach(t => println(t.srcAttr.name + " is followed by " + t.dstAttr.name))

  /*  Computation of the vertices' Degrees  */

  /*

  Calculez le degré (Nombre de relations sortantes) de chaque noeuds en faisant appel
  à la méthode \texttt{aggregateMessage}.

   */
  print("Tu as acheté trop de talkie")
  val outDegreeVertexRDD = toyGraph.aggregateMessages[Int](_.sendToSrc(1), _+_)
  outDegreeVertexRDD.collect().foreach(println)

  /* Calcul de la distance du plus long chemin sortant de chaque noeud. */

  /*

  Créez une nouvelle classe \texttt{UserWithDistance} pour inclure la distance aux noeuds du graphe.

  */

  case class UserWithDistance(name: String, nickname: String, distance: Int)

  /*
  * Transformez les VD des noeuds du graphe en type UserWithDistance et initialisez la distance à 0.

  * Définissez la méthode vprog (VertexId, VD, A) => VD. Cette fonction met à jour les données
    VD du noeud ayant un identifiant VD en prenant en compte le message A

  * Définissez la méthode sendMsg EdgeTriplet[VD, ED] => 	Iterator[(VertexId, A)].
    Ayant les données des noeuds obtenues par la méthode vprog VD et des relations ED.
    Cette fonction traite ces données pour créer un message $A$ et l'envoyer ensuite au noeuds destinataires.

  * Définissez la méthode mergeMsg (A, A) => A. Cette méthode est identique à la méthode reduce
    qui traite tous les messages reçus par un noeud et les réduit en une seule valeur.

  *

   */

  val toyGraphWithDistance = Pregel(toyGraph.mapVertices((_, vd) => UserWithDistance(vd.name, vd.nickname, 0)),
        initialMsg=0, maxIterations=2,
        activeDirection = EdgeDirection.Out) (
        (_:VertexId, vd:UserWithDistance, a:Int) => UserWithDistance(vd.name, vd.nickname, math.max(a, vd.distance)),
        (et:EdgeTriplet[UserWithDistance, Int]) => Iterator((et.dstId, et.srcAttr.distance+1)),
        (a:Int, b:Int) => math.max(a,b))
  toyGraphWithDistance.vertices.foreach(user =>println(user))
}
*/