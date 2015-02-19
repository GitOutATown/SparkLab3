package lab.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// https://spark.apache.org/docs/latest/graphx-programming-guide.html
object GraphX_tut_1 extends App {
	val sc = new SparkContext("local", "First_GraphX_tut", System.getenv("SPARK_HOME"))
	
	// Create an RDD for the vertices
	val users: RDD[(VertexId, (String, String))] = sc.parallelize(
	    Array(
	        (3L, ("rxin", "student")), 
	        (7L, ("jgonzal", "postdoc")),
	    		(5L, ("franklin", "prof")), 
	    		(2L, ("istoica", "prof"))
    		)
	)
	
	// Create an RDD for edges
	val relationships: RDD[Edge[String]] = sc.parallelize(
	    Array(
	        Edge(3L, 7L, "collab"),    
	        Edge(5L, 3L, "advisor"),
            Edge(2L, 5L, "colleague"),
            Edge(2L, 5L, "friend"),
            Edge(5L, 7L, "pi")
        )
    )
    
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    
    // Count all users which are postdocs
    val postDocs = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    
    // Count all the edges where src > dst
    val countSrcGreaterDst = graph.edges.filter(e => e.srcId > e.dstId).count
    
    println("postDocs: " + postDocs 
        + "\ncountSrcGreaterDst: " + countSrcGreaterDst)
    
    // All relationships that are friends
    val friends = graph.edges.filter(e => e.attr == "friend")
    
    friends.foreach(e => println("friend attr: " + e.attr 
        + "\nsrcId: " + e.srcId + "\ndstId: " + e.dstId))
    
    println("---- people -----")
    graph.vertices.foreach(v => println("v._1:" + v._1 + " v._2:" + v._2))
    
    println("------- for loop --------")
    for {
  		person <- graph.vertices
  		relation <- friends.collect
  		
  		//if person._1 == relation.srcId // || person._1 == relation.dstId
    } println("person._1:" + person._1 + " relation.srcId:" + relation.srcId + " relation.dstId:" + relation.dstId)
    
    println("---- Friends ----------")
    //filtered.foreach(f => println(f._2))
        
}





