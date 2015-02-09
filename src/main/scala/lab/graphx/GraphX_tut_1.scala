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
    
    // Diagnostic
    println("postDocs: " + postDocs + "\ncountSrcGreaterDst: " + countSrcGreaterDst)
}





