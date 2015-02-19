package lab.graphx

import org.apache.spark.graphx._

/**
 * RW: Notice the basic Scala case class inheritance pattern...
 * 
 * https://spark.apache.org/docs/latest/graphx-programming-guide.html
 * In some cases it may be desirable to have vertices with different property types 
 * in the same graph. This can be accomplished through inheritance. For example to 
 * model users and products as a bipartite graph we might do the following:
 */
object GraphX_tut_2 {
	class VertexProperty
	case class UserProperty(val name: String) extends VertexProperty
	case class ProductProperty(val name: String, val price: Double) extends VertexProperty
	// The graph might then have the type:
	var graph: Graph[VertexProperty, String] = null
}