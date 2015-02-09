package lab.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Tut_1 {
	val sc = new SparkContext("local", "First_GraphX_tut", System.getenv("SPARK_HOME"))
                                                  //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
                                                  //| ies
                                                  //| 15/02/08 22:06:06 INFO SecurityManager: Changing view acls to: hieronymus
                                                  //| 15/02/08 22:06:06 INFO SecurityManager: Changing modify acls to: hieronymus
                                                  //| 15/02/08 22:06:06 INFO SecurityManager: SecurityManager: authentication disa
                                                  //| bled; ui acls disabled; users with view permissions: Set(hieronymus); users 
                                                  //| with modify permissions: Set(hieronymus)
                                                  //| 15/02/08 22:06:07 INFO Slf4jLogger: Slf4jLogger started
                                                  //| 15/02/08 22:06:07 INFO Remoting: Starting remoting
                                                  //| 15/02/08 22:06:07 INFO Remoting: Remoting started; listening on addresses :[
                                                  //| akka.tcp://sparkDriver@10.15.1.69:51347]
                                                  //| 15/02/08 22:06:07 INFO Utils: Successfully started service 'sparkDriver' on 
                                                  //| port 51347.
                                                  //| 15/02/08 22:06:07 INFO SparkEnv: Registering MapOutputTracker
                                                  //| 15/02/08 22:06:07 INFO SparkEnv: Registering BlockManagerMaster
                                                  //| 15/02/08 22:06:07 INFO DiskBlockManager: Created local directory at /v
                                                  //| Output exceeds cutoff limit.
	
	// val userGraph: Graph[(String, String), String] // Error: no instantiation
	
	val users: RDD[(VertexId, (String, String))] =
  sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
                                                  //> users  : org.apache.spark.rdd.RDD[(org.apache.spark.graphx.VertexId, (String
                                                  //| , String))] = ParallelCollectionRDD[0] at parallelize at lab.graphx.Tut_1.sc
                                                  //| ala:13
  // Probably I should do this in a regular object. Not sure that Scala worksheet really is the best accomodation for Spark.
}