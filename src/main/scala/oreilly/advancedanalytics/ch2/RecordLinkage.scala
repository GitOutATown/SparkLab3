package oreilly.advancedanalytics.ch2

import org.apache.spark._
import akka.dispatch.Foreach
//import org.apache.spark.rdd.RDD

object RecordLinkage {
  
	case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

	def main(args: Array[String]): Unit = {
		val sc = new SparkContext("local", "Basic_1", System.getenv("SPARK_HOME"))
	
		val rawblocks = sc.textFile("../../Spark/workspace_tuts/Advanced_Analytics_with_Spark_workspace_1/linkage")
		
		val take_10 = rawblocks.take(10)
		println("---- take_10:")
		take_10.foreach(println)
		
		//val take_10_notHeaderLines = take_10.filter(!isHeader(_))
		//val head = rawblocks.take(5)
		//val mds_local = take_10.filter(!isHeader(_)).map(parse(_))
		
		val all_parsed = rawblocks.filter(!isHeader(_)).map(parse(_))
		all_parsed.cache
		
		/* 
	    Blows up! Why?
	    val all_count = all_notHeaderLines.count
	    println("all_count: " + all_count)
		*/
		
		val all_head = all_parsed.take(10)
		println("----- all_head:")
		all_head.foreach(println)
		
		
	}
	
	def isHeader(line: String): Boolean = line.contains("id_1")

	def toDouble(s: String) = {
	    if("?" == s) Double.NaN else s.toDouble
	}
	
	def parse(line: String) = {
	    val pieces = line.split(",")
	    val id1 = pieces(0).toInt
	    val id2 = pieces(1).toInt
	    val scores = pieces.slice(2,10).map(toDouble)
	    val matched = pieces(11).toBoolean
	    MatchData(id1, id2, scores, matched)
	}
}