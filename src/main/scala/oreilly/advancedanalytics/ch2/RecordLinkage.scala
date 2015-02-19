package oreilly.advancedanalytics.ch2

import java.lang.Double.isNaN

import org.apache.spark._
import akka.dispatch.Foreach
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

object RecordLinkage {
  
	case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

	def main(args: Array[String]): Unit = {
		val sc = new SparkContext("local", "Basic_1", System.getenv("SPARK_HOME"))
	
		val rawblocks = sc.textFile("../../Spark/workspace_tuts/Advanced_Analytics_with_Spark_workspace_1/linkage")
		
		val first_take_10 = rawblocks.take(10)
		println("---- take_10:")
		first_take_10.foreach(println)
		
		val first_take_10_parsed = first_take_10.filter(!isHeader(_)).map(parse(_))
		
		val all_parsed = rawblocks.filter(!isHeader(_)).map(parse(_))
		all_parsed.cache
		
		/* 
	    Blows up! Why?
	    val all_count = all_notHeaderLines.count
	    println("all_count: " + all_count)
		*/
		
		val all_parsed_head = all_parsed.take(10)
		println("----- parsed_head:")
		all_parsed_head.foreach(println)
		
		// Aggregation vs. GroupedBy
		
		val grouped = first_take_10_parsed.groupBy(md => md.matched)
		println("----- first_take_parsed_head:")
		grouped.mapValues(_.size).foreach(println)
		
		// BLOWING UP! NEED TO DIAGNOSE THIS. MEMORY PROBLEM?
		//val matchCounts = all_parsed.map(md => md.matched).countByValue()
		val matchCounts = all_parsed.map(md => md.matched).countByValue()
		println("matchCounts: " + matchCounts)
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