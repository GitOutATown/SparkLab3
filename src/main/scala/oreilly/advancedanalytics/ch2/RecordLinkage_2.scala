package oreilly.advancedanalytics.ch2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

// https://github.com/sryza/aas/blob/master/ch02-intro/src/main/scala/com/cloudera/datascience/intro/RunIntro.scala

case class MatchData(id1: Int, id2: Int,
  scores: Array[Double], matched: Boolean)

object RecordLinkage_2 extends Serializable {

	def main(args: Array[String]): Unit = {
		val sc = new SparkContext("local", "RecordLinkage_2", System.getenv("SPARK_HOME"))
	  
		val rawblocks = sc.textFile("../../Spark/workspace_tuts/Advanced_Analytics_with_Spark_workspace_1/linkage")
	  
		val rawblocks_take_10 = rawblocks.take(10)
		println("---- rawblocks_take_10")
		rawblocks_take_10.foreach(println)
	  
		val sampleLine = rawblocks_take_10(5)
		val samplePieces = sampleLine.split(",")
	  
		println("==== sampleLine samplePieces:")
		for(i <- 0 until samplePieces.length) {
			println(i + ": " + samplePieces(i))
		}
	  
		def isHeader(line: String) = line.contains("id_1")
	  
		val noheader = rawblocks.filter(x => !isHeader(x))
	  
		def parse(line: String) = {
			val pieces = line.split(',')
			val id1 = toInt(pieces,0)
			val id2 = toInt(pieces,1)
			val scores = pieces.slice(2, 11).map(toDouble)
			val matched = toBoolean(pieces,11)
			MatchData(id1, id2, scores, matched)
		}
	  
		val parsed = noheader.map(line => parse(line))
		parsed.cache()
		val matchCounts = parsed.map(md => md.matched).countByValue
		val matchCountsSeq = matchCounts.toSeq
		matchCountsSeq.sortBy(_._2).foreach(println)
	} // end main
	
	/* Parsing helpers */
	
	def toDouble(s: String) = {
		if ("?".equals(s)) { Double.NaN }
		else {
			try {
				s.toDouble
			} catch { case e:NumberFormatException => Double.NaN }
		}
	}
	def toInt(s: Array[String], index: Int) = {
		try {
			s(index).toInt
		} catch { 
		  	case nfe:NumberFormatException => Int.MinValue
		  	case aoe:ArrayIndexOutOfBoundsException => Int.MinValue
		}
	}
	def toBoolean(s: Array[String], index: Int) = {
		try {
    	  	  	s(index).toBoolean
	  	  	} catch {
	  	  		case aioe:ArrayIndexOutOfBoundsException => false
	  	  		case	 iae:IllegalArgumentException => false	
	  	  	}
	}
}




