package oreilly.learningspark.ch2

import org.apache.spark._
import org.apache.spark.SparkContext._

object IntroLab1 {
    
    def main (args: Array[String]): Unit = {
        val sc = new SparkContext("local", "IntroLab1", System.getenv("SPARK_HOME"))
        
        val textFile = sc.textFile("resources/README_sample_text.md")
        
        val textCnt = textFile.count() // Action
        println(s"textCnt: $textCnt")
        
        val textFirst = textFile.first()
        
        val linesWithSpark = textFile.filter { line => line.contains("Spark") }
        println(s"linesWithSpark: " + linesWithSpark)
        println(s"linesWithSpark.count: " + linesWithSpark.count)
        val lwsCache = linesWithSpark.cache()
        println(s"lwsCache: $lwsCache")
        
        val longestLine = textFile.map(line => 
            line.split(" ").size).reduce((a,b) => Math.max(a, b))
            
        val wordsCount1 = textFile.flatMap(line => 
            line.split(" ")).map(word => 
                (word, 1)).reduceByKey((a,b) => a+b)
        
        // THIS IS INCORRECT, EVIDENTLY...
        val words = for {
            line <- textFile
            wordInd <- line.split(" ").map { word => (word, 1) }
        } yield wordInd
        val wordsCount2 = words.reduceByKey((a,b) => a+b)
        // DISPARITY!!!!!!!!!!!!!
        println(s"wordsCount1:$wordsCount1 wordsCount2:$wordsCount2")
        /*
        println("=========>> wordsCount1.collect:")
        wordsCount1.collect.foreach(println(_))
        println("=========>> wordsCount2.collect:")
        wordsCount2.collect.foreach(println(_))
        */
        
    }
}