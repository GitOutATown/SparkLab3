package oreilly.learningspark.ch4_keyvalue

import org.apache.spark._
import org.apache.spark.SparkContext._

object KeyValue_lab_2 {

    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setAppName("SimpleApp").setMaster("local")
        val sc = new SparkContext(conf)
        
        val filePath = "resources/README_sample_text.md"
        val textData = sc.textFile(filePath, 2)
        
        // Creating RDD[(String, String)] with first word of line as key
        val pairs = textData.map(line => {
            //println("-->line: " + line)
            //println("line.length: " + line.length())
            val splitWords = line.split(" ")
            //println("splitLine.length: " + splitLine.length)
            if(splitWords.length > 0) (splitWords(0), line)
            else ("", line)
        })
                
        val pairsCached = pairs.cache()
        println("====> initial pairsCached.count: " + pairsCached.count())
        pairsCached.foreach(println(_))
        
        /** Transformations **/
        // Not the most profound examples
        
        val combined = pairsCached.reduceByKey((x, y) => 
            "\nline >>>" + x + "\nline >>>" + y)
        println("====> reduceByKey")
        combined.take(10).foreach(println(_))
        
        // Word count for document
        val words = textData.flatMap { line => line.split(" ") }
        val wordCount_v1 = words.map(word => 
            (word, 1)).reduceByKey((w1, w2) => w1 + w2)
        println("wordCount_v1: " + wordCount_v1)
        
        // Instance count for each word in the document
        val wordCount_v2 = textData.flatMap(x => x.split(" ")).countByValue()
        println("wordCount_v2: " + wordCount_v2)
        
        val grouped = pairsCached.groupByKey()
        println("====> groupByKey")
        grouped.take(10).foreach(println(_))
        
        val sameKey = pairsCached.mapValues(line => line)
        println("====> mapValues")
        sameKey.take(10).foreach(println(_))
        
        val filtered = pairsCached.filter{case (k,v) => 
            v.length > 1 && v.length < 20}
        println("===>filter:")
        filtered.take(10).foreach(println(_))
    }
}




