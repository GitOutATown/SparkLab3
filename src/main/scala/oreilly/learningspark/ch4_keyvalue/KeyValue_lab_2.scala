package oreilly.learningspark.ch4_keyvalue

import org.apache.spark._
import org.apache.spark.SparkContext._

object KeyValue_lab_2 {

    def main(args: Array[String]): Unit = {
        
        val filePath = "resources/README_sample_text.md"
        val conf = new SparkConf().setAppName("SimpleApp").setMaster("local")
        val sc = new SparkContext(conf)
        
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
        println("====> combined")
        combined.take(10).foreach(println(_))
        
        val grouped = pairsCached.groupByKey()
        println("====> grouped")
        grouped.take(10).foreach(println(_))
        
        val sameKey = pairsCached.mapValues(line => line)
        println("====> sameKey")
        sameKey.take(10).foreach(println(_))
        
        val filtered = pairsCached.filter{case (k,v) => 
            v.length > 1 && v.length < 20}
        println("===>filtered:")
        filtered.take(10).foreach(println(_))
    }
}




