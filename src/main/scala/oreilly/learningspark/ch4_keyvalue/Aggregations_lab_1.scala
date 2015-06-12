package oreilly.learningspark.ch4_keyvalue

import org.apache.spark._
import org.apache.spark.SparkContext._

object Aggregations_lab_1 {
    
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setAppName("Aggregations").setMaster("local")
        val sc = new SparkContext(conf)
        
        val pairs = sc.parallelize(List(
            ("panda", 0), ("pink", 3), ("pirate", 3), ("panda", 1), ("pink", 4)
        ))
        
        // reduceByKey
        val mapValRedKey = pairs.mapValues(v => (v, 1)).reduceByKey(
            (a, b) => (a._1 + b._1, a._2 + b._2)
        )
        println("mapValues.reduceByKey:")
        mapValRedKey.foreach(println(_))
    }

}



