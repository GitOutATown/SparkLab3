package oreilly.learningspark.ch4_keyvalue

import org.apache.spark._
import org.apache.spark.SparkContext._

object Aggregations_lab_1 {
    
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setAppName("Aggregations").setMaster("local")
        val sc = new SparkContext(conf)
        
        // ------------------------- //
        
        val pairs = sc.parallelize(List(
            ("panda", 0), ("pink", 3), ("pirate", 3), ("panda", 1), ("pink", 4)
        ))
        
        // reduceByKey
        val mapValReduceKey = pairs.mapValues(v => (v, 1)).reduceByKey(
            // pairwise sequential reduction: 
            // a is value, b is number of instances
            (a, b) => (a._1 + b._1, a._2 + b._2)
        )
        println("mapValues.reduceByKey:")
        mapValReduceKey.foreach(println(_))
        /* Output:
           (panda,(1,2)) // (key, (sum values, number of instances)) 
           (pirate,(3,1))
           (pink,(7,2))
         */
        
        // combineByKey
        
    }

}



