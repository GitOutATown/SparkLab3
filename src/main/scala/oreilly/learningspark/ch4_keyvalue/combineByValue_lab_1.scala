package oreilly.learningspark.ch4_keyvalue

import org.apache.spark._
import org.apache.spark.SparkContext._

object combineByValue_lab_1 {
    
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setAppName("combineByValue_lab_1").setMaster("local")
        val sc = new SparkContext(conf)
        
        val keys = sc.parallelize(List("a", "b", "c", "d", "a", "b", "c", "d", "c", "c"))
        val values = sc.parallelize(List(1,2,3,4,5,3,2,7,1,6))
        val zipped = keys zip values
        println("zipped: " + zipped.collect().mkString(", "))
        
        val combined = zipped.combineByKey(
            (v) => (v, 1),
            (combiner: (Int, Int), value) => (combiner._1 + value, combiner._2 + 1), 
            (combinerA: (Int, Int), combinerB: (Int, Int)) => 
                (combinerA._1 + combinerB._1, combinerA._2 + combinerB._2)
        ) map {
            case (key, value) => (key, value._1 / value._2.toFloat)
        }
        
        combined.collectAsMap().foreach(println(_)) 
    }

}