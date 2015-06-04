package oreilly.advancedanalytics.lab

import org.apache.spark._
import org.apache.spark.rdd.RDD

object Basic_1 {

    def main(args: Array[String]): Unit = {
        
        val sc = new SparkContext("local", "Basic_1", System.getenv("SPARK_HOME"))
        
        val rdd_1 = sc.parallelize(Array(1,2,3,4,5))
        val even = rdd_1.filter(_ % 2 == 0)
        println("even.count: " + even.count)
        println(even.first)
    }

}