package oreilly.learningspark.ch4_keyvalue

import org.apache.spark._
import org.apache.spark.SparkContext._

object foldByKey_lab_1 {
    // http://homepage.cs.latrobe.edu.au/zhe/ZhenHeSparkRDDAPIExamples.html#foldByKey
    
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setAppName("foldByKey_lab_1").setMaster("local")
        val sc = new SparkContext(conf)
        
        /* Zhen He says:
         * Very similar to fold, but performs the folding separately for each key of the RDD. This function is only available if the RDD consists of two-component tuples.
         */
        // Concatinates String values
        val rddStrList = sc.parallelize(List("dog", "horse", "giraph", "ocelot", "racoon", "cat", "owl", "bear", "monkey", "gnu", "ant", "wombat"), 2)
        val rddTupLenStr = rddStrList.map(str => (str.length, str)) // RDD[(length, string)]
        val concat = rddTupLenStr.foldByKey("")(_ + _).collect // This is the work: key is length, value is string. Accumulation of strings as concatination.
        println("concat: " + concat.mkString(", "))
        
        // Counts String values
        val rddLengthAcc = rddStrList.map(str => (str.length, 1)) // Here value type is the accumulator count unit Int 1.
        val count = rddLengthAcc.foldByKey(0)(_ + _).collect
        println("count: " + count.mkString(", "))
    }

}