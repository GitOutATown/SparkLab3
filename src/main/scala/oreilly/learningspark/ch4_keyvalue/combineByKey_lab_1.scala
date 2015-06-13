package oreilly.learningspark.ch4_keyvalue

import org.apache.spark._
import org.apache.spark.SparkContext._

object combineByKey_lab_1 {
    
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setAppName("combineByValue_lab_1").setMaster("local")
        val sc = new SparkContext(conf)
        
        val keys = sc.parallelize(List("a", "b", "c", "d", "a", "b", "c", "d", "c", "c"))
        val values = sc.parallelize(List(1,2,3,4,5,3,2,7,1,6))
        val zipped = keys zip values
        println("zipped: " + zipped.collect().mkString(", "))
        
        // combineByKey can be used when you are combining elements but your return type differs from your input value type.
        // 'value' is literally the value from the (key, value) tuple, which could be any types.
        // Note that 'key' is not even visible here. It's part is all automatic and indemic to the (higher order) functional pattern.
        val combined = zipped.combineByKey(
            // (value, 1)  value is just the value listed in the values RDD, or the ._2 half of the tuple. ; 1 is just a accumulator of the counting index incrementing over the (k,v) tuple provided as denominator of the mean ratio which is the ultimate product for each key.
            (value) => (value, 1), // Combiner type pattern. createCombiner, which turns a V into a C (e.g., creates a one-element list)
            (combiner: (Int, Int), value) => (combiner._1 + value, combiner._2 + 1), // This is the combiner's behavioral action, as aligned with its type. The type, of course is modelling a purpose/process/value/computation. Or, as PySpark doc says: [Think of it as (call it)] mergeValue, to merge a V into a C (e.g., adds it to the end of a list)
            (combinerA: (Int, Int), combinerB: (Int, Int)) => // Pretty straight forward addition (accumulation)
                (combinerA._1 + combinerB._1, combinerA._2 + combinerB._2) // mergeCombiners, to combine two C’s into a single one.
        ) map {
            // Ah! Here is the actual flavor of the work that this function accomplishes, chained here to a map function that computes the average value for the key achieved by the accumulation of the values and counting their quantity as value entities.
            case (key, value) => (key, value._1 / value._2.toFloat)
        }
        
        combined.collectAsMap().foreach(println(_)) 
    }

}