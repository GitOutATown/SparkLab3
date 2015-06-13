package oreilly.learningspark.ch4_keyvalue

import org.apache.spark._
import org.apache.spark.SparkContext._

object combineByValue_lab_2 {
    
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setAppName("combineByValue_lab_2").setMaster("local")
        val sc = new SparkContext(conf)
        
        // http://homepage.cs.latrobe.edu.au/zhe/ZhenHeSparkRDDAPIExamples.html#combineByKey
        
        val values = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 2)
        val keys = sc.parallelize(List(1,1,2,2,2,1,2,2,2), 2)
        val zipped = keys.zip(values)
        println(zipped)
        
        // def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C): RDD[(K, C)]
        val combined = zipped.combineByKey(                         // : RDD[(K, C)]
            List(_),                                    // createCombiner: V => C
            (combiner:List[String], value:String) => value :: combiner,       // mergeValue: (C, V) => C
            (combinerA:List[String], combinerB:List[String]) => combinerA ::: combinerB // mergeCombiners: (C, C) => C
        )
        
        println("combined: " + combined.collect.mkString(", "))
        // d: (2,List(salmon, gnu, bee, bear, wolf, rabbit)), (1,List(cat, dog, turkey))

    }

}