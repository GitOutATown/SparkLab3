package oreilly.learningspark.ch4_keyvalue

import org.apache.spark._
import org.apache.spark.SparkContext._

object KeyValue_lab_3 {
    
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("SimpleApp").setMaster("local")
        val sc = new SparkContext(conf)
        
        val pairs = sc.parallelize(List((1, 2), (4, 6), (3, 6), (3, 4)))
        val other = sc.parallelize(List((3, 9)))
        
        //// Transformations on one pair RDD ////
        
        // RDD[(Int, Int)]
        val combined = pairs.reduceByKey((v1, v2) => v1 + v2)
        println("===>reduceByKey:")
        combined.foreach(println(_))
        
        // RDD[(Int, Iterable[Int])]
        val grouped = pairs.groupByKey()
        println("===>groupByKey:")
        grouped.foreach(println(_))
        
        // RDD[(Int, U)]
        val mapped = pairs.mapValues(v => "value * 10: " + (v * 10))
        println("===>mapValues:")
        mapped.foreach(println(_))
        
        // (f: Int => TraversableOnce[U]): RDD[(Int, U)]
        val flatMapped = pairs.flatMapValues(v => (v to 5))
        println("===>flatMapValues:")
        flatMapped.foreach(println(_))
        
        val keys = pairs.keys
        println("===>keys:")
        keys.foreach(println(_))
        
        val values = pairs.values
        println("===>values:")
        values.foreach(println(_))
        
        val sorted = pairs.sortByKey()
        println("===>sortByKey:")
        sorted.foreach(println(_))
        
        //// Transformations on two pair RDDs ////
        
        val subtByKey = pairs.subtractByKey(other)
        println("===>subtractByKey")
        subtByKey.foreach(println(_))
        
        // by keys with matches only
        val joined = pairs.join(other)
        println("===>join:")
        joined.foreach(println(_))
        
        // by keys with matches only as Option
        val rtOutJoin = pairs.rightOuterJoin(other)
        println("===>rightOuterJoin:")
        rtOutJoin.foreach(println(_))
        
        // by all keys as Option
        val lfOutJoin = pairs.leftOuterJoin(other)
        println("===>leftOuterJoin:")
        lfOutJoin.foreach(println(_))
        
        // RDD[(K, (Iterable[V], Iterable[W]))]
        val coGroup = pairs.cogroup(other)
        println("===>cogroup")
        coGroup.foreach(println(_))        
    }
}
/*




*/