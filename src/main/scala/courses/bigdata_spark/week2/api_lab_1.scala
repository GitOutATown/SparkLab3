package courses.bigdata_spark.week2

import org.apache.spark._
import org.apache.spark.SparkContext._

object api_lab_1 {
    
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setAppName("api_lab_1").setMaster("local")
        val sc = new SparkContext(conf)
        
        //println("===> sc.version: " + sc.version)
        //sc.parallelize(List(1, 2, 3, 4), 4)
        
        // ---------------------- //
        
        val rangeData = 1 until 10001
        val listData = rangeData.toList
        
        //println("data(0): " + data(0))
        //println("data.length: " + data.length)
        
        val rangeRDD = sc.parallelize(rangeData, 2)
        val listRDD = sc.parallelize(rangeData, 2)
        
        rangeRDD.setName("Morris")
        //println("rangeRDD.toDebugString: " + rangeRDD.toDebugString)
        
        listRDD.setName("Henry")
        //println("listRDD.toDebugString: " + listRDD.toDebugString)
        
        def subt(value: Int) = value - 1
        
        val subtRDD = rangeRDD.map(subt)
        println("subtRDD.toDebugString: " + subtRDD.toDebugString)
        
        //val collected = subtRDD.collect()
        //println(collected.length)
        //println("subtRDD.count: " + subtRDD.count)
        
        def lessThanTen(value: Int): Boolean = value < 10
        val filtered = subtRDD.filter(lessThanTen)
        filtered.collect
        
        // In line, chained, more compact and expressive
        subtRDD.filter(_ < 10).collect
        
        val takenOrdered = filtered.takeOrdered(4)
        val reverseOrdered = filtered.takeOrdered(4)(Ordering.by(i => i * -1))
        val toppermost = filtered.top(4)
        
        /*
        println("filtered.takeOrdered(4): " + takenOrdered.mkString(" "))
        println("filtered.takeOrdered(4) reversed: " + 
            reverseOrdered.mkString(" "))
        println("filtered.top(4): " + toppermost.mkString(" "))
        */
        
        val reduced = filtered.reduce((v1, v2) => v1 + v2)
        //println("reduced: " + reduced)
        
        val sample = rangeRDD.sample(true, .02)
        val collectedSample = sample.collect()
        //println(collectedSample.mkString(" "))
    }

}