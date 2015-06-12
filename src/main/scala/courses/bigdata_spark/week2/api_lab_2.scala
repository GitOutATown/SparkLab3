package courses.bigdata_spark.week2

import org.apache.spark._
import org.apache.spark.SparkContext._

object api_lab_2 {
    
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setAppName("api_lab_1").setMaster("local")
        val sc = new SparkContext(conf)
        
        // countByValue
        val repetitiveRDD = sc.parallelize(List(1, 2, 3, 1, 2, 3, 1, 2, 1, 2, 3, 3, 3, 4, 5, 4, 6))
        val cntValMap = repetitiveRDD.countByValue()
        println("cntValMap: " + cntValMap)
        
        import scala.collection.immutable.ListMap
        println("cntValMap sort by key: " + ListMap(cntValMap.toSeq.sortBy(_._1):_*))
        println("cntValMap sortWith ascending: " + ListMap(cntValMap.toSeq.sortWith(_._1 < _._1):_*))
        println("cntValMap sortWith decending: " + ListMap(cntValMap.toSeq.sortWith(_._1 > _._1):_*))
    }

}