package lab.actions

import org.apache.spark._
import org.apache.spark.SparkContext._

object takeOrdered_lab_1 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("takeOrdered_lab_1").setMaster("local")
        val sc = new SparkContext(conf)
        
        val list = sc.parallelize(List(5,3,1,2))
        
        val orderedAscending = list.takeOrdered(3)
        
        //val ordering: Ordering[Int] = Ordering.by(i => i * -1)
        //val orderedDecending = list.takeOrdered(3)(ordering)
        val orderedDecending = list.takeOrdered(3)(Ordering.by(i => i * -1))
        
        println("=====> orderedAscending: " + orderedAscending.mkString(" ")) // 1,2,3
        println("=====> orderedDecending: " + orderedDecending.mkString(" ")) // 5,3,2
    }
}