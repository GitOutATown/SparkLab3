package courses.bigdata_spark.week2

import org.apache.spark._
import org.apache.spark.SparkContext._

object flatMap_lab_1 {
    
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setAppName("flatMap_lab_1").setMaster("local")
        val sc = new SparkContext(conf)
        
        // Let's create a new base RDD to work from
        val wordsList = List("cat", "elephant", "rat", "rat", "cat")
        val wordsRDD = sc.parallelize(wordsList, 2)
        
        // using map
        val singularAndPluralWordsRDDMap = wordsRDD.map(w => (w, w + "s"))
        println("words to plural map: " + 
            singularAndPluralWordsRDDMap.collect.mkString(" "))
        
        // using flatMap for the given example doesn't make sense. Something I'm not getting from the Python example.
        // see my other flatMap example for range in oreilly.learningspark.ch4_keyvalue.KeyValue_lab_3
    }

}