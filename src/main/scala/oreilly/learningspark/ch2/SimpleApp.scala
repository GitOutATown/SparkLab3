package oreilly.learningspark.ch2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf

object SimpleApp {
    
    def main(args: Array[String]): Unit = {
        val filePath = "resources/README_sample_text.md"
        val conf = new SparkConf().setAppName("SimpleApp").setMaster("local")
        val sc = new SparkContext(conf)
        val textData = sc.textFile(filePath, 2).cache()
        val countALines = textData.filter { line => line.contains("a") }.count()
        val countZLines = textData.filter { line => line.contains("z") }.count()
        println("Lines with a: %s, Lines with z: %s".format(countALines, countZLines))
    }
}