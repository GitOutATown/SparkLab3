package courses.bigdata_spark.week3

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.util.matching.Regex

object TextAnalysisEntityRes6 {
    
    // Context
    val conf = new SparkConf().setAppName("TextAnalysisEntityRes").setMaster("local")
    val sc = new SparkContext(conf)
    
    // Path
    val resource_path = "src/main/scala/courses/bigdata_spark/data/er/"
    
    // Data files
    val GOOGLE = "Google.csv"   
    val GOOGLE_SMALL = "Google_small.csv"
    val AMAZON = "Amazon.csv"
    val AMAZON_SMALL = "Amazon_small.csv"
    val GOLD_STANDARD = "Amazon_Google_perfectMapping.csv"
    val STOPWORDS = "stopwords.txt"
    
    // ------ Parsing ------------- //
        
    val stopwords = sc.textFile(resource_path + "stopwords.txt")
            .filter(w => w != "").collect.toSet
    
    val dataFilePattern = """^(.+),"(.+)",(.*),(.*),(.*)""".r
        
    def removeQuotes(s: String): String = {
        s.filter(ch => ch != '\"')
    }
    
    type FailedRecord = (String, Int)
    type ParsedRecord = (String, String)
    
    def parseDataFileLine(datafileLine: String, pattern: Regex)
        :Either[FailedRecord, ParsedRecord] = {
        pattern.findFirstIn(datafileLine) match {
            case None => Left(datafileLine, -1)
            case Some(success) =>
                val pattern(grp1, grp2, grp3, grp4, _) = datafileLine
                if (grp1 == "\"id\"") Left(datafileLine, 0)
                else {
                    val product = grp2 + " " + grp3 + " " + grp4
                    Right(removeQuotes(grp1), product)
                }
           }
    }
        
    def parseData(filePath: String) = {
        sc.textFile(filePath).map(line => parseDataFileLine(line, dataFilePattern))
    }
    
    def loadData(filePath: String) = {
        val raw = parseData(filePath)
        
        val failed = raw.filter(record => record match{
            case Left(record) => true
            case Right(record) => false
        })
        failed.take(10).foreach(line => 
            println("Invalid datafile line: " + (filePath, line)))
        
        val valid = raw.filter(record => record match{
            case Right(record) => true
            case Left(record) => false
        }).map(record => record.right.get).cache
        
        val rawCount = raw.count()
        val failedCount = failed.count()
        val validCount = valid.count()
        
        println(s"$filePath -> Read $rawCount lines, successfully parsed $validCount lines, failed to parse $failedCount lines")
        valid
    }
    
    // ------ Bag of words --------- //
    
    val split_regex = """\W+"""

    def tokenize(str: String): List[String] = {
        //str.split(split_regex)
        str.split(split_regex).filter(s => !stopwords.contains(s)).toList
    }
    
    // I'd rather use this than tokenize
    def preprocess(str: String): List[String] = {
        val splits = str.split("""[ !?,.:;()"]+""").map(_.toLowerCase)
        splits.map { s => s.replaceAll("(?m)^[ \t]*\r?\n", "") }
        .filter(s => !stopwords.contains(s)).toList
    }
    
    def countTokens(vendorRDD: RDD[(String, List[String])]): Int = {
        vendorRDD.map{ case (id, tokens) => tokens.length }.reduce(_ + _)
    }
    
    def findBiggestRecord(vendorRDD: RDD[(String, List[String])])
        :List[(String, List[String])] = {
        vendorRDD.map{ case (id, tokens) => (tokens.length, (id, tokens))}
        .sortByKey(false).map{ case (length, (id, tokens)) => (id, tokens) }
        .collect.toList
    }
    
    // ------ main --------------- //
    
    def main(args: Array[String]): Unit = {
        
        // Load data
        val googleSmallData = loadData(resource_path + GOOGLE_SMALL)
        val googleData = loadData(resource_path + GOOGLE)
        val amazonSmallData = loadData(resource_path + AMAZON_SMALL)
        val amazonData = loadData(resource_path + AMAZON)
        
        // Examine
        googleSmallData.take(3) foreach{
            record => println(record._1 + ": " + record._2) 
        }
        println
        googleData.take(3) foreach{
            record => println(record._1 + ": " + record._2) 
        }
        println
        amazonSmallData.take(3) foreach{
            record => println(record._1 + ": " + record._2) 
        }
        println
        amazonData.take(3) foreach{
            record => println(record._1 + ": " + record._2) 
        }
        
        // Bag of words
        
        val amazonRecToToken = amazonSmallData.map {
            //case (id, product) => (id, preprocess(product))
            case (id, product) => (id, tokenize(product))
        }
        
        val googleRecToToken = googleSmallData.map {
            //case (id, product) => (id, preprocess(product))
            case (id, product) => (id, tokenize(product))
        }
        
        // diagnostic
        val totalTokens = countTokens(amazonRecToToken) + countTokens(googleRecToToken)
        println("totalTokens: " + totalTokens)
        
        val biggestRecordAmazon = findBiggestRecord(amazonRecToToken).head
        println("biggestRecordAmazon: " + biggestRecordAmazon._1 + ": " +
                biggestRecordAmazon._2.length)
        
    } // end main
    
}






