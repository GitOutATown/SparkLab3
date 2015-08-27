package courses.bigdata_spark.week3

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.util.matching.Regex

object TextAnalysisEntityRes4 {
    
    def main(args: Array[String]): Unit = {
        
        // Context
        val conf = new SparkConf().setAppName("TextAnalEntityRes").setMaster("local")
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
        
        trait RecordContent
        case class ParsedRecord(val id: String, product: (String, String, String)) extends RecordContent
        case class FailedRecord(val line: String) extends RecordContent
        abstract class Record(val record: RecordContent, val flag: Int)
        case class Parsed(override val record: ParsedRecord, override val flag: Int) extends Record(record, flag)
        case class Failed(override val record: FailedRecord, override val flag: Int) extends Record(record, flag)
        // TODO companion objects to simplify type construction.
        
        val dataFilePattern = """^(.+),"(.+)",(.*),(.*),(.*)""".r
        
        def removeQuotes(s: String): String = {
            s.filter(ch => ch != '\"')
        }
        
        def parseDataFileLine(datafileLine: String, pattern: Regex): Record = {
            pattern.findFirstIn(datafileLine) match {
                case None => Failed(FailedRecord(datafileLine), -1)
                case Some(success) =>
                    val pattern(grp1, grp2, grp3, grp4, _) = datafileLine
                    if (grp1 == "\"id\"") Failed(FailedRecord(datafileLine), 0)
                    else {
                        val product = (grp2, grp3, grp4)
                        Parsed(ParsedRecord(removeQuotes(grp1), product), 1)
                    }
               }
        }
        
        def parseData(filePath: String) = {
            sc.textFile(filePath).map(line => parseDataFileLine(line, dataFilePattern))
        }
        
        def loadData(filePath: String): RDD[RecordContent] = {
            val raw = parseData(filePath)
            
            val failed = raw.filter(record => record.flag == -1).map(record => record.record)
            failed.take(10).foreach(line => println("Invalid datafile line: " + (filePath, line)))
            
            val valid = raw.filter(record => record.flag == 1).map(record => record.record).cache
            
            val rawCount = raw.count()
            val failedCount = failed.count()
            val validCount = valid.count()
            
            println(s"$filePath -> Read $rawCount lines, successfully parsed $validCount lines, failed to parse $failedCount lines")
            valid
        }
        
        val googleSmallData = loadData(resource_path + GOOGLE_SMALL)
        val googleData = loadData(resource_path + GOOGLE)
        val amazonSmallData = loadData(resource_path + AMAZON_SMALL)
        val amazonData = loadData(resource_path + AMAZON)
        
    }
    
}






