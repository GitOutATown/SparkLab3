package oreilly.learningspark.ch4_keyvalue

import org.apache.spark._
import org.apache.spark.SparkContext._

object mapPartitions_lab_1 {
    // http://homepage.cs.latrobe.edu.au/zhe/ZhenHeSparkRDDAPIExamples.html#foldByKey
        
    def main(args: Array[String]): Unit = {
                
        val conf = new SparkConf().setAppName("foldByKey_lab_1").setMaster("local")
        val sc = new SparkContext(conf)
                
        /* Zhen He says:
         * This is a specialized map that is called only once for each partition. The entire content of the respective partitions is available as a sequential stream of values via the input argument (Iterarator[T]). The custom function must return yet another Iterator[U]. The combined result iterators are automatically converted into a new RDD. 
         * Please note, that the tuples (3,4) and (6,7) are missing from the 
         * following result due to the partitioning we chose.
         */
        // def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U]
        
        ///// Example 1 ////////////////////////
        
        val rddInt = sc.parallelize(1 to 9, 3)
        
        def myfunc[T](iter: Iterator[T]) : Iterator[(T, T)] = {
            var res = List[(T, T)]()
            var pre = iter.next
            while (iter.hasNext)
            {
                val cur = iter.next;
                res .::= (pre, cur)
                pre = cur;
            }
            res.iterator
        }
            
        println("Example 1: " + rddInt.mapPartitions(myfunc).collect.mkString(", "))
        // res0: Array[(Int, Int)] = Array((2,3), (1,2), (5,6), (4,5), (8,9), (7,8))
    
        ///// Example 2 /////////////////////
        
        val rddInt2 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)
        
        def myfunc2(iter: Iterator[Int]) : Iterator[Int] = {
            var res = List[Int]()
            while (iter.hasNext) {
                val cur = iter.next;
                res = res ::: List.fill(scala.util.Random.nextInt(10))(cur) // Adds the elements of a given list in front of this list.
            }
            res.iterator
        }
        
        println("Example 2: " + rddInt2.mapPartitions(myfunc2).collect.mkString(", "))
    
        /* some of the number are not outputted at all. This is because the 
         * random number generated for it is zero.
         */
        //res8: Array[Int] = Array(1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 5, 7, 7, 7, 9, 9, 10)
    
        ///// Example 3 ///////////////////
        // The above program can also be written using flatMap as follows.
        
        //val rddInt3  = sc.parallelize(1 to 10, 3)
        val flatMapVersion = rddInt2.flatMap(List.fill(scala.util.Random.nextInt(10))(_))
        println("Example 3: " + flatMapVersion.collect.mkString(", "))
    
        ///// mapPartitionWithIndex ////////////////
        
        /* Similar to mapPartitions, but takes two parameters. The first parameter is the index of the partition and the second is an iterator through all the items within this partition. The output is an iterator containing the list of items after applying whatever transformation the function encodes.
         */
        // def mapPartitionsWithIndex[U: ClassTag](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false): RDD[U]
        def myfunc3(index: Int, iter: Iterator[Int]) : Iterator[String] = {
            iter.toList.map(x => index + "," + x).iterator
        }
        
        val mapPartIndex = rddInt2.mapPartitionsWithIndex(myfunc3)
        println("mapPartitionsWithIndex: " + mapPartIndex.collect().mkString(", "))
        //res10: Array[String] = Array(0,1, 0,2, 0,3, 1,4, 1,5, 1,6, 2,7, 2,8, 2,9, 2,10)
    
    } // end main
}







