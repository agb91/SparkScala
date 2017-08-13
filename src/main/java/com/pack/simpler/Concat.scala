package com.pack.simpler

import org.apache.spark.SparkConf
import com.pack.spark.parser.MyDate
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

class Concat {
  
  val mergerAll: ( Array[RDD[ (String) ]] ) => RDD[ ( String ) ] =
   (allMappedRDD : Array[RDD[ (String) ]]) => 
   {
      var result = allMappedRDD(0)
      
      for ( i <- 1 to (allMappedRDD.length - 1) ) {
         result = result.union( allMappedRDD(i) )
      }
      result
   }  
  
}