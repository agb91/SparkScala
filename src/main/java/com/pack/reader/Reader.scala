package com.pack.reader


import org.apache.spark.SparkConf
import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.parser.Parsers
import com.pack.spark.parser.MyDate

class Reader {
  
   def readCsv( path: String , sc: SparkContext ): RDD[(String)] =
   {
       val test = sc.textFile( path )
       test
   }
  
}