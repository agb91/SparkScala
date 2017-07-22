package com.pack.spark


import collection.mutable.Stack
import com.pack.reader._
import org.scalatest._
import com.holdenkarau.spark.testing._



import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.parser.Parsers
import com.pack.spark.parser.MyDate
import com.pack.spark.SingleETFAnalyzer
import com.pack.spark.MergerMultipleETF
import com.pack.spark.Preprocessor

class GeneralTest extends FunSuite with SharedSparkContext {
  
    
  var parser : Parsers = null
  var preprocess : Preprocessor = null
  var reader: Reader = null
  var singleETFAnalyzer: SingleETFAnalyzer = null
  var beginDate : MyDate = null
  var endDate : MyDate = null
  
  
  def fixture() =
  {
     parser = new Parsers with Serializable
     preprocess = new Preprocessor with Serializable
     singleETFAnalyzer = new SingleETFAnalyzer with Serializable
     reader = new Reader with Serializable
    
     beginDate = parser.dateFormatter("2007-01-01", "yyyy-MM-dd" )
     endDate = parser.dateFormatter("2016-01-01" , "yyyy-MM-dd")
     
  }
    
  
}