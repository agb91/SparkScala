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
import com.pack.spark.ConcatWeightMap234
import com.pack.spark.Mapper678
import com.pack.spark.Preprocessor

class GeneralTest extends FunSuite with SharedSparkContext {
  
    
  var parser : Parsers = null
  var preprocess : Preprocessor = null
  var reader: Reader = null
  var concatWeightMap234: ConcatWeightMap234 = null
  var beginDate : MyDate = null
  var endDate : MyDate = null
  var mapper678 : Mapper678 = null
  var reducer5 : Reducer5 = null
    
  
  
  def fixture() =
  {
     parser = new Parsers with Serializable
     preprocess = new Preprocessor with Serializable
     concatWeightMap234 = new ConcatWeightMap234 with Serializable
     reader = new Reader with Serializable
     mapper678 = new Mapper678 with Serializable
     reducer5 = new Reducer5 with Serializable
    
     beginDate = parser.dateFormatter("2007-01-01", "yyyy-MM-dd" )
     endDate = parser.dateFormatter("2016-01-01" , "yyyy-MM-dd")
     
  }
    
  
}