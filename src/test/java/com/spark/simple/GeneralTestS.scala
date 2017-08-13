package com.spark.simple

import collection.mutable.Stack
import com.pack.simpler.MagicWeight

import com.pack.reader._
import org.scalatest._
import com.holdenkarau.spark.testing._


import org.apache.spark.SparkConf
import com.pack.reader._
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.parser.Parsers
import com.pack.spark.parser.MyDate
import com.pack.simpler.PreprocessorSimple
import com.pack.simpler.MapperSimple
import com.pack.simpler.ReducerSimple

class GeneralTestS extends FunSuite with SharedSparkContext {
  
    
  var parser : Parsers = null
  var preprocess : PreprocessorSimple = null
  var mapper : MapperSimple = null 
  var reducer : ReducerSimple = null
  var reader: Reader = null
  var beginDate : MyDate = null
  var endDate : MyDate = null
  var old: MagicWeight = null
  
  
  def fixture() =
  {
     parser = new Parsers with Serializable
     preprocess = new PreprocessorSimple with Serializable
     reader = new Reader with Serializable
     mapper = new MapperSimple with Serializable
     reducer = new ReducerSimple with Serializable
    
     beginDate = parser.dateFormatter("2007-01-01", "yyyy-MM-dd" )
     endDate = parser.dateFormatter("2016-01-01" , "yyyy-MM-dd")
     
     var old = new MagicWeight with Serializable
  }
    
  
}