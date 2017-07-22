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



class PreprocessorTest extends GeneralTest {
  
  test("reader") {
     fixture()
     var rdd1 = reader.readCsv( "src/test/resources/basic.csv" , sc )
 
     assert( rdd1.collect().length == 6 )   
     assert( rdd1.collect()(1) == "2/2/2007,1,1,1, 1 , 101" )
     assert( rdd1.collect()(4) == "1/12/2009,1,1,1, 1, 120" )
  
  }
  
  test( "preprocessor" )
  {
    fixture()
    var rddReader = reader.readCsv( "src/test/resources/basic.csv" , sc )
     
    var rdd1 = preprocess.preProcess( parser.parseDouble("10000"), sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, 
        "dd/mm/yyyy", rddReader )
        
    assert( rdd1.collect().length == 5 )
  }
  
  
}