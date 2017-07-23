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
 
     assert( rdd1.collect().length == 10 )   
     assert( rdd1.collect()(1) == "2/2/2007,1,1,1, 1 , 101" )
     assert( rdd1.collect()(4) == "1/7/2008,1,1,1, 1 , 175" )
  
  }
  
  test( "preprocessor" )
  {
    fixture()
    var rddReader = reader.readCsv( "src/test/resources/basic.csv" , sc )
     
    var rdd1 = preprocess.preProcess( parser.parseDouble("10000"), sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, 
        "dd/mm/yyyy", rddReader )
    assert( rdd1.collect().length == 7 )
    assert( rdd1.collect()(2).equalsIgnoreCase(  "1/7/2008,175.0,175.0,16.666666666666664,16.666666666666664"  ) )
    assert( rdd1.collect()(3).equalsIgnoreCase(  "1/1/2009,120.0,175.0,-31.428571428571427,0.0"  ) )
    assert( rdd1.collect()(4).equalsIgnoreCase(  "1/3/2009,120.0,175.0,0.0,0.0"  ) )
    assert( rdd1.collect()(5).equalsIgnoreCase(  "1/5/2009,240.0,240.0,100.0,100.0"  ) )
    
  }
  
  test( "firstAnalysis" )
  {
    fixture()
    var rddReader = reader.readCsv( "src/test/resources/basic.csv" , sc )
    var rdd1 = preprocess.preProcess( parser.parseDouble("10000"), sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, 
        "dd/mm/yyyy", rddReader )
    
    // it give back an RDD: YearName(index), variationPC from janaury,drawdownPC AT THE MOMENT, for each month
    //, weight
    var mappedRDD1 = singleETFAnalyzer.mapperResult( rdd1 ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, "dd/MM/yyyy" ) 
   
    assert( mappedRDD1.collect().length == 7 )
    assert( mappedRDD1.collect()(6)._1.equalsIgnoreCase("discarded") )
    assert( mappedRDD1.collect()(0)._1.equalsIgnoreCase("discarded") )
    
    assert( mappedRDD1.collect()(2)._2(0) == 175.0 )
    assert( mappedRDD1.collect()(2)._2(1) == 166666.66666666666 )
    assert( mappedRDD1.collect()(2)._2(2) == 0.0 )
    assert( mappedRDD1.collect()(2)._2(3) == 10000.0 )
    assert( mappedRDD1.collect()(2)._2(4) == 1.0 )
    
    assert( mappedRDD1.collect()(3)._2(0) == 120.0 )
    assert( mappedRDD1.collect()(3)._2(1) == 0.0 )
    assert( mappedRDD1.collect()(3)._2(2) == 314285.71428571426 )
    assert( mappedRDD1.collect()(3)._2(3) == 10000.0 )
    assert( mappedRDD1.collect()(3)._2(4) == 1.0 )
    
    assert( mappedRDD1.collect()(4)._2(0) == 120.0 )
    assert( mappedRDD1.collect()(4)._2(1) == 0.0 )
    assert( mappedRDD1.collect()(4)._2(2) == 314285.71428571426 )
    assert( mappedRDD1.collect()(4)._2(3) == 10000.0 )
    assert( mappedRDD1.collect()(4)._2(4) == 1.0 )
    
    assert( mappedRDD1.collect()(5)._2(0) == 240.0 )
    assert( mappedRDD1.collect()(5)._2(1) == 1000000.0 )
    assert( mappedRDD1.collect()(5)._2(2) == 0.0 )
    assert( mappedRDD1.collect()(5)._2(3) == 10000.0 )
    assert( mappedRDD1.collect()(5)._2(4) == 1.0 )
    
        
  }
  
  
  
}