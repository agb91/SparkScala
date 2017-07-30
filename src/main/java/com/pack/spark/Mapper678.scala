package com.pack.spark


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.parser.Parsers

class Mapper678 {
  
  // 0 is total variation ever 
  //1 is DD, find the worst ever
  // all is divided by total capital in the map..
    def secondAccumulate (accumulator: Array[Double], toAdd: Array[Double]) : Array[Double] =
  {
    var result = Array[Double](0,0,0,0)
    result(0) = accumulator(0) + toAdd(0)
    if( accumulator(1) > toAdd(1) )
    {
      result(1) = accumulator(1) 
    }
    else
    {
      result(1) = toAdd(1)
    }
    
    if( toAdd(2) < accumulator(2) ) //minyear
    {
      result(2) = toAdd(2)
    }
    else
    {
      result(2) = accumulator(2)
    }
    
    if( toAdd(3) > accumulator(3) ) //maxyear
    {
      result(3) = toAdd(3)
    }
    else
    {
      result(3) = accumulator(3)
    }
    
    result
  }
  
  //1 = totalVariationWeighted, 2 totalDrawdown weighted, 3 totalCApital  
  val secondMapperETF: ( RDD[ (String, Array[Double]) ], Parsers, Double ) => RDD[ (String, Array[Double]) ] =
   ( input: RDD[ (String, Array[Double]) ] , parser : Parsers, tw : Double) =>
  {
    val result = input.map(line=>
      {
        var year = line._1.split("-")(0)
        var month = line._1.split("-")(1)
        if( month.equalsIgnoreCase("12") )
        {
          var tuple = new Array[Double](4)
          tuple(0) = line._2(1) / tw //divided by total weight..
          tuple(1) = line._2(2) / tw //divided by total weight..
          tuple(2) =  parser.parseDouble( year )
          tuple(3) =  parser.parseDouble( year )
          ( "accepted" , tuple )
        }
        else
        {
          ( "discarded" , new Array[Double](4) )
        }
        
      })
      result
  }
  
 
  
  //give back for each year variation of the year and capital at the moment
  val secondReducerETF: (RDD[ (String, Array[Double]) ]) => RDD[(String, Array[Double])] =
    (mappedRDD : RDD[ (String, Array[Double]) ]) => 
  {
    var result = mappedRDD.reduceByKey( secondAccumulate )  
    result
  }
   
     
   
  
}