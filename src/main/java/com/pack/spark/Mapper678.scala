package com.pack.spark


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.parser.Parsers

class Mapper678 {
  
  // 0 is total variation so capital divided per the initial capital, 
  //1 is capital at the moment splitted in indexes.. sum it, 
    def secondAccumulate (accumulator: Array[Double], toAdd: Array[Double]) : Array[Double] =
  {
    var result = Array[Double](0,0)
    result(0) = accumulator(0) + toAdd(0) 
    result(1) = accumulator(1) + toAdd(1) 
    result
  }
  
  //1 = totalVariationWeighted, 2 totalDrawdown weighted, 3 totalCApital  
  val secondMapperETF: ( RDD[ (String, Array[Double]) ]) => RDD[ (String, Array[Double]) ] =
   ( input: RDD[ (String, Array[Double]) ]) =>
  {
    val result = input.map(line=>
      {
        var name = line._1.split("-")(0)
        var month = line._1.split("-")(1)
        if( month.equalsIgnoreCase("12") )
        {
          var tuple = new Array[Double](2)
          tuple(0) = line._2(1)/line._2(3)
          tuple(1) = line._2(2)/line._2(3)
          ( name , tuple )
        }
        else
        {
          ( "discarded" , new Array[Double](2) )
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