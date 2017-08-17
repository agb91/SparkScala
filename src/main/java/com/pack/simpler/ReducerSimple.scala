package com.pack.simpler

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.parser.Parsers

class ReducerSimple {
 
  
  var thisParser : Parsers = new Parsers with Serializable
  
  /*
   *      tuple(0) = variationW1
          tuple(1) = worstDDW1
      */
   def accumulate (accumulator: Array[Double], toAdd: Array[Double] )
  : Array[Double] =
  {
    var result = Array[Double](0.0 , 0.0 , 0.0 ) 
    var totalVariation = accumulator(0) + toAdd(0)
    var worstDD = 0.0
    if( thisParser.parseDouble( accumulator(1) ) < thisParser.parseDouble( toAdd(1) ) )
    {
      worstDD = thisParser.parseDouble( toAdd(1) ) 
    }
    else
    {
      worstDD = thisParser.parseDouble( accumulator(1) )
    }
    result(0) = totalVariation
    result(1) = worstDD
    var variationPoints: Double = totalVariation
    
    var DDpoints : Double = 0.0
    println("worst DD: " + worstDD)
    worstDD = worstDD*2
    if(worstDD > 40)
    {
      DDpoints = -1000000.0
    }
    else
    {
      if(worstDD > 25)
      {
        DDpoints = - (worstDD * 2)
      }
      else
      {
        DDpoints = - (worstDD)
      }
    }
    
    result(2) = variationPoints + DDpoints //vote
    return result
  }
  
  
  //variation totalPC, worsDDPC, vote
   val reduce: ( RDD[ (String, Array[Double] ) ]) => ( RDD[ (String, Array[Double] ) ]) =
     (mappedRDD : RDD[ (String, Array[Double]) ]) => {
       
    var r = mappedRDD.reduceByKey( accumulate ) 
    
    r
    
    
   }
  
 
  
}