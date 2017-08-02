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
  
  //0 = variation PC w, 1 = DD worst PC w, 2 date worst DD
   def accumulate (accumulator: Array[Any], toAdd: Array[Any] )
  : Array[Any] =
  {
    //println("---> accum = " + accumulator.mkString("-"))
    //println("---> toAdd = " + toAdd.mkString("-"))
    var result = Array[Any](0.0 , 0.0 , "-") 
    var totalVariation = thisParser.parseDouble( accumulator(0) ) + thisParser.parseDouble( toAdd(0) )
    var worstDD = 0.0
    var worstDate = "-"
    if( thisParser.parseDouble( accumulator(1) ) < thisParser.parseDouble( toAdd(1) ) )
    {
      worstDD = thisParser.parseDouble( toAdd(1) ) 
      worstDate = toAdd(2).toString()
    }
    else
    {
      worstDD = thisParser.parseDouble( accumulator(1) )
      worstDate = accumulator(2).toString()
    }
    
    result(0) = totalVariation
    result(1) = worstDD
    result(2) = worstDate
    result
  }
  
   val reduce: ( RDD[ (String, Array[Any] ) ]) => RDD[ ( String, Array[Any] ) ] =
     (mappedRDD : RDD[ (String, Array[Any]) ]) => {
    var result = mappedRDD.reduceByKey( accumulate )  
    result
  }
  
 
  
}