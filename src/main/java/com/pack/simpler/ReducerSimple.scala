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
  
  /*tuple(0) = variationW1
    tuple(1) = worstDDW1
    tuple(2) = magicWeight1
    tuple(3) = variationW2
    tuple(4) = worstDDW2
    tuple(5) = magicWeight2
    tuple(6) = variationW3
    tuple(7) = worstDDW3
    tuple(8) = magicWeight3
      */
   def accumulate (accumulator: Array[Any], toAdd: Array[Any] )
  : Array[Any] =
  {
    //println("---> accum = " + accumulator.mkString("-"))
    //println("---> toAdd = " + toAdd.mkString("-"))
    var result = Array[Any](0.0 , 0.0 , new MagicWeight(), 0.0 , 0.0 , new MagicWeight(), 0.0 , 0.0 , new MagicWeight()) 
    var totalVariation1 = thisParser.parseDouble( accumulator(0) ) + thisParser.parseDouble( toAdd(0) )
    var totalVariation2 = thisParser.parseDouble( accumulator(3) ) + thisParser.parseDouble( toAdd(3) )
    var totalVariation3 = thisParser.parseDouble( accumulator(6) ) + thisParser.parseDouble( toAdd(6) )
    var worstDD1 = 0.0
    if( thisParser.parseDouble( accumulator(1) ) < thisParser.parseDouble( toAdd(1) ) )
    {
      worstDD1 = thisParser.parseDouble( toAdd(1) ) 
    }
    else
    {
      worstDD1 = thisParser.parseDouble( accumulator(1) )
    }
    var worstDD2 = 0.0
    if( thisParser.parseDouble( accumulator(4) ) < thisParser.parseDouble( toAdd(4) ) )
    {
      worstDD2 = thisParser.parseDouble( toAdd(4) ) 
    }
    else
    {
      worstDD2 = thisParser.parseDouble( accumulator(4) )
    }
    var worstDD3 = 0.0
    if( thisParser.parseDouble( accumulator(7) ) < thisParser.parseDouble( toAdd(7) ) )
    {
      worstDD3 = thisParser.parseDouble( toAdd(7) ) 
    }
    else
    {
      worstDD3 = thisParser.parseDouble( accumulator(7) )
    }
    
    result(0) = totalVariation1
    result(1) = worstDD1
    result(3) = toAdd(2)
    result(4) = totalVariation2
    result(5) = worstDD2
    result(6) = toAdd(5)
    result(7) = totalVariation3
    result(8) = worstDD3
    result(9) = toAdd(8)
    return result
  }
  
   val reduce: ( RDD[ (String, Array[Any] ) ]) => RDD[ ( String, Array[Any] ) ] =
     (mappedRDD : RDD[ (String, Array[Any]) ]) => {
    var result = mappedRDD.reduceByKey( accumulate ) 
    result
  }
  
 
  
}