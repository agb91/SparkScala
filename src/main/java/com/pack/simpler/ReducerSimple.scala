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
 			tuple(0) = variationW1
      tuple(1) = worstDDW1 //stock
      tuple(2) = worstDDW2 //bond
      tuple(3) = worstDDW3 //other anti-cyclic stabilizer
  */
   def accumulate (accumulator: Array[Double], toAdd: Array[Double] )
  : Array[Double] =
  {
    var result = Array[Double](0.0 , 0.0 , 0.0, 0.0 , 0.0) 
    var totalVariation = accumulator(0) + toAdd(0)
    var worstDD1 = 0.0 //stock
    var worstDD2 = 0.0 //bond
    var worstDD3 = 0.0 //other
    if( thisParser.parseDouble( accumulator(1) ) < thisParser.parseDouble( toAdd(1) ) ) //the worst DD1
    {
      worstDD1 = thisParser.parseDouble( toAdd(1) ) 
    }
    else
    {
      worstDD1 = thisParser.parseDouble( accumulator(1) )
    }
    
    if( thisParser.parseDouble( accumulator(2) ) < thisParser.parseDouble( toAdd(2) ) ) // the worst DD2
    {
      worstDD2 = thisParser.parseDouble( toAdd(2) ) 
    }
    else
    {
      worstDD2 = thisParser.parseDouble( accumulator(2) )
    }
    
    if( thisParser.parseDouble( accumulator(3) ) < thisParser.parseDouble( toAdd(3) ) ) // the worst DD3
    {
      worstDD3 = thisParser.parseDouble( toAdd(3) ) 
    }
    else
    {
      worstDD3 = thisParser.parseDouble( accumulator(3) )
    }
    
    result(0) = totalVariation
    result(1) = worstDD1
    result(2) = worstDD2
    result(3) = worstDD3
    var variationPoints: Double = totalVariation
    
    var DDpoints : Double = 0.0
    //println("worst DD summed: " + (worstDD1 + worstDD2) )
    var worstDDT = worstDD1 + worstDD2 + worstDD3
    if(worstDDT > 40)
    {
      DDpoints = -1000000.0
    }
    else
    {
      if(worstDDT > 25)
      {
        DDpoints = - (worstDDT * 2)
      }
      else
      {
        DDpoints = - (worstDDT)
      }
    }
    
    result(4) = variationPoints + DDpoints //vote
    return result
  }
  
  
  //variation totalPC, worsDDPC, vote
   val reduce: ( RDD[ (String, Array[Double] ) ]) => ( RDD[ (String, Array[Double] ) ]) =
     (mappedRDD : RDD[ (String, Array[Double]) ]) => {
       
    var r = mappedRDD.reduceByKey( accumulate ) 
    
    r
    
    
   }
  
 
  
}