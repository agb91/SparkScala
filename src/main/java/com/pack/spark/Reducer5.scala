
package com.pack.spark


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.parser.Parsers


class Reducer5 {
  
   //merge by years and name
  //yearName is index ,0 is value,1 = variationFromJanuaryWeighted, 2 = drawdownPCWeighted, 3 = total 
  //capital of the various products f.e. 10000+20000 = 30000 
  def accumulateMerged (accumulator: Array[Double], toAdd: Array[Double] )
  : Array[Double] =
  {
    var result = Array[Double](-1,-1,-1,-1,-1) //variationFromJanuary-weighted, drawdow now weighted, totalCapital
    var totalVariation = accumulator(1) + toAdd(1) //to divide per total capital
    var totalDrawdown = accumulator(2) + toAdd(2) //to divide per total capital
    var totalCapital = toAdd(3) 
    result(0) = ( accumulator(0)*accumulator(3) ) + ( toAdd(0)*toAdd(3) ) //useless
    result(1) = totalVariation
    result(2) = totalDrawdown
    result(3) = totalCapital
    result(4) = accumulator(4) + toAdd(4) //counter
    result
  }
  
  //merge by years and month
  //yearName is index ,0 is value,1 = variationFromJanuary, 2 = drawdownPC, 3 = capital
   val reducerETFMerged: ( RDD[ (String, Array[Double] ) ]) => RDD[ ( String, Array[Double] ) ] =
     (mappedRDD : RDD[ (String, Array[Double]) ]) => {
    //mappedRDD.foreach(println)
    var result = mappedRDD.reduceByKey( accumulateMerged )  
    result
  }
  
  
}