package com.pack.spark


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.parser.Parsers

class MergerMultipleETF {
  
  // 0 is total variation so capital divided per the initial capital, 
  //1 is capital at the moment splitted in indexes.. sum it, 
    def secondAccumulate (accumulator: Array[Double], toAdd: Array[Double]) : Array[Double] =
  {
    
    var result = Array[Double](0,0,0)
    result(1) = accumulator(1) + toAdd(1) 
    result(2) = accumulator(2) + toAdd(2) 
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
  
  //merge by years and name
  //yearName is index ,0 is value,1 = variationFromJanuaryWeighted, 2 = drawdownPCWeighted, 3 = capital
  def accumulateMerged (accumulator: Array[Double], toAdd: Array[Double] )
  : Array[Double] =
  {
    var result = Array[Double](-1,-1,-1,-1,-1) //variationFromJanuary-weighted, drawdow now weighted, totalCapital
    var totalVariation = accumulator(1) + toAdd(1) //to divide per total capital
    var totalDrawdown = accumulator(2) + toAdd(2) //to divide per total capital
    var totalCapital = accumulator(3) + toAdd(3) 
    result(0) = ( accumulator(0)*accumulator(3) ) + ( toAdd(0)*toAdd(3) ) //useless
    result(1) = totalVariation
    result(2) = totalDrawdown
    result(3) = totalCapital
    result(4) = accumulator(4) + toAdd(4) //counter
    result
  }
  
  //give back for each year variation of the year and capital at the moment
  val secondReducerETF: (RDD[ (String, Array[Double]) ]) => RDD[(String, Array[Double])] =
    (mappedRDD : RDD[ (String, Array[Double]) ]) => 
  {
    var result = mappedRDD.reduceByKey( secondAccumulate )  
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
     
     
   val mergerAll: ( Array[RDD[ (String, Array[Double] ) ]] ) => RDD[ ( String, Array[Double] ) ] =
   (allMappedRDD : Array[RDD[ (String, Array[Double]) ]]) => 
   {
      var result = allMappedRDD(0).filter(f => !f._1.equalsIgnoreCase("discarded")  )
      
      for ( i <- 1 to (allMappedRDD.length - 1) ) {
         result = result.union( allMappedRDD(i) ).filter(h => !h._1.equalsIgnoreCase("discarded") )
      }
      result
   }  
  
}