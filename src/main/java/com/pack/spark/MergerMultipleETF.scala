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
  
    
    //actually id does nothing: it simply modifies the name in order to merge the index no matter the years doing
    // an historical result
  val secondMapperETF: ( RDD[ (String, Array[Double]) ]) => RDD[ (String, Array[Double]) ] =
   ( input: RDD[ (String, Array[Double]) ]) =>
  {
    val result = input.map(line=>
      {
        var name = line._1.split("-")(0)
        
        var tuple = new Array[Double](3)
        tuple(0) = line._2(0)
        tuple(1) = line._2(1)
        tuple(2) = line._2(2)
        ( name , tuple )
      })
      result
  }
  
  //merge by years and name
  //yearName is index ,0 is variation to cumulate, 1 is capital to copy, 2 is drawdown to find worst
  def accumulateMerged (accumulator: Array[Double], toAdd: Array[Double] )
  : Array[Double] =
  {
    var result = Array[Double](0,0,0)
    result(0) = accumulator(0) + toAdd(0)
    result(1) = toAdd(1)
    var maxDD = 0.0
    if(toAdd(2) > accumulator(2))
    {
      maxDD = toAdd(2)
    }
    else
    {
      maxDD = accumulator(2)
    }
    result(2) = maxDD
    //println( "variazione questa: " + toAdd(0) + "; finora: " + accumulator(0) + "; quindi risulta: " + result(0) )
 
    result
  }
  
  //give back for each year variation of the year and capital at the moment
  val secondReducerETF: (RDD[ (String, Array[Double]) ]) => RDD[(String, Array[Double])] =
    (mappedRDD : RDD[ (String, Array[Double]) ]) => 
  {
    var result = mappedRDD.reduceByKey( secondAccumulate )  
    result
  }
  
  //merge by years and name
  //yearName is index ,0 is variation to cumulate, 1 is capital to copy, 2 is drawdown to find worst
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
      for ( x <- allMappedRDD ) {
         result = result.union(x).filter(h => !h._1.equalsIgnoreCase("discarded") )
      }
      result
   }  
  
}