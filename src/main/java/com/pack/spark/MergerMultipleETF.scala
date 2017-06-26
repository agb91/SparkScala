package com.pack.spark


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.Parsers

class MergerMultipleETF {
  
    def secondAccumulate (accumulator: Array[Double], toAdd: Array[Double]) : Array[Double] =
  {
    
    var result = Array[Double](0,0)
    result(0) = accumulator(0) + toAdd(0)
    result(1) = accumulator(1) + toAdd(1)
    result
  }
  
  val secondMapperETF: ( RDD[ (String, Array[Double]) ]) => RDD[ (String, Array[Double]) ] =
   ( input: RDD[ (String, Array[Double]) ]) =>
  {
    val result = input.map(line=>
      {
        var name = line._1.split("-")(0)
        
        var tuple = new Array[Double](2)
        tuple(0) = line._2(0)
        tuple(1) = line._2(1)
        
        ( name , tuple )
      })
      result
  }
  
  
  def accumulateMerged (accumulator: Array[Double], toAdd: Array[Double] )
  : Array[Double] =
  {
    var result = Array[Double](0,0)
    result(0) = accumulator(0) + toAdd(0)
    result(1) = toAdd(1)
    //println( "variazione questa: " + toAdd(0) + "; finora: " + accumulator(0) + "; quindi risulta: " + result(0) )
 
    result
  }
  
  val secondReducerETF: (RDD[ (String, Array[Double]) ]) => RDD[(String, Array[Double])] =
    (mappedRDD : RDD[ (String, Array[Double]) ]) => 
  {
    var result = mappedRDD.reduceByKey( secondAccumulate )  
    result
  }
  
   val reducerETFMerged: ( RDD[ (String, Array[Double] ) ]) => RDD[ ( String, Array[Double] ) ] =
     (mappedRDD : RDD[ (String, Array[Double]) ]) => {
    //mappedRDD.foreach(println)
    var result = mappedRDD.reduceByKey( accumulateMerged )  
    result
  }
  
  
}