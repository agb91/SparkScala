package com.pack.spark


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.Parsers
import com.pack.spark.SingleETFAnalyzer

object MainAnalyzer {
  
  def secondAccumulate (accumulator: Array[Double], toAdd: Array[Double]) : Array[Double] =
  {
    
    var result = Array[Double](0,0)
    result(0) = accumulator(0) + toAdd(0)
    result(1) = accumulator(1) + toAdd(1)
    result
  }
  
  def secondMapperETF( input: RDD[ (String, Array[Double]) ]): RDD[ (String, Array[Double]) ] =
  {
    return input.map(line=>
      {
        var name = line._1.split("-")(0)
        
        var tuple = new Array[Double](2)
        tuple(0) = line._2(0)
        tuple(1) = line._2(1)
        
        ( name , tuple )
      })
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
  
  def secondReducerETF(mappedRDD : RDD[ (String, Array[Double]) ]) : RDD[(String, Array[Double])] = 
  {
    var result = mappedRDD.reduceByKey( secondAccumulate )  
    result
  }
  
    def reducerETFMerged(mappedRDD : RDD[ (String, Array[Double]) ]) : RDD[(String, Array[Double])] =
  {
    //mappedRDD.foreach(println)
    var result = mappedRDD.reduceByKey( accumulateMerged )  
    result
  }
  
  
  
    
  def main(args: Array[String]) = {

    val parser = new Parsers with Serializable
    val singleETFAnalyzer = new SingleETFAnalyzer with Serializable
    
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("MainAnalyzer")
      .setMaster("local")
    val sc = new SparkContext(conf)
   
    var beginDate = parser.dateFormatter("1990-01-01")
    var endDate = parser.dateFormatter("2018-01-01")
    
    var mappedRDD1 = singleETFAnalyzer.mapperResult( "src/main/resources/TotalStockHistory.csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Vangard Total Stock" , beginDate , endDate, parser ) 
        
    var mappedRDD2 = singleETFAnalyzer.mapperResult( "src/main/resources/TotalBondHistory.csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Vangard Total Bond" , beginDate , endDate, parser ) 
        
        
        
    var merged = mappedRDD1.union(mappedRDD2).filter(f => !f._1.equalsIgnoreCase("discarded")) 

    
    var reducedRDD = reducerETFMerged(merged)    
        
    reducedRDD.collect().foreach( f => 
        {
          println("\n anno-nome:" + f._1 + ":  variazione" + f._2(0) + "; capitale: " + f._2(1)  )
        } )  
        
    /*var secondMapped = secondMapperETF(reducedRDD)
    secondMapped.foreach(f => 
        {
          //println("id:" + f._1 + ":  variazione : " + f._2(0) + "; capital : " + f._2(1)  )
        } )
    
    var finalSum = secondReducerETF(secondMapped)
            
    finalSum.foreach(f => 
        {
          println("id:" + f._1 + ":  variazione : " + f._2(0) + "; capital : " + f._2(1)  )
        } )
    */

    sc.stop 
  }
}