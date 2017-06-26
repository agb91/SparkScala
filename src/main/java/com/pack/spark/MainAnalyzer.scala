package com.pack.spark


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.Parsers
import com.pack.spark.SingleETFAnalyzer
import com.pack.spark.MergerMultipleETF

object MainAnalyzer {
      
  def main(args: Array[String]) = {

    val parser = new Parsers with Serializable
    val singleETFAnalyzer = new SingleETFAnalyzer with Serializable
    
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("MainAnalyzer")
      .setMaster("local")
    val sc = new SparkContext(conf)
   
    var beginDate = parser.dateFormatter("2007-01-01")
    var endDate = parser.dateFormatter("2018-01-01")
    
    var mappedRDD1 = singleETFAnalyzer.mapperResult( "src/main/resources/TotalStockHistory.csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Vangard Total Stock" , beginDate , endDate, parser ) 
        
    var mappedRDD2 = singleETFAnalyzer.mapperResult( "src/main/resources/TotalBondHistory.csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Vangard Total Bond" , beginDate , endDate, parser ) 
        
        
  // until here 2 different RDDs, splitted in months
        
        
    var merged = mappedRDD1.union(mappedRDD2).filter(f => !f._1.equalsIgnoreCase("discarded")) 
    merged.foreach(f=> 
      {
        //println("\n first: nome:" + f._1 + ":  variazione" + f._2(0) + "; capitale: " + f._2(1)  )
      }
    )
    //now they are merged in one through a union, still months
    
    val merger = new MergerMultipleETF with Serializable
    
    var reducedRDD = merger.reducerETFMerged(merged)    
        
    reducedRDD.foreach( f => 
        {
          //println("\n anno-nome:" + f._1 + ":  variazione" + f._2(0) + "; capitale: " + f._2(1)  )
        } )  
        
        //NOW ARE YEARS!, dividen in bond e stock
        
    var secondMapped = merger.secondMapperETF(reducedRDD)
    secondMapped.foreach(f => 
        {
          //println("id:" + f._1 + ":  variazione : " + f._2(0) + "; capital : " + f._2(1)  )
        } )
    
    var finalSum = merger.secondReducerETF(secondMapped)
            
    finalSum.foreach(f => 
        {
          if(f._1.equalsIgnoreCase( endDate.getYear.toString() ) || f._1.equalsIgnoreCase( "2017" ) )
          {
            println("id:" + f._1 + "; capital : " + f._2(1)  )
          }
        } )

    sc.stop 
  }
}