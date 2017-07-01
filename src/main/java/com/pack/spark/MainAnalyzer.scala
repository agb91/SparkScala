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

import com.pack.spark.Preprocessor

object MainAnalyzer {
      
  def main(args: Array[String]) = {
    
    
    val parser = new Parsers with Serializable
    val preprocess = new Preprocessor with Serializable
    val singleETFAnalyzer = new SingleETFAnalyzer with Serializable
    
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("MainAnalyzer")
      .setMaster("local")
    val sc = new SparkContext(conf)
   
    var beginDate = parser.dateFormatter("1950-01-01")
    var endDate = parser.dateFormatter("2018-01-01")
    
    //STANDART WAY TO ANALYSE:
    
    preprocess.preProcess( "src/main/resources/TotalStockHistory.csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Vangard Total Stock" , beginDate , endDate, parser )
        
        
    preprocess.preProcess( "src/main/resources/TotalBondHistory.csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Vangard Total Bond" , beginDate , endDate, parser )
        
        
    preprocess.preProcess( "src/main/resources/BondGlobalIta(BarclaysGlobalAggregateBond).csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Borsa Italiana Euro Hedged Global Bond" , beginDate , endDate, parser )
      
    preprocess.preProcess( "src/main/resources/BorsaItalianaETFSP500EUR-hedged.csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Borsa Italiana Euro Hedged SP500" , beginDate , endDate, parser )     
    //END    
    
        
    //SPARK WAY TO ANALYSE    
   /* var mappedRDD1 = singleETFAnalyzer.mapperResult( "src/main/resources/TotalStockHistory.csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Vangard Total Stock" , beginDate , endDate, parser ) 
        
    var mappedRDD2 = singleETFAnalyzer.mapperResult( "src/main/resources/TotalBondHistory.csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Vangard Total Bond" , beginDate , endDate, parser ) 
        
    var mappedRDD3 = singleETFAnalyzer.mapperResult( "src/main/resources/PHAU.MI.csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Gold" , beginDate , endDate, parser ) 
        
        
  // until here different RDDs, splitted in months
   
    val merger = new MergerMultipleETF with Serializable
  //now are merged together  
    var merged = merger.mergerAll( Array(mappedRDD1, mappedRDD2, mappedRDD3) )     
        
    merged.foreach(f=> 
      {
        //println("\n first: nome:" + f._1 + ":  variazione" + f._2(0) + "; capitale: " + f._2(1)  )
      }
    )
    //now they are merged in one through a union, still months
    
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

    sc.stop*/ 
  }
}