package com.pack.spark


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.parser.Parsers
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
   
    var beginDate = parser.dateFormatter("1950-01-01", "yyyy-MM-dd" )
    var endDate = parser.dateFormatter("2018-01-01" , "yyyy-MM-dd")
    
    //STANDART WAY TO ANALYSE:
    
    var rdd1 = preprocess.preProcess( "src/main/resources/BorsaItalianaETFSP500EUR-hedged.csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, "MM/dd/yyyy" )
        
    var rdd2 = preprocess.preProcess( "src/main/resources/BondGlobalIta(BarclaysGlobalAggregateBond).csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Borsa Italiana Euro Hedged Global Bond" , beginDate , endDate, parser, "yyyy-MM-dd" )
          
    //SPARK WAY TO ANALYSE    
    var mappedRDD1 = singleETFAnalyzer.mapperResult( rdd1 ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, "dd/MM/yyyy" ) 
        
    var mappedRDD2 = singleETFAnalyzer.mapperResult( rdd2 ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Borsa Italiana Euro Hedged Global Bond" , beginDate , endDate, parser, "dd/MM/yyyy" ) 
        
    var arrayMapped = Array( mappedRDD1 , mappedRDD2 )    
  // until here different RDDs, splitted in months
   
    val merger = new MergerMultipleETF with Serializable
  //now are merged together  
    
    var merged = merger.mergerAll( arrayMapped )     
    /*merged.foreach(f=> 
      {
        println("\n first: nome:" + f._1 + ":  variazione" + f._2(0) + "; capitale: " + f._2(1) + 
            "; drawdawn ora: " + f._2(2)  )
      }
    )*/
    //now they are merged in one through a union, still months
    
    var reducedRDD = merger.reducerETFMerged(merged)    
        
    /*reducedRDD.foreach( f => 
        {
          println("\n anno-nome:" + f._1 + ":  variazione" + f._2(0) + "; capitale: " + f._2(1) 
              + "; drawdown ora: "  + f._2(2) )
        } )  
      */  
        //NOW ARE YEARS!, dividen in bond e stock
        
    var secondMapped = merger.secondMapperETF(reducedRDD)
    /*secondMapped.foreach(f => 
        {
          println("id:" + f._1 + ":  variazione : " + f._2(0) + "; capital : " + f._2(1) +
              "; drawdawn ora: " + f._2(2) )
        } )
    */
    var finalSum = merger.secondReducerETF(secondMapped)
            
    finalSum.foreach(f => 
        {
          if(f._1.equalsIgnoreCase( endDate.getYear.toString() ) || f._1.equalsIgnoreCase( "2017" ) )
          {
            println("id:" + f._1 + "; capital : " + f._2(1)  )
          }
        } )
        
        TROVA MAX DRAWDOWN

    sc.stop 
  }
}