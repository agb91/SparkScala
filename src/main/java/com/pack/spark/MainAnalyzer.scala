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
  
  def printerPreprocessor( toPrint: RDD[(String)] )
  {
    toPrint.foreach(f => 
      {   
        println("proprocessed: the word is: " + f) 
      }
      )
  }
      
  def printerMapper( toPrint: RDD[(String, Array[Double])] )
  {
    toPrint.foreach(f => 
      {   
        println("name: " + f._1 + ":  variazione" + f._2(0) + "; capitale: " + f._2(1) + 
            "; drawdawnPC ora: " + f._2(2)  ) 
      }
      )
  }
  
  def printerReducedFirst( toPrint: RDD[(String, Array[Double])] )
  {
    toPrint.foreach(f => 
      {   
        println("first reducer: name: " + f._1 + ":  variazione" + f._2(0) + "; capitale: " + f._2(1) + 
            "; drawdawnPC ora: " + f._2(2)  ) 
      }
      )
  }
  
  
  def main(args: Array[String]) = {
    
    val parser = new Parsers with Serializable
    val preprocess = new Preprocessor with Serializable
    val singleETFAnalyzer = new SingleETFAnalyzer with Serializable
    
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("MainAnalyzer")
      .setMaster("local")
    val sc = new SparkContext(conf)
   
    var beginDate = parser.dateFormatter("2006-01-01", "yyyy-MM-dd" )
    var endDate = parser.dateFormatter("2018-01-01" , "yyyy-MM-dd")
    
    var yearsLong = endDate.yyyy - beginDate.yyyy //how does it last?
    
    
    //STANDART WAY TO ANALYSE (for, not spark):
    //it returns a string: date, value, maxvalue, variationPC
    
    //rdd1 is the daily one (let's cry)
    var rdd1 = preprocess.preProcess( "src/main/resources/BorsaItalianaETFSP500EUR-hedged.csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, "MM/dd/yyyy" )
       
    var rdd2 = preprocess.preProcess( "src/main/resources/BondGlobalIta(BarclaysGlobalAggregateBond).csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "BOND Euro Hedged Global borsa italiana" , beginDate , endDate, parser, "yyyy-MM-dd" )
          
    //printerPreprocessor( rdd1 ) //it is a string: date, value, maxvalue, variationPC
      
        
    //SPARK WAY TO ANALYSE, SO PARALLELIZED!
        
        // it give back an RDD: YearName(index), variation, capital, drawdownPC AT THE MOMENT, for each month
    var mappedRDD1 = singleETFAnalyzer.mapperResult( rdd1 ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, "dd/MM/yyyy" ) 
        
    var mappedRDD2 = singleETFAnalyzer.mapperResult( rdd2 ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "BOND Euro Hedged Global borsa italiana" , beginDate , endDate, parser, "dd/MM/yyyy" ) 
        
    //printerMapperFirst(mappedRDD1)//YearName(index), variation, capital, drawdownPC AT THE MOMENT, for each month
      
    //now we merge them (simple concatenate!!!) still splidden in moths 
    var arrayMapped = Array( mappedRDD1 , mappedRDD2 )    
   
    val merger = new MergerMultipleETF with Serializable
    
    var merged = merger.mergerAll( arrayMapped )     
    
    //printerMapper(merged)//YearName(index), variation, capital, drawdownPC AT THE MOMENT, for each month, MERGED
    
    //merge by years and name
  //yearName is index ,0 is variation to cumulate, 1 is capital to copy, 2 is drawdown to find worst
    var reducedRDD = merger.reducerETFMerged(merged)    
  
    //printerReduced(reducedRDD)//YearName(index), cumulative variation, capital now, worst drawdownPC
    
    //simply modify the index: now is only years!
    //year is index ,0 is variation to cumulate, 1 is capital to copy, 2 is drawdown to find worst
    var secondMapped = merger.secondMapperETF(reducedRDD)
    
    //printerMapper(secondMapped)//Year(index), variation, capital, drawdownPC AT THE MOMENT, for each year
  
    //give back for each year variation of the year and capital at the moment
    var finalSum = merger.secondReducerETF(secondMapped)
            FUCK WE NEED TO PROCESS THE CAPITAL IN THE PREPROCESSSSS.. maybe the first mapper 
            simply do the variation of the year and the SECOND PREPROCESS EVALUE CAPUTAL...
    finalSum.foreach(f => 
        {
          if(f._1.equalsIgnoreCase( endDate.yyyy.toString() ) || f._1.equalsIgnoreCase( "2017" ) )
          {
            
            println("id:" + f._1 + "average yield per year: " + (( f._2(1) / (100  *arrayMapped.length) )/yearsLong) 
                + "% ; final capital : " + f._2(1)  )
          }
        } )
      
    sc.stop 
  }
}