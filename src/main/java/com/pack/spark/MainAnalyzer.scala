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
  
  def printerMapperFirst( toPrint: RDD[(String, Array[Double])] )
  {
    toPrint.foreach(f => 
      {   
        println("name: " + f._1 + ",  value: " + f._2(0) +  
            "; variazioneFromJanuary " + f._2(1) + ", drawdawnPC ora: " + f._2(2)
            + "; peso-capitale: " + f._2(3) ) 
      }
      )
  }
      //0 = totalVariationWeighted, 1 totalDrawdown weighted  
  def printerMapper( toPrint: RDD[(String, Array[Double])] )
  {
    toPrint.foreach(f => 
      {   
        if( !f._1.equalsIgnoreCase("discarded") )
        {
          println("name: " + f._1 + ":  variazione pesata: " + f._2(0) + "; DD pesato: " + f._2(1) ) 
        }
      }
      )
  }
  
  def printerReduced5( toPrint: RDD[(String, Array[Double])] )
  {
    toPrint.foreach(f => 
      {   
        println("passo5: name: " + f._1 + ":  variazione pesatp" + f._2(1) + 
            "; drawdawnPC pesato: " + f._2(2) + "; total capital:" + f._2(3)  ) 
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
   
    var beginDate = parser.dateFormatter("2007-01-01", "yyyy-MM-dd" )
    var endDate = parser.dateFormatter("2016-01-01" , "yyyy-MM-dd")
    
    var yearsLong = endDate.yyyy - beginDate.yyyy //how does it last?
    
  /*  Nuove idee: 
    *1)il preprocess ci da liste di meseanno-nome, variazioni[RISPETTO AL VALORE DI GENNAIOOO], DD del monento divisi per mese
    *2) concatenazione di vari prodotti
    *3) dai i pesi a ogni prodotto, 2-3 sono fatti in un solo ciclo
    *4) il primo parallelo MAP prepara un vettore meseAnno-> variazione ,1, DD, peso
    *5) in primo parallelo REDUCE mergia i diversi prodotti con lo stesso mese-anno sommando variazioni PESATE
        e ha un contatore che trova il loro numero{sommapesi} [è ovvio? NO perchè magari un prodotto quella parte di
          tempo non ce l''ha!!!]
    *6) il secondo parallelo MAP divide la variazione per il numero dei prodotti{sommapesi} NO REDUCE SUBITO
    *7) il terzo parallelo MAP tiene solo dicembre come mese con variazione da inizio anno e DD di dicembre 
    8 ) seriale con le ottenute variazioni calcola il capitale alla fine
   */
        //STANDART WAY TO ANALYSE (for, not spark):
    
    //1)-----------------------------------------------------------------------------------------------
   
    //it returns a string: date, value, maxvalue, variationPC, variationPCfromJanaury
    //rdd1 is the daily one (let's cry)
    var rdd1 = preprocess.preProcess( "src/main/resources/BorsaItalianaETFSP500EUR-hedged.csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, "MM/dd/yyyy" )
       
    var rdd2 = preprocess.preProcess( "src/main/resources/BondGlobalIta(BarclaysGlobalAggregateBond).csv" ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "BOND Euro Hedged Global borsa italiana" , beginDate , endDate, parser, "yyyy-MM-dd" )
          
    //printerPreprocessor( rdd1 ) //it is a string: date, value, maxvalue, variationPC
    
    //2-3-4)-----------------------------------------------------------------------------------------------
       
        
        
    //SPARK WAY TO ANALYSE, SO PARALLELIZED!
        
    // it give back an RDD: YearName(index), variationPC from janaury,drawdownPC AT THE MOMENT, for each month
    //, weight
    var mappedRDD1 = singleETFAnalyzer.mapperResult( rdd1 ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, "dd/MM/yyyy" ) 
        
    var mappedRDD2 = singleETFAnalyzer.mapperResult( rdd2 ,
        "src/main/resources/output.txt", parser.parseDouble("20000"), sc, 
        "BOND Euro Hedged Global borsa italiana" , beginDate , endDate, parser, "dd/MM/yyyy" ) 
        
    //printerMapperFirst(mappedRDD2)//YearName(index), variationFromJanuary, drawdownPC AT THE MOMENT, for each month
      //, weight
    //now we merge them (simple concatenate!!!) still splidden in moths 
    var arrayMapped = Array( mappedRDD1 , mappedRDD2 )    
   
    val merger = new MergerMultipleETF with Serializable
    
    var merged = merger.mergerAll( arrayMapped )     
    
    //printerMapperFirst(merged)//Year-month(index), variationFromJanuary, capital, drawdownPC AT THE MOMENT, for each month, MERGED
    
    
    
    //5)-----------------------------------------------------------------------------------------------
    
    
    
    //merge by years and name
  //yearName is index ,0 is value,1 = variationFromJanuaryWeighted, 2 = drawdownPCWeighted, 3 = capital
  // 4 = 1
          
    var reducedRDD = merger.reducerETFMerged(merged)    
  
    //printerReduced5(reducedRDD)//YearMonmth(index), cumulative variation, capital now, worst drawdownPC
   
    //6-7)-----------------------------------------------------------------------------------------------
    
    
    //1 = totalVariationWeighted, 2 totalDrawdown weighted, 3 totalCApital
    var secondMapped = merger.secondMapperETF(reducedRDD)
    
    printerMapper(secondMapped)//year->weightedVariation, weighted drawdown
  
    //give back for each year variation of the year and capital at the moment
    /*var finalSum = merger.secondReducerETF(secondMapped)
    finalSum.foreach(f => 
        {
          if(f._1.equalsIgnoreCase( endDate.yyyy.toString() ) || f._1.equalsIgnoreCase( "2017" ) )
          {
            
            println("id:" + f._1 + "average yield per year: " + (( f._2(1) / (100  *arrayMapped.length) )/yearsLong) 
                + "% ; final capital : " + f._2(1)  )
          }
        } )
      */
    sc.stop 
  }
}