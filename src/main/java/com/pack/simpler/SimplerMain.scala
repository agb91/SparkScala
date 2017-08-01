package com.pack.simpler

import org.apache.spark.SparkConf
import com.pack.reader._
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.parser.Parsers

import com.pack.simpler.PreprocessorSimple


object MainAnalyzer {
  
  /*
    1) preprocess prendo un valore ogni anno, l'ultimo, con la variazione da gennaio, 
    il DD peggiore ma giorno per giorno non a riferimento annuale
  
  	2) map peso per i vari prodotti variazione e DD, scarto quelli fuori dal range di tempo
  
  	3) reduce prendo variazione totale e DD peggiore, 
  */
  
  def main(args: Array[String]) = {
    
    val reader = new Reader with Serializable
    val parser = new Parsers with Serializable
    val preprocess = new PreprocessorSimple with Serializable
    
    var bw = 20000.0
    var sw = 20000.0
    var tw = bw + sw
    
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("MainAnalyzer")
      .setMaster("local")
    val sc = new SparkContext(conf)
   
    var beginDate = parser.dateFormatter("2007-01-01", "yyyy-MM-dd" )
    var endDate = parser.dateFormatter("2009-01-01" , "yyyy-MM-dd")
    
    
    
    var datasRDD1 = reader.readCsv( "src/main/resources/BorsaItalianaETFSP500EUR-hedged.csv" , sc )
    var datasRDD2 = reader.readCsv( "src/main/resources/BondGlobalIta(BarclaysGlobalAggregateBond).csv" , sc )
    
    
    
    var rdd1 = preprocess.preProcess( sw , sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, "MM/dd/yyyy", datasRDD1 )
       
        
   sc.stop 
  }
}