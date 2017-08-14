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
import scala.util.Sorting.quickSort

object MainAnalyzer {
  
  /*
    1) preprocess prendo un valore ogni anno, l'ultimo, con la variazione da gennaio, 
    il DD peggiore ma giorno per giorno non a riferimento annuale, indice è tipo-anno
    
    do
    
  	2) map peso per i vari prodotti variazione e DD, scarto quelli fuori dal range di tempo. to a ogni tipo 
  	//un peso prima casuale e dopo influenzato dal migliore, diviso in 3 versioni per ogni map, reduce poi
  	 * mi voterà la migliore
  
  	3) reduce prendo variazione totale e DD peggiore, 
  	
  	while reduce mi dice che i pesi sono buoni
  * */
  
  def main(args: Array[String]) = {
    
    val reader = new Reader with Serializable
    val parser = new Parsers with Serializable
    val preprocess = new PreprocessorSimple with Serializable
    val mapper = new MapperSimple with Serializable
    val reducer = new ReducerSimple with Serializable
    val concat = new Concat with Serializable
    
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("MainAnalyzer")
      .setMaster("local")
    val sc = new SparkContext(conf)
   
    var beginDate = parser.dateFormatter("2000-01-01", "yyyy-MM-dd" )
    var endDate = parser.dateFormatter("2019-01-01" , "yyyy-MM-dd")
    
    
    
    var datasRDD1 = reader.readCsv( "src/main/resources/BorsaItalianaETFSP500EUR-hedged.csv" , sc )
    var datasRDD2 = reader.readCsv( "src/main/resources/BondGlobalIta(BarclaysGlobalAggregateBond).csv" , sc )
    
    
    
    var rdd1 = preprocess.preProcess( sc, 
        "stock" , beginDate , endDate, parser, "MM/dd/yyyy", datasRDD1 )
    
    var rdd2 = preprocess.preProcess( sc, 
        "bond" , beginDate , endDate, parser, "MM/dd/yyyy", datasRDD1 )
        
    var arrayToMerge = new Array[RDD[ (String) ]](2)
    arrayToMerge(0) = rdd1
    arrayToMerge(1) = rdd2
    var rddT = concat.mergerAll( arrayToMerge )    
        
    var w = new MagicWeight()
    w.weights(0) = 5000.0
    w.weights(1) = 5000.0
   
    /*do
    {
        var mapped = mapper.mapper( rddT, parser, w , beginDate.yyyy, endDate.yyyy )
        var reduced = reducer.reduce( mapped )
        var reducedArray = reduced.collect()(0)._2
        var vote1 = parser.parseDouble( reducedArray(0) ) - parser.parseDouble( reducedArray(1) )
        var vote2 = parser.parseDouble( reducedArray(3) ) - parser.parseDouble( reducedArray(4) )
        var vote3 = parser.parseDouble( reducedArray(6) ) - parser.parseDouble( reducedArray(7) )
        if(vote1 > vote2 && vote1 > vote3)
        {
          w.weights(0) =  reducedArray(2).asInstanceOf[MagicWeight].weights(0)
          w.weights(1) =  reducedArray(2).asInstanceOf[MagicWeight].weights(1)
        }
        if(vote2 > vote1 && vote2 > vote3)
        {
          w.weights(0) =  reducedArray(5).asInstanceOf[MagicWeight].weights(0)
          w.weights(1) =  reducedArray(5).asInstanceOf[MagicWeight].weights(1) 
        }
        if(vote3 > vote1 && vote3 > vote2)
        {
          w.weights(0) =  reducedArray(8).asInstanceOf[MagicWeight].weights(0) 
          w.weights(1) =  reducedArray(8).asInstanceOf[MagicWeight].weights(1)
        }
    }while(false)
      */
   sc.stop 
  }
}