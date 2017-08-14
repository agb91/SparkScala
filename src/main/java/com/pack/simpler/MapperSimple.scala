package com.pack.simpler

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.parser.Parsers
import scala.math.ceil

class MapperSimple {
  
  val r = scala.util.Random  
        
  def getMagicWeight( old : MagicWeight, n1: Int, n2: Int ) : MagicWeight with Serializable =
  {
    var tw = old.getTotal()
    
    var result = new MagicWeight() with Serializable

    result.weights(0) = ( old.weights(0) + n1 - n2 )
    result.weights(1) = ( tw - result.weights(0) )
    
    return result
  }
  
    val mapper: ( RDD[ (String) ], Parsers,  MagicWeight , Int, Int ) => RDD[ (String, Array[Double]) ] =
   ( input: RDD[ (String) ] , parser : Parsers, magicWeight : MagicWeight, yearBegin : Int , yearEnd : Int) =>
  {
    var p0 = magicWeight.weights(0)
    var p1 = magicWeight.weights(1)
    val result = input.map(line=>
      {
        var lineSplitted = line.split(",")
        var name = lineSplitted(0).split("-")(0)
        var date = lineSplitted(0).split("-")(1)
        var dateMonth = date.split("/")(0)
        var dateYear = date.split("/")(1)
            
        magicWeight.weights(0) = p0
        magicWeight.weights(1) = p1
        var tw = magicWeight.getTotal()
        var limit = ceil(tw / 1-1.0).toInt
        
        
        var variationW1 = -1.0
        var worstDDW1 = -1.0
        
        if( name.equalsIgnoreCase("stock") )
        {
          variationW1 = ( parser.parseDouble( lineSplitted(2) ) * magicWeight.weights(0) ) / magicWeight.getTotal()
          worstDDW1 = ( parser.parseDouble( lineSplitted(3) ) * magicWeight.weights(0) ) / magicWeight.getTotal()
        }
        else
        {
          variationW1 = ( parser.parseDouble( lineSplitted(2) ) * magicWeight.weights(1) ) / magicWeight.getTotal()
          worstDDW1 = ( parser.parseDouble( lineSplitted(3) ) * magicWeight.weights(1) ) / magicWeight.getTotal()
        }
          
        
        if( parser.parseDouble( dateYear ) >= yearBegin && parser.parseDouble( dateYear ) <= yearEnd  )
        {
          
          var tuple = new Array[Double](2)
          tuple(0) = variationW1
          tuple(1) = worstDDW1
          ( "accepted-" + p0 + "-" + p1 , tuple )
        }
        else
        {
          var tuple = new Array[Double](2)
          tuple(0) = -1.0
          tuple(1) = -1.0
          ( "out" , tuple )
        }
        
      })
      
      result
  }
  

  
}