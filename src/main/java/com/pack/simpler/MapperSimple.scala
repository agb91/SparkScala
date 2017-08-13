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
  
    val mapper: ( RDD[ (String) ], Parsers,  MagicWeight , Int, Int ) => RDD[ (String, Array[Any]) ] =
   ( input: RDD[ (String) ] , parser : Parsers, oldMagicWeight : MagicWeight, yearBegin : Int , yearEnd : Int) =>
  {
    var p0 = oldMagicWeight.weights(0)
    var p1 = oldMagicWeight.weights(1)
    val result = input.map(line=>
      {
        var lineSplitted = line.split(",")
        var name = lineSplitted(0).split("-")(0)
        var date = lineSplitted(0).split("-")(1)
        var dateMonth = date.split("/")(0)
        var dateYear = date.split("/")(1)
            
        oldMagicWeight.weights(0) = p0
        oldMagicWeight.weights(1) = p1
        var tw = oldMagicWeight.getTotal()
        var limit = ceil(tw / 10.0).toInt
        
        var magicWeight1 = getMagicWeight( oldMagicWeight , r.nextInt( ceil(tw / 10.0).toInt ) , r.nextInt( ceil(tw / 10.0).toInt ) )
        var magicWeight2 = getMagicWeight( oldMagicWeight , r.nextInt( ceil(tw / 10.0).toInt ) , r.nextInt( ceil(tw / 10.0).toInt ) )
        var magicWeight3 = getMagicWeight( oldMagicWeight , r.nextInt( ceil(tw / 10.0).toInt ) , r.nextInt( ceil(tw / 10.0).toInt ) )
        
        var variationW1 = 0.0
        var variationW2 = 0.0
        var variationW3 = 0.0
        var worstDDW1 = 0.0
        var worstDDW2 = 0.0
        var worstDDW3 = 0.0
        
        if( name.equalsIgnoreCase("stock") )
        {
          variationW1 = ( parser.parseDouble( lineSplitted(2) ) * magicWeight1.weights(0) ) / magicWeight1.getTotal()
          
          variationW2 = ( parser.parseDouble( lineSplitted(2) ) * magicWeight2.weights(0) ) / magicWeight2.getTotal()
          variationW3 = ( parser.parseDouble( lineSplitted(2) ) * magicWeight3.weights(0) ) / magicWeight3.getTotal()
          worstDDW1 = ( parser.parseDouble( lineSplitted(3) ) * magicWeight1.weights(0) ) / magicWeight1.getTotal()
          worstDDW2 = ( parser.parseDouble( lineSplitted(3) ) * magicWeight2.weights(0) ) / magicWeight2.getTotal()
          worstDDW3 = ( parser.parseDouble( lineSplitted(3) ) * magicWeight3.weights(0) ) / magicWeight3.getTotal()
        }
        else
        {
          variationW1 = ( parser.parseDouble( lineSplitted(2) ) * magicWeight1.weights(1) ) / magicWeight1.getTotal()
          variationW2 = ( parser.parseDouble( lineSplitted(2) ) * magicWeight2.weights(1) ) / magicWeight2.getTotal()
          variationW3 = ( parser.parseDouble( lineSplitted(2) ) * magicWeight3.weights(1) ) / magicWeight3.getTotal()
          worstDDW1 = ( parser.parseDouble( lineSplitted(3) ) * magicWeight1.weights(1) ) / magicWeight1.getTotal()
          worstDDW2 = ( parser.parseDouble( lineSplitted(3) ) * magicWeight2.weights(1) ) / magicWeight2.getTotal()
          worstDDW3 = ( parser.parseDouble( lineSplitted(3) ) * magicWeight3.weights(1) ) / magicWeight3.getTotal()
        }
          
        
        if( parser.parseDouble( dateYear ) >= yearBegin && parser.parseDouble( dateYear ) <= yearEnd  )
        {
          
          var tuple = new Array[Any](9)
          tuple(0) = variationW1
          tuple(1) = worstDDW1
          tuple(2) = magicWeight1
          tuple(3) = variationW2
          tuple(4) = worstDDW2
          tuple(5) = magicWeight2
          tuple(6) = variationW3
          tuple(7) = worstDDW3
          tuple(8) = magicWeight3
          ( "accepted" , tuple )
        }
        else
        {
          var tuple = new Array[Any](9)
          tuple(0) = 0.0
          tuple(1) = 0.0
          tuple(2) = 0.0
          tuple(3) = 0.0
          tuple(4) = 0.0
          tuple(5) = 0.0
          tuple(6) = 0.0
          tuple(7) = 0.0
          tuple(8) = 0.0
          
          ( "out" , tuple )
        }
        
      })
      
      result
  }
  

  
}