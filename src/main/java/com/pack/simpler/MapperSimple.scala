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
  
  def getListMW( old : MagicWeight ) : Array[MagicWeight] =
  {
    var result = new Array[MagicWeight](3)
    var a = 0
    for( a <-0 to 2){
    
      var n1 = r.nextInt( 3000 )
      var n2 = r.nextInt( 3000 )
      var toAdd = getMagicWeight( old , n1 , n2 )
      result(a) = toAdd
    }
    
    result
    
  }
        
  def getMagicWeight( old : MagicWeight, n1: Int, n2: Int ) : MagicWeight with Serializable =
  {
    var tw = old.getTotal()
    
    var result = new MagicWeight() with Serializable

    result.weights(0) = ( old.weights(0) + n1 - n2 )
    if( result.weights(0) < 0 )
    {
      result.weights(0) = 0
    }
    result.weights(1) = ( tw - result.weights(0) )
    
    return result
  }
  
    val mapper: ( RDD[ (String) ], Parsers,  MagicWeight) => RDD[ (String, Array[Double]) ] =
   ( input: RDD[ (String) ] , parser : Parsers, magicWeight : MagicWeight) =>
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
        var worstDDW2 = -1.0
        
        if( name.equalsIgnoreCase("stock") )
        {
          variationW1 = ( parser.parseDouble( lineSplitted(2) ) * magicWeight.weights(0) ) / magicWeight.getTotal()
          worstDDW1 = ( parser.parseDouble( lineSplitted(3) ) * magicWeight.weights(0) ) / magicWeight.getTotal()
        }
        else
        {
          variationW1 = ( parser.parseDouble( lineSplitted(2) ) * magicWeight.weights(1) ) / magicWeight.getTotal()
          worstDDW2 = ( parser.parseDouble( lineSplitted(3) ) * magicWeight.weights(1) ) / magicWeight.getTotal()
        }
        
        var tuple = new Array[Double](3)
        tuple(0) = variationW1
        tuple(1) = worstDDW1 //stock
        tuple(2) = worstDDW2 //bond
        
        ( "accepted-" + p0 + "-" + p1 , tuple )
      
      })
      
      result
  }
  

  
}