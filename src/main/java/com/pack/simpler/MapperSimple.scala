package com.pack.simpler

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.parser.Parsers

class MapperSimple {
  
    val mapper: ( RDD[ (String) ], Parsers, Double, Double, Int, Int ) => RDD[ (String, Array[Any]) ] =
   ( input: RDD[ (String) ] , parser : Parsers, sw:Double ,tw : Double, yearBegin : Int , yearEnd : Int) =>
  {
    val result = input.map(line=>
      {
        var lineSplitted = line.split(",")
        var date = lineSplitted(0)
        var dateMonth = date.split("/")(0)
        var dateYear = date.split("/")(1)
        var variationW = ( parser.parseDouble( lineSplitted(2) ) * sw ) / tw
        var worstDDW = ( parser.parseDouble( lineSplitted(3) ) * sw ) / tw
        var worstDateDelta = lineSplitted(4)
       
        
        if( parser.parseDouble( dateYear ) >= yearBegin && parser.parseDouble( dateYear ) <= yearEnd  )
        {
          
          var tuple = new Array[Any](3)
          tuple(0) = variationW
          tuple(1) = worstDDW
          tuple(2) =  worstDateDelta
          ( "accepted" , tuple )
        }
        else
        {
          var tuple = new Array[Any](3)
          tuple(0) = 0.0
          tuple(1) = 0.0
          tuple(2) = "-"
          
          ( "out" , tuple )
        }
        
      })
      
      result
  }
  

  
}