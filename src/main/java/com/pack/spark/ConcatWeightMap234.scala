package com.pack.spark

import org.apache.spark.SparkConf
import com.pack.spark.parser.MyDate
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.parser.Parsers

class ConcatWeightMap234() {
  
  val mergerAll: ( Array[RDD[ (String, Array[Double] ) ]] ) => RDD[ ( String, Array[Double] ) ] =
   (allMappedRDD : Array[RDD[ (String, Array[Double]) ]]) => 
   {
      var result = allMappedRDD(0).filter(f => !f._1.equalsIgnoreCase("discarded")  )
      
      for ( i <- 1 to (allMappedRDD.length - 1) ) {
         result = result.union( allMappedRDD(i) ).filter(h => !h._1.equalsIgnoreCase("discarded") )
      }
      result
   }  
  
  def percentDifference( old: Double, now: Double) : Double =
  {
    var diff = now - old
    var percent = (diff / old) * 100
    if(percent.isNaN() || percent.isInfinite)
    {
      percent = 0.0
    }
    //println("now = " + now + "; old: " + old + "; diff abs : " + diff + "; percent = " + percent)
    percent
  }
  
  def updateCapital( old: Double, percent: Double) : Double =
  {
    var result = old + (old * percent / 100)
    //println("variation rate is : " + percent + "; old: " + old + "; new: " + result)
    result
  }
  
 // give back a RDD: YearName, variationPCFromJanuary, drawdown(instant, not need to comulate it) indexed by YEAR-NAME
  val mapperResult: (RDD[(String)],String,Double,SparkContext,String,MyDate,MyDate,Parsers,String,Double) => 
    RDD[(String, Array[Double])] = 
    ( input: RDD[(String)] , output: String, capital: Double, sc: SparkContext, name: String, 
      beginDate: MyDate, endDate: MyDate, parserSent: Parsers, dateFormat: String, tw : Double) => 
  {
    val test = input
    //datePrint + "," + value + ","  + maxValue.value + "," + variationPC + "," + variationFromJanuary  
    test.map(word => //for each word
      {
        val variable = word.split(",")
        val date = variable.array(0)
        val df = parserSent.dateFormatter(date, dateFormat)
        var _capital = capital
        if( df.before( endDate.dd, endDate.mm, endDate.yyyy ) 
            && df.after( beginDate.dd, beginDate.mm, beginDate.yyyy ) )
        {
          var value = parserSent.parseDouble( variable.array(1) )
          var variationFromJanuary = parserSent.parseDouble( variable.array(4) )
          var variationFromJanuaryWeighted = variationFromJanuary * capital
          var maxValue = parserSent.parseDouble( variable.array(2) )
          var drawdownPC = ( (maxValue - value) / maxValue) * 100
          var drawdownPCWeighted = drawdownPC * capital 
          var tuple = new Array[Double](5)
          tuple(0) = value
          tuple(1) = variationFromJanuaryWeighted
          tuple(2) = drawdownPCWeighted
          tuple(3) = tw //the total value of the various products..
          tuple(4) = 1.0
          //println(tuple(1))
          ( (df.yyyy + "-" + df.mm) , tuple)
          
        }
        else
        {
          var tuple = new Array[Double](5)
          tuple(0) = 1.0
          tuple(1) = 1.0
          tuple(2) = 1.0
          tuple(3) = 1.0
          tuple(4) = 1.0
          ("discarded" , tuple)
        }
      } )
  }
  
  
  /*val reducerResult = (mappedRDD : RDD[ (Int, Array[Double]) ]) => 
  {
    var result = mappedRDD.reduceByKey( accumulate )  
    result
  }*/
  

 
  
}