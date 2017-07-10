package com.pack.spark

import org.apache.spark.SparkConf
import com.pack.spark.parser.MyDate
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.parser.Parsers
import com.pack.spark.SingleETFAnalyzer

class SingleETFAnalyzer() {
  
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
  
  //accumulate: variation, copy capital and find max of yearly drawdown (respect the previous max, no matter 
  //in which years this max is)
  def accumulate (accumulator: Array[Double], toAdd: Array[Double]) : Array[Double] =
  {
    var result = Array[Double](0,0,0)
    result(0) = accumulator(0) + toAdd(0)
    result(1) = toAdd(1)
    var maxDD = 0.0;
    if( toAdd(2) > accumulator(2) )
    {
      maxDD = toAdd(2)
    }
    else
    {
      maxDD = accumulator(2)
    }
    result(2) = maxDD
    result
  }
  
 // give back: variation, capital, drawdown(instant, not need to comulate it) indexed by YEAR-NAME
  val mapperResult: (RDD[(String)],String,Double,SparkContext,String,MyDate,MyDate,Parsers,String) => RDD[(String, Array[Double])] = 
    ( input: RDD[(String)] , output: String, capital: Double, sc: SparkContext, name: String, 
      beginDate: MyDate, endDate: MyDate, parserSent: Parsers, dateFormat: String) => 
  {
    val test = input
    var previousValue = parserSent.parseDouble("0")
    var previousCapital = capital
    
    //println("outside: " + test.collect().length ) 
    test.map(word => //for each word
      {
        val variable = word.split(",")
        val date = variable.array(0)
        val df = parserSent.dateFormatter(date, dateFormat)
        
        if( df.before( endDate.dd, endDate.mm, endDate.yyyy ) 
            && df.after( beginDate.dd, beginDate.mm, beginDate.yyyy ) )
        {
          val value = parserSent.parseDouble( variable.array(1) )
          var variation = percentDifference( previousValue , value )
          var capital = updateCapital( previousCapital, variation )
          previousValue = parserSent.parseDouble( value );
          previousCapital = parserSent.parseDouble(capital)
          var maxValue = parserSent.parseDouble( variable.array(2) )
          var drawdownPC = ( (maxValue - value) / maxValue)
          
          var tuple = new Array[Double](3)
          tuple(0) = variation
          tuple(1) = capital
          tuple(2) = drawdownPC
          //println(tuple(1))
          ( (df.yyyy + "-" + name) , tuple)
          
        }
        else
        {
          ("discarded" , new Array[Double](3))
        }
      } )
  }
  
  
  val reducerResult = (mappedRDD : RDD[ (Int, Array[Double]) ]) => 
  {
    var result = mappedRDD.reduceByKey( accumulate )  
    result
  }
  

 
  
}