package com.pack.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.Parsers
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
  
  def accumulate (accumulator: Array[Double], toAdd: Array[Double]) : Array[Double] =
  {
    var result = Array[Double](0,0)
    result(0) = accumulator(0) + toAdd(0)
    result(1) = toAdd(1)
    result
  }
  
 
  val mapperResult: (String,String,Double,SparkContext,String,Date,Date,Parsers) => RDD[(String, Array[Double])] = 
    ( input: String , output: String, capital: Double, sc: SparkContext, name: String, 
      beginDate: Date, endDate: Date, parserSent: Parsers) => 
  {
    val test = sc.textFile( input )
    var previousValue = parserSent.parseDouble("0")
    var previousCapital = capital
    //println("outside: " + test.collect().length ) 
    test.map(word => //for each word
      {
        val variable = word.split(",")
        
        
        val date = variable.array(0)
        val df = parserSent.dateFormatter(date)
        
        if( df.before(endDate) && df.after(beginDate) )
        {
          val open = parserSent.parseDouble( variable.array(5) )
          var variation = percentDifference( previousValue , open )
          var capital = updateCapital( previousCapital, variation )
          previousValue = parserSent.parseDouble(open);
          previousCapital = parserSent.parseDouble(capital)
          
          var tuple = new Array[Double](2)
          tuple(0) = variation
          tuple(1) = capital
          //println(tuple(1))
          ( (df.getYear+1900 + "-" + name) , tuple)
          
        }
        else
        {
          ("discarded" , new Array[Double](2))
        }
      } )
  }
  
  
  val reducerResult = (mappedRDD : RDD[ (Int, Array[Double]) ]) => 
  {
    var result = mappedRDD.reduceByKey( accumulate )  
    result
  }
  

 
  
}