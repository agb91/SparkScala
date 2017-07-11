package com.pack.spark

import org.apache.spark.SparkConf
import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.pack.spark.parser.Parsers
import com.pack.spark.parser.MyDate

class Preprocessor {
  
  
  object StringAccumulatorParam extends AccumulatorParam[String] {

      def zero(initialValue: String): String = {
          ""
      }
  
      def addInPlace(s1: String, s2: String): String = {
          s"$s1 $s2"
      }
  }
  
  
  def preProcess( input: String , output: String, capital: Double, sc: SparkContext, name: String, 
      beginDate: MyDate, endDate: MyDate, parserSent: Parsers, dateFormat: String) : RDD[(String)] =
  {
   val test = sc.textFile( input )
   var list: List[Double] = List()
   
   var maxValue = sc.accumulator(0.0)
   var maxDate = sc.accumulator("")(StringAccumulatorParam)
   var worstDrawdown = sc.accumulator(0.0)
   var worstDrawdownPC = sc.accumulator(0.0)
   var worstDateDelta = sc.accumulator("")(StringAccumulatorParam) // two dates, the one of the max and the one of the worst
   
   var newTextArray = Array[String]()

   var valueBefore = 0.0
   
   
   //it returns a string: date, value, maxvalue, variationPC
   test.collect().foreach(word => //for each word
   {
      var variable = word.split(",")  
      var value = parserSent.parseDouble( variable(5) )
      val date = variable.array(0)
      
      var dateFormatted = parserSent.dateFormatter(date, dateFormat)
      
      val day = dateFormatted.dd
      var drawdown = 0.0
      var drawdownPC = 0.0
      if(day == 1 )
      { 
        drawdown = maxValue.value - value
        drawdownPC = drawdown / maxValue.value
        if(value>maxValue.value) // here is the new max!
        {
          maxValue.setValue(value)
          maxDate.setValue(date)
          drawdown = 0
          drawdownPC = 0
        }else
        {
          if( drawdown > worstDrawdown.value )// here is the new shittest situation
          {
            worstDrawdown.setValue(drawdown)
            worstDateDelta.setValue( maxDate.value + " --> " + date)
            worstDrawdownPC.setValue( worstDrawdown.value / maxValue.value ) 
          }
        }
        
        var variationPC = (value - valueBefore) / value
        valueBefore = value
        var datePrint = dateFormatted.dd + "/" + (dateFormatted.mm) + "/" + (dateFormatted.yyyy) 
        var piece = datePrint + "," + value + "," + maxValue.value + "," + variationPC  
        newTextArray = newTextArray :+ piece
      }
   })
   
   var newText = sc.parallelize(newTextArray)
   //var pathName = "/home/andrea/scala/Starter/src/main/resources/OUTPUT/" + name
   
   //newText.saveAsTextFile(pathName)
   
   println( name.toUpperCase() + ": worst moment was: -" + (worstDrawdownPC.value*100) + " perCent; realized between the dates:  " 
       + worstDateDelta.value)
   return newText
  }  
    
  
}