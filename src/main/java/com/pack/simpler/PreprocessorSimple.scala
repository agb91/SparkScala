package com.pack.simpler

import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.pack.spark.parser.Parsers
import com.pack.spark.parser.MyDate

class PreprocessorSimple {
    
  def preProcess( sc: SparkContext, name: String, 
      beginDate: MyDate, endDate: MyDate, parserSent: Parsers, dateFormat: String, datas: RDD[(String)], beginYear : Int, endYear: Int ) 
  : RDD[(String)] =
  {
   var list: List[String] = List()
   
   var maxValue = 0.0
   var maxDate = new MyDate(1,1,1)
   var worstDrawdown = 0.0
   var worstDrawdownPC = 0.0
   var worstDateDelta = "none"
   var dateBefore = new MyDate(1,1,1)
   var valueBefore = 0.0
   var variationPC = 0.0
   
   
   //it returns a string: date, value, maxvalue, variationPC, variationPC from Janaury
   datas.collect().foreach(word => //for each word
   {
     if(word.length()>0)
     {
        var variable = word.split(",")  
        var value = parserSent.parseDouble( variable(5).trim )
        val date = variable.array(0).trim
        var dateFormatted = parserSent.dateFormatter(date, dateFormat)
        
        var drawdown = 0.0
        var drawdownPC = 0.0
        
        //take the data iff: it is a new month or ( it is the beginning year ) or (dateBefore not exists)
        if( (dateFormatted.yyyy != dateBefore.yyyy) && (dateFormatted.yyyy >= beginYear)
            && (dateFormatted.yyyy <= endYear) ) // every january
        { 
            dateBefore = dateFormatted
            drawdown = maxValue - value
            drawdownPC = drawdown / maxValue * 100
            if(value>maxValue) // here is the new max!
            {
              maxValue = value 
              maxDate = dateFormatted
              drawdown = 0.0
              drawdownPC = 0.0
            }
            else
            {
              if( drawdown > worstDrawdown )// here is the new shittest situation
              {
                worstDrawdown = drawdown
                worstDateDelta = maxDate.toStr() + " --> " + dateFormatted.toStr()
                worstDrawdownPC = worstDrawdown / maxValue * 100
              }
            }
            if( valueBefore != 0.0 )
            {
               variationPC = ( (value - valueBefore) / valueBefore) * 100
            }
            
            valueBefore = value
            var datePrint =  (dateFormatted.mm) + "/" + (dateFormatted.yyyy)
            // IN SCALA YOU MUSTN'T SPLIT THE FOLLOWING LINE INTO TWO! DON'T PRESS ENTER
            var piece = name + "-" +datePrint + "," + value + "," + variationPC + "," + worstDrawdownPC + "," + worstDateDelta
            list = list :+ piece
        }
     }
   })
   
   println( name + ": worst DD is " + worstDrawdownPC + " % ; between dates: " + worstDateDelta )
   
   return sc.parallelize(list)
  }  
    
 
  
}