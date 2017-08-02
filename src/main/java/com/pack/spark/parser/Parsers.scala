package com.pack.spark.parser

import java.util.Date
import java.util.Calendar

class Parsers {
  
  def parseDouble(expectedNumber: Any): Double = 
  {
    if ( expectedNumber == null )
    {
      parseDouble(0.0)
    }
    
    else
    {
      try{
        expectedNumber match {
          case s: String => s.toDouble
          case i: Int => i.toDouble
          case l: Long => l.toDouble
          case d: Double => d
        }
      }
      catch
      {
        case _ => parseDouble(0.0)
      }
    }
    
}
    
     
  def dateFormatter (str: String , format: String) : MyDate =
  {
    var date: MyDate = null
    if(format.equalsIgnoreCase("yyyy-MM-dd") )
    {
      var chunks = str.split( "-" )
      date = new MyDate( chunks(2).toInt ,chunks(1).toInt, chunks(0).toInt )
    }
    if(format.equalsIgnoreCase("MM/dd/yyyy") )
    {
      var chunks = str.split( "/" )
      date = new MyDate( chunks(1).toInt ,chunks(0).toInt, chunks(2).toInt )
    }
    if(format.equalsIgnoreCase("dd/MM/yyyy") )
    {
      var chunks = str.split( "/" )
      date = new MyDate( chunks(0).toInt ,chunks(1).toInt, chunks(2).toInt )
    }
    date
  }
   
}