package com.pack.spark

import java.util.Date

class Parsers {
  
  def parseDouble(expectedNumber: Any): Double = 
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

    
     
  def dateFormatter (str: String) : Date =
  {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val format2 = new java.text.SimpleDateFormat("dd/MM/yyyy")
         
    try{
     format.format(new java.util.Date())
     var d = format.parse(str)
     d
    }catch
    {
        case _: Throwable => //println("Got some other kind of exception, let's try with alternative dates")
        format2.format(new java.util.Date())
        var d2 = format2.parse(str)
        d2
    }
  }
  
}