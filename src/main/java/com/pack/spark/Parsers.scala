package com.pack.spark

import java.util.Date
import java.util.Calendar

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
  
  def dayFormatter (str: String) : Int =
  {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val format2 = new java.text.SimpleDateFormat("dd/MM/yyyy")
    
    val cal = Calendar.getInstance()
         
    try{
     format.format(new java.util.Date())
     var d = format.parse(str)
     cal.setTime(d)
     var dayOfMonth = cal.get(Calendar.DAY_OF_MONTH)
     return dayOfMonth
    }catch
    {
        case _: Throwable => //println("Got some other kind of exception, let's try with alternative dates")
        format2.format(new java.util.Date())
        var d2 = format2.parse(str)
        cal.setTime(d2)
        var dayOfMonth = cal.get(Calendar.DAY_OF_MONTH) 
        return dayOfMonth
    }
  }
  
}