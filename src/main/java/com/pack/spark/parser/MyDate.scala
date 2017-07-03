package com.pack.spark.parser

class MyDate(_dd:Int, _mm:Int, _yyyy:Int ) {
  
  var dd: Int = _dd
  var mm: Int = _mm
  var yyyy: Int = _yyyy
  
  def toStr(): String =
  {
    return dd + "-" + mm + "-" + yyyy
  }
  
}