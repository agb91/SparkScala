package com.pack.spark.parser

class MyDate(_dd:Int, _mm:Int, _yyyy:Int ) extends Serializable{
  
  var dd: Int = _dd
  var mm: Int = _mm
  var yyyy: Int = _yyyy
  
  
  //if THIS DATE is before PARAMETER DATE
  def before( ddCheck:Int, mmCheck:Int, yyyyCheck:Int ): Boolean =
  {
    
    if( yyyyCheck > yyyy )
    {
      return true
    }
    
    if(yyyyCheck==yyyy && mmCheck>mm)
    {
      return true
    }
    
    if(yyyyCheck==yyyy && mmCheck==mm && ddCheck>dd)
    {
      return true
    }
    
    return false
  }
  
  
  //if THIS DATE is after PARAMETER DATE
  def after( ddCheck:Int, mmCheck:Int, yyyyCheck:Int ): Boolean =
  {
    return !before( ddCheck:Int, mmCheck:Int, yyyyCheck:Int )
  }
  
  def toStr(): String =
  {
    return dd + "-" + mm + "-" + yyyy
  }
  
}