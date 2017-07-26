package com.pack.spark.parser

class MyDate(_dd:Int, _mm:Int, _yyyy:Int ) extends Serializable{
  
  var dd: Int = _dd
  var mm: Int = _mm
  var yyyy: Int = _yyyy
  
  def sameDate( ddCheck:Int, mmCheck:Int, yyyyCheck:Int ): Boolean =
  {
    if ( (this.dd == ddCheck) && (this.mm == mmCheck) && (this.yyyy == yyyyCheck)  )
    {
      return true
    }
    return false
  }

  def sameDate( other : MyDate ): Boolean =
  {
    if ( (this.dd == other.dd) && (this.mm == other.mm) && (this.yyyy == other.yyyy)  )
    {
      return true
    }
    return false
  }
  
  def sameMonth( mmCheck:Int ): Boolean =
  {
    if ( this.mm == mmCheck )
    {
      return true
    }
    return false
  }

  def sameMonth( other : MyDate ): Boolean =
  {
    if ( this.mm == other.mm )
    {
      return true
    }
    return false
  }
  
  //if THIS DATE is before PARAMETER DATE
  def before( ddCheck:Int, mmCheck:Int, yyyyCheck:Int ): Boolean =
  {
    
    if( yyyyCheck >= yyyy )
    {
      return true
    }
    
    if(yyyyCheck==yyyy && mmCheck>=mm)
    {
      return true
    }
    
    if(yyyyCheck==yyyy && mmCheck==mm && ddCheck>=dd)
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