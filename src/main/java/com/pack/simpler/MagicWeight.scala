package com.pack.simpler

class MagicWeight {
  
  var weights = new Array[ Double ](2)
  
  var vote = 0.0
  
  
  
   
  def getTotal() : Double = 
  {
    return ( weights(0) + weights(1) )
  }
  
  def toStr() : String = 
  {
    "w0:  " + weights(0).toString() + " ;;  w1: " + weights(1).toString()
  }
  
}