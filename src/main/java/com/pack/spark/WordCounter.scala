package com.pack.spark


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object WordCounter {
  
  def clean ( str:String) : String = {
     var result = str.replace(";", "")
     result = result.replace(".", "")
     result = result.replace(",", "")
     result = result.replace(" ", "")
     return result  
  }
  
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
  
  
  def percentDifference( old: Double, now: Double) : Double =
  {
    var diff = now - old
    var percent = (diff / old) * 100
    if( percent > 30 || percent < -30)
    {
      println(" ATTENZIONE: " + percent)
    }
    //println("now = " + now + "; old: " + old + "; diff abs : " + diff + "; percent = " + percent)
    percent
    
  }
  
  def updateCapital( old: Double, percent: Double) : Double =
  {
    //println("variation: " + (old * percent) )
    var result = old + (old * percent / 100)
    result
  }
  
  def accumulate (accumulator: Array[Double], toAdd: Array[Double]) : Array[Double] =
  {
    
    /*println("old 0: " + accumulator(0) )
    println("old 1: " + accumulator(1) )
    println("new 0: " + toAdd(0) )
    println("new 1: " + toAdd(1) )
    println(accumulator.length)
    println(toAdd.length)*/
    var result = Array[Double](0,0,0)
    //println(result.length)
    result(0) = accumulator(0) + toAdd(0)
    result(1) = accumulator(1) + toAdd(1)
    result(2) = toAdd(2)
    result
  }
  
  def evalueETF( input: String , output: String, capital: Double, sc: SparkContext, name: String) =
  {
    //Read some example file to a test RDD
    val test = sc.textFile( input )
    
    println("WORK ON: " + name);
    
    var previousValue = parseDouble("0")
    var previousCapital = capital
    
    var countOK = 0;
    var countKO = 0;
    
    test.flatMap { line => //for each line
      line.split(",") //split the line in word by word.
    }
      
    
    test.map { word => //for each word
        val variable = word.split(",")
        //variable.foreach( println )
        
        val date = variable.array(0)
        val open = parseDouble( variable.array(1) )
        //println( "date: " + date )
        //println( "new value: " + open + ";  old value: " + previousValue )
        var variation = percentDifference( previousValue , open )
        if(variation.isNaN() || variation.isInfinite)
        {
          variation = 0.0
        }
        
        var capital = updateCapital( previousCapital, variation )
        //println( "varying: " + variation )  
        previousValue = parseDouble(open);
        previousCapital = parseDouble(capital)
        
        
        
        var tuple = new Array[Double](3)
        tuple(0) = 1.0
        tuple(1) = variation
        tuple(2) = capital
        
        (name , tuple)
        
        /*if( variation > 0.0 )
        {
          ("gain" , tuple)
        }
        else
        {
          ("loss" , tuple)
        }*/
       }
      .reduceByKey( accumulate ).collect().foreach( f => 
        {
          println(f._1)
          f._2.foreach(println)
        } )  
  }
    
  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    
    evalueETF( "src/main/resources/VT.csv" , "src/main/resources/output.txt", parseDouble("10000"), sc, "Vangard Total Stock" )


    sc.stop 
  }
}