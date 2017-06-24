package com.pack.spark


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.PairRDDFunctions
import java.util.Date
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
    if(percent.isNaN() || percent.isInfinite)
    {
      percent = 0.0
    }
    //println("now = " + now + "; old: " + old + "; diff abs : " + diff + "; percent = " + percent)
    percent
  }
  
  def updateCapital( old: Double, percent: Double) : Double =
  {
    var result = old + (old * percent / 100)
    //println("variation rate is : " + percent + "; old: " + old + "; new: " + result)
    result
  }
  
  def accumulate (accumulator: Array[Double], toAdd: Array[Double]) : Array[Double] =
  {
    var result = Array[Double](0,0)
    result(0) = accumulator(0) + toAdd(0)
    result(1) = toAdd(1)
    result
  }
  
  def secondAccumulate (accumulator: Array[Double], toAdd: Array[Double]) : Array[Double] =
  {
    
    var result = Array[Double](0,0)
    result(0) = accumulator(0) + toAdd(0)
    result(1) = accumulator(1) + toAdd(1)
    result
  }
  
  def accumulateMerged (accumulator: Array[Double], toAdd: Array[Double] )
  : Array[Double] =
  {
    var result = Array[Double](0,0)
    result(0) = accumulator(0) + toAdd(0)
    result(1) = toAdd(1)
    //println( "variazione questa: " + toAdd(0) + "; finora: " + accumulator(0) + "; quindi risulta: " + result(0) )
 
    result
  }
  
  def dateFormatter (str: String) : Date =
  {
     val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
     format.format(new java.util.Date())
     var d = format.parse(str)
     d
  }
  
  def mapperETF( input: String , output: String, capital: Double, sc: SparkContext, name: String, 
      beginDate: Date, endDate: Date) : RDD[ (String, Array[Double]) ] =
  {
    val test = sc.textFile( input )
    
    var previousValue = parseDouble("0")
    var previousCapital = capital
   
    return test.map { word => //for each word
        val variable = word.split(",")
        
        val date = variable.array(0)
        val df = dateFormatter(date)
        
        if( df.before(endDate) && df.after(beginDate) )
        {
          val open = parseDouble( variable.array(5) )
          var variation = percentDifference( previousValue , open )
          var capital = updateCapital( previousCapital, variation )
          previousValue = parseDouble(open);
          previousCapital = parseDouble(capital)
          
          var tuple = new Array[Double](2)
          tuple(0) = variation
          tuple(1) = capital
          
          ( (df.getYear+1900 + "-" + name) , tuple)
        }
        else
        {
          ("discarded" , new Array[Double](2))
        }
      } 
  }
  
  
  
  def secondMapperETF( input: RDD[ (String, Array[Double]) ]): RDD[ (String, Array[Double]) ] =
  {
    return input.map(line=>
      {
        var name = line._1.split("-")(0)
        
        var tuple = new Array[Double](2)
        tuple(0) = line._2(0)
        tuple(1) = line._2(1)
        
        ( name , tuple )
      })
  }
  
  def secondReducerETF(mappedRDD : RDD[ (String, Array[Double]) ]) : RDD[(String, Array[Double])] = 
  {
    var result = mappedRDD.reduceByKey( secondAccumulate )  
    result
  }
  
  def reducerETF(mappedRDD : RDD[ (Int, Array[Double]) ]) : RDD[(Int, Array[Double])] = 
  {
    var result = mappedRDD.reduceByKey( accumulate )  
    result
  }
  
  def reducerETFMerged(mappedRDD : RDD[ (String, Array[Double]) ]) : RDD[(String, Array[Double])] =
  {
    //mappedRDD.foreach(println)
    var result = mappedRDD.reduceByKey( accumulateMerged )  
    result
  }
  
    
  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
   
    var beginDate = dateFormatter("1990-01-01")
    var endDate = dateFormatter("2018-01-01")
    
    var mappedRDD1 = mapperETF( "src/main/resources/TotalStockHistory.csv" , "src/main/resources/output.txt", parseDouble("10000"), sc, 
        "Vangard Total Stock" , beginDate , endDate )
        
    var mappedRDD2 = mapperETF( "src/main/resources/TotalBondHistory.csv" , "src/main/resources/output.txt", parseDouble("10000"), sc, 
        "Vangard Total Bond" , beginDate , endDate )
        
    var merged = mappedRDD1.union(mappedRDD2).filter(f => !f._1.equalsIgnoreCase("discarded")) 
    
    /*merged.foreach(f => 
        {
          println("id:" + f._1 + ":  variazione Primo: " + f._2(0) + "; capital Primo: " + f._2(1)  )
        } )*/
    
    var reducedRDD = reducerETFMerged(merged)    
        
    reducedRDD.collect().foreach( f => 
        {
          //println("\n anno-nome:" + f._1 + ":  variazione" + f._2(0) + "; capitale: " + f._2(1)  )
        } )  
        
    var secondMapped = secondMapperETF(reducedRDD)
    secondMapped.foreach(f => 
        {
          //println("id:" + f._1 + ":  variazione : " + f._2(0) + "; capital : " + f._2(1)  )
        } )
    
    var finalSum = secondReducerETF(secondMapped)
            
    finalSum.foreach(f => 
        {
          println("id:" + f._1 + ":  variazione : " + f._2(0) + "; capital : " + f._2(1)  )
        } )
    
   // evalueETF( "src/main/resources/TotalBondHistory.csv" , "src/main/resources/output.txt", parseDouble("10000"), sc, 
    //    "Vangard Total Bond" , beginDate , endDate )    
        
    //evalueETF( "src/main/resources/PHAU.MI.csv" , "src/main/resources/output.txt", parseDouble("10000"), sc, 
     //   "Gold" , beginDate , endDate )    


    sc.stop 
  }
}