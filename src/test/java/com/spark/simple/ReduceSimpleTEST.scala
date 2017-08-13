package com.spark.simple

import com.spark.simple.GeneralTestS

class ReduceSimpleTEST  extends GeneralTestS {
  
  /*
  test("Mapper") {
     
    fixture()
    
    var w1 = 5000.0
    var w2 = 10000.0
    var wt = w1 + w2
          
    var rdd1 = reader.readCsv( "src/test/resources/simpleReducer.csv" , sc )
    var rddProcessed = preprocess.preProcess( 1000, sc, "test", beginDate, endDate, parser, "dd/mm/yyyy", rdd1 ) 
    var rddMapper = mapper.mapper( rddProcessed, parser, w1, wt, beginDate.yyyy , endDate.yyyy )
    var reduced = reducer.reduce( rddMapper )
    
    assert( reduced.collect().length == 2 )
    assert( reduced.collect()(1)._1.equalsIgnoreCase("accepted") )
    assert( reduced.collect()(0)._1.equalsIgnoreCase("out") )
    assert( reduced.collect()(1)._2(0) == 200.0 * w1 / wt )
    assert( reduced.collect()(1)._2(1) == 0.0 * w1 / wt )
    assert( reduced.collect()(1)._2(2).toString().equalsIgnoreCase("none") )
    
    
  } 
 */
  
}