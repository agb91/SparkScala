package com.spark.simple

import com.spark.simple.GeneralTestS


class MapperSimpleTEST extends GeneralTestS{
  
  test("Mapper") {
     
    fixture()
    
    var w1 = 5000.0
    var w2 = 10000.0
    var wt = w1 + w2
          
    var rdd1 = reader.readCsv( "src/test/resources/simpleMapper.csv" , sc )
    var rddProcessed = preprocess.preProcess( 1000, sc, "test", beginDate, endDate, parser, "dd/mm/yyyy", rdd1 ) 
     
    var rddMapper = mapper.mapper( rddProcessed, parser, w1, wt, beginDate.yyyy , endDate.yyyy )
    
    assert( rddMapper.collect().size == 8 )
    
    var ok = 0
    var ko = 0
    rddMapper.collect().foreach( f => 
    {
      if ( f._1.equalsIgnoreCase( "out" ) )
      {
        ko += 1
      }
      else
      {
        ok +=1
      }
        
    })
    
    
    assert( ok == 6 )
    assert( ko == 2 )  
    assert( rddMapper.collect()(0)._2(0) == null )
    assert( rddMapper.collect()(1)._2(0) == parser.parseDouble( rddProcessed.collect()(1).split(",")(2) )*w1/wt )
    assert( rddMapper.collect()(1)._2(1) == parser.parseDouble( rddProcessed.collect()(1).split(",")(3) )*w1/wt )
    
    
  } 
 
  
}