package com.spark.simple

import com.spark.simple.GeneralTestS
import com.pack.simpler.MagicWeight


class MapperSimpleTEST extends GeneralTestS{
  
  test("methods") 
  {
    fixture()
    
    old = new MagicWeight with Serializable
    
    old.weights(0) = 5000
    old.weights(1) = 5000
    var mw = mapper.getMagicWeight(old, 2000, 2000)
    assert( mw.weights(0) == old.weights(0) )
    assert( mw.weights(1) == old.weights(1) )
    assert( mw.getTotal() == old.getTotal() )
    
    mw = mapper.getMagicWeight(old, 1000, 3000)
    assert( mw.weights(0) == (old.weights(0) - 2000) )
    assert( mw.weights(1) == (old.weights(1) + 2000))
    assert( mw.getTotal() == old.getTotal() )

  }
  
  
  test("Mapper") {
     
    fixture()
    
    var rdd1 = reader.readCsv( "src/test/resources/verySimpleMapper.csv" , sc )
    var rddProcessed = preprocess.preProcess( sc, "stock", beginDate, endDate, parser, "dd/mm/yyyy", rdd1 ) 
    
    old = new MagicWeight with Serializable
    old.weights(0) = 4999
    old.weights(1) = 5001
    var rddMapper = mapper.mapper( rddProcessed , parser, old , 1900 , 2100 )
    
    assert( rddMapper.collect().size == 2 )
    assert( rddMapper.collect()(0)._1.equalsIgnoreCase("accepted-4999.0-5001.0") )
    assert( rddMapper.collect()(0)._2.length == 2 )
    
    assert( rddMapper.collect()(1)._2(1) == 0.0  ) // no drawdown here..  
    
    assert( parser.parseDouble( rddMapper.collect()(1)._2(0) ) > 20.0 ) // variation weighted can be about 20-30 more or less..   
    
    assert( parser.parseDouble( rddMapper.collect()(1)._2(0) ) < 30.0 ) // variation weighted can be about 20-30 more or less..   
    
    
    
  } 
  
}