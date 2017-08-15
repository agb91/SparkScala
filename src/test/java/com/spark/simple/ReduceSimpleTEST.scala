package com.spark.simple

import com.spark.simple.GeneralTestS
import com.pack.simpler.MagicWeight

class ReduceSimpleTEST  extends GeneralTestS {
  
  
  test("Reducer") {
     
    fixture()
    
          
    var rdd1 = reader.readCsv( "src/test/resources/simpleReducerS.csv" , sc )
    var rddProcessed1 = preprocess.preProcess( sc, "stock", beginDate, endDate, parser, "dd/mm/yyyy", rdd1 ) 
    
    var rdd2 = reader.readCsv( "src/test/resources/simpleReducerB.csv" , sc )
    var rddProcessed2 = preprocess.preProcess( sc, "bond", beginDate, endDate, parser, "dd/mm/yyyy", rdd2 ) 
    
    var arrayRDD = Array( rddProcessed1 , rddProcessed2 )
    
    var rddProcessed = concat.mergerAll( arrayRDD )
      
    var MW1 = new MagicWeight with Serializable
    MW1.weights(0) = 5000
    MW1.weights(1) = 5000
    var MW2 = new MagicWeight with Serializable
    MW2.weights(0) = 1000
    MW2.weights(1) = 9000
    var MW3 = new MagicWeight with Serializable
    MW3.weights(0) = 9000
    MW3.weights(1) = 1000
    
    
    var rddMapper1 = mapper.mapper( rddProcessed , parser, MW1 , 2000 , 2100 )
    var rddMapper2 = mapper.mapper( rddProcessed , parser, MW2 , 2000 , 2100 )
    var rddMapper3 = mapper.mapper( rddProcessed , parser, MW3 , 2000 , 2100 )
    
    //variation totalPC, worsDDPC, vote
    var reduced1 = reducer.reduce( rddMapper1 )
    var reduced2 = reducer.reduce( rddMapper2 )
    var reduced3 = reducer.reduce( rddMapper3 )
    
    
    assert( reduced1.collect()(1)._1.equalsIgnoreCase( "accepted-5000.0-5000.0" ) )
    assert( reduced2.collect()(1)._1.equalsIgnoreCase( "accepted-1000.0-9000.0" ) )
    assert( reduced3.collect()(1)._1.equalsIgnoreCase( "accepted-9000.0-1000.0" ) )
    
    assert( reduced3.collect()(1)._2(0)  > reduced1.collect()(1)._2(0) )
    assert( reduced3.collect()(1)._2(0)  > reduced2.collect()(1)._2(0) )
    
    
    assert( reduced3.collect()(1)._2(1)  > reduced1.collect()(1)._2(1) )
    assert( reduced3.collect()(1)._2(1)  > reduced2.collect()(1)._2(1) )
    
    
    
    
  } 
  
}