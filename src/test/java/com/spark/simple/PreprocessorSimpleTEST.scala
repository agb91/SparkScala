package com.spark.simple

import com.spark.simple.GeneralTestS

class PreprocessorSimpleTEST  extends GeneralTestS {
  
   test("reader") {
     fixture()
     var rdd1 = reader.readCsv( "src/test/resources/simple.csv" , sc )
 
     assert( rdd1.collect().length == 9 )   
     assert( rdd1.collect()(0) == "2/2/2007,1,1,1, 1 , 100" )
     assert( rdd1.collect()(4) == "" )
  
  }
   
  test("Preprocess") {
     
    fixture()
    
    
          
     var rdd1 = reader.readCsv( "src/test/resources/simple.csv" , sc )
     var rddProcessed = preprocess.preProcess( 1000, sc, "test", beginDate, endDate, parser, "dd/mm/yyyy", rdd1 ) 
     
     assert( rddProcessed.collect().length == 6 )
     assert( rddProcessed.collect()(0).equalsIgnoreCase( "2/2007,100.0,0.0,0.0,none" ) )
     assert( rddProcessed.collect()(2).equalsIgnoreCase( "1/2009,100.0,-33.33333333333333,0.3333333333333333,1-1-2008 --> 1-1-2009" ) )
  } 
 
  
}