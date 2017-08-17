package com.spark.simple

import com.spark.simple.GeneralTestS

class PreprocessorSimpleTEST  extends GeneralTestS {
  
   test("reader") {
     fixture()
     var rdd1 = reader.readCsv( "src/test/resources/simple.csv" , sc )
 
     assert( rdd1.collect().length == 10 )   
     assert( rdd1.collect()(1) == "2/2/2007,1,1,1, 1 , 100" )
     assert( rdd1.collect()(5) == "" )
  
  }
   
  test("Preprocess") {
     
    fixture()
    
    
          
     var rdd1 = reader.readCsv( "src/test/resources/simple.csv" , sc )
     var rddProcessed = preprocess.preProcess( sc, "test", beginDate, endDate, parser, "dd/mm/yyyy", rdd1 , 2007 , 2100) 
     
     assert( rddProcessed.collect().length == 6 )
     assert( rddProcessed.collect()(0).equalsIgnoreCase( "test-2/2007,100.0,0.0,0.0,none" ) )
     assert( rddProcessed.collect()(2).equalsIgnoreCase( "test-1/2009,100.0,-33.33333333333333,33.33333333333333,1-1-2008 --> 1-1-2009" ) )
  } 
 
  
}