package com.pack.spark


class PreprocessorTest extends GeneralTest {
  
  test("reader") {
     fixture()
     var rdd1 = reader.readCsv( "src/test/resources/basic.csv" , sc )
 
     assert( rdd1.collect().length == 10 )   
     assert( rdd1.collect()(1) == "2/2/2007,1,1,1, 1 , 101" )
     assert( rdd1.collect()(4) == "1/7/2008,1,1,1, 1 , 175" )
  
  }
  
  test( "preprocessor" )
  {
    fixture()
    var rddReader = reader.readCsv( "src/test/resources/basic.csv" , sc )
     
    var rdd1 = preprocess.preProcess( parser.parseDouble("10000"), sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, 
        "dd/mm/yyyy", rddReader )
    assert( rdd1.collect().length == 7 )
    assert( rdd1.collect()(2).equalsIgnoreCase(  "1/7/2008,175.0,175.0,16.666666666666664,16.666666666666664"  ) )
    assert( rdd1.collect()(3).equalsIgnoreCase(  "1/1/2009,120.0,175.0,-31.428571428571427,0.0"  ) )
    assert( rdd1.collect()(4).equalsIgnoreCase(  "1/3/2009,120.0,175.0,0.0,0.0"  ) )
    assert( rdd1.collect()(5).equalsIgnoreCase(  "1/5/2009,240.0,240.0,100.0,100.0"  ) )
    
  }
  
  
  
  
  
}