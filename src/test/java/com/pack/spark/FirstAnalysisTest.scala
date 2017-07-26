package com.pack.spark

class FirstAnalysisTest extends GeneralTest {
   
  test( "firstAnalysis" )
  {
    fixture()
    var rddReader = reader.readCsv( "src/test/resources/basic.csv" , sc )
    var rdd1 = preprocess.preProcess( parser.parseDouble("10000"), sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, 
        "dd/mm/yyyy", rddReader )
    
    // it give back an RDD: YearName(index), variationPC from janaury,drawdownPC AT THE MOMENT, for each month
    //, weight
    var mappedRDD1 = singleETFAnalyzer.mapperResult( rdd1 ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, "dd/MM/yyyy" ) 
   
    assert( mappedRDD1.collect().length == 7 )
    assert( mappedRDD1.collect()(6)._1.equalsIgnoreCase("discarded") )
    assert( mappedRDD1.collect()(0)._1.equalsIgnoreCase("discarded") )
    
    assert( mappedRDD1.collect()(2)._2(0) == 175.0 )
    assert( mappedRDD1.collect()(2)._2(1) == 166666.66666666666 )
    assert( mappedRDD1.collect()(2)._2(2) == 0.0 )
    assert( mappedRDD1.collect()(2)._2(3) == 10000.0 )
    assert( mappedRDD1.collect()(2)._2(4) == 1.0 )
    
    assert( mappedRDD1.collect()(3)._2(0) == 120.0 )
    assert( mappedRDD1.collect()(3)._2(1) == 0.0 )
    assert( mappedRDD1.collect()(3)._2(2) == 314285.71428571426 )
    assert( mappedRDD1.collect()(3)._2(3) == 10000.0 )
    assert( mappedRDD1.collect()(3)._2(4) == 1.0 )
    
    assert( mappedRDD1.collect()(4)._2(0) == 120.0 )
    assert( mappedRDD1.collect()(4)._2(1) == 0.0 )
    assert( mappedRDD1.collect()(4)._2(2) == 314285.71428571426 )
    assert( mappedRDD1.collect()(4)._2(3) == 10000.0 )
    assert( mappedRDD1.collect()(4)._2(4) == 1.0 )
    
    assert( mappedRDD1.collect()(5)._2(0) == 240.0 )
    assert( mappedRDD1.collect()(5)._2(1) == 1000000.0 )
    assert( mappedRDD1.collect()(5)._2(2) == 0.0 )
    assert( mappedRDD1.collect()(5)._2(3) == 10000.0 )
    assert( mappedRDD1.collect()(5)._2(4) == 1.0 )
    
        
  }
  
}