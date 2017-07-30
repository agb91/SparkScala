package com.pack.spark

class ConcatWeightMapTest  extends GeneralTest {
  
   
  test( "mapper234 One" )
  {
    fixture()
    var rddReader = reader.readCsv( "src/test/resources/basic.csv" , sc )
    var rdd1 = preprocess.preProcess( parser.parseDouble("10000"), sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, 
        "dd/mm/yyyy", rddReader )
    
    // it give back an RDD: YearName(index), variationPC from janaury,drawdownPC AT THE MOMENT, for each month
    //, weight
    var mappedRDD1 = concatWeightMap234.mapperResult( rdd1 ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "Borsa Italiana SP500 EUR-hedged" , beginDate , endDate, parser, "dd/MM/yyyy", 10000.0 ) 
   
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
  
   test("merger of two with weights") {
  
   fixture()
   
   var m1 = 10.0
   var m2 = 10000.0
   var tw = m1 + m2
   
   var rddReader1 = reader.readCsv( "src/test/resources/merge1.csv" , sc )
   var rddReader2 = reader.readCsv( "src/test/resources/merge2.csv" , sc )
   
   assert( rddReader1.collect().size == 3 )
   assert( rddReader2.collect().size == 2 )
   
   var rdd1 = preprocess.preProcess( m1, sc, 
        "M1" , beginDate , endDate, parser, 
        "dd/mm/yyyy", rddReader1 )
   
   var rdd2 = preprocess.preProcess( m2, sc, 
        "M2" , beginDate , endDate, parser, 
        "dd/mm/yyyy", rddReader2 )
    
   assert( rdd1.collect().size == 2 )
   assert( rdd2.collect().size == 2 )
    
        
    // it give back an RDD: YearName(index), variationPC from janaury,drawdownPC AT THE MOMENT, for each month
    //, weight
    var mappedRDD1 = concatWeightMap234.mapperResult( rdd1 ,
        "src/main/resources/output.txt", m1, sc, 
        "M1" , beginDate , endDate, parser, "dd/MM/yyyy" , tw ) 
        
    var mappedRDD2 = concatWeightMap234.mapperResult( rdd2 ,
        "src/main/resources/output.txt", m2, sc, 
        "M1" , beginDate , endDate, parser, "dd/MM/yyyy" , tw)    
        
        
        
    var arrayMapped = Array( mappedRDD1 , mappedRDD2 )      
        
    var merged = concatWeightMap234.mergerAll( arrayMapped )     
    
    assert( merged.collect().size == 4 )
    assert( merged.collect()(0)._1.equalsIgnoreCase("2009-2") )
    assert( merged.collect()(2)._1.equalsIgnoreCase("2010-1") )
    assert( merged.collect()(1)._2(1) == 100 * m1 )
    assert( merged.collect()(3)._2(1) == ( (10.0 / 190.0) * 100) * m2 )
  }
 
  
}