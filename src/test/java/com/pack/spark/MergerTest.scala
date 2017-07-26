package com.pack.spark

class MergerTest  extends GeneralTest {
  
  test("merger") {
  
   fixture()
   var rddReader1 = reader.readCsv( "src/test/resources/merge1.csv" , sc )
   var rddReader2 = reader.readCsv( "src/test/resources/merge2.csv" , sc )
   
   assert( rddReader1.collect().size == 2 )
   assert( rddReader2.collect().size == 1 )
   
   var rdd1 = preprocess.preProcess( parser.parseDouble("10000"), sc, 
        "M1" , beginDate , endDate, parser, 
        "dd/mm/yyyy", rddReader1 )
   
   var rdd2 = preprocess.preProcess( parser.parseDouble("10000"), sc, 
        "M2" , beginDate , endDate, parser, 
        "dd/mm/yyyy", rddReader2 )
    
   assert( rdd1.collect().size == 1 )
   assert( rdd2.collect().size == 1 )
    
        
    // it give back an RDD: YearName(index), variationPC from janaury,drawdownPC AT THE MOMENT, for each month
    //, weight
    var mappedRDD1 = singleETFAnalyzer.mapperResult( rdd1 ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "M1" , beginDate , endDate, parser, "dd/MM/yyyy" ) 
        
    var mappedRDD2 = singleETFAnalyzer.mapperResult( rdd2 ,
        "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
        "M1" , beginDate , endDate, parser, "dd/MM/yyyy" )    
        
        
        
    var arrayMapped = Array( mappedRDD1 , mappedRDD2 )      
        
    var merged = merger.mergerAll( arrayMapped )     
    
    assert( merged.collect().size == 2 )
    assert( merged.collect()(0)._1.equalsIgnoreCase("2009-2") )
    assert( merged.collect()(1)._1.equalsIgnoreCase("2010-1") )
  }
  
  
  test( "reducer" )
  {
       fixture()
       var rddReader1 = reader.readCsv( "src/test/resources/basic.csv" , sc )
       var rdd1 = preprocess.preProcess( parser.parseDouble("10000"), sc, 
            "M1" , beginDate , endDate, parser, 
            "dd/mm/yyyy", rddReader1 )
        var mappedRDD1 = singleETFAnalyzer.mapperResult( rdd1 ,
            "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
            "M1" , beginDate , endDate, parser, "dd/MM/yyyy" ) 
        var reducedRDD = merger.reducerETFMerged(mappedRDD1)
        assert( reducedRDD.collect().length == 6 )
        
        var arrayMapped = Array( mappedRDD1 , mappedRDD1 )      
        
        var merged = merger.mergerAll( arrayMapped )     
        
        var reducedRDD2 = merger.reducerETFMerged( merged )
        assert( reducedRDD2.collect().length == (6 - 1) )  //five because it merge with same year-month, but discard 1 discarded!
        assert( reducedRDD2.collect()(0)._2.length == 5 )   
        assert( reducedRDD2.collect()(0)._2(1) >= 0 )
        assert( reducedRDD2.collect()(0)._2(2) >= 0 )
        assert( reducedRDD2.collect()(0)._2(3) >= 0 )
        
  }
  
  test( "secondMapper" )
  {
       fixture()
       var rddReader1 = reader.readCsv( "src/test/resources/basic.csv" , sc )
       var rdd1 = preprocess.preProcess( parser.parseDouble("10000"), sc, 
            "M1" , beginDate , endDate, parser, 
            "dd/mm/yyyy", rddReader1 )
       var mappedRDD1 = singleETFAnalyzer.mapperResult( rdd1 ,
            "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
            "M1" , beginDate , endDate, parser, "dd/MM/yyyy" ) 
       var arrayMapped = Array( mappedRDD1 , mappedRDD1 )      
       var merged = merger.mergerAll( arrayMapped )     
       var reducedRDD = merger.reducerETFMerged( merged )
       var secondMapped = merger.secondMapperETF(reducedRDD)todo qui si aspetta i dati ogni anni solo a dicembre, 
       serve che cambi set su cui lavorare per avere qualche 12...
    
     
  }
  
}