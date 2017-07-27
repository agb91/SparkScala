package com.pack.spark

class Reducer5Test  extends GeneralTest {
  
  test( "reducer" )
  {
       fixture()
       var rddReader1 = reader.readCsv( "src/test/resources/basic.csv" , sc )
       var rdd1 = preprocess.preProcess( parser.parseDouble("10000"), sc, 
            "M1" , beginDate , endDate, parser, 
            "dd/mm/yyyy", rddReader1 )
        var mappedRDD1 = concatWeightMap234.mapperResult( rdd1 ,
            "src/main/resources/output.txt", parser.parseDouble("10000"), sc, 
            "M1" , beginDate , endDate, parser, "dd/MM/yyyy" ) 
        var reducedRDD = reducer5.reducerETFMerged(mappedRDD1)
        assert( reducedRDD.collect().length == 6 )
        
        var arrayMapped = Array( mappedRDD1 , mappedRDD1 )      
        
        var merged = concatWeightMap234.mergerAll( arrayMapped )     
        
        var reducedRDD2 = reducer5.reducerETFMerged( merged )
        assert( reducedRDD2.collect().length == (6 - 1) )  //five because it merge with same year-month, but discard 1 discarded!
        assert( reducedRDD2.collect()(0)._2.length == 5 )   
        assert( reducedRDD2.collect()(0)._2(1) >= 0 )
        assert( reducedRDD2.collect()(0)._2(2) >= 0 )
        assert( reducedRDD2.collect()(0)._2(3) >= 0 )
        
  }
  
  
}