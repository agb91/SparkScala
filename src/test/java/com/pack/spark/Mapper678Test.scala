package com.pack.spark

class Mapper678Test  extends GeneralTest {
  
  
  
  test( "secondMapper" )
  {
       fixture()
       var w1 = 10000.0 // I'll use the same two times 
       
       var rddReader1 = reader.readCsv( "src/test/resources/year.csv" , sc )
       var rdd1 = preprocess.preProcess( w1, sc, 
            "M1" , beginDate , endDate, parser, 
            "dd/mm/yyyy", rddReader1 )
       var mappedRDD1 = concatWeightMap234.mapperResult( rdd1 ,
            "src/main/resources/output.txt", w1 , sc, 
            "M1" , beginDate , endDate, parser, "dd/MM/yyyy", w1 + w1 ) 
       var arrayMapped = Array( mappedRDD1 , mappedRDD1 )      
       var merged = concatWeightMap234.mergerAll( arrayMapped )     
       var reducedRDD = reducer5.reducerETFMerged( merged )
       var secondMapped = mapper678.secondMapperETF(reducedRDD , parser, w1 + w1)
       var discarded = 0
       var accepted = 0
       secondMapped.collect().foreach( f =>
         {
           if( f._1.equalsIgnoreCase( "discarded" ) )
           {
             discarded += 1
           }
           else
           {
             accepted +=1
           }
         } 
       )
       assert( discarded == secondMapped.collect().length -2  )
       assert( accepted == 2  )
     
  }
  
  test( "secondReducer" )
  {
       var w1 = 10000.0 // I'll use the same two times 
  
       fixture()
       var rddReader1 = reader.readCsv( "src/test/resources/year.csv" , sc )
       var rdd1 = preprocess.preProcess( w1, sc, 
            "M1" , beginDate , endDate, parser, 
            "dd/mm/yyyy", rddReader1 )
       var mappedRDD1 = concatWeightMap234.mapperResult( rdd1 ,
            "src/main/resources/output.txt", w1 , sc, 
            "M1" , beginDate , endDate, parser, "dd/MM/yyyy", w1 + w1 ) 
       var arrayMapped = Array( mappedRDD1 , mappedRDD1 )      
       var merged = concatWeightMap234.mergerAll( arrayMapped )     
       var reducedRDD = reducer5.reducerETFMerged( merged )
       var secondMapped = mapper678.secondMapperETF(reducedRDD, parser , w1 + w1)
       var finalSum = mapper678.secondReducerETF(secondMapped) // collassa per anno
       
       assert( finalSum.collect().length == 2 ) // discarded and accepter  
       assert( finalSum.collect()(0)._1.equalsIgnoreCase( "discarded" ) )
       assert( finalSum.collect()(1)._1.equalsIgnoreCase( "accepted" ) )
  
  }
  
}