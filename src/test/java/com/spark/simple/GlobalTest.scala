package com.spark.simple

import com.spark.simple.GeneralTestS
import com.pack.simpler.MagicWeight

class GlobalTest extends GeneralTestS{
  
  
  test("all the processs")
  {
      fixture()
      var goal = 30 //the grade to reach
            
      var rdd1 = reader.readCsv( "src/test/resources/simpleReducerS.csv" , sc )
      var rddProcessed1 = preprocess.preProcess( sc, "stock", beginDate, endDate, parser, "dd/mm/yyyy", rdd1 ) 
      
      var rdd2 = reader.readCsv( "src/test/resources/simpleReducerB.csv" , sc )
      var rddProcessed2 = preprocess.preProcess( sc, "bond", beginDate, endDate, parser, "dd/mm/yyyy", rdd2 ) 
      
      var arrayRDD = Array( rddProcessed1 , rddProcessed2 )
      
      var rddT = concat.mergerAll( arrayRDD )
      
      var w = new MagicWeight() //seed
      w.weights(0) = 5000.0
      w.weights(1) = 5000.0
     
      var voteMax = 0.0
      var best : MagicWeight = w
      var continue : Boolean = true
      do
      {
          var listMW = mapper.getListMW( best )
          for ( mw : MagicWeight <- listMW ) { 
            var mapped = mapper.mapper( rddT, parser, mw , 2000, 2100 )
            var reduced = reducer.reduce( mapped )
            var reducedArray = reduced.collect()(0)._2
            var vote = reducedArray(2)
            if( vote > voteMax )
            {
              voteMax = vote
              best = mw
            }
            if(voteMax > goal)
            {
              continue = false
            }
            println("vote max: " + voteMax + ";  for mw: " + best.toStr() )
          }
          
      }while( continue )
  }
  
}