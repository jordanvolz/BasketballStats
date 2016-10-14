import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.mutable.ListBuffer

//**********
//Computing Similar Players
//**********

//load in players data
val dfPlayers=sqlContext.sql("select * from players")
val pStats=dfPlayers.sort(dfPlayers("name"),dfPlayers("exp") asc).map(x=>(x.getString(1),(x.getDouble(50),x.getDouble(40),x.getInt(2),x.getInt(3),Array(x.getDouble(31),x.getDouble(32),x.getDouble(33),x.getDouble(34),x.getDouble(35),x.getDouble(36),x.getDouble(37),x.getDouble(38),x.getDouble(39)),x.getInt(0)))).groupByKey()
val excludeNames=dfPlayers.filter(dfPlayers("year")===1980).select(dfPlayers("name")).map(x=>x.mkString).toArray.mkString(",")

//combine players seasons into one long array
val sStats = pStats.flatMap { case(player,stats) => 
     var exp:Int = 0
     var aggArr = Array[Double]()
     var eList = ListBuffer[(String, Int, Int, Array[Double])]()
     stats.foreach{ case(nTot,zTot,year,age,statline,experience) => 
          if (!excludeNames.contains(player)){
              aggArr ++= Array(statline(0), statline(1), statline(2), statline(3), statline(4), statline(5), statline(6), statline(7), statline(8))  
              eList += ((player, exp, year, aggArr))
              exp+=1
          }}
          (eList)
}

//key by experience
val sStats1 = sStats.keyBy(x => x._2)

//match up players with everyone else of the same experience
val sStats2 = sStats1.join(sStats1)

//calculate distance
val sStats3 = sStats2.map { case(experience,(player1,player2)) => (experience,player1._1,player2._1,player1._3,math.sqrt(Vectors.sqdist(Vectors.dense(player1._4),Vectors.dense(player2._4)))/math.sqrt(Vectors.dense(player2._4).size))
}

//filter out players compared to themselves and convert to Row object
val similarity = sStats3.filter(x => (x._2!=x._3)).map(x => Row(x._1,x._4,x._2,x._3,x._5))

//schema for similar players
val schemaS = StructType(
     StructField("experience", IntegerType, true) ::
     StructField("year", IntegerType, true) ::
     StructField("name", StringType, true) ::
     StructField("similar_player", StringType, true) ::
     StructField("similarity_score", DoubleType, true) :: Nil
)


//create data frame
val dfSimilar = sqlContext.createDataFrame(similarity,schemaS)
dfSimilar.cache

//save as table
dfSimilar.saveAsTable("similar")


//**********
//Regression 
//**********

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics


val statArray = Array("zfg","zft","z3p","ztrb","zast","zstl","zblk","ztov","zpts")

for (stat <- statArray){
  //set up vector with features
  val features = Array("exp", stat+"0")
  val assembler = new VectorAssembler()
  assembler.setInputCols(features)
  assembler.setOutputCol("features")

  //lineare regression
  val lr = new LinearRegression()

  //set up parameters
  val builder = new ParamGridBuilder()
  builder.addGrid(lr.regParam, Array(0.1, 0.01, 0.001))
  builder.addGrid(lr.fitIntercept)
  builder.addGrid(lr.elasticNetParam, Array(0.0, 0.25, 0.5, 0.75, 1.0))
  val paramGrid = builder.build()

  //define pipline
  val pipeline = new Pipeline()
  pipeline.setStages(Array(assembler, lr))

  //set up tvs
  val tvs = new TrainValidationSplit()
  tvs.setEstimator(pipeline)
  tvs.setEvaluator(new RegressionEvaluator)
  tvs.setEstimatorParamMaps(paramGrid)
  tvs.setTrainRatio(0.75)

  //define train and test data
  val trainData = sqlContext.sql("select name, year, exp, mp, " + stat + "0," + stat + "  as label from ml where year<2017")
  val testData = sqlContext.sql("select name, year, exp, mp, " + stat + "0," + stat + "  as label from ml where year=2017")

  //create model
  val model = tvs.fit(trainData)

  //create predictions
  val predictions = model.transform(testData).select("name", "year", "prediction","label")

  //Get RMSE
  val rm = new RegressionMetrics(predictions.rdd.map(x => (x(2).asInstanceOf[Double], x(3).asInstanceOf[Double])))
  //println("Mean Squared Error " + stat + " : " + rm.meanSquaredError)
  println("Root Mean Squared Error " + stat + " : " + rm.rootMeanSquaredError)

//save as temp table
  predictions.registerTempTable(stat + "_temp")

}

//add up all individual predictions and save as a table
val regression_total=sqlContext.sql("select zfg_temp.name, zfg_temp.year, z3p_temp.prediction + zfg_temp.prediction + zft_temp.prediction + ztrb_temp.prediction + zast_temp.prediction + zstl_temp.prediction + zblk_temp.prediction + ztov_temp.prediction + zpts_temp.prediction as prediction, z3p_temp.label + zfg_temp.label + zft_temp.label + ztrb_temp.label + zast_temp.label + zstl_temp.label + zblk_temp.label + ztov_temp.label + zpts_temp.label as label from z3p_temp, zfg_temp, zft_temp, ztrb_temp, zast_temp, zstl_temp, zblk_temp, ztov_temp, zpts_temp where zfg_temp.name=z3p_temp.name and z3p_temp.name=zft_temp.name and zft_temp.name=ztrb_temp.name and ztrb_temp.name=zast_temp.name and zast_temp.name=zstl_temp.name and zstl_temp.name=zblk_temp.name and zblk_temp.name=ztov_temp.name and ztov_temp.name=zpts_temp.name")
regression_total.saveAsTable("regression_total")


