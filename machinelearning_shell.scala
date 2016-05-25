import org.apache.spark.sql.types._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.mutable.ListBuffer

//**********
//Computing Similar Players
//**********

val sStats = pStats.flatMap{ case(x,y)=>{
     var exp = 0
     var aggArr = Array[Double]()
     var eList = ListBuffer[(String, Int, Int, Array[Double])]()
     y.foreach( z => {
          if (!excludeNames.contains(z._1)){
              aggArr ++ =Array(z._5(0), z._5(1), z._5(2), z._5(3), z._5(4), z._5(5), z._5(6), z._5(7), z._5(8))  
              eList += ListBuffer((x, exp, z._3, aggArr))
              exp += 1
          }
          })
          (eList)
     }
}

//key by experience
val sStats1 = sStats.keyBy(x=>x._2)

//match up players with everyone else of the same experience
val sStats2 = sStats1.join(sStats1)

//calculate distance
val sStats3 = sStats2.map(x=>(x._1,x._2._1._1,x._2._2._1,x._2._1._3,math.sqrt(Vectors.sqdist(Vectors.dense(x._2._1._4),Vectors.dense(x._2._2._4)))/math.sqrt(Vectors.dense(x._2._2._4).size)))

//filter out players compared to themselves and convert to Row object
val similarity = sStats3.filter(x => !(x._2==x._3)).map(x=>Row(x._1,x._4,x._2,x._3,x._5))

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
  val trainData = sqlContext.sql("select name, year, exp, mp, " + stat + "0," + stat + "  as label from ml3 where year<2016")
  val testData = sqlContext.sql("select name, year, exp, mp, " + stat + "0," + stat + "  as label from ml3 where year=2017")

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

val regression_total = sqlContext.sql("select distinct t7.name, t7.prediction+zpts_temp.prediction as prediction, t7.actual+zpts_temp.label as actual from zpts_temp join (select distinct t6.name, t6.prediction+ztov_temp.prediction as prediction, t6.actual+ztov_temp.label as actual from ztov_temp join (select distinct t5.name, t5.prediction+zblk_temp.prediction as prediction, t5.actual+zblk_temp.label as actual from zblk_temp join (select distinct t4.name, t4.prediction+zstl_temp.prediction as prediction, t4.actual+zstl_temp.label as actual from zstl_temp join (select distinct t3.name, t3.prediction+zast_temp.prediction as prediction, t3.actual+zast_temp.label as actual from zast_temp join (select distinct t2.name, t2.prediction+ztrb_temp.prediction as prediction, t2.actual+ztrb_temp.label as actual from ztrb_temp join (select distinct t1.name, t1.prediction+z3p_temp.prediction as prediction, t1.actual+z3p_temp.label as actual from z3p_temp join (select distinct zfg_temp.name, zfg_temp.prediction+zft_temp.prediction as prediction, zfg_temp.label+zft_temp.label as actual from zfg_temp join zft_temp where zfg_temp.name=zft_temp.name) as t1  where t1.name=z3p_temp.name) as t2  where t2.name=ztrb_temp.name) as t3  where t3.name=zast_temp.name) as t4  where t4.name=zstl_temp.name) as t5  where t5.name=zblk_temp.name) as t6  where t6.name=ztov_temp.name) as t7  where t7.name=zpts_temp.name")
regression_total.saveAsTable("regression_total")

