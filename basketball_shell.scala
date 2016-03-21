//********************
//Copyright 2015 Cloudera (http://www.cloudera.com)
//Authored by Jordan Volz (jordan.volz@cloudera.com)
//********************

//********************
//DATA
//********************
//Data: bball data, NBA league data per player. Each year is a separate file, goes back to 1980
//format: Rk,Player,Pos,Age,Tm,G,GS,MP,FG,FGA,FG%,3P,3PA,3P%,2P,2PA,2P%,eFG%,FT,FTA,FT%,ORB,DRB,TRB,AST,STL,BLK,TOV,PF,PTS
//********************

//********************
//SET-UP
//********************

//process files so that each line includes the year
for (i <- 1980 to 2016){
     println(i)
     val yearStats = sc.textFile(s"/user/cloudera/BasketballStats/leagues_NBA_$i*")
     yearStats.filter(x => x.contains(",")).map(x =>  (i,x)).saveAsTextFile(s"/user/cloudera/BasketballStatsWithYear/$i/")
}


//********************
//CODE
//********************
//Cut and Paste into the Spark Shell. Use :paste to enter "cut and paste mode" and CTRL+D to process
//spark-shell --master yarn-client
//********************


//********************
//Classes, Helper Functions + Variables
//********************
import org.apache.spark.util.StatCounter
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.mutable.ListBuffer

//helper funciton to compute normalized value
def statNormalize(stat:Double, max:Double, min:Double)={
     if (stat>0){
          stat/max
     }else{
          stat/min*(-1)
     }
}

//Holds initial bball stats + weighted stats + normalized stats
@serializable case class BballData (year: Int, name: String, position: String, age:Int, team: String, gp: Int, gs: Int, mp: Double,stats: Array[Double], statsZ:Array[Double]=Array[Double](),valueZ:Double=0,statsN:Array[Double]=Array[Double](),valueN:Double=0,experience:Double=0)

//parse a stat line into a BBallDataZ object
def bbParse(input: String,bStats: scala.collection.Map[String,Double]=Map.empty,zStats: scala.collection.Map[String,Double]=Map.empty)={
     val line=input.replace(",,",",0,")
     val pieces=line.substring(1,line.length-1).split(",")
     val year=pieces(0).toInt
     val name=pieces(2)
     val position=pieces(3)
     val age=pieces(4).toInt
     val team=pieces(5)
     val gp=pieces(6).toInt
     val gs=pieces(7).toInt
     val mp=pieces(8).toDouble
     val stats=pieces.slice(9,31).map(x=>x.toDouble)
     var statsZ:Array[Double]=Array.empty
     var valueZ:Double=Double.NaN
     var statsN:Array[Double]=Array.empty
     var valueN:Double=Double.NaN

     if (!bStats.isEmpty){
         val fg=(stats(0)-bStats.apply(year.toString+"_FG_avg"))/bStats.apply(year.toString+"_FG_stdev")
         val fga=stats(1)/bStats.apply(year.toString+"_FGA_avg")
         val fgp=(stats(2)-bStats.apply(year.toString+"_FG%_avg"))/bStats.apply(year.toString+"_FG%_stdev")
         val tp=(stats(3)-bStats.apply(year.toString+"_3P_avg"))/bStats.apply(year.toString+"_3P_stdev")
         val tpa=(stats(4)-bStats.apply(year.toString+"_3PA_avg"))/bStats.apply(year.toString+"_3PA_stdev")
         val tpp=(stats(5)-bStats.apply(year.toString+"_3P%_avg"))/bStats.apply(year.toString+"_3P%_stdev")
         val dp=(stats(6)-bStats.apply(year.toString+"_2P_avg"))/bStats.apply(year.toString+"_2P_stdev")
         val dpa=(stats(7)-bStats.apply(year.toString+"_2PA_avg"))/bStats.apply(year.toString+"_2PA_stdev")
         val dpp=(stats(8)-bStats.apply(year.toString+"_2P%_avg"))/bStats.apply(year.toString+"_2P%_stdev")
         val efg=(stats(9)-bStats.apply(year.toString+"_eFG%_avg"))/bStats.apply(year.toString+"_eFG%_stdev")
         val ft=(stats(10)-bStats.apply(year.toString+"_FT_avg"))/bStats.apply(year.toString+"_FT_stdev")
         val fta=stats(11)/bStats.apply(year.toString+"_FTA_avg")
         val ftp=(stats(12)-bStats.apply(year.toString+"_FT%_avg"))/bStats.apply(year.toString+"_FT%_stdev")
         val orb=(stats(13)-bStats.apply(year.toString+"_ORB_avg"))/bStats.apply(year.toString+"_ORB_stdev")
         val drb=(stats(14)-bStats.apply(year.toString+"_DRB_avg"))/bStats.apply(year.toString+"_DRB_stdev")
         val trb=(stats(15)-bStats.apply(year.toString+"_TRB_avg"))/bStats.apply(year.toString+"_TRB_stdev")
         val ast=(stats(16)-bStats.apply(year.toString+"_AST_avg"))/bStats.apply(year.toString+"_AST_stdev")
         val stl=(stats(17)-bStats.apply(year.toString+"_STL_avg"))/bStats.apply(year.toString+"_STL_stdev")
         val blk=(stats(18)-bStats.apply(year.toString+"_BLK_avg"))/bStats.apply(year.toString+"_BLK_stdev")
         val tov=(stats(19)-bStats.apply(year.toString+"_TOV_avg"))/bStats.apply(year.toString+"_TOV_stdev")*(-1)
         val pf=(stats(20)-bStats.apply(year.toString+"_PF_avg"))/bStats.apply(year.toString+"_PF_stdev")
         val pts=(stats(21)-bStats.apply(year.toString+"_PTS_avg"))/bStats.apply(year.toString+"_PTS_stdev")
         statsZ=Array(fgp*fga,ftp*fta,tp,trb,ast,stl,blk,tov,pts)
         valueZ = statsZ.reduce(_+_)

         if (!zStats.isEmpty){
             val zfg=(fgp*fga-zStats.apply(year.toString+"_FG_avg"))/zStats.apply(year.toString+"_FG_stdev")
             val zft=(ftp*fta-zStats.apply(year.toString+"_FT_avg"))/zStats.apply(year.toString+"_FT_stdev")
             val fgN=statNormalize(zfg,(zStats.apply(year.toString+"_FG_max")-zStats.apply(year.toString+"_FG_avg"))/zStats.apply(year.toString+"_FG_stdev"),(zStats.apply(year.toString+"_FG_min")-zStats.apply(year.toString+"_FG_avg"))/zStats.apply(year.toString+"_FG_stdev"))
             val ftN=statNormalize(zft,(zStats.apply(year.toString+"_FT_max")-zStats.apply(year.toString+"_FT_avg"))/zStats.apply(year.toString+"_FT_stdev"),(zStats.apply(year.toString+"_FT_min")-zStats.apply(year.toString+"_FT_avg"))/zStats.apply(year.toString+"_FT_stdev"))
             val tpN=statNormalize(tp,zStats.apply(year.toString+"_3P_max"),zStats.apply(year.toString+"_3P_min"))
             val trbN=statNormalize(trb,zStats.apply(year.toString+"_TRB_max"),zStats.apply(year.toString+"_TRB_min"))
             val astN=statNormalize(ast,zStats.apply(year.toString+"_AST_max"),zStats.apply(year.toString+"_AST_min"))
             val stlN=statNormalize(stl,zStats.apply(year.toString+"_STL_max"),zStats.apply(year.toString+"_STL_min"))
             val blkN=statNormalize(blk,zStats.apply(year.toString+"_BLK_max"),zStats.apply(year.toString+"_BLK_min"))
             val tovN=statNormalize(tov,zStats.apply(year.toString+"_TOV_max"),zStats.apply(year.toString+"_TOV_min"))
             val ptsN=statNormalize(pts,zStats.apply(year.toString+"_PTS_max"),zStats.apply(year.toString+"_PTS_min"))
             statsZ=Array(zfg,zft,tp,trb,ast,stl,blk,tov,pts)
             valueZ = statsZ.reduce(_+_)
             statsN=Array(fgN,ftN,tpN,trbN,astN,stlN,blkN,tovN,ptsN)
             valueN=statsN.reduce(_+_)   
         }
     } 
     BballData(year, name, position, age, team, gp, gs, mp, stats,statsZ,valueZ,statsN,valueN)
}
   
//stat counter class -- need printStats method to print out the stats. Useful for transformations
class bballStatCounter extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double): bballStatCounter = {
    if (x.isNaN) {
      missing += 1
    } else {
      stats.merge(x)
    }
    this
  }

  def merge(other: bballStatCounter): bballStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }

  def printStats(delim: String): String= {
     stats.count + delim + stats.mean + delim + stats.stdev + delim + stats.max + delim + stats.min
  }

  override def toString: String = {
    "stats: " + stats.toString + " NaN: " + missing
  }
}

object bballStats extends Serializable {
  def apply(x: Double) = new bballStatCounter().add(x)
}

//process raw data into zScores and nScores
def processStats(stats0:org.apache.spark.rdd.RDD[String],txtStat:Array[String],bStats: scala.collection.Map[String,Double]=Map.empty,zStats: scala.collection.Map[String,Double]=Map.empty)={
    //parse stats
    val stats1=stats0.map(x=>bbParse(x,bStats,zStats))
    
    //group by year
    val stats2={if(bStats.isEmpty){    
                stats1.keyBy(x=>x.year).map(x=>(x._1,x._2.stats)).groupByKey()
            }else{
                stats1.keyBy(x=>x.year).map(x=>(x._1,x._2.statsZ)).groupByKey()
            }
    }    

    //map each stat to StatCounter
    val stats3=stats2.map{case (x,y)=>(x,y.map(a=>a.map(b=>bballStats(b))))}

    //merge all stats together
    val stats4=stats3.map{case (x,y)=>(x,y.reduce((a,b)=>a.zip(b).map{ case (c,d)=>c.merge(d)}))}

    //combine stats with label and pull label out
    val stats5=stats4.map{case (x,y)=>(x,txtStat.zip(y))}.map{x=>(x._2.map{case (y,z)=>(x._1,y,z)})}

    //separate each stat onto its own line and print out the Stats to a String
    val stats6=stats5.flatMap(x=>x.map(y=>(y._1,y._2,y._3.printStats(","))))

    //turn stat tuple into key-value pairs with corresponding agg stat
    val stats7=stats6.flatMap{case(a,b,c)=>{
         val pieces=c.split(",")
             val count=pieces(0)
              val mean=pieces(1)
              val stdev=pieces(2)
              val max=pieces(3)
              val min=pieces(4)
             Array((a+"_"+b+"_"+"count",count.toDouble),(a+"_"+b+"_"+"avg",mean.toDouble),(a+"_"+b+"_"+"stdev",stdev.toDouble),(a+"_"+b+"_"+"max",max.toDouble),(a+"_"+b+"_"+"min",min.toDouble))
         }
     }
     stats7
}

//process stats for age or experience
def processStatsAgeOrExperience(stats0:org.apache.spark.rdd.RDD[(Int, Array[Double])], label:String)={


    //group elements by age
    val stats1=stats0.groupByKey()

    //turn values into StatCounter objects
    val stats2=stats1.map{case(x,y)=>(x,y.map(z=>z.map(a=>bballStats(a))))}

    //Reduce rows by merging StatCounter objects
    val stats3=stats2.map{case (x,y)=>(x,y.reduce((a,b)=>a.zip(b).map{case(c,d)=>c.merge(d)}))}

    //turn data into RDD[Row] object for dataframe
    val stats4=stats3.map(x=>Array(Array(x._1.toDouble),x._2.flatMap(y=>y.printStats(",").split(",")).map(y=>y.toDouble)).flatMap(y=>y)).map(x=>Row(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),x(20)))

    //create schema for age table
    val schema =StructType(
         StructField(label, IntegerType, true) ::
         StructField("valueZ_count", DoubleType, true) ::
         StructField("valueZ_mean", DoubleType, true) ::
         StructField("valueZ_stdev", DoubleType, true) ::
         StructField("valueZ_max", DoubleType, true) ::
         StructField("valueZ_min", DoubleType, true) ::
         StructField("valueN_count", DoubleType, true) ::
         StructField("valueN_mean", DoubleType, true) ::
         StructField("valueN_stdev", DoubleType, true) ::
         StructField("valueN_max", DoubleType, true) ::
         StructField("valueN_min", DoubleType, true) ::
         StructField("deltaZ_count", DoubleType, true) ::
         StructField("deltaZ_mean", DoubleType, true) ::
         StructField("deltaZ_stdev", DoubleType, true) ::
         StructField("deltaZ_max", DoubleType, true) ::
         StructField("deltaZ_min", DoubleType, true) ::
         StructField("deltaN_count", DoubleType, true) ::
         StructField("deltaN_mean", DoubleType, true) ::
         StructField("deltaN_stdev", DoubleType, true) ::
         StructField("deltaN_max", DoubleType, true) ::
         StructField("deltaN_min", DoubleType, true) :: Nil
    )

    //create data frame
    sqlContext.createDataFrame(stats4,schema)
}

//********************
//Processing + Transformations
//********************


//********************
//Compute Aggregate Stats Per Year
//********************

//read in all stats
val stats=sc.textFile("/user/cloudera/BasketballStatsWithYear/*/*")

//filter out junk rows, clean up data entry errors as well
val filteredStats=stats.filter(x => !x.contains("FG%")).filter(x => x.contains(",")).map(x=>x.replace("*","").replace(",,",",0,"))
filteredStats.cache()

//process stats and save as map
val txtStat=Array("FG","FGA","FG%","3P","3PA","3P%","2P","2PA","2P%","eFG%","FT","FTA","FT%","ORB","DRB","TRB","AST","STL","BLK","TOV","PF","PTS")
val aggStats=processStats(filteredStats,txtStat).collectAsMap

//collect rdd into map and broadcast
val broadcastStats=sc.broadcast(aggStats)


//********************
//Compute Z-Score Stats Per Year
//********************

//parse stats, now tracking weights
val txtStatZ=Array("FG","FT","3P","TRB","AST","STL","BLK","TOV","PTS")
val zStats=processStats(filteredStats,txtStatZ,broadcastStats.value).collectAsMap

//collect rdd into map and broadcast
val zBroadcastStats=sc.broadcast(zStats)


//********************
//Compute Normalized Stats Per Year
//********************

//parse stats, now normalizing
val nStats=filteredStats.map(x=>bbParse(x,broadcastStats.value,zBroadcastStats.value))

//map RDD to RDD[Row] so that we can turn it into a dataframe
val nPlayer = nStats.map(x => Row.fromSeq(Array(x.name,x.year,x.age,x.position,x.team,x.gp,x.gs,x.mp) ++ x.stats ++ x.statsZ ++ Array(x.valueZ) ++ x.statsN ++ Array(x.valueN))) 

//create schema for the data frame
val schemaN =StructType(
     StructField("name", StringType, true) ::
     StructField("year", IntegerType, true) ::
     StructField("age", IntegerType, true) ::
     StructField("position", StringType, true) ::
     StructField("team", StringType, true) ::
     StructField("gp", IntegerType, true) ::
     StructField("gs", IntegerType, true) ::
     StructField("mp", DoubleType, true) ::
     StructField("FG", DoubleType, true) ::
     StructField("FGA", DoubleType, true) ::
     StructField("FGP", DoubleType, true) ::
     StructField("3P", DoubleType, true) ::
     StructField("3PA", DoubleType, true) ::
     StructField("3PP", DoubleType, true) ::
     StructField("2P", DoubleType, true) ::
     StructField("2PA", DoubleType, true) ::
     StructField("2PP", DoubleType, true) ::
     StructField("eFG", DoubleType, true) ::
     StructField("FT", DoubleType, true) ::
     StructField("FTA", DoubleType, true) ::
     StructField("FTP", DoubleType, true) ::
     StructField("ORB", DoubleType, true) ::
     StructField("DRB", DoubleType, true) ::
     StructField("TRB", DoubleType, true) ::
     StructField("AST", DoubleType, true) ::
     StructField("STL", DoubleType, true) ::
     StructField("BLK", DoubleType, true) ::
     StructField("TOV", DoubleType, true) ::
     StructField("PF", DoubleType, true) ::
     StructField("PTS", DoubleType, true) ::
     StructField("zFG", DoubleType, true) ::
     StructField("zFT", DoubleType, true) ::
     StructField("z3P", DoubleType, true) ::
     StructField("zTRB", DoubleType, true) ::
     StructField("zAST", DoubleType, true) ::
     StructField("zSTL", DoubleType, true) ::
     StructField("zBLK", DoubleType, true) ::
     StructField("zTOV", DoubleType, true) ::
     StructField("zPTS", DoubleType, true) ::
     StructField("zTOT", DoubleType, true) ::
     StructField("nFG", DoubleType, true) ::
     StructField("nFT", DoubleType, true) ::
     StructField("n3P", DoubleType, true) ::
     StructField("nTRB", DoubleType, true) ::
     StructField("nAST", DoubleType, true) ::
     StructField("nSTL", DoubleType, true) ::
     StructField("nBLK", DoubleType, true) ::
     StructField("nTOV", DoubleType, true) ::
     StructField("nPTS", DoubleType, true) ::
     StructField("nTOT", DoubleType, true) :: Nil
)

//create data frame
val dfPlayersT=sqlContext.createDataFrame(nPlayer,schemaN)

//save all stats as a temp table
dfPlayersT.registerTempTable("tPlayers")

//calculate experience levels
val dfPlayers=sqlContext.sql("select age-min_age as exp,tPlayers.* from tPlayers join (select name,min(age)as min_age from tPlayers group by name) as t1 on tPlayers.name=t1.name order by tPlayers.name, exp ")

//save as table
dfPlayers.saveAsTable("Players")
//filteredStats.unpersist()

//********************
//ANALYSIS
//********************

//Calculate value by age + experience

//group data by player name
val pStats=nStats.map(x=>(x.name,(x.valueN,x.valueZ,x.age,x.year,x.statsZ))).groupByKey()
pStats.cache

//for each player, go through all the years and calculate the change in valueZ and valueN, save into two lists 
//one for age, one for experience
//exclude players who played in 1980 from experience, as we only have partial data for them
val excludeNames=dfPlayers.filter(dfPlayers("year")===1980).select(dfPlayers("name")).map(x=>x.mkString).toArray.mkString(",")

val pStats1=pStats.map{ case(name,stats) => 
     var last =0
     var deltaZ = 0.0
     var deltaN = 0.0
     var valueZ = 0.0
     var valueN = 0.0
     var exp = 0
     val aList = ListBuffer[(Int,Array[Double])]()
     val eList = ListBuffer[(Int,Array[Double])]()
     stats.foreach( z => {
          if (last>0){
               deltaN = z._1 - valueN
               deltaZ = z._2 - valueZ
          }else{
               deltaN = Double.NaN
               deltaZ = Double.NaN
          }
          valueN = z._1
          valueZ = z._2
          last = z._3
          aList += ((last, Array(valueZ,valueN,deltaZ,deltaN)))
          if (!excludeNames.contains(z._1)){
              eList += ((exp, Array(valueZ,valueN,deltaZ,deltaN)))
              exp += 1
          }
     })
     (aList,eList)
}

pStats1.cache


//********************
//compute age stats
//********************

//extract out the age list
val pStats2=pStats1.flatMap{case(x,y)=>x}

//create age data frame
val dfAge=processStatsAgeOrExperience(pStats2, "age")

//save as table
dfAge.saveAsTable("Age")

//extract out the experience list
val pStats3=pStats1.flatMap{case(x,y)=>y}

//create experience dataframe
val dfExperience=processStatsAgeOrExperience(pStats3,"Experience")

//save as table
dfExperience.saveAsTable("Experience")

pStats1.unpersist()
