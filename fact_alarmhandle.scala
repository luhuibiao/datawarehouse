package datawarehouse

/*
报警 时间指标计算
报警处置
报警响应
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object fact_alarmhandle {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("fact_alarmhandle").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //
    val  currentdate= LocalDate.now()
    //hive
    hiveContext.sql("use datawarehouse")
    hiveContext.sql(s"  select  distinct cloudalarmalarmid,isfinish,alarmtime,alarmcreatetime,replytime,passivereplyusername from  fact_alarm  where     alarmLASTMODIFY<\'${currentdate}\'    ")
      .registerTempTable("factalarmhandletemp")



    //
    //val  lastdate=currentdate.minusDays(1)
    //hiveContext.sql(s"  select  distinct  cloudalarmalarmid,alarmtime,alarmcreatetime,replytime,passivereplyusername from  fact_alarm  where   alarmLASTMODIFY>=\'${lastdate}\'  and  alarmLASTMODIFY<\'${currentdate}\'  ")
      // .registerTempTable("factalarmhandletemp")

    hiveContext.sql(s"select  distinct  cloudalarmalarmid  from   factalarmhandletemp  ")
      .registerTempTable("handlealarm0")


    hiveContext.sql(s"  select    cloudalarmalarmid,min(replytime) as  replytime   from     factalarmhandletemp   where passivereplyusername='operateTime'  group by cloudalarmalarmid   ")
      .registerTempTable("onereply0")


    hiveContext.sql(s"  select  distinct    (datediff(b.replytime,alarmtime)*24*60)+(cast(hour(b.replytime)*60+minute(b.replytime)+second(b.replytime)/60 as bigint)   -cast(hour(alarmtime)*60+minute(alarmtime)+second(alarmtime)/60 as bigint)) as firsthandleduration,  (datediff(b.replytime,alarmcreatetime)*24*60)+  (cast(hour(b.replytime)*60+minute(b.replytime)+second(b.replytime)/60 as bigint)  -cast(hour(alarmcreatetime)*60+minute(alarmcreatetime)+second(alarmcreatetime)/60 as bigint)) as  firsthandleduration2,  a.cloudalarmalarmid,  b.replytime as  firsthandletime   from     factalarmhandletemp  a  join  onereply0 b on  a.cloudalarmalarmid=b.cloudalarmalarmid  where passivereplyusername='operateTime' ")
      .registerTempTable("onereply")

    hiveContext.sql(s"  select   cloudalarmalarmid,min(replytime) as  replytime   from     factalarmhandletemp   where isfinish=1  and  passivereplyusername='operateTime2'  group by cloudalarmalarmid  ")
      .registerTempTable("secondreply0")

    hiveContext.sql(s"  select  distinct    (datediff(d.replytime,alarmtime)*24*60)+(cast(hour(d.replytime)*60+minute(d.replytime)+second(d.replytime)/60 as bigint)   -cast(hour(alarmtime)*60+minute(alarmtime)+second(alarmtime)/60 as bigint)) as secondhandleduration,  (datediff(d.replytime,alarmcreatetime)*24*60)+  (cast(hour(d.replytime)*60+minute(d.replytime)+second(d.replytime)/60 as bigint)  -cast(hour(alarmcreatetime)*60+minute(alarmcreatetime)+second(alarmcreatetime)/60 as bigint)) as  secondhandleduration2,  c.cloudalarmalarmid,  d.replytime as  secondhandletime  from     factalarmhandletemp  c  join  secondreply0   d on  c.cloudalarmalarmid=d.cloudalarmalarmid  where isfinish=1  and  passivereplyusername='operateTime2' ")
      .registerTempTable("secondreply")

    hiveContext.sql(s" select   distinct     factalarmhandletemp.cloudalarmalarmid,response.responsetime,  (datediff(response.responsetime,alarmtime)*24*60)+(cast(hour(response.responsetime)*60+minute(response.responsetime)+second(response.responsetime)/60 as bigint)   -cast(hour(alarmtime)*60+minute(alarmtime)+second(alarmtime)/60 as bigint)) as responseduration,  (datediff(response.responsetime,alarmcreatetime)*24*60)+  (cast(hour(response.responsetime)*60+minute(response.responsetime)+second(response.responsetime)/60 as bigint)  -cast(hour(alarmcreatetime)*60+minute(alarmcreatetime)+second(alarmcreatetime)/60 as bigint)) as  responseduration2   from  factalarmhandletemp   join (select  cloudalarmalarmid,min(replytime) as responsetime     from  factalarmhandletemp  group  by  cloudalarmalarmid) response  on  factalarmhandletemp.cloudalarmalarmid=response.cloudalarmalarmid ")
      .registerTempTable("response2")


    hiveContext.sql(s" SELECT  distinct '0012' as  sourceflag,handlealarm0.cloudalarmalarmid,  " +
      s"   firsthandletime,(case  when firsthandleduration<0 then  firsthandleduration2 else firsthandleduration end) as firsthandleduration, "+
      s"   secondhandletime,(case  when  secondhandleduration<0  then secondhandleduration2  else  secondhandleduration  end)  as secondhandleduration, "+
      s"   responsetime,(case when responseduration<0 then responseduration2 else responseduration  end) as responseduration" +
      s"   from    handlealarm0 left  join  onereply  on  handlealarm0.cloudalarmalarmid=onereply.cloudalarmalarmid "+
      s"   left  join secondreply  on  handlealarm0.cloudalarmalarmid=secondreply.cloudalarmalarmid "+
      s"   left  join response2   on   handlealarm0.cloudalarmalarmid=response2.cloudalarmalarmid ")
      .registerTempTable("handlealarm2")


    hiveContext.sql("use datawarehouse")
    hiveContext.sql("drop table if  exists   fact_alarmhandle")
    hiveContext.sql("create table if not exists fact_alarmhandle as  select    * from   handlealarm2 ")


    //val data=hiveContext.sql("select    * from  handlealarm2 ")
    //data.write.mode("append").saveAsTable("fact_alarmhandle")




    sc.stop

  }

}
