package datawarehouse
/*
布防状态事实表
次处代码不再重复使用
datamarket.deploystatus
生成
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object fact_deploystatus {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("fact_deploystatus").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import hiveContext.implicits._
    import hiveContext._
    //
    val  currentdate= LocalDate.now()
    //hive
    /*
    hiveContext.sql("use datawarehouse")
    hiveContext.sql(s"  select  distinct cloudalarmcustomid,cloudalarmorgid,clientnumber,acttime,originalstatus  from  fact_deploywithdraw    where  actdate=\'$day\'  ")
      .registerTempTable("custom_deploy")


    //sql(s"create table  default.time2d22 as
    //sql(s"select daytime from dict_calendar  where  daytime>=\'$day\'  and  daytime<date_add(\'$day\',1)   ")
    sql(s"select daytime from dict_calendar  where  daytime=date_add(\'$day\',1)    ")
      .registerTempTable("time")


    sql(s"select distinct cloudalarmcustomid,cloudalarmorgid,clientnumber,\'${lastday2}\'   as  acttime,1 as originalstatus from custom_deploy")
      .registerTempTable("prepare")

    sql("select * from custom_deploy  union select * from prepare")
      .registerTempTable("combine")

    //sql("drop table default.big_join")    create table default.big_join as
    sql(s" select  A.cloudalarmcustomid,A.cloudalarmorgid,A.clientnumber,B.daytime as  timenode,max(A.acttime) as acttime from combine A join  time B   where  A.acttime<B.daytime  group by A.cloudalarmcustomid,A.cloudalarmorgid,A.clientnumber,B.daytime ")
      .registerTempTable("big_join")

    //sql("drop table default.select_last_action")   create table default.select_last_action as
    sql(s" select A.cloudalarmcustomid,A.cloudalarmorgid,A.clientnumber,A.timenode,A.acttime,B.originalstatus from  big_join A  inner join combine B  on A.cloudalarmcustomid=B.cloudalarmcustomid  AND A.cloudalarmorgid = B.cloudalarmorgid AND A.clientnumber = B.clientnumber AND A.acttime = B.acttime")
      .registerTempTable("select_last_action")

    //sql("drop table default.E_R")  create table  default.E_R  as
    sql("select cloudalarmcustomid,cloudalarmorgid,timenode,sign(sum(originalstatus-1)) as deploystatus  from  select_last_action group by cloudalarmcustomid,cloudalarmorgid,timenode")
      .registerTempTable("E_R")


    hiveContext.sql("use datawarehouse")
    hiveContext.sql("drop table if  exists   fact_deploystatus")
    hiveContext.sql("create table if not exists fact_deploystatus as  select    * from   E_R ")


    //val data=hiveContext.sql("select    * from  E_R ")
    //data.write.mode("append").saveAsTable("fact_deploystatus")




    sc.stop
    */
  }

}

