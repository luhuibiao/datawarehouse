package datawarehouse
/*

 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object dim {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    //sparkConf.set("spark.driver.maxResultSize", "8g")
    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url = "jdbc:mysql://192.168.11.25:3306/cloudalarm"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val url1 = "jdbc:mysql://192.168.11.25:3306/koalaplatform"
    val prop1 = new java.util.Properties
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "ty123456")
    val t_alarm_alarminfo = sqlContext.read.jdbc(url, "t_alarm_alarminfo", prop)
    val t_alarm_sectoractivitylog = sqlContext.read.jdbc(url, "t_alarm_sectoractivitylog", prop)
    val t_alarm_alarmreply = sqlContext.read.jdbc(url, "t_alarm_alarmreply", prop)
    val t_useractionlog = sqlContext.read.jdbc(url1, "t_useractionlog", prop1)
    //val t_sys_code_items = sqlContext.read.jdbc(url, "t_sys_code_items",prop)
    //val t_sys_code_category = sqlContext.read.jdbc(url, "t_sys_code_category",prop)
    t_alarm_alarminfo.registerTempTable("t_alarm_alarminfo")
    t_alarm_sectoractivitylog.registerTempTable("t_alarm_sectoractivitylog")
    t_alarm_alarmreply.registerTempTable("t_alarm_alarmreply")
    t_useractionlog.registerTempTable("t_useractionlog")
    //t_sys_code_items.registerTempTable("t_sys_code_items")
    //t_sys_code_category.registerTempTable("t_sys_code_category")


    //基础信息
    val currentdate = LocalDate.now()
    //初次全量  左闭右开
    val df = sqlContext.sql(s"SELECT ID as actionid,USERID,ACTTYPE,MEMO,'0011' as  sourceflag  from  t_useractionlog  where      LASTMODIFY<\'${currentdate}\' ")
    val df1 = sqlContext.sql(s"SELECT ID as replyid,ALARMID,(case FROMTYPE when 0 then '考拉'  when  1  then '中心外部'  end) as fromtype,MEMO,'0012' as sourceflag  from  t_alarm_alarmreply  where      LASTMODIFY<\'${currentdate}\' ")
    val df2 = sqlContext.sql(s"SELECT ID as alarmid,EXT_ALARMTYPE,EXT_ALARMDETAIL,EXT_CLIENTID,EXT_ERFLAG,EXT_ZONEID,EXT_OPERNAME,EXT_RESULT,EXT_PRERESULT,EXT_NOTE,EXT_HANDLETIME,EXT_FRT,EXT_FGT,EXT_FAT,EXT_FOT,EXT_FGR,EXT_FGM,EXT_APRI,FromCustomID,LONGITUDE,LATITUDE,POSITIONERROR,DISPOSESTATUS,ALARMTYEPE,ALARMPEOPLEID,Original_AlarmType,Original_AlarmCode,Alarm_Position,DataType,'0012' as sourceflag  from  t_alarm_alarminfo  where      LASTMODIFY<\'${currentdate}\' ")
    //.collect.foreach(println)
    //增量     左闭右开
    val lastdate = currentdate.minusDays(1)
    //val df =sqlContext.sql(s"SELECT ID as actionid,USERID,ACTTYPE,MEMO  from  t_useractionlog  where  LASTMODIFY>=\'${lastdate}\'  and     LASTMODIFY<\'${currentdate}\' ")
    //val df1=sqlContext.sql(s"SELECT ID as replyid,ALARMID,(case FROMTYPE when 0 then '考拉'  when  1  then '中心外部'  end) as fromtype,MEMO  from  t_useractionlog  where    LASTMODIFY>=\'${lastdate}\'  and   LASTMODIFY<\'${currentdate}\' ")
    //val  df2=  sqlContext.sql(s"SELECT ID as alarmid,EXT_ALARMTYPE,EXT_ALARMDETAIL,EXT_CLIENTID,EXT_ERFLAG,EXT_ZONEID,EXT_OPERNAME,EXT_RESULT,EXT_PRERESULT,EXT_NOTE,EXT_HANDLETIME,EXT_FRT,EXT_FGT,EXT_FAT,EXT_FOT,EXT_FGR,EXT_FGM,EXT_APRI,FromCustomID,LONGITUDE,LATITUDE,POSITIONERROR,DISPOSESTATUS,ALARMTYEPE,ALARMPEOPLEID,Original_AlarmType,Original_AlarmCode,Alarm_Position,DataType,'0012' as sourceflag  from  t_alarm_alarminfo  where   LASTMODIFY>=\'${lastdate}\'  and   LASTMODIFY<\'${currentdate}\' ")
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_useraction")
    df1.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_alarmreply")
    df2.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_alarm")
    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_useraction/part*")
      .registerTempTable("dim_useractiontemp")

    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_alarmreply/part*")
      .registerTempTable("dim_alarmreplytemp")

    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_alarm/part*")
      .registerTempTable("dim_alarmtemp")

    hiveContext.sql("use datawarehouse")
    //初始化  全量
    hiveContext.sql("drop table if  exists   datawarehouse.dim_useraction")
    hiveContext.sql("create table if not exists  datawarehouse.dim_useraction as  select  * from   dim_useractiontemp")

    hiveContext.sql("drop table if  exists   datawarehouse.dim_alarmreply")
    hiveContext.sql("create table if not exists  datawarehouse.dim_alarmreply as  select  * from   dim_alarmreplytemp")

    hiveContext.sql("drop table if  exists   datawarehouse.dim_alarm")
    hiveContext.sql("create table if not exists  datawarehouse.dim_alarm as  select  * from   dim_alarmtemp")

    //增量
    //val data=hiveContext.sql("select  * from   dim_useractiontemp")
    //data.write.mode("append").saveAsTable("dim_useraction")
    //val data1=hiveContext.sql("select  * from   dim_alarmreplytemp")
    //data1.write.mode("append").saveAsTable("dim_alarmreply")
    //val data2=hiveContext.sql("select  * from   dim_alarmtemp")
    //data2.write.mode("append").saveAsTable("dim_alarm")

    sc.stop


}

}
