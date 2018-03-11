package datawarehouse
import java.time.LocalDate
/*
dict
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

object dict {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dict").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    //sparkConf.set("spark.driver.maxResultSize", "8g")
    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url = "jdbc:mysql://192.168.11.25:3306/test"
    //val url1 = "jdbc:mysql://192.168.11.25:3306/test"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    //prop.setProperty("autoReconnect", "true")
    //val dict_calendar = sqlContext.read.jdbc(url, "dict_calendar", prop)
    //dict_calendar.registerTempTable("dict_calendar")

    //val  dict_dbinfo=sqlContext.read.jdbc(url, "dict_dbinfo", prop)
    //dict_dbinfo.registerTempTable("dict_dbinfo")

    val  dict_guid=sqlContext.read.jdbc(url, "dict_guid2", prop)
    dict_guid.registerTempTable("dict_dwguid")
    //基础信息
    //val currentdate = LocalDate.now()
    //初次全量  左闭右开
    //val df = sqlContext.sql(s" SELECT  *  from  dict_calendar ")
    //val df1 = sqlContext.sql(s" SELECT  *  from  dict_dbinfo ")
    val df2 = sqlContext.sql(s" SELECT  *  from  dict_dwguid ")
    //.collect.foreach(println)
    //增量     左闭右开
    //val lastdate = currentdate.minusDays(1)

   // df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dict_calendar")
    //df1.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dict_dbinfo")
    df2.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dict_dwguid")

    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dict_calendar/part*")
     // .registerTempTable("dict_calendartemp")

    //hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dict_dbinfo/part*")
     // .registerTempTable("dict_dbinfotemp")

    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dict_dwguid/part*")
    //hiveContext.read.text("hdfs://192.168.11.21:8020/data/hive/warehouse/datawarehouse.db/dict_dwguid/part*")
      .registerTempTable("dict_dwguidtemp")

    hiveContext.sql("use  datawarehouse")
    //初始化  全量
    //hiveContext.sql("drop table if  exists   dict_calendar ")
    //hiveContext.sql("create table if not exists dict_calendar as  select  * from   dict_calendartemp")
    //
    //hiveContext.sql("drop table if  exists   dict_dbinfo ")
   // hiveContext.sql("create table if not exists dict_dbinfo as  select  * from   dict_dbinfotemp")

    //
    hiveContext.sql("drop table if  exists   dict_dwguid ")
    hiveContext.sql("create table if not exists dict_dwguid as  select  * from   dict_dwguidtemp")


    sc.stop

  }
}
