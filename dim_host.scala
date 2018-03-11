package datawarehouse

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate

object dim_host {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim_host").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)

    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    val url2 = "jdbc:mysql://192.168.11.25:3306/cloudalarm"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")

    val t_alarm_hostinfo = sqlContext.read.jdbc(url2, "t_alarm_hostinfo", prop)

    t_alarm_hostinfo.registerTempTable("t_alarm_hostinfo")
    //基础信息
    val  currentdate= LocalDate.now()
    //初次全量 左开右闭

    val df =sqlContext.sql(s"select distinct a.ID as cloudalarmhostid,'0012' as sourceflag,a.ORGANIZATIONID as  cloudalarmorgid,a.EXT_CLIENTID as clientnumber,a.EXT_NAME as  name,a.EXT_TYPE  as type,a.EXT_CONTROLER as  controler,a.EXT_ADDRESS as address,a.CUSTOMID as  cloudalarmcustomid,a.PROPERTY_INT as  iskeyhost,a.SET_PROBE,a.DataType,a.DISABLE  from   t_alarm_hostinfo a    where     a.LASTMODIFY<\'${currentdate}\' ")
    //增量   左右封闭区间
    val  lastdate=currentdate.minusDays(1)
    //val df =sqlContext.sql(s"select distinct a.ID as platformcustomid,b.ID as cloudalarmcustomid,'0011' as sourceflag,a.*  from   pl_openaccount a  left join  t_sys_custominfo  b  on  b.PLID=a.ID  where   a.Updatetime>= \'${lastdate}\' and  a.Updatetime<\'${currentdate}\' ")
    //列
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_host")


    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_host/part*")
      .registerTempTable("dim_hosttemp")

    hiveContext.sql("use datawarehouse")
    //初始化  全量
    hiveContext.sql("drop table if  exists   dim_host")
    hiveContext.sql("create table if not exists  dim_host as  select  'current'  as rowindicator,d.guid as dwhostid,c.*,(case   iskeyhost  when 1 then '非关键主机'  else '关键主机'  end) as  keyhost   from   dim_hosttemp c inner join  dict_dwguid  d  on  c.cloudalarmhostid=d.id  ")
    //增量
    //val data=hiveContext.sql("select  d.guid as dwcustomid,c.* from   dim_hosttemp c inner join  dict_guid  d  on  c.platformcustomid=d.id  ")
    //data.write.mode("append").saveAsTable("dim_host")



    sc.stop

  }



}
