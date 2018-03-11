package datawarehouse
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object dim_sector {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim_sector").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)

    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    val url2 = "jdbc:mysql://192.168.11.25:3306/cloudalarm"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")

    val t_alarm_sectorinfo = sqlContext.read.jdbc(url2, "t_alarm_sectorinfo", prop)

    t_alarm_sectorinfo.registerTempTable("t_alarm_sectorinfo")
    //基础信息
    val  currentdate= LocalDate.now()
    //初次全量 左开右闭
    //val df =sqlContext.sql(s"select  a.ID as pfid,b.ID as caID,'0011' as sourceflag,a.* from  pl_userinfo a  left join  t_sys_employee  b  on  a.ID=b.PLID  where     a.Updatetime<\'${currentdate}\' ")
    val df =sqlContext.sql(s"select distinct a.ID as  cloudalarmsectorid,'0012' as sourceflag,a.ORGANIZATIONID as cloudalarmorgid,a.EXT_CLIENTID as clientnumber,a.EXT_ZONEID as zoneid,a.EXT_NAME as name,a.EXT_ADDR as address,a.CUSTOMID as cloudalarmcustomid,a.SET_PROBE,a.DataType   from   t_alarm_sectorinfo a    where     a.LASTMODIFY<\'${currentdate}\' ")
    //增量   左右封闭区间
    val  lastdate=currentdate.minusDays(1)
    //val df =sqlContext.sql(s"select distinct a.ID as platformcustomid,b.ID as cloudalarmcustomid,'0011' as sourceflag,a.*  from   pl_openaccount a  left join  t_sys_custominfo  b  on  b.PLID=a.ID  where   a.Updatetime>= \'${lastdate}\' and  a.Updatetime<\'${currentdate}\' ")
    //列
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_sector")


    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_sector/part*")
      .registerTempTable("dim_sectortemp")

    hiveContext.sql("use datawarehouse")
    //初始化  全量
    hiveContext.sql("drop table if  exists   dim_sector")
    hiveContext.sql("create table if not exists  dim_sector as  select  'current'  as rowindicator,d.guid as dwsectorid,c.* from   dim_sectortemp c inner join  dict_dwguid  d  on  c.cloudalarmsectorid=d.id  ")
    //增量
    //val data=hiveContext.sql("select  d.guid as dwcustomid,c.* from   dim_customtemp c inner join  dict_guid  d  on  c.platformcustomid=d.id  ")
    //data.write.mode("append").saveAsTable("dim_sector")



    sc.stop

  }

}
