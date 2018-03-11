package datawarehouse
/*
增量抽取昨天的数据
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate

object dim_custom {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim_custom").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)

    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url1 = "jdbc:mysql://192.168.11.25:3306/koalaplatform"
    val url2 = "jdbc:mysql://192.168.11.25:3306/cloudalarm"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val pl_openaccount = sqlContext.read.jdbc(url1, "pl_openaccount",prop)
    val t_sys_custominfo = sqlContext.read.jdbc(url2, "t_sys_custominfo", prop)
    val pl_contract_app_resource_rel = sqlContext.read.jdbc(url1, "pl_contract_app_resource_rel",prop)
    val pl_app_resource = sqlContext.read.jdbc(url1, "pl_app_resource", prop)

    pl_openaccount.registerTempTable("pl_openaccount")
    t_sys_custominfo.registerTempTable("t_sys_custominfo")
    pl_contract_app_resource_rel.registerTempTable("pl_contract_app_resource_rel")
    pl_app_resource.registerTempTable("pl_app_resource")


    //基础信息
    val  currentdate= LocalDate.now()
    //初次全量 左开右闭
    //val df =sqlContext.sql(s"select  a.ID as pfid,b.ID as caID,'0011' as sourceflag,a.* from  pl_userinfo a  left join  t_sys_employee  b  on  a.ID=b.PLID  where     a.Updatetime<\'${currentdate}\' ")
    val df =sqlContext.sql(s"select distinct     resource.resource_name as  resourcename,resource.resource_code as resourcecode,resource.resource_desc as resourcedesc,a.ID as platformcustomid,b.ID as cloudalarmcustomid,'0011' as sourceflag,a.AccountName as customname,a.*  from   pl_openaccount a  left join  t_sys_custominfo  b  on  b.PLID=a.ID left join  	pl_contract_app_resource_rel  rel   on  a.ID=rel.open_account_id  JOIN   pl_app_resource  resource  ON     concat(',',app_resource_ids,',')    like  concat('%,' ,resource.id ,',%')       where     a.Updatetime<\'${currentdate}\' ")
    //增量   左右封闭区间
    val  lastdate=currentdate.minusDays(1)
    //val df =sqlContext.sql(s"select distinct a.ID as platformcustomid,b.ID as cloudalarmcustomid,'0011' as sourceflag,a.*  from   pl_openaccount a  left join  t_sys_custominfo  b  on  b.PLID=a.ID  where   a.Updatetime>= \'${lastdate}\' and  a.Updatetime<\'${currentdate}\' ")
    //列 缺失
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_custom")
    //df.write.mode(SaveMode.Overwrite).save("/data/hivetemp/dim_user")

    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_custom/part*")
      //hiveContext.read.load("/data/hivetemp/dim_user/*.parquet")
      .registerTempTable("dim_customtemp")
    //hiveContext.sql("create table  default.user2 as select * from  dim_usertemp")
    //hiveContext.sql("select  *  from dim_custom ")
    //.collect.foreach(println)
    hiveContext.sql("use datawarehouse")
    //初始化  全量
    hiveContext.sql("drop table if  exists   dim_custom")
    hiveContext.sql("create table if not exists  dim_custom as  select  'current'  as rowindicator,d.guid as dwcustomid,c.* from   dim_customtemp c inner join  dict_dwguid  d  on  c.platformcustomid=d.id  ")
    //增量
    //val data=hiveContext.sql("select  d.guid as dwcustomid,c.* from   dim_customtemp c inner join  dict_guid  d  on  c.platformcustomid=d.id  ")
    //data.write.mode("append").saveAsTable("dim_user")



    sc.stop

  }
}
