package datawarehouse
/*
第四层  传统意义上的网点
之上 是 二级行
再 之上 是  分支行
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object dim_alarmbranchnet {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim_alarmbranchnet").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url = "jdbc:mysql://192.168.11.25:3306/cloudalarm"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val t_sys_organization = sqlContext.read.jdbc(url, "t_sys_organization", prop)
    val t_sys_custominfo = sqlContext.read.jdbc(url, "t_sys_custominfo",prop)
    val t_sys_code_items = sqlContext.read.jdbc(url, "t_sys_code_items",prop)
    val t_sys_code_category = sqlContext.read.jdbc(url, "t_sys_code_category",prop)
    t_sys_organization.registerTempTable("t_sys_organization")
    t_sys_custominfo.registerTempTable("t_sys_custominfo")
    t_sys_code_items.registerTempTable("t_sys_code_items")
    t_sys_code_category.registerTempTable("t_sys_code_category")

    //????
    val  currentdate= LocalDate.now()
    //????  ????
    val df =sqlContext.sql(s"select  '0012' as  Sourceflag,m.customId,m.customName,m.orgId,m.orgName,m.orglevel,m.orgplid,m.TYPE  branchnettype,n.PARENT_ID secondnetid,n.NAME  secondnetname,n.TYPE  secondnettype,t.PARENT_ID branchid,t.NAME branchname,t.TYPE   branchtype from  (SELECT custom.ID AS customId,custom.CUSTOMNAME AS customName,org.ID AS orgId,org.NAME AS orgName,org.LEVEL as  orglevel,org.PLID as  orgplid,org.TYPE,org.STATUS,org.UPDATE_DATE,orgLevel.NAME AS orgLevelName FROM  t_sys_organization org INNER JOIN t_sys_custominfo custom ON org.CUSTOMID = custom.ID  LEFT JOIN  (SELECT item.CODE AS CODE,item.NAME AS NAME,category.CUSTOMID AS CUSTOMID FROM t_sys_code_items item INNER JOIN t_sys_code_category category ON category.ID = item.CODE_CATEGORY_ID  AND category.CATEGORY_CODE = 'hierarchyType' AND item.IS_VISIBLE = 'T')   orgLevel ON org.TYPE = orgLevel.CODE AND org.CUSTOMID = orgLevel.CUSTOMID)  m   left  join   (select    b.PARENT_ID,b.ID,a.NAME,a.TYPE  from  t_sys_organization  a join  t_sys_organization  b  on a.ID=b.PARENT_ID )  n  on m.orgId=n.ID  left  join  (select    b.PARENT_ID,b.ID,a.NAME,a.TYPE  from  t_sys_organization  a join  t_sys_organization  b  on a.ID=b.PARENT_ID )  t  on  n.PARENT_ID=t.ID   WHERE  m.TYPE='04' and m.STATUS!=-1  ")
    //and  m.UPDATE_DATE<\'${currentdate}\' ")

    //.collect.foreach(println)
    //??     ??????
    //val  lastdate=currentdate.minusDays(1)
    //
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_alarmbranchnet")


    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_alarmbranchnet/part*")
      .registerTempTable("dim_alarmbranchnettemp")

    //.collect.foreach(println)
    hiveContext.sql("use datawarehouse")
    //???  ??
    hiveContext.sql("drop table if  exists  dim_alarmbranchnet")
    hiveContext.sql("create table if not exists  dim_alarmbranchnet as  select  * from   dim_alarmbranchnettemp")
    //??
    //val data=hiveContext.sql("select  * from   dim_alarmbranchnettemp")
    //data.write.mode("append").saveAsTable("dim_alarmbranchnet")



    sc.stop

  }




}