package datawarehouse
/*
增量抽取昨天的数据
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object dim_organization {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim_organization").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)

    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url1 = "jdbc:mysql://192.168.11.25:3306/koalaplatform"
    val url2 = "jdbc:mysql://192.168.11.25:3306/cloudalarm"
    val url3 = "jdbc:mysql://192.168.11.25:3306/tycloud2"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val pl_organization = sqlContext.read.jdbc(url1, "pl_organization",prop)
    val pl_codeitems = sqlContext.read.jdbc(url1, "pl_codeitems",prop)
    val pl_codecategory = sqlContext.read.jdbc(url1, "pl_codecategory",prop)
    val t_sys_organization = sqlContext.read.jdbc(url2, "t_sys_organization", prop)
    val ts_organizationinfo = sqlContext.read.jdbc(url3, "ts_organizationinfo", prop)
    pl_organization.registerTempTable("pl_organization")
    t_sys_organization.registerTempTable("t_sys_organization")
    ts_organizationinfo.registerTempTable("ts_organizationinfo")
    pl_codeitems.registerTempTable("pl_codeitems")
    pl_codecategory.registerTempTable("pl_codecategory")

    //基础信息
    val  currentdate= LocalDate.now()
    //初次全量 左开右闭
    //val df =sqlContext.sql(s"select  a.ID as pfid,b.ID as caID,'0011' as sourceflag,a.* from  pl_userinfo a  inner join  t_sys_employee  b  on  a.ID=b.PLID  where     a.Updatetime<\'${currentdate}\' ")
    val df =sqlContext.sql(s"select  org.ID as platformorgid,b.ID as cloudalarmorgid,c.OI_ID_INT as serviceorgid,org.OpenAccountID as platformcustomid,org.ParentID as  platformparentid,b.PARENT_ID as cloudalarmparentid,b.CUSTOMID  as  cloudalarmcustomid,orgLevelCODE,orgLevelNAME,orgTypeCODE,orgTypeNAME,org.*   from   pl_organization org  inner join  t_sys_organization  b  on  b.PLID=org.ID " +
      s"  inner join  ts_organizationinfo  c on  c.PLID=org.ID "+
      s" LEFT JOIN (  	SELECT  		item.Code AS orgLevelCODE,  		item.Name AS orgLevelNAME,  		category.OpenAccountID AS platformCUSTOMID  	FROM  		pl_codeitems item  	INNER JOIN pl_codecategory category ON category.ID = item.CategoryID  	AND category.CategoryCode = 'hierarchyType'  	AND item.IsVisible = 'T'  ) orgLevel ON org.Type = orgLevel.orgLevelCODE  AND org.OpenAccountID = orgLevel.platformCUSTOMID " +
      s" LEFT JOIN (  	SELECT  		item.Code AS orgTypeCODE,  		item.Name AS orgTypeNAME,  		category.OpenAccountID AS platformCUSTOMID  	FROM  		pl_codeitems item  	INNER JOIN pl_codecategory category ON category.ID = item.CategoryID  	AND category.CategoryCode = 'organizationType'  	AND item.IsVisible = 'T'  ) orgType ON org.OrgType = orgType.orgTypeCODE  AND org.OpenAccountID = orgType.platformCUSTOMID " +
      s" where     org.Updatetime<\'${currentdate}\' ")
    //增量   左右封闭区间
    //val  lastdate=currentdate.minusDays(1)
    //val df =sqlContext.sql(s"select  a.ID as platformorgid,b.ID as cloudalarmorgid,'0011' as sourceflag,a.*  from   pl_organization a  left join  t_sys_organization  b  on  b.PLID=a.ID  where   a.Updatetime>= \'${lastdate}\' and  a.Updatetime<\'${currentdate}\' ")
    //列
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_organization")


    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_organization/part*")
      .registerTempTable("dim_organizationtemp")



    hiveContext.sql("use datawarehouse")
    //初始化  全量
    hiveContext.sql("drop table if  exists   dim_organization")
    hiveContext.sql("create table if not exists  dim_organization as  select  '0011' as sourceflag,'current'  as rowindicator,d.guid as dworgid,c.*,substr(level,1,4) as orglevel4,substr(level,1,8) as  orglevel8,substr(level,1,12) as  orglevel12,substr(level,1,16) as  orglevel16  from   dim_organizationtemp c inner join  dict_dwguid  d  on  c.platformorgid=d.id  ")
    //增量
    //val data=hiveContext.sql("select  d.guid as dworgid,c.* from   dim_organizationtemp c inner join  dict_guid  d  on  c.platformorgid=d.id  ")
    //data.write.mode("append").saveAsTable("dim_organization")



    sc.stop

}
}
