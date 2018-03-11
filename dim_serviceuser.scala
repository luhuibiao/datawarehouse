package datawarehouse
/*
服务维保人员信息
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object dim_serviceuser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim_serviceuser").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val url = "jdbc:mysql://192.168.11.25:3306/tycloud2"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val TS_TechnicianAreaRelation = sqlContext.read.jdbc(url, "TS_TechnicianAreaRelation", prop)
    val Global_UserInfo = sqlContext.read.jdbc(url, "Global_UserInfo", prop)
    val TS_CustomDutyContactRelation = sqlContext.read.jdbc(url, "TS_CustomDutyContactRelation", prop)
    val  global_userrolerelation=sqlContext.read.jdbc(url, "global_userrolerelation", prop)
    TS_TechnicianAreaRelation.registerTempTable("TS_TechnicianAreaRelation")
    TS_CustomDutyContactRelation.registerTempTable("TS_CustomDutyContactRelation")
    Global_UserInfo.registerTempTable("Global_UserInfo")
    global_userrolerelation.registerTempTable("global_userrolerelation")

    //
    val currentdate = LocalDate.now()
    //
    val df = sqlContext.sql(s" select  userinfo.UI_ID_INT as serviceuserid,"+
      s" area.COMPANY_ID as servicecompanyid,"+
      s" area.TAR_AREAID_INT  as  servicecompanyorgid,"+
      s" customrel.CDCR_CUSTOMID_INT as  servicedutybranchnetid,"+
      s" userinfo.*,rolerel.URR_ROLEID_INT  as  serviceroleid "+
      s" from   Global_UserInfo       userinfo"+
      s" left  join   TS_CustomDutyContactRelation    customrel"+
      s" on   userinfo.UI_ID_INT = customrel.CDCR_USERID_INT"+
      s" left  join  TS_TechnicianAreaRelation  area"+
      s" on   area.TAR_TECHNICIANID_INT=userinfo.UI_ID_INT "+
      s" left  join  global_userrolerelation  rolerel on   userinfo.UI_ID_INT=rolerel.URR_USERID_INT ")
    //where a.LASTMODIFY_DATE<\'${currentdate}\' ")
    //.collect.foreach(println)
    //
    val lastdate = currentdate.minusDays(1)
    //val df =sqlContext.sql(s"SELECT ID as actionid,USERID,ACTTYPE,MEMO  from  t_useractionlog  where  LASTMODIFY>=\'${lastdate}\'  and     LASTMODIFY<\'${currentdate}\' ")
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_serviceuser")
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_serviceuser/part*")
      .registerTempTable("dim_serviceusertemp")


    hiveContext.sql("use datawarehouse")
    //
    hiveContext.sql("drop table   if  exists   dim_serviceuser")
    hiveContext.sql("create table if not exists  dim_serviceuser as  select  	'0014' AS sourceflag,(case  UI_ORG_OR_COMPANY when 0 then '客户' when  1  then '工程商'  end)  as orgorcompany,UI_ORG_OR_COMPANY as  orgorcompanytype,*   from   dim_serviceusertemp")


    //
    // val data=hiveContext.sql("select  * from   dim_serviceusertemp")
    // data.write.mode("append").saveAsTable("dim_serviceuser")
    sc.stop

  }
}


