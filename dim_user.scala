package datawarehouse
/*
增量抽取昨天的数据
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
//import java.util.UUID

object dim_user {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim_user").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)

    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url1 = "jdbc:mysql://192.168.11.25:3306/koalaplatform"
    val url2 = "jdbc:mysql://192.168.11.25:3306/cloudalarm"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val pl_userinfo = sqlContext.read.jdbc(url1, "pl_userinfo",prop)
    val t_sys_employee = sqlContext.read.jdbc(url2, "t_sys_employee", prop)
    pl_userinfo.registerTempTable("pl_userinfo")
    t_sys_employee.registerTempTable("t_sys_employee")
    //基础信息
    val  currentdate= LocalDate.now()
    //初次全量 左开右闭
    //val df =sqlContext.sql(s"select  a.ID as pfid,b.ID as caID,'0011' as sourceflag,a.* from  pl_userinfo a  inner join  t_sys_employee  b  on  a.ID=b.PLID  where     a.Updatetime<\'${currentdate}\'
    // a.ProductValue,a.PropertyCompany, a.PropertyContact, a.PropertyTel, a.mobileSystemName, a.mobileSystemVersion, a.mobileManufacturer, a.mobileModel, a.appVersion, a.appProduct, a.lastAppAccessTime from  pl_userinfo a  inner join  t_sys_employee  b  on  a.ID=b.PLID
     val df =sqlContext.sql(s"select distinct  a.ID as platformuserid,b.ID as cloudalarmuserid,'0011' as sourceflag,a.SID, md5(a.LoginName)  as  LoginName, a.PassWord, a.Name, a.NickName, a.Sex, md5(a.ContactTel) as ContactTel, md5(a.QQ) as QQ, a.Cornet, a.Image, md5(a.Email) as Email, a.UserType, a.OrgID  as  platformorgid, a.PostID  as platformpostid, a.OpenAccountID  as platformcustomid, a.Type, a.Status, a.StatusDate,  a.CreateTime, a.Updatetime,  a.isAdmin, a.IsMaintenance, a.MaintenancePost, a.Job_Type, a.Brief, a.Birthday, a.Address, a.WeChat, a.AliPay  from  pl_userinfo a  left join  t_sys_employee  b  on  a.ID=b.PLID  where     a.Updatetime<\'${currentdate}\' ")
    //增量   左右封闭区间
    val  lastdate=currentdate.minusDays(1)
    //val df =sqlContext.sql(s"select distinct  a.ID as platformuserid,b.ID as cloudalarmuserid,'0011' as sourceflag,a.SID, md5(a.LoginName)  as  LoginName, a.PassWord, a.Name, a.NickName, a.Sex, md5(a.ContactTel) as ContactTel, md5(a.QQ) as QQ, a.Cornet, a.Image, md5(a.Email) as Email, a.UserType, a.OrgID  as  platformorgid, a.PostID  as platformpostid, a.OpenAccountID  as  platformcustomid, a.Type, a.Status, a.StatusDate, a.CreateTime, a.Updatetime,  a.isAdmin, a.IsMaintenance, a.MaintenancePost, a.Job_Type, a.Brief, a.Birthday, a.Address,  a.WeChat, a.AliPay from  pl_userinfo a  left join  t_sys_employee  b  on  a.ID=b.PLID  where   a.Updatetime>= \'${lastdate}\' and  a.Updatetime<\'${currentdate}\' ")
    //MaintenancePost 之后的列 缺失
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_user")
    //df.write.mode(SaveMode.Overwrite).save("/data/hivetemp/dim_user")

    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_user/part*")
      .registerTempTable("dim_usertemp")


    //.collect.foreach(println)
   hiveContext.sql("use datawarehouse")
    //初始化  全量
    hiveContext.sql("drop table if  exists   datawarehouse.dim_user")
    hiveContext.sql("create table if not exists  datawarehouse.dim_user as  select  'current'  as rowindicator,d.guid as   dwuserid,c.* from   dim_usertemp c inner join  dict_dwguid  d  on  c.platformuserid=d.id  ")
    //无法增量
    //hiveContext.sql("create table if not exists  datawarehouse.dim_user as  select     Row_Number() OVER(order  by   platformuserid) as userkey,*  from   dim_usertemp   ")
    //hiveContext.sql("create table if not exists  dim_user as  select  b.guid as dwuserid,a.pfid,a.caID,a.sourceflag,a.SID,a.LoginName, a.PassWord, a.Name, a.NickName, a.Sex, a.ContactTel, a.QQ, a.Cornet, a.Image, a.Email, a.UserType, a.OrgID, a.PostID, a.PostName, a.OpenAccountID, a.Type, a.Status, a.StatusDate, a.InsertUserId, a.InsertUser, a.CreateTime, a.UpdateUserId, a.UpdateUser, a.Updatetime, a.ProductValue, a.isAdmin, a.IsMaintenance, a.MaintenancePost, a.Job_Type, a.Brief, a.Birthday, a.Address, a.PropertyCompany, a.PropertyContact, a.PropertyTel, a.WeChat, a.AliPay, a.mobileSystemName, a.mobileSystemVersion, a.mobileManufacturer, a.mobileModel, a.appVersion, a.appProduct, a.lastAppAccessTime from   dim_usertemp a  join  dict_guid  b  on a.pfid=b.id  ")
    //增量
    //val data=hiveContext.sql("select  d.guid as dwuserid,c.*  from   dim_usertemp c inner join  dict_dwguid  d  on  c.platformuserid=d.id")
    //data.write.mode("append").saveAsTable("dim_user")

    //无须再过滤
    //where  to_date(LASTMODIFY)>=DATE_SUB(date(current_timestamp()),100)  and  to_date(LASTMODIFY)<date(current_timestamp())  ")
    //println(s"$currentdate")
    //println(s"$lastdate")

    sc.stop

  }
}
