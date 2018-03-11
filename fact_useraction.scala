package datawarehouse
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object fact_useraction {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("fact_useraction").setMaster("spark://192.168.11.21:7077")
    //sparkConf.set("spark.sql.inMemoryColumnStorage.batchSize", "10000")
    //sparkConf.set("spark.sql.codegen", "true")
    //sparkConf.set("spark.sql.inMemoryColumnStorage.compressed", "true")

    val sc = new SparkContext(sparkConf)

    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url = "jdbc:mysql://192.168.11.25:3306/test"
    //val url = "jdbc:mysql://192.168.11.25:3306/test"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    //val pl_userinfo = sqlContext.read.jdbc(url, "pl_userinfo",prop)
    //val pl_organization = sqlContext.read.jdbc(url, "pl_organization",prop)
    //val pl_openaccount = sqlContext.read.jdbc(url, "pl_openaccount",prop)
    //val t_useractionlog = sqlContext.read.jdbc(url, "t_useractionlog20170831",prop)
    //val t_useractionlog = sqlContext.read.jdbc(url, "t_useractionlogfrom20170901to20180228",prop)
    val t_useractionlog = sqlContext.read.jdbc(url, "t_useractionlog",prop)
    //val pl_post = sqlContext.read.jdbc(url, "pl_post",prop)
    //pl_userinfo.registerTempTable("pl_userinfo")
    //pl_organization.registerTempTable("pl_organization")
    //pl_openaccount.registerTempTable("pl_openaccount")
    t_useractionlog.registerTempTable("t_useractionlog")
    //pl_post.registerTempTable("pl_post")
    //基础信息
    val  currentdate= LocalDate.now()
    //初次全量 左开右闭
    //val df =sqlContext.sql(s"select   account.AccountName,org.OrgName,userinfo.ID  as  USERID,  userinfo.Name as UserName,post.PostName,  userinfo.CreateTime,userinfo.StatusDate as canceltime,  userinfo.Status,  (case  userinfo.Status  when 1  then '离职'  else '在职'  end)  AS ISONPost,action.ID as ACTIONID,action.ACTDATE  as  actiontime,  action.LASTMODIFY as  actionLASTMODIFY,'0011' as sourceflag   from    	pl_userinfo   userinfo  left  join    	pl_organization org   on  userinfo.OrgID=org.ID     join  pl_openaccount  account  on  org.OpenAccountID=account.ID   	left join  pl_post  post  on   post.ID=userinfo.PostID  left  join  t_useractionlog  action  on  userinfo.ID=action.USERID   where userinfo.Status=0  and  org.Status=0  ")
    //val df =sqlContext.sql(s"select   account.ID  as  platformcustomid,org.ID  as platformorgid,userinfo.ID  as  platformUSERID,   userinfo.CreateTime,userinfo.StatusDate as canceltime,  userinfo.Status,  (case  userinfo.Status  when 1  then '离职'  else '在职'  end)  AS ISONPost,action.ID as ACTIONID,action.ACTDATE  as  actiontime, action.ACTTYPE  as actiontype,action.MEMO as actionmemo, action.LASTMODIFY as  actionLASTMODIFY,'0011' as sourceflag   from    	pl_userinfo   userinfo  left  join    	pl_organization org   on  userinfo.OrgID=org.ID     join  pl_openaccount  account  on  org.OpenAccountID=account.ID   	left join  pl_post  post  on   post.ID=userinfo.PostID  left  join  t_useractionlog  action  on  userinfo.ID=action.USERID   where userinfo.Status=0  and  org.Status=0  ")
    val df =sqlContext.sql(s" SELECT      '0011' AS sourceflag,action.USERID as  platformuserid,    	action.ID  as  platformactionid,    	action.ACTDATE AS actiontime,    	action.ACTTYPE AS actiontype,    	action.MEMO AS actionmemo,    	action.LASTMODIFY AS actionLASTMODIFY    FROM t_useractionlog action  where   action.LASTMODIFY<\'${currentdate}\' ")
    // where  action.LASTMODIFY>='2017-01-01 00:00:00'  and  action.LASTMODIFY<\'${currentdate}\' ")
    //val df =sqlContext.sql(s" SELECT      action.USERID as  platformuserid,    	action.ID  as  platformactionid,    	action.ACTDATE AS actiontime,    	action.ACTTYPE AS actiontype,    	action.MEMO AS actionmemo,    	action.LASTMODIFY AS actionLASTMODIFY    FROM t_useractionlog action  where  action.LASTMODIFY>='2018-02-01 00:00:00'  and  action.LASTMODIFY<'2018-03-01 00:00:00'   ")
    //val df =sqlContext.sql(s" SELECT      '0011' AS sourceflag,action.USERID as  platformuserid,    	action.ID  as  platformactionid,    	action.ACTDATE AS actiontime,    	action.ACTTYPE AS actiontype,    	action.MEMO AS actionmemo,    	action.LASTMODIFY AS actionLASTMODIFY    FROM t_useractionlog action  where  action.ID>=404870  ")
    //增量   左右封闭区间
    val  lastdate=currentdate.minusDays(1)
    //val df =sqlContext.sql(s" SELECT      '0011' AS sourceflag,action.USERID as  platformuserid,    	action.ID  as  platformactionid,    	action.ACTDATE AS actiontime,    	action.ACTTYPE AS actiontype,    	action.MEMO AS actionmemo,    	action.LASTMODIFY AS actionLASTMODIFY    FROM t_useractionlog action  where    action.LASTMODIFY>=\'${lastdate}\' and     action.LASTMODIFY<\'${currentdate}\' ")
    //
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/fact_useraction")
    //df.write.mode(SaveMode.Overwrite).save("/data/hivetemp/dim_user")

    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/fact_useraction/part*")
      .registerTempTable("fact_useractiontemp")

    hiveContext.sql("use datawarehouse")
    //初始化  全量
    //hiveContext.sql("drop table if  exists   fact_useraction")
    //hiveContext.sql("create table if not exists  datawarehouse.fact_useraction as  select  * from  fact_useractiontemp  ")
    //hiveContext.sql("create table if not exists  fact_useraction as  SELECT    account.dwcustomid,    org.dworgid, userinfo.dwUSERID,  userinfo.CreateTime,   userinfo.StatusDate AS canceltime,    userinfo.STATUS,    (    CASE userinfo. STATUS    WHEN 1 THEN    '离职'    ELSE    '在职'    END    ) AS ISONPost,    action.ACTIONID,    action.actiontime,    action.actiontype,    action.actionmemo,    action.actionLASTMODIFY,    '0011' AS sourceflag    FROM    dim_user userinfo    LEFT JOIN dim_organization org ON userinfo.platformorgid = org.platformorgid    JOIN dim_custom account ON org.platformcustomid = account.platformcustomid    LEFT JOIN dim_post post ON post.platformpostid = userinfo.platformpostid    LEFT JOIN    fact_useractiontemp action ON userinfo.platformuserid = action.USERID   ")
    //hiveContext.sql("create table if not exists  fact_useraction as  SELECT    userinfo.dwUSERID,action.*,'0011' AS sourceflag    FROM    dim_user userinfo    JOIN    fact_useractiontemp action ON userinfo.platformuserid = action.platformuserid   ")
    //hiveContext.sql("create table if not exists  fact_useraction as  SELECT    *  FROM      fact_useractiontemp    ")
    //增量
    val data=hiveContext.sql("select  * from  fact_useractiontemp  ")
    data.write.mode("append").saveAsTable("fact_useraction")



    sc.stop

  }
}
