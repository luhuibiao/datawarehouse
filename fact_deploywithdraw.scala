package datawarehouse
/*
全量是 截至到昨天的数据
增量是 昨天的数据
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object fact_deploywithdraw {
    def main(args: Array[String]) {
      val sparkConf = new SparkConf().setAppName("factdeploywithdraw").setMaster("spark://192.168.11.21:7077")
      val sc = new SparkContext(sparkConf)
      //sparkConf.set("spark.driver.maxResultSize", "8g")
      //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      val url = "jdbc:mysql://192.168.11.25:3306/cloudalarm"
      val prop = new java.util.Properties
      prop.setProperty("user", "root")
      prop.setProperty("password", "ty123456")
      val t_alarm_sectoractivitylog = sqlContext.read.jdbc(url, "t_alarm_sectoractivitylog", prop)
      //val t_sys_organization = sqlContext.read.jdbc(url, "t_sys_organization", prop)
      //val t_sys_custominfo = sqlContext.read.jdbc(url, "t_sys_custominfo", prop)
      val t_alarm_hostinfo = sqlContext.read.jdbc(url, "t_alarm_hostinfo", prop)
      //val t_alarm_alarmtyperelation = sqlContext.read.jdbc(url, "t_alarm_alarmtyperelation", prop)
      //val t_sys_code_items = sqlContext.read.jdbc(url, "t_sys_code_items", prop)
      //val t_sys_code_category = sqlContext.read.jdbc(url, "t_sys_code_category", prop)
      t_alarm_sectoractivitylog.registerTempTable("t_alarm_sectoractivitylog")
      //t_sys_organization.registerTempTable("t_sys_organization")
      //t_sys_custominfo.registerTempTable("t_sys_custominfo")
      t_alarm_hostinfo.registerTempTable("t_alarm_hostinfo")
      //t_alarm_alarmtyperelation.registerTempTable("t_alarm_alarmtyperelation")
      //t_sys_code_items.registerTempTable("t_sys_code_items")
      //t_sys_code_category.registerTempTable("t_sys_code_category")


      //基础信息
      val currentdate = LocalDate.now()
      //初次全量  左闭右开
      val df = sqlContext.sql(s" SELECT  	a.CUSTOMID AS  cloudalarmCustomID,  	a.ORGANIZATIONID AS cloudalarmOrgID,  	a.ID AS cloudalarmdeploywithdrawid,    b.ID  AS  cloudalarmhostid,  	a.EXT_CLIENTID AS ClientNumber,  	 	a.ACTDATE AS ACTTIME,  	a. STATUS AS originalStatus,a.LASTMODIFY  FROM  	t_alarm_sectoractivitylog a  JOIN t_alarm_hostinfo b ON a.EXT_CLIENTID = b.EXT_CLIENTID  AND a.CUSTOMID = b.CUSTOMID  WHERE   b.PROPERTY_INT != 1  AND a.LASTMODIFY <\'${currentdate}\'  ")
      //.collect.foreach(println)
      //增量     左闭右开
      val  lastdate=currentdate.minusDays(1)
      //val df =sqlContext.sql(s"SELECT c.CUSTOMID AS CaCustomID,c.CUSTOMNAME,a.ORGANIZATIONID AS CaOrgID,c.NAME AS OrgName,c.LEVEL AS OrgLEVEL,orgLevel.CODE as orgLevelCODE,orgLevel.NAME as orgLevelName,c.TYPE AS OrgType,orgType.CODE as  orgTypeCODE,     orgType.NAME as orgTypeName,a.ID  as deploywithdrawid,a.EXT_CLIENTID as ClientNumber,to_date(a.ACTDATE) as ACTDATE,a.ACTDATE as ACTTIME,a.STATUS as originalStatus,(case   a.STATUS  when 1 then '布防'  when 2 then  '撤防' end) as Status,'0012' as Sourceflag     from  t_alarm_sectoractivitylog  a   join  t_alarm_hostinfo  b     on a.EXT_CLIENTID=b.EXT_CLIENTID and a.CUSTOMID=b.CUSTOMID      JOIN   (SELECT  ab.ID,ab.TYPE,ab.LEVEL,ab.CUSTOMID,ab.NAME,ab.ORGANIZATIONTYPE,ab.STATUS,ac.CUSTOMNAME     from            t_sys_organization ab join      t_sys_custominfo ac on ab.CUSTOMID = ac.ID) c  ON a.ORGANIZATIONID=c.ID       and a.CUSTOMID=c.CUSTOMID        LEFT JOIN (SELECT  item.CODE,item.NAME,   category.CUSTOMID  FROM  t_sys_code_items item  INNER JOIN    t_sys_code_category category ON category.ID = item.CODE_CATEGORY_ID   AND category.CATEGORY_CODE = 'hierarchyType' AND item.IS_VISIBLE = 'T') orgLevel     ON c.TYPE=orgLevel.CODE  AND c.CUSTOMID=orgLevel.CUSTOMID   LEFT JOIN (SELECT  item.CODE,item.NAME,category.CUSTOMID  FROM   t_sys_code_items item INNER JOIN t_sys_code_category category ON category.ID = item.CODE_CATEGORY_ID   AND category.CATEGORY_CODE = 'organizationType'   AND item.IS_VISIBLE = 'T') orgType ON c.ORGANIZATIONTYPE = orgType.CODE   AND     c.CUSTOMID = orgType.CUSTOMID   where   c.STATUS!=-1  and   b.PROPERTY_INT!=1 and   a.LASTMODIFY>=\'${lastdate}\'  and  a.LASTMODIFY<\'${currentdate}\' ")
      //列 缺失
      df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/fact_deploywithdraw")
      //df.write.mode(SaveMode.Overwrite).save("/data/hivetemp/fact_inspect")

      //hive
      val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
      hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/fact_deploywithdraw/part*")
        //hiveContext.read.load("/data/hivetemp/dim_user/*.parquet")
        .registerTempTable("fact_deploywithdrawtemp")
      //hiveContext.sql("create table  default.user2 as select * from  dim_usertemp")
      //hiveContext.sql("select  *  from dim_custom ")
      //.collect.foreach(println)
      hiveContext.sql("use datawarehouse")
      //初始化  全量
      hiveContext.sql("drop table if  exists   datawarehouse.fact_deploywithdraw")
      hiveContext.sql("create table if not exists  datawarehouse.fact_deploywithdraw as  select  to_date (deploy.ACTTIME) AS ACTDATE,(case deploy.originalStatus  when 1 then '布防' when 2  then '撤防' end) as status,host.dwhostid,org.dworgid,'0012' AS Sourceflag,deploy.* from   fact_deploywithdrawtemp deploy  join   dim_organization org ON  deploy.cloudalarmorgid = org.cloudalarmorgid "+
       s" join  dim_host host on  deploy.clientnumber=host.clientnumber ")
        //增量
      //val data=hiveContext.sql("select  * from   fact_deploywithdrawtemp")
      //data.write.mode("append").saveAsTable("fact_deploywithdraw")


      sc.stop

    }




  }

