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
object fact_alarm {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("factalarm").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    //sparkConf.set("spark.driver.maxResultSize", "8g")
    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url = "jdbc:mysql://192.168.11.25:3306/cloudalarm"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val t_alarm_alarminfo = sqlContext.read.jdbc(url, "t_alarm_alarminfo",prop)
    //val t_sys_organization = sqlContext.read.jdbc(url, "t_sys_organization", prop)
    //val t_sys_custominfo = sqlContext.read.jdbc(url, "t_sys_custominfo",prop)
    val t_alarm_alarmreply = sqlContext.read.jdbc(url, "t_alarm_alarmreply", prop)
    val t_alarm_alarmtyperelation = sqlContext.read.jdbc(url, "t_alarm_alarmtyperelation",prop)
    //val t_sys_code_items = sqlContext.read.jdbc(url, "t_sys_code_items",prop)
    //val t_sys_code_category = sqlContext.read.jdbc(url, "t_sys_code_category",prop)
    t_alarm_alarminfo.registerTempTable("t_alarm_alarminfo")
    //t_sys_organization.registerTempTable("t_sys_organization")
    //t_sys_custominfo.registerTempTable("t_sys_custominfo")
    t_alarm_alarmreply.registerTempTable("t_alarm_alarmreply")
    t_alarm_alarmtyperelation.registerTempTable("t_alarm_alarmtyperelation")
    //t_sys_code_items.registerTempTable("t_sys_code_items")
    //t_sys_code_category.registerTempTable("t_sys_code_category")






    //基础信息
    val  currentdate= LocalDate.now()
    //初次全量  左闭右开  and d.CODEID is not null
    //val df =sqlContext.sql(s"SELECT    	b.CUSTOMID AS CaCustomID,    	b.CUSTOMNAME AS CUSTOMNAME,    	a.ORGANIZATIONID AS CaOrgID,    	b. NAME AS OrgName,    	b. LEVEL AS OrgLEVEL,    	a.ID AS alarmid,    	(    		CASE    		WHEN a.EXT_ALARMTIME IS NULL THEN    			a.CreateTime    		ELSE    			a.EXT_ALARMTIME    		END    	) AS EXT_ALARMTIME,    	a.ISFINISH AS ISFINISH,    	a.EXT_ALARMCODE AS EXT_ALARMCODE,    	a.CreateTime AS CreateTime,    	a.LASTMODIFY AS LASTMODIFY,    	e.ID AS REPLYID,    	e.USERNAME,    	e.REPLYUSERNAME,    	e.REPLYDATE,    	d.ALARMTYPECODE AS AlarmTypeCode,    	d.CODENAME AS CodeName,    	'0012' AS Sourceflag    FROM    	t_alarm_alarminfo a    JOIN (    	SELECT    		ab.ID,    		ab.TYPE,    		ab. LEVEL,    		ab.CUSTOMID,    		ab. NAME,    		ab.ORGANIZATIONTYPE,    		ab. STATUS,    		ac.CUSTOMNAME    	FROM    		t_sys_organization ab    	JOIN t_sys_custominfo ac ON ab.CUSTOMID = ac.ID    ) b ON a.ORGANIZATIONID = b.ID    LEFT JOIN t_alarm_alarmreply e ON a.ID = e.ALARMID    LEFT JOIN t_alarm_alarmtyperelation d ON a.EXT_ALARMCODE = d.CODEID    AND b.CUSTOMID = d.CUSTOMID     WHERE   b.STATUS!=-1    and   a.EXT_ERFLAG='E'   and   a.ISTEST=0  and	  a.LASTMODIFY>='2016-12-01 00:00:00'  and  a.LASTMODIFY<\'${currentdate}\' ")
    //val  df=sqlContext.sql(s" SELECT    	a.ORGANIZATIONID AS cloudalarmorgid,    	a.ID AS cloudalarmalarmid,    	(    		CASE    		WHEN a.EXT_ALARMTIME IS NULL THEN    			a.CreateTime    		ELSE    			a.EXT_ALARMTIME    		END    	)  as alarmtime,    	a.ISFINISH AS isfinish,    	a.EXT_ALARMCODE AS codeid,    	a.CreateTime AS alarmcreatetime, a.EXT_HANDLETIME    as   cancelalarmtime,    	a.LASTMODIFY AS alarmlastmodify, REPLACE(REPLACE(REPLACE(a.REMARK , \\r, ''), \\n, ''),\\t,'')   as alarmmemo,a.EXT_CLIENTID  as clientnumber,a.EXT_ZONEID  as  zoneid,    	e.ID AS cloudalarmreplyid,      e.REPLYID  as replyid,     e.REPLYUSERNAME  as passivereplyusername,    	e.USERID  as cloudalarmuserid,    	e.REPLYDATE as replytime, REPLACE(REPLACE(REPLACE(e.MEMO , \\r, ''), \\n, ''),\\t,'')  as replymemo,e.LASTMODIFY  as  replylastmodify   FROM    	t_alarm_alarminfo a     LEFT JOIN t_alarm_alarmreply e ON a.ID = e.ALARMID    WHERE  a.EXT_ALARMTYPE!='测试'  and  a.EXT_ERFLAG = 'E'    AND a.ISTEST = 0     and	  a.LASTMODIFY>='2017-01-01 00:00:00'  and  a.LASTMODIFY<\'${currentdate}\' ")
    val  df=sqlContext.sql(s" SELECT    	a.ORGANIZATIONID AS cloudalarmorgid,    	a.ID AS cloudalarmalarmid,    	(    		CASE    		WHEN a.EXT_ALARMTIME IS NULL THEN    			a.CreateTime    		ELSE    			a.EXT_ALARMTIME    		END    	)  as alarmtime,    	a.ISFINISH AS isfinish,    	a.EXT_ALARMCODE AS codeid,    	a.CreateTime AS alarmcreatetime, a.EXT_HANDLETIME    as   cancelalarmtime,    	a.LASTMODIFY AS alarmlastmodify, a.REMARK    as alarmmemo,a.EXT_CLIENTID  as clientnumber,a.EXT_ZONEID  as  zoneid,    	e.ID AS cloudalarmreplyid,      e.REPLYID  as replyid,     e.REPLYUSERNAME  as passivereplyusername,    	e.USERID  as cloudalarmuserid,    	e.REPLYDATE as replytime, e.MEMO   as replymemo,e.LASTMODIFY  as  replylastmodify   FROM    	t_alarm_alarminfo a     LEFT JOIN t_alarm_alarmreply e ON a.ID = e.ALARMID    WHERE  a.EXT_ALARMTYPE!='测试'  and  a.EXT_ERFLAG = 'E'    AND a.ISTEST = 0     and	  a.LASTMODIFY>='2017-01-01 00:00:00'  and  a.LASTMODIFY<\'${currentdate}\' ")
    //val  df=sqlContext.sql(s" SELECT    	a.ORGANIZATIONID AS cloudalarmorgid,    	a.ID AS cloudalarmalarmid,    	(    		CASE    		WHEN a.EXT_ALARMTIME IS NULL THEN    			a.CreateTime    		ELSE    			a.EXT_ALARMTIME    		END    	)  as alarmtime,    	a.ISFINISH AS isfinish,    	a.EXT_ALARMCODE AS codeid,    	a.CreateTime AS alarmcreatetime, a.EXT_HANDLETIME    as   cancelalarmtime,    	a.LASTMODIFY AS alarmlastmodify,a.EXT_CLIENTID  as clientnumber,a.EXT_ZONEID  as  zoneid,    	e.ID AS cloudalarmreplyid,      e.REPLYID  as replyid,     e.REPLYUSERNAME  as passivereplyusername,    	e.USERID  as cloudalarmuserid,    	e.REPLYDATE as replytime,e.LASTMODIFY  as  replylastmodify   FROM    	t_alarm_alarminfo a     LEFT JOIN t_alarm_alarmreply e ON a.ID = e.ALARMID    WHERE  a.EXT_ALARMTYPE!='测试'  and  a.EXT_ERFLAG = 'E'    AND a.ISTEST = 0     and	  a.LASTMODIFY>='2017-01-01 00:00:00'  and  a.LASTMODIFY<\'${currentdate}\' ")
    //.collect.foreach(println)
    //增量     左闭右开
    val  lastdate=currentdate.minusDays(1)
    //val df =sqlContext.sql(s"SELECT b.CUSTOMID AS CaCustomID,b.CUSTOMNAME AS CUSTOMNAME,a.ORGANIZATIONID AS CaOrgID,      b.NAME AS OrgName,b.LEVEL AS OrgLEVEL,orgLevel.CODE as orgLevelCODE,      orgLevel.NAME as orgLevelName,b.TYPE AS OrgType,orgType.CODE as  orgTypeCODE,orgType.NAME as orgTypeName,a.ID as alarmid,(case  when  a.EXT_ALARMTIME  is null then   a.CreateTime  else a.EXT_ALARMTIME  end)  AS EXT_ALARMTIME,    	a.ISFINISH AS ISFINISH,a.EXT_ALARMCODE AS EXT_ALARMCODE,a.CreateTime AS CreateTime,    	a.LASTMODIFY AS LASTMODIFY,e.REPLYUSERNAME AS ResponseUserName,e.REPLYDATE AS ResponseDate,    	h.REPLYUSERNAME AS HandleUserName,h.REPLYDATE AS HandleDate,d.ALARMTYPECODE AS AlarmTypeCode,    	d.CODENAME AS CodeName,'0012' as  Sourceflag    FROM    	     t_alarm_alarminfo a    JOIN (SELECT  ab.ID,ab.TYPE,ab.LEVEL,ab.CUSTOMID,ab.NAME,ab.ORGANIZATIONTYPE,ab.STATUS,ac.CUSTOMNAME   from            t_sys_organization ab join      t_sys_custominfo ac on ab.CUSTOMID = ac.ID) b ON a.ORGANIZATIONID = b.ID    LEFT JOIN      t_alarm_alarmreply e ON a.ID = e.ALARMID    AND e.REPLYUSERNAME = 'operateTime'    LEFT JOIN      t_alarm_alarmreply h ON a.ID = h.ALARMID    AND h.REPLYUSERNAME = 'operateTime2'    LEFT JOIN      t_alarm_alarmtyperelation d     ON a.EXT_ALARMCODE = d.CODEID and  b.CUSTOMID = d.CUSTOMID    LEFT JOIN (SELECT  item. CODE AS CODE,item. NAME AS NAME,    category.CUSTOMID AS CUSTOMID  FROM  t_sys_code_items item  INNER JOIN t_sys_code_category category ON category.ID = item.CODE_CATEGORY_ID    AND category.CATEGORY_CODE = 'hierarchyType' AND item.IS_VISIBLE = 'T') orgLevel ON b.TYPE = orgLevel.CODE    AND b.CUSTOMID=orgLevel.CUSTOMID    LEFT JOIN (SELECT  item. CODE AS CODE,item. NAME AS NAME,category.CUSTOMID AS CUSTOMID FROM    t_sys_code_items item INNER JOIN t_sys_code_category category ON category.ID = item.CODE_CATEGORY_ID    AND category.CATEGORY_CODE = 'organizationType'    AND item.IS_VISIBLE = 'T') orgType ON b.ORGANIZATIONTYPE = orgType.CODE    AND     b.CUSTOMID = orgType.CUSTOMID    WHERE   b.STATUS!=-1  and   a.EXT_ERFLAG='E'   and   a.ISTEST=0  and
    //  a.LASTMODIFY>=\'${lastdate}\'  and  a.LASTMODIFY<\'${currentdate}\' ")

    //列
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/fact_alarm")
    //df.write.mode(SaveMode.Overwrite).save("/data/hivetemp/fact_inspect")

    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/fact_alarm/part*")
      .registerTempTable("fact_alarmtemp")

    //hiveContext.sql("drop table if   exists  default.fact_alarm")
    //hiveContext.sql("create table if  not  exists  default.fact_alarm  as  select  *,'0012' as  sourceflag from fact_alarmtemp ")

    hiveContext.sql("use datawarehouse")
    //初始化  全量
    hiveContext.sql("drop table if  exists   fact_alarm")
    hiveContext.sql("create table if not exists  fact_alarm  as  select  user2.dwuserid,org.dworgid,codeid,isfinish,replyid,alarmcreatetime,alarmlastmodify,alarmmemo,alarmtime,cancelalarmtime,clientnumber,alarm.cloudalarmorgid,cloudalarmreplyid,cloudalarmalarmid,alarm.cloudalarmuserid,passivereplyusername,replylastmodify,replymemo,replytime,zoneid,'0012' AS Sourceflag  from   fact_alarmtemp alarm left join   dim_organization org ON  alarm.cloudalarmorgid = org.cloudalarmorgid left join  dim_user  user2 on alarm.cloudalarmuserid=user2.cloudalarmuserid  ")
    //增量
    //val data=hiveContext.sql(" select  user2.dwuserid,org.dworgid,codeid,isfinish,replyid,alarmcreatetime,alarmlastmodify,alarmmemo,alarmtime,cancelalarmtime,clientnumber,alarm.cloudalarmorgid,cloudalarmreplyid,cloudalarmalarmid,alarm.cloudalarmuserid,passivereplyusername,replylastmodify,replymemo,replytime,zoneid,'0012' AS Sourceflag  from   fact_alarmtemp alarm  left  join   dim_organization org ON  alarm.cloudalarmorgid = org.cloudalarmorgid left join  dim_user  user2 on alarm.cloudalarmuserid=user2.cloudalarmuserid  ")
    //data.write.mode("append").saveAsTable("fact_alarm")

    sc.stop

  }

}
