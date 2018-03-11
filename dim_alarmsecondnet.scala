package datawarehouse
/*
第三层 传统意义的 二级行
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object dim_alarmsecondnet {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim_alarmsecondnet").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    //sparkConf.set("spark.driver.maxResultSize", "8g")
    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
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
    //AND m.TYPE = '03'
    val df =sqlContext.sql(s"select  m.orgLevelName,m.orgLevelCode,m.orgTypeName,m.orgTypeCode,'0012' AS Sourceflag,m.orglevel,m.orglevel12,m.customId,m.customName,m.secondnetid,m.secondnetname,n.PARENT_ID  branchid,n.NAME branchname,n.TYPE  branchtype    from      (SELECT    	orgLevel. NAME AS orgLevelName,    	orgLevel. CODE AS orgLevelCode,    	orgType. NAME AS orgTypeName,    	orgType. CODE AS orgTypeCode,org.LEVEL as orglevel,substr(org.LEVEL, 1, 12) AS orglevel12,    	custom.ID AS customId,    	custom.CUSTOMNAME AS customName,    	org.ID AS secondnetid,    	org. NAME AS secondnetname,org.STATUS,org.TYPE,org.UPDATE_DATE    FROM    	t_sys_organization org    INNER JOIN t_sys_custominfo custom ON org.CUSTOMID = custom.ID    LEFT JOIN (    	SELECT    		item. CODE AS CODE,    		item. NAME AS NAME,    		category.CUSTOMID AS CUSTOMID    	FROM    		t_sys_code_items item    	INNER JOIN t_sys_code_category category ON category.ID = item.CODE_CATEGORY_ID    	AND category.CATEGORY_CODE = 'hierarchyType'    	AND item.IS_VISIBLE = 'T'    ) orgLevel ON org.TYPE = orgLevel. CODE    AND org.CUSTOMID = orgLevel.CUSTOMID    LEFT JOIN (    	SELECT    		item. CODE AS CODE,    		item. NAME AS NAME,    		category.CUSTOMID AS CUSTOMID    	FROM    		t_sys_code_items item    	INNER JOIN t_sys_code_category category ON category.ID = item.CODE_CATEGORY_ID    	AND category.CATEGORY_CODE = 'organizationType'    	AND item.IS_VISIBLE = 'T'    ) orgType ON org.ORGANIZATIONTYPE = orgType. CODE    AND org.CUSTOMID = orgType.CUSTOMID)   m  left  JOIN (    	SELECT    		b.PARENT_ID,    		b.ID,    		a. NAME,    		a.TYPE    	FROM    		t_sys_organization a    	JOIN t_sys_organization b ON a.ID = b.PARENT_ID    ) n ON m.secondnetid = n.ID    WHERE     	m. STATUS != - 1    AND  length(m.orglevel)= 12    AND m.UPDATE_DATE <\'${currentdate}\' ")
    //.collect.foreach(println)
    //??     ??????
    //val  lastdate=currentdate.minusDays(1)

    //? ??
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_alarmsecondnet")


    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_alarmsecondnet/part*")
      .registerTempTable("dim_alarmsecondnettemp")

    //.collect.foreach(println)
    hiveContext.sql("use datawarehouse")
    //???  ??
    hiveContext.sql("drop table if  exists   dim_alarmsecondnet")
    hiveContext.sql("create table if not exists  dim_alarmsecondnet as  select  * from  dim_alarmsecondnettemp")
    //??
    //val data=hiveContext.sql("select  * from   dim_alarmsecondnettemp")
    //data.write.mode("append").saveAsTable("dim_alarmsecondnet")



    sc.stop

  }




}