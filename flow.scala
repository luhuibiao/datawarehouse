package datawarehouse

import java.time.LocalDate
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

object flow {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("flow").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    //sparkConf.set("spark.driver.maxResultSize", "8g")
    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url = "jdbc:mysql://192.168.11.25:3306/koalaplatform"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val t_wf_form = sqlContext.read.jdbc(url, "t_wf_form", prop)
    t_wf_form.registerTempTable("t_wf_form")
    val t_wf_category = sqlContext.read.jdbc(url, "t_wf_category", prop)
    t_wf_category.registerTempTable("t_wf_category")
    val t_wf_form_data = sqlContext.read.jdbc(url, "t_wf_form_data", prop)
    t_wf_form_data.registerTempTable("t_wf_form_data")
    val t_wf_data_resource = sqlContext.read.jdbc(url, "t_wf_data_resource", prop)
    t_wf_data_resource.registerTempTable("t_wf_data_resource")


    //基础信息
    //val currentdate = LocalDate.now()
    //初次全量  左闭右开
    val df = sqlContext.sql(s"select  cat.CATEGORY_NAME,cat.OPENACCOUNT_ID,   form.Id  as formid,form.FORM_NAME,form.DRAFT_VERSION_ID  as draftId,   (case form.PUBLISH_FLAG  when  '0' then '未发布'  when  '1'  then  '已发布'  end) as  publish   ,formdata.DATA_ID,resource.NAME   FROM   t_wf_form   form     	inner  JOIN  t_wf_category  cat   	on cat.Id = form.FLOW_TYPE_ID     inner join  t_wf_form_data  formdata    on  formdata.FORM_ID=form.Id    inner join   t_wf_data_resource  resource     on  formdata.DATA_ID=resource.Id   where  form.IsDelete=0 ")
    //.collect.foreach(println)
    //增量     左闭右开
    //val lastdate = currentdate.minusDays(1)

    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/flowtemp/fact_flowform")
    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/flowtemp/fact_flowform/part*")
      .registerTempTable("fact_flowformtemp")


    hiveContext.sql("use  datawarehouse")
    //初始化  全量
    hiveContext.sql("drop table if  exists   fact_flowform")
    hiveContext.sql("create table if not exists  fact_flowform  as  select  * from   fact_flowformtemp")


    sc.stop
  }

}
