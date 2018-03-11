package datawarehouse
/*
fact
检查监督图像评论相关的数据ETL
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object fact_inspect_imagecomment {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("fact_inspect_imagecomment").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    //sparkConf.set("spark.driver.maxResultSize", "8g")
    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url = "jdbc:mysql://192.168.11.25:3306/koalaplatform"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    //val inspect_task = sqlContext.read.jdbc(url, "inspect_task",prop)
    //val pl_organization = sqlContext.read.jdbc(url, "pl_organization", prop)
    //val pl_openaccount = sqlContext.read.jdbc(url, "pl_openaccount",prop)
    //val pl_codeitems = sqlContext.read.jdbc(url, "pl_codeitems", prop)
    //val pl_codecategory = sqlContext.read.jdbc(url, "pl_codecategory",prop)
    //val pl_userinfo = sqlContext.read.jdbc(url, "pl_userinfo", prop)
    val inspect_task_detail = sqlContext.read.jdbc(url, "inspect_task_detail",prop)
    //val inspect_task_calculation_mode = sqlContext.read.jdbc(url, "inspect_task_calculation_mode", prop)
    //val inspect_templet_detail = sqlContext.read.jdbc(url, "inspect_templet_detail",prop)
    val inspect_report = sqlContext.read.jdbc(url, "inspect_report", prop)
    val inspect_task_detail_rectification = sqlContext.read.jdbc(url, "inspect_task_detail_rectification", prop)
    val inspect_task_image=sqlContext.read.jdbc(url,"inspect_task_image",prop)
    val inspect_comment=sqlContext.read.jdbc(url,"inspect_comment",prop)
    //inspect_task.registerTempTable("inspect_task")
    //pl_organization.registerTempTable("pl_organization")
    //pl_openaccount.registerTempTable("pl_openaccount")
    //pl_codeitems.registerTempTable("pl_codeitems")
    //pl_codecategory.registerTempTable("pl_codecategory")
    //pl_userinfo.registerTempTable("pl_userinfo")
    inspect_task_detail.registerTempTable("inspect_task_detail")
    //inspect_task_calculation_mode.registerTempTable("inspect_task_calculation_mode")
    //inspect_templet_detail.registerTempTable("inspect_templet_detail")
    inspect_report.registerTempTable("inspect_report")
    inspect_task_detail_rectification.registerTempTable("inspect_task_detail_rectification")
    inspect_task_image.registerTempTable("inspect_task_image")
    inspect_comment.registerTempTable("inspect_comment")
    //基础信息
    val  currentdate= LocalDate.now()
    //初次全量  左闭右开
    val df =sqlContext.sql(s"select  (case   imagetaskdetail.Type  when  1 then  '任务详情'  when  2  then '整改复查详情'  end)  as  Type,     imagetaskdetail.imageId,imagetaskdetail.RelationId as  imageRelationId,imagetaskdetail.taskdetailId,     rect.Id  as  rectId,report.Id as  reportId,comm.Id as  commId,     comm.Comment,imagetaskdetail.Path     from      (select    image.Type,image.Id  as imageId,image.RelationId,image.Path,     detail.Id as  taskdetailId,detail.TaskId     from   inspect_task_image  image       left join  inspect_task_detail  detail       on  image.RelationId=detail.Id  and  image.Type=1)  imagetaskdetail      left join  inspect_task_detail_rectification  rect        on  imagetaskdetail.RelationId=rect.Id  and  imagetaskdetail.Type=2     left  join  inspect_report  report       on  imagetaskdetail.TaskId=report.TaskId     left  join  inspect_comment  comm       on   report.Id=comm.ReportId    where report.Updatetime<\'${currentdate}\' ")
    //.collect.foreach(println)
    //增量     左闭右开区间
    val  lastdate=currentdate.minusDays(1)
    //val df =sqlContext.sql(s"  task.CreateTime>= \'${lastdate}\' and   task.CreateTime<\'${currentdate}\' ")
    //列
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/fact_inspect_imagecomment")
    //df.write.mode(SaveMode.Overwrite).save("/data/hivetemp/fact_inspect")

    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/fact_inspect_imagecomment/part*")
      .registerTempTable("fact_inspect_imagecommenttemp")
    hiveContext.sql("use datawarehouse")
    //初始化  全量
    hiveContext.sql("drop table if  exists   datawarehouse.fact_inspect_imagecomment")
    hiveContext.sql("create table if not exists  datawarehouse.fact_inspect_imagecomment as  select  * from   fact_inspect_imagecommenttemp")
    //增量
    //val data=hiveContext.sql("select  * from   fact_inspect_imagecommenttemp")
    //data.write.mode("append").saveAsTable("fact_inspect_imagecomment")



    sc.stop

  }
}
