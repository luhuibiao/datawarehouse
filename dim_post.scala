package datawarehouse
/*
增量抽取昨天的数据
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object dim_post {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim_post").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)

    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url1 = "jdbc:mysql://192.168.11.25:3306/koalaplatform"
    val url2 = "jdbc:mysql://192.168.11.25:3306/cloudalarm"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val pl_post = sqlContext.read.jdbc(url1, "pl_post",prop)
    //val t_sys_organization = sqlContext.read.jdbc(url2, "t_sys_organization", prop)
    pl_post.registerTempTable("pl_post")
    //t_sys_organization.registerTempTable("t_sys_organization")
    //基础信息
    val  currentdate= LocalDate.now()
    //初次全量 左开右闭
    //val df =sqlContext.sql(s"select  a.ID as platformpostid,b.ID as cloudalarmpostid,'0011' as sourceflag,a.*  from   pl_organization a  left join  t_sys_organization  b  on  b.PLID=a.ID  where     a.Updatetime<\'${currentdate}\' ")
    val df =sqlContext.sql(s"select  a.ID as platformpostid,'0011' as sourceflag,a.*  from   pl_post a    where     a.Updatetime<\'${currentdate}\' ")
    //增量   左右封闭区间
    //val  lastdate=currentdate.minusDays(1)
    //val df =sqlContext.sql(s"select  a.ID as platformpostid,b.ID as cloudalarmpostid,'0011' as sourceflag,a.*  from   pl_post a  left join  t_sys_organization  b  on  b.PLID=a.ID  where   a.Updatetime>= \'${lastdate}\' and  a.Updatetime<\'${currentdate}\' ")

    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_post")
    //df.write.mode(SaveMode.Overwrite).save("/data/hivetemp/dim_post")

    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_post/part*")
      //hiveContext.read.load("/data/hivetemp/dim_user/*.parquet")
      .registerTempTable("dim_posttemp")
    //hiveContext.sql("create table  default.user2 as select * from  dim_usertemp")
    //hiveContext.sql("select  *  from dim_custom ")
    //.collect.foreach(println)
    hiveContext.sql("use datawarehouse")
    //初始化  全量
    hiveContext.sql("drop table if  exists   datawarehouse.dim_post")
    hiveContext.sql("create table if not exists  datawarehouse.dim_post as  select  'current'  as rowindicator,d.guid as dwpostid,c.* from   dim_posttemp c inner join  dict_dwguid  d  on  c.platformpostid=d.id  ")
    //增量
    //val data=hiveContext.sql("select  d.guid as dwpostid,c.* from   dim_posttemp c inner join  dict_guid  d  on  c.platformpostid=d.id  ")
    //data.write.mode("append").saveAsTable("dim_post")



    sc.stop

  }

}
