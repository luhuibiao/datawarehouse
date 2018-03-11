package datawarehouse
/*
dim
基础信息
数据库来源标识
0011 aliplatform
0012 alicloudalarm
0013 aligaoxiao
0014 alitycoud2
0015 alipersonal
 */
import java.time.LocalDate

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
object baseinfo {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    //sparkConf.set("spark.driver.maxResultSize", "8g")
    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url = "jdbc:mysql://192.168.11.25:3306/report"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val snsbranchnettime = sqlContext.read.jdbc(url, "snsbranchnettime", prop)
    snsbranchnettime.registerTempTable("snsbranchnettime")
    //基础信息
    //val currentdate = LocalDate.now()
    //初次全量  左闭右开
    val df = sqlContext.sql(s"SELECT  *  from  snsbranchnettime ")
    //.collect.foreach(println)
    //增量     左闭右开
    //val lastdate = currentdate.minusDays(1)

    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/snsbranchnettime")
    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/snsbranchnettime/part*")
      .registerTempTable("snsbranchnettimetemp")


    hiveContext.sql("use default")
    //初始化  全量
    hiveContext.sql("drop table if  exists   snsbranchnettime")
    hiveContext.sql("create table if not exists  snsbranchnettime as  select  * from   snsbranchnettimetemp")


    sc.stop

  }
}
