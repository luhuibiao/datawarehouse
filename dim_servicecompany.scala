package datawarehouse
/*
服务工程商信息
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object dim_servicecompany {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim_companyorg").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val url = "jdbc:mysql://192.168.11.25:3306/tycloud2"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val   ts_companyinfo=sqlContext.read.jdbc(url, "ts_companyinfo",prop)
    ts_companyinfo.registerTempTable("ts_companyinfo")

    //
    val currentdate = LocalDate.now()
    //
    val df = sqlContext.sql(s"SELECT    COMPANY_ID as servicecompanyid,*   from   ts_companyinfo ")
    //  where a.LASTMODIFY_DATE<\'${currentdate}\' ")
    //  .collect.foreach(println)
    //
    val lastdate = currentdate.minusDays(1)
    //val df =sqlContext.sql(s"SELECT ID as actionid,USERID,ACTTYPE,MEMO  from  t_useractionlog  where  LASTMODIFY>=\'${lastdate}\'  and     LASTMODIFY<\'${currentdate}\' ")
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_companyorg")
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_companyorg/part*")
      .registerTempTable("dim_servicecompanytemp")


    hiveContext.sql("use datawarehouse")
    //
    hiveContext.sql("drop table   if  exists   dim_servicecompany")
    hiveContext.sql("create table if not exists  dim_servicecompany as  select  	'0014' AS sourceflag,* from   dim_servicecompanytemp")


    //
    // val data=hiveContext.sql("select  * from   dim_servicecompanytemp")
    // data.write.mode("append").saveAsTable("dim_servicecompany")



    sc.stop


  }
}
