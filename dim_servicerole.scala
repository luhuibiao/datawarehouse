package datawarehouse
/*
·þÎñ½ÇÉ«
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object dim_servicerole {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim_companyorg").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val url = "jdbc:mysql://192.168.11.25:3306/tycloud2"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val   global_roleinfo=sqlContext.read.jdbc(url, "global_roleinfo",prop)
    global_roleinfo.registerTempTable("global_roleinfo")

    //
    val currentdate = LocalDate.now()
    //
    val df = sqlContext.sql(s"SELECT    RI_ID_INT as serviceroleid,RI_ROLENAME_VARC  as  rolename,RI_ISDEFAULT_BIT  as  IsDefaultRole,*  from   global_roleinfo ")
    //  where a.LASTMODIFY_DATE<\'${currentdate}\' ")
    //  .collect.foreach(println)
    //
    val lastdate = currentdate.minusDays(1)
    //val df =sqlContext.sql(s"SELECT ID as actionid,USERID,ACTTYPE,MEMO  from  t_useractionlog  where  LASTMODIFY>=\'${lastdate}\'  and     LASTMODIFY<\'${currentdate}\' ")
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_companyorg")
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_companyorg/part*")
      .registerTempTable("dim_serviceroletemp")


    hiveContext.sql("use datawarehouse")
    //
    hiveContext.sql("drop table   if  exists   dim_servicerole")
    hiveContext.sql("create table if not exists  dim_servicerole as  select  	'0014' AS sourceflag,* from   dim_serviceroletemp")


    //
    // val data=hiveContext.sql("select  * from   dim_serviceroletemp")
    // data.write.mode("append").saveAsTable("dim_servicerole")



    sc.stop


  }
}
