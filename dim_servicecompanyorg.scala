package datawarehouse
/*
服务
维保公司组织机构=维保人员所在area地区信息
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object dim_servicecompanyorg {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim_companyorg").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val url = "jdbc:mysql://192.168.11.25:3306/tycloud2"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val   TS_CompanyOrganization=sqlContext.read.jdbc(url, "TS_CompanyOrganization",prop)
    TS_CompanyOrganization.registerTempTable("TS_CompanyOrganization")

    //
    val currentdate = LocalDate.now()
    //
    val df = sqlContext.sql(s"SELECT      a.CO_ID as servicecompanyorgid,a.CO_ORGNAME as companyorgname,a.CO_LEVEL as  companyorglevel,    a.PLID  as  platformcompanyorgid,    b.CO_ID as  servicecompanyorgparentid,b.CO_ORGNAME  as  companyorgparentname,    b.CO_LEVEL  as  companyorgparentlevel,b.PLID as  platformcompanyorgparentid,    c.CO_ID as  servicecompanyorggrandparentid,c.CO_ORGNAME  as  companyorggrandparentname,    c.CO_LEVEL  as  companyorggrandparentlevel,c.PLID  as  platformcompanyorggrandparentid,    a.COMPANY_ID  as  servicecompanyid      from  TS_CompanyOrganization a    left join   TS_CompanyOrganization  b    on  a.CO_PARENTID=b.CO_ID    left  join    TS_CompanyOrganization  c    on  b.CO_PARENTID=c.CO_ID  where a.LASTMODIFY_DATE<\'${currentdate}\' ")
    //.collect.foreach(println)
    //
    val lastdate = currentdate.minusDays(1)
    //val df =sqlContext.sql(s"SELECT ID as actionid,USERID,ACTTYPE,MEMO  from  t_useractionlog  where  LASTMODIFY>=\'${lastdate}\'  and     LASTMODIFY<\'${currentdate}\' ")
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_companyorg")
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_companyorg/part*")
      .registerTempTable("dim_companyorgtemp")


    hiveContext.sql("use datawarehouse")
    //
    hiveContext.sql("drop table   if  exists   dim_servicecompanyorg")
    hiveContext.sql("create table if not exists  dim_servicecompanyorg as  select  	'0014' AS sourceflag,* from   dim_companyorgtemp")


    //
    // val data=hiveContext.sql("select  * from   dim_companyorgtemp")
    // data.write.mode("append").saveAsTable("dim_companyorg")



    sc.stop


  }
}
