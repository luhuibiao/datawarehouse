package datawarehouse
/*
增量抽取昨天的数据
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object dim_role {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim_role").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)

    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url1 = "jdbc:mysql://192.168.11.25:3306/koalaplatform"
    //val url2 = "jdbc:mysql://192.168.11.25:3306/cloudalarm"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val pl_user_role_rel = sqlContext.read.jdbc(url1, "pl_user_role_rel", prop)
    val pl_roles = sqlContext.read.jdbc(url1, "pl_roles", prop)
    pl_user_role_rel.registerTempTable("pl_user_role_rel")
    pl_roles.registerTempTable("pl_roles")
    //基础信息
    val currentdate = LocalDate.now()
    //初次全量 左开右闭
    val df = sqlContext.sql(s" select  rolerel.UserID  as  platformuserid,role.ID as platformroleid, '0011' as sourceflag,           role.IsDefaultRole,       role.RoleName  as  rolename,       role.CreateTime,role.Updatetime      from  pl_user_role_rel   rolerel     	LEFT JOIN  pl_roles role  ON rolerel.RoleID = role.ID where     role.Updatetime<\'${currentdate}\' ")
    //增量   左右封闭区间
    //val  lastdate=currentdate.minusDays(1)
    //val df =sqlContext.sql(s"select  a.ID as platformorgid,b.ID as cloudalarmorgid,'0011' as sourceflag,a.*  from   pl_organization a  left join  t_sys_organization  b  on  b.PLID=a.ID  where   a.Updatetime>= \'${lastdate}\' and  a.Updatetime<\'${currentdate}\' ")
    //列
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_role")
    //df.write.mode(SaveMode.Overwrite).save("/data/hivetemp/dim_user")

    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_role/part*")
      //hiveContext.read.load("/data/hivetemp/dim_user/*.parquet")
      .registerTempTable("dim_roletemp")
    //hiveContext.sql("create table  default.user2 as select * from  dim_usertemp")
    //hiveContext.sql("select  *  from dim_custom ")
    //.collect.foreach(println)
    hiveContext.sql("use datawarehouse")
    //初始化  全量
    hiveContext.sql("drop table if  exists   dim_role")
    hiveContext.sql("create table if not exists  dim_role as  select  'current'  as rowindicator,d.guid as dwroleid,c.* from   dim_roletemp c inner join  dict_dwguid  d  on  c.platformroleid=d.id  ")
    //增量
    //val data=hiveContext.sql("select  d.guid as dwroleid,c.* from   dim_roletemp c inner join  dict_dwguid  d  on  c.platformroleid=d.id  ")
    //data.write.mode("append").saveAsTable("dim_role")


    sc.stop

  }
}
