package datawarehouse
/*
第二层 传统意义的分支行
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object dim_alarmbranch {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("dim_alarmbranch").setMaster("spark://192.168.11.21:7077")
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
    //????  ????  and org.TYPE='02'
    val df =sqlContext.sql(s"SELECT  orgLevel.NAME AS orgLevelName,orgLevel.CODE AS orgLevelCode,orgType.NAME AS  orgTypeName,orgType.CODE AS  orgTypeCode,'0012' as  Sourceflag,org.LEVEL as  orglevel,substr(org.LEVEL,1,8)  as  orglevel8,custom.ID AS customId,custom.CUSTOMNAME AS customName,org.ID AS branchid,org.NAME AS branchname    FROM  t_sys_organization org  INNER JOIN t_sys_custominfo custom ON org.CUSTOMID = custom.ID  LEFT JOIN  (SELECT item.CODE AS CODE,item.NAME AS NAME,category.CUSTOMID AS CUSTOMID FROM t_sys_code_items item INNER JOIN t_sys_code_category category ON category.ID = item.CODE_CATEGORY_ID  AND category.CATEGORY_CODE = 'hierarchyType' AND item.IS_VISIBLE = 'T')   orgLevel ON org.TYPE = orgLevel.CODE AND org.CUSTOMID = orgLevel.CUSTOMID  LEFT JOIN (SELECT     item.CODE AS CODE,item.NAME AS NAME,category.CUSTOMID AS CUSTOMID     FROM     t_sys_code_items item     INNER JOIN t_sys_code_category category ON category.ID = item.CODE_CATEGORY_ID     AND category.CATEGORY_CODE = 'organizationType'     AND item.IS_VISIBLE = 'T' ) orgType ON org.ORGANIZATIONTYPE = orgType.CODE     AND org.CUSTOMID = orgType.CUSTOMID   WHERE org.STATUS != - 1    AND  length(org.LEVEL)= 8   and  org.UPDATE_DATE<\'${currentdate}\' ")

    //.collect.foreach(println)
    //??     ??????
    //val  lastdate=currentdate.minusDays(1)
    //val df =sqlContext.sql(s"SELECT	 orgdetail.*,task.Id AS taskID,task.TaskName,org.OrgName,cKMan.Name AS checKMan,task.CheckTime  AS CheckTime,ResMan.Name AS ResMan,task.CreateTime,CASE WHEN task.ReportTime IS NULL THEN  '???'	WHEN v_status.detail_Status IS NULL THEN	'???'	WHEN v_status.detail_Status = 2 THEN	'???'	WHEN v_status.detail_Status = 3 THEN		'???'       WHEN v_status.detail_Status = 4 THEN				'????'	ELSE	''     END AS status,md.ModeName AS ModeName,task.ReportTime AS ReportTime,report.ObtainScore AS ObtainScore,report.TotalScore AS TotalScore,report.RelativeScore  AS RelativeScore,report.PassCnt AS PassCnt,report.TotalCnt AS TotalCnt,detail.Updatetime	as detailUpdatetime,detail.Id   as detailID,detail.CheckScore as detailCheckScore,detail.Description as detailDescription,case when detail.Status=0 then '??' when detail.Status=1 then '????' when detail.Status=2 then '???' when detail.Status=3 then '???'  else '????' end as  detailStatus,case when tem_detail.HasSon = 1 then '?'  else  '?'  end as	HasSon,tem_detail.Name as standard,tem_detail.Content as checkstandard,tem_detail.Score  as standardScore,tem_detail.Level as Level,case when rect.Type=1 then '??' when rect.Type=2 then '??' else '' end as action,rect.Description as actionDescription,case when rect.Status=1 then '??'  when rect.Status=-1 then '???' else '' end as actionResult,rect.CreateTime AS actionTime FROM      inspect_task task	INNER JOIN       pl_organization org ON task.CustomId = org.ID join  (SELECT  custom.ID AS customId,custom.AccountName AS customName,org.ID AS orgId,org.OrgName AS orgName,orgLevel.NAME AS orgLevelName,orgType.NAME AS orgTypeName FROM  pl_organization org  INNER JOIN pl_openaccount custom ON org.OpenAccountID = custom.ID    LEFT JOIN (SELECT item.Code AS CODE,item.Name AS NAME,category.OpenAccountID AS CUSTOMID  FROM  pl_codeitems item INNER JOIN pl_codecategory category ON category.ID = item.CategoryID  AND category.CategoryCode = 'hierarchyType'  AND item.IsVisible = 'T') orgLevel ON org.Type= orgLevel.CODE  AND org.OpenAccountID = orgLevel.CUSTOMID LEFT JOIN (SELECT  item.Code AS CODE,item.Name AS NAME,category.OpenAccountID AS CUSTOMID  FROM  pl_codeitems item INNER JOIN pl_codecategory category ON category.ID = item.CategoryID  AND category.CategoryCode = 'organizationType'  AND item.IsVisible = 'T') orgType ON org.OrgType= orgType.CODE  AND org.OpenAccountID = orgType.CUSTOMID WHERE  org.Status != - 1)	 orgdetail  on   org.ID=orgdetail.orgId   and  		org.OpenAccountID=orgdetail.customId LEFT JOIN   pl_userinfo cKMan   ON cKMan.ID = task.CheckManId	 LEFT JOIN     pl_userinfo ResMan ON ResMan.ID = task.ResponsibleManId			INNER JOIN        inspect_task_detail detail ON detail.TaskId = task.Id	and detail.IsDelete=0	INNER JOIN        inspect_templet_detail tem_detail ON tem_detail.Id = detail.TempletDetailId  LEFT JOIN        inspect_task_calculation_mode md ON md.Id = task.CalculationModeId   LEFT JOIN        inspect_report report ON report.TaskId = task.Id	LEFT JOIN       inspect_task_detail_rectification rect		on rect.TaskDetailId=detail.Id and rect.IsDelete=0  	LEFT JOIN (SELECT	MIN(detail1.Status) AS detail_Status,task1.Id AS task_id	FROM	      inspect_task task1  INNER JOIN        inspect_task_detail detail1 ON detail1.TaskId = task1.Id	 AND detail1.Status  IN (2,3,4)      AND detail1.IsDelete = 0	INNER JOIN        inspect_templet_detail tem_detail1 ON tem_detail1.Id = detail1.TempletDetailId	AND tem_detail1.HasSon = 0		WHERE	       task1.IsDelete = 0	GROUP BY	task1.Id)  v_status   ON   v_status.task_id = task.Id	WHERE	task.IsDelete = 0   and   task.CreateTime<\'${currentdate}\' and  task.CreateTime>= \'${lastdate}\' and  task.CreateTime<\'${currentdate}\' ")
    //? ??
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/dim_alarmbranch")


    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/dim_alarmbranch/part*")

      .registerTempTable("dim_alarmbranchtemp")

    //.collect.foreach(println)
    hiveContext.sql("use datawarehouse")
    //???  ??
    hiveContext.sql("drop table if  exists   dim_alarmbranch")
    hiveContext.sql("create table if not exists  dim_alarmbranch as  select  * from   dim_alarmbranchtemp")
    //??
    //val data=hiveContext.sql("select  * from   dim_alarmbranchtemp")
    //data.write.mode("append").saveAsTable("dim_alarmbranch")



    sc.stop

  }




}