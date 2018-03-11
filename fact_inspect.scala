package datawarehouse
/*
fact
检查监督相关的数据ETL
*/
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object fact_inspect {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("factinspect").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    //sparkConf.set("spark.driver.maxResultSize", "8g")
    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url = "jdbc:mysql://192.168.11.25:3306/koalaplatform"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val inspect_task = sqlContext.read.jdbc(url, "inspect_task",prop)
    val pl_organization = sqlContext.read.jdbc(url, "pl_organization", prop)
    val pl_openaccount = sqlContext.read.jdbc(url, "pl_openaccount",prop)
    val pl_codeitems = sqlContext.read.jdbc(url, "pl_codeitems", prop)
    val pl_codecategory = sqlContext.read.jdbc(url, "pl_codecategory",prop)
    val pl_userinfo = sqlContext.read.jdbc(url, "pl_userinfo", prop)
    val inspect_task_detail = sqlContext.read.jdbc(url, "inspect_task_detail",prop)
    val inspect_task_calculation_mode = sqlContext.read.jdbc(url, "inspect_task_calculation_mode", prop)
    val inspect_templet_detail = sqlContext.read.jdbc(url, "inspect_templet_detail",prop)
    val inspect_report = sqlContext.read.jdbc(url, "inspect_report", prop)
    val inspect_task_detail_rectification = sqlContext.read.jdbc(url, "inspect_task_detail_rectification", prop)

    inspect_task.registerTempTable("inspect_task")
    pl_organization.registerTempTable("pl_organization")
    pl_openaccount.registerTempTable("pl_openaccount")
    pl_codeitems.registerTempTable("pl_codeitems")
    pl_codecategory.registerTempTable("pl_codecategory")
    pl_userinfo.registerTempTable("pl_userinfo")
    inspect_task_detail.registerTempTable("inspect_task_detail")
    inspect_task_calculation_mode.registerTempTable("inspect_task_calculation_mode")
    inspect_templet_detail.registerTempTable("inspect_templet_detail")
    inspect_report.registerTempTable("inspect_report")
    inspect_task_detail_rectification.registerTempTable("inspect_task_detail_rectification")

    //基础信息
    val  currentdate= LocalDate.now()
    //初次全量  左闭右开
    //full column
    //sql(s"SELECT	 orgdetail.*,task.Id AS taskID,task.TaskName,cKMan.Name AS checKMan,task.CheckTime  AS CheckTime,ResMan.Name AS ResMan,task.CreateTime,CASE WHEN task.ReportTime IS NULL THEN  '待检查'	WHEN v_status.detail_Status IS NULL THEN	'已完成'	WHEN v_status.detail_Status = 2 THEN	'待整改'	WHEN v_status.detail_Status = 3 THEN		'待复查'       WHEN v_status.detail_Status = 4 THEN				'整改完成'	ELSE	''     END AS status,md.ModeName AS ModeName,task.ReportTime AS ReportTime,report.ObtainScore AS ObtainScore,report.TotalScore AS TotalScore,report.RelativeScore  AS RelativeScore,report.PassCnt AS PassCnt,report.TotalCnt AS TotalCnt,detail.Updatetime	as detailUpdatetime,detail.Id   as detailID,detail.CheckScore as detailCheckScore,detail.Description as detailDescription,case when detail.Status=0 then '创建' when detail.Status=1 then '检查完毕' when detail.Status=2 then '待整改' when detail.Status=3 then '待复查'  else '整改完成' end as  detailStatus,case when tem_detail.HasSon = 1 then '否'  else  '是'  end as	HasSon,tem_detail.Name as standard,tem_detail.Content as checkstandard,tem_detail.Score  as standardScore,tem_detail.Level as Level,case when rect.Type=1 then '整改' when rect.Type=2 then '复查' else '' end as action,rect.Description as actionDescription,case when rect.Status=1 then '通过'  when rect.Status=-1 then '不通过' else '' end as actionResult,rect.CreateTime AS actionTime FROM      inspect_task task	INNER JOIN       pl_organization org ON task.CustomId = org.ID join  (SELECT  custom.ID AS customId,custom.AccountName AS customName,org.ID AS orgId,org.OrgName AS orgName,orgLevel.NAME AS orgLevelName,orgType.NAME AS orgTypeName FROM  pl_organization org  INNER JOIN pl_openaccount custom ON org.OpenAccountID = custom.ID    LEFT JOIN (SELECT item.Code AS CODE,item.Name AS NAME,category.OpenAccountID AS CUSTOMID  FROM  pl_codeitems item INNER JOIN pl_codecategory category ON category.ID = item.CategoryID  AND category.CategoryCode = 'hierarchyType'  AND item.IsVisible = 'T') orgLevel ON org.Type= orgLevel.CODE  AND org.OpenAccountID = orgLevel.CUSTOMID LEFT JOIN (SELECT  item.Code AS CODE,item.Name AS NAME,category.OpenAccountID AS CUSTOMID  FROM  pl_codeitems item INNER JOIN pl_codecategory category ON category.ID = item.CategoryID  AND category.CategoryCode = 'organizationType'  AND item.IsVisible = 'T') orgType ON org.OrgType= orgType.CODE  AND org.OpenAccountID = orgType.CUSTOMID WHERE  org.Status != - 1)	 orgdetail  on   org.ID=orgdetail.orgId   and  		org.OpenAccountID=orgdetail.customId LEFT JOIN   pl_userinfo cKMan   ON cKMan.ID = task.CheckManId	 LEFT JOIN     pl_userinfo ResMan ON ResMan.ID = task.ResponsibleManId			INNER JOIN        inspect_task_detail detail ON detail.TaskId = task.Id	and detail.IsDelete=0	INNER JOIN        inspect_templet_detail tem_detail ON tem_detail.Id = detail.TempletDetailId  LEFT JOIN        inspect_task_calculation_mode md ON md.Id = task.CalculationModeId   LEFT JOIN        inspect_report report ON report.TaskId = task.Id	LEFT JOIN       inspect_task_detail_rectification rect		on rect.TaskDetailId=detail.Id and rect.IsDelete=0  	LEFT JOIN (SELECT	MIN(detail1.Status) AS detail_Status,task1.Id AS task_id	FROM	      inspect_task task1  INNER JOIN        inspect_task_detail detail1 ON detail1.TaskId = task1.Id	 AND detail1.Status  IN (2,3,4)      AND detail1.IsDelete = 0	INNER JOIN        inspect_templet_detail tem_detail1 ON tem_detail1.Id = detail1.TempletDetailId	AND tem_detail1.HasSon = 0		WHERE	       task1.IsDelete = 0	GROUP BY	task1.Id)  v_status   ON   v_status.task_id = task.Id	WHERE	task.IsDelete = 0   and   task.CreateTime<\'${currentdate}\' ")
    // list  可度量 指标
    //val df =sqlContext.sql(s"SELECT	 orgdetail.*,task.Id AS taskID,task.TaskName,cKMan.Name AS checKMan,task.CheckTime  AS CheckTime,ResMan.Name AS ResMan,task.CreateTime,CASE WHEN task.ReportTime IS NULL THEN  '待检查'	WHEN v_status.detail_Status IS NULL THEN	'已完成'	WHEN v_status.detail_Status = 2 THEN	'待整改'	WHEN v_status.detail_Status = 3 THEN		'待复查'       WHEN v_status.detail_Status = 4 THEN				'整改完成'	ELSE	''     END AS status,task.ReportTime AS ReportTime,report.ObtainScore AS ObtainScore,report.TotalScore AS TotalScore,report.RelativeScore  AS RelativeScore,report.PassCnt AS PassCnt,report.TotalCnt AS TotalCnt,detail.Updatetime	as detailUpdatetime,detail.Id   as detailID,detail.CheckScore as detailCheckScore,case when detail.Status=0 then '创建' when detail.Status=1 then '检查完毕' when detail.Status=2 then '待整改' when detail.Status=3 then '待复查'  else '整改完成' end as  detailStatus,case when tem_detail.HasSon = 1 then '否'  else  '是'  end as	HasSon,tem_detail.Score  as standardScore,tem_detail.Level as Level,case when rect.Type=1 then '整改' when rect.Type=2 then '复查' else '' end as action,case when rect.Status=1 then '通过'  when rect.Status=-1 then '不通过' else '' end as actionResult,rect.CreateTime AS actionTime FROM      inspect_task task	INNER JOIN       pl_organization org ON task.CustomId = org.ID join  (SELECT  custom.ID AS customId,custom.AccountName AS customName,org.ID AS orgId,org.OrgName AS orgName,orgLevel.NAME AS orgLevelName,orgType.NAME AS orgTypeName FROM  pl_organization org  INNER JOIN pl_openaccount custom ON org.OpenAccountID = custom.ID    LEFT JOIN (SELECT item.Code AS CODE,item.Name AS NAME,category.OpenAccountID AS CUSTOMID  FROM  pl_codeitems item INNER JOIN pl_codecategory category ON category.ID = item.CategoryID  AND category.CategoryCode = 'hierarchyType'  AND item.IsVisible = 'T') orgLevel ON org.Type= orgLevel.CODE  AND org.OpenAccountID = orgLevel.CUSTOMID LEFT JOIN (SELECT  item.Code AS CODE,item.Name AS NAME,category.OpenAccountID AS CUSTOMID  FROM  pl_codeitems item INNER JOIN pl_codecategory category ON category.ID = item.CategoryID  AND category.CategoryCode = 'organizationType'  AND item.IsVisible = 'T') orgType ON org.OrgType= orgType.CODE  AND org.OpenAccountID = orgType.CUSTOMID WHERE  org.Status != - 1)	 orgdetail  on   org.ID=orgdetail.orgId   and  		org.OpenAccountID=orgdetail.customId LEFT JOIN   pl_userinfo cKMan   ON cKMan.ID = task.CheckManId	 LEFT JOIN     pl_userinfo ResMan ON ResMan.ID = task.ResponsibleManId			INNER JOIN        inspect_task_detail detail ON detail.TaskId = task.Id	and detail.IsDelete=0	INNER JOIN        inspect_templet_detail tem_detail ON tem_detail.Id = detail.TempletDetailId   LEFT JOIN        inspect_report report ON report.TaskId = task.Id	LEFT JOIN       inspect_task_detail_rectification rect		on rect.TaskDetailId=detail.Id and rect.IsDelete=0  	LEFT JOIN (SELECT	MIN(detail1.Status) AS detail_Status,task1.Id AS task_id	FROM	      inspect_task task1  INNER JOIN        inspect_task_detail detail1 ON detail1.TaskId = task1.Id	 AND detail1.Status  IN (2,3,4)      AND detail1.IsDelete = 0	INNER JOIN        inspect_templet_detail tem_detail1 ON tem_detail1.Id = detail1.TempletDetailId	AND tem_detail1.HasSon = 0		WHERE	       task1.IsDelete = 0	GROUP BY	task1.Id)  v_status   ON   v_status.task_id = task.Id	WHERE	task.IsDelete = 0     and   task.CreateTime<\'${currentdate}\' ")
    val df =sqlContext.sql(s" SELECT   	org.ID AS platformorgid,   	task.Id AS taskID,   	task.TaskName,   	task.CheckManId AS CheckManuserId,   	task.ResponsibleManId AS ResponsibleManuserId,   	task.CheckTime AS taskCheckTime,   	task.CreateTime  as  taskCreateTime,   " +
      s"	(  CASE   WHEN task.ReportTime IS NULL THEN   			'待检查'   		WHEN v_status.detail_Status IS NULL THEN   			'已完成'   		WHEN v_status.detail_Status = 2 THEN   			'待整改'   		WHEN v_status.detail_Status = 3 THEN   			'待复查'   		WHEN v_status.detail_Status = 4 THEN   			'整改完成'   		ELSE   ''   		END   	) AS taskstatus,   	" +
      s"  detail.CreateTime  as  detailcreatetime,detail.Updatetime AS detailUpdatetime,   	detail.Id AS detailID,   	detail.CheckScore AS detailCheckScore,   	detail.Description AS detailproblemdesc," +
      s"   	(  CASE   		WHEN detail.Status = 0 THEN   			'创建'   	WHEN detail.Status = 1 THEN   			'检查完毕'   		WHEN detail.Status = 2 THEN   			'待整改'   		WHEN detail.Status = 3 THEN   			'待复查'   		ELSE   			'整改完成'   		END   	) AS detailStatus,   	(   		CASE   WHEN tem_detail.HasSon = 1 THEN   			'否'   		ELSE   '是'   		END   	) AS HasSon,  " +
      s" 	tem_detail.Score AS templetstandardScore,   	tem_detail.Level AS templetLEVEL,   	(   		CASE   		WHEN rect.Type = 1 THEN   			'整改'   		WHEN rect.Type = 2 THEN   			'复查'   		ELSE   		''   		END   	) AS rectType,  " +
      s" 	rect.Id  as rectid,( CASE   WHEN rect.Status = 1 THEN   			'通过'   		WHEN rect.Status=- 1 THEN   			'不通过'   		ELSE   		''   		END   	) AS rectSTATUS,     rect.CreateTime AS rectCreateTime,rect.Updatetime as rectupdatetime,  " +
      s"   task.ReportTime,   	report.ObtainScore,   	report.TotalScore,   	report.RelativeScore,   	report.PassCnt,     report.ProblemCnt,   	report.TotalCnt  " +
      s"   FROM   	inspect_task task   INNER JOIN pl_organization org ON task.CustomId = org.ID   INNER JOIN inspect_task_detail detail ON detail.TaskId = task.Id   AND detail.IsDelete = 0   INNER JOIN inspect_templet_detail tem_detail ON tem_detail.Id = detail.TempletDetailId   LEFT JOIN inspect_report report ON report.TaskId = task.Id   LEFT JOIN inspect_task_detail_rectification rect ON rect.TaskDetailId = detail.Id   AND rect.IsDelete = 0 " +
      s"  LEFT JOIN (   	SELECT   		MIN(detail1.Status) AS detail_Status,   		task1.Id AS task_id   " +
      s"	FROM   		inspect_task task1   	INNER JOIN inspect_task_detail detail1 ON detail1.TaskId = task1.Id   	AND detail1.Status IN (2, 3, 4)   	AND detail1.IsDelete = 0   	INNER JOIN inspect_templet_detail tem_detail1 ON tem_detail1.Id = detail1.TempletDetailId   	AND tem_detail1.HasSon = 0   	WHERE   		task1.IsDelete = 0   	GROUP BY   		task1.Id   ) v_status ON v_status.task_id = task.Id " +
      s"  WHERE   task.IsDelete = 0   AND  task.CreateTime<\'${currentdate}\' ")
        //.collect.foreach(println)
    //增量     左闭右开区间
    val  lastdate=currentdate.minusDays(1)
    //val df =sqlContext.sql(s"SELECT	 orgdetail.*,task.Id AS taskID,task.TaskName,cKMan.Name AS checKMan,task.CheckTime  AS CheckTime,ResMan.Name AS ResMan,task.CreateTime,CASE WHEN task.ReportTime IS NULL THEN  '待检查'	WHEN v_status.detail_Status IS NULL THEN	'已完成'	WHEN v_status.detail_Status = 2 THEN	'待整改'	WHEN v_status.detail_Status = 3 THEN		'待复查'       WHEN v_status.detail_Status = 4 THEN				'整改完成'	ELSE	''     END AS status,task.ReportTime AS ReportTime,report.ObtainScore AS ObtainScore,report.TotalScore AS TotalScore,report.RelativeScore  AS RelativeScore,report.PassCnt AS PassCnt,report.TotalCnt AS TotalCnt,detail.Updatetime	as detailUpdatetime,detail.Id   as detailID,detail.CheckScore as detailCheckScore,case when detail.Status=0 then '创建' when detail.Status=1 then '检查完毕' when detail.Status=2 then '待整改' when detail.Status=3 then '待复查'  else '整改完成' end as  detailStatus,case when tem_detail.HasSon = 1 then '否'  else  '是'  end as	HasSon,tem_detail.Score  as standardScore,tem_detail.Level as Level,case when rect.Type=1 then '整改' when rect.Type=2 then '复查' else '' end as action,case when rect.Status=1 then '通过'  when rect.Status=-1 then '不通过' else '' end as actionResult,rect.CreateTime AS actionTime FROM      inspect_task task	INNER JOIN       pl_organization org ON task.CustomId = org.ID join  (SELECT  custom.ID AS customId,custom.AccountName AS customName,org.ID AS orgId,org.OrgName AS orgName,orgLevel.NAME AS orgLevelName,orgType.NAME AS orgTypeName FROM  pl_organization org  INNER JOIN pl_openaccount custom ON org.OpenAccountID = custom.ID    LEFT JOIN (SELECT item.Code AS CODE,item.Name AS NAME,category.OpenAccountID AS CUSTOMID  FROM  pl_codeitems item INNER JOIN pl_codecategory category ON category.ID = item.CategoryID  AND category.CategoryCode = 'hierarchyType'  AND item.IsVisible = 'T') orgLevel ON org.Type= orgLevel.CODE  AND org.OpenAccountID = orgLevel.CUSTOMID LEFT JOIN (SELECT  item.Code AS CODE,item.Name AS NAME,category.OpenAccountID AS CUSTOMID  FROM  pl_codeitems item INNER JOIN pl_codecategory category ON category.ID = item.CategoryID  AND category.CategoryCode = 'organizationType'  AND item.IsVisible = 'T') orgType ON org.OrgType= orgType.CODE  AND org.OpenAccountID = orgType.CUSTOMID WHERE  org.Status != - 1)	 orgdetail  on   org.ID=orgdetail.orgId   and  		org.OpenAccountID=orgdetail.customId LEFT JOIN   pl_userinfo cKMan   ON cKMan.ID = task.CheckManId	 LEFT JOIN     pl_userinfo ResMan ON ResMan.ID = task.ResponsibleManId			INNER JOIN        inspect_task_detail detail ON detail.TaskId = task.Id	and detail.IsDelete=0	INNER JOIN        inspect_templet_detail tem_detail ON tem_detail.Id = detail.TempletDetailId   LEFT JOIN        inspect_report report ON report.TaskId = task.Id	LEFT JOIN       inspect_task_detail_rectification rect		on rect.TaskDetailId=detail.Id and rect.IsDelete=0  	LEFT JOIN (SELECT	MIN(detail1.Status) AS detail_Status,task1.Id AS task_id	FROM	      inspect_task task1  INNER JOIN        inspect_task_detail detail1 ON detail1.TaskId = task1.Id	 AND detail1.Status  IN (2,3,4)      AND detail1.IsDelete = 0	INNER JOIN        inspect_templet_detail tem_detail1 ON tem_detail1.Id = detail1.TempletDetailId	AND tem_detail1.HasSon = 0		WHERE	       task1.IsDelete = 0	GROUP BY	task1.Id)  v_status   ON   v_status.task_id = task.Id	WHERE	task.IsDelete = 0  and   task.CreateTime>= \'${lastdate}\' and   task.CreateTime<\'${currentdate}\' ")
    //列
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/fact_inspect")
    //df.write.mode(SaveMode.Overwrite).save("/data/hivetemp/fact_inspect")

    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/fact_inspect/part*")
      //hiveContext.read.load("/data/hivetemp/dim_user/*.parquet")
      .registerTempTable("fact_inspecttemp")
    //hiveContext.sql("create table  default.user2 as select * from  dim_usertemp")
    //hiveContext.sql("select  *  from dim_custom ")
    //.collect.foreach(println)
    hiveContext.sql("use datawarehouse")
    //初始化  全量
    hiveContext.sql("drop table if  exists   fact_inspect")
    hiveContext.sql("create table if not exists  fact_inspect as  select  * from   fact_inspecttemp")
    //增量
    //val data=hiveContext.sql("select  * from   fact_inspecttemp")
    //data.write.mode("append").saveAsTable("fact_inspect")



    sc.stop

  }
}
