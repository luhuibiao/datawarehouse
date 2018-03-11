package datawarehouse
/*
服务巡检ETL
 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object fact_serviceinspect {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("factserviceinspect").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val url = "jdbc:mysql://192.168.11.25:3306/tycloud2"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
    val TS_InspectionMain = sqlContext.read.jdbc(url, "TS_InspectionMain",prop)
    val TS_InspectionItem = sqlContext.read.jdbc(url, "TS_InspectionItem", prop)
    val TS_InspectionInfo = sqlContext.read.jdbc(url, "TS_InspectionInfo",prop)
    val TS_Code = sqlContext.read.jdbc(url, "TS_Code", prop)
    val ts_inspection_custom_setting = sqlContext.read.jdbc(url, "ts_inspection_custom_setting",prop)

    TS_InspectionMain.registerTempTable("TS_InspectionMain")
    TS_InspectionItem.registerTempTable("TS_InspectionItem")
    TS_Code.registerTempTable("TS_Code")
    TS_InspectionInfo.registerTempTable("TS_InspectionInfo")
    ts_inspection_custom_setting.registerTempTable("ts_inspection_custom_setting")

    //
    val  currentdate= LocalDate.now()
    //
    val df =sqlContext.sql(s" select     		main.IM_INSPECTIONNO_VARC as  inspectnumber,      main.IM_CUSTOMID_INT as servicebranchnetid,      setting.ICS_Frequency_Int   as     inspectFrequencybymonth,      main.IM_ID_INT      as  insepctmainid,      main.IM_DUTYMAN_INT   as    servicedutymanuserid,      main.IM_IMPLEMENTATIONMAN_INT  as  serviceimplementuserid,      main.IM_SCHEDULE_TIME  as   inspectSCHEDULETIME,      main.IM_START_TIME  as   inspectStartTime,      main.IM_END_TIME  as   inspectendtime,    " +
      s"  main_status.C_CODENAME_VARC  as  inspectstatus,      main.IM_DOCUMENT_STATUS_INT  as  inspectoriginalstatus,      main.IM_COMPANY_ID   as  servicecompanyid,      info.II_INSPECTION_NAME   as   inspectitemNAME,      item_status.C_CODENAME_VARC  as  inspectitemstatus,      item.ID_ITEM_STATUS_INT  as inspectitemOriginalstatus  	" +
      s"	from   TS_InspectionMain  main  		inner join TS_InspectionItem item     		 on   item.ID_IM_ID_INT = main.IM_ID_INT  		 and  item.ISDELETE_BIT = 0          	inner join TS_InspectionInfo info     		 on    item.ID_II_ID_INT=info.II_ID_INT  		" +
      s"  inner join TS_Code main_status     		 on   main_status.C_TYPE_VARC = '1001'  		and   main_status.C_CODE_INT= main.IM_DOCUMENT_STATUS_INT  		inner join TS_Code item_status     		 on   item_status.C_TYPE_VARC = '1002'  		and   item_status.C_CODE_INT= item.ID_ITEM_STATUS_INT      left join  ts_inspection_custom_setting   setting  on  setting.ICS_CustomId_Int=main.IM_CUSTOMID_INT      and  setting.ISDELETE_BIT=0 " +
      s"   where  main.LASTMODIFY_DATE<\'${currentdate}\' ")
    //.collect.foreach(println)
    // 增量
    val  lastdate=currentdate.minusDays(1)
    //val df =sqlContext.sql(s"SELECT	 orgdetail.*,task.Id AS taskID,task.TaskName,cKMan.Name AS checKMan,task.CheckTime  AS CheckTime,ResMan.Name AS ResMan,task.CreateTime,CASE WHEN task.ReportTime IS NULL THEN  '???'	WHEN v_status.detail_Status IS NULL THEN	'???'	WHEN v_status.detail_Status = 2 THEN	'???'	WHEN v_status.detail_Status = 3 THEN		'???'       WHEN v_status.detail_Status = 4 THEN				'????'	ELSE	''     END AS status,task.ReportTime AS ReportTime,report.ObtainScore AS ObtainScore,report.TotalScore AS TotalScore,report.RelativeScore  AS RelativeScore,report.PassCnt AS PassCnt,report.TotalCnt AS TotalCnt,detail.Updatetime	as detailUpdatetime,detail.Id   as detailID,detail.CheckScore as detailCheckScore,case when detail.Status=0 then '??' when detail.Status=1 then '????' when detail.Status=2 then '???' when detail.Status=3 then '???'  else '????' end as  detailStatus,case when tem_detail.HasSon = 1 then '?'  else  '?'  end as	HasSon,tem_detail.Score  as standardScore,tem_detail.Level as Level,case when rect.Type=1 then '??' when rect.Type=2 then '??' else '' end as action,case when rect.Status=1 then '??'  when rect.Status=-1 then '???' else '' end as actionResult,rect.CreateTime AS actionTime FROM      inspect_task task	INNER JOIN       pl_organization org ON task.CustomId = org.ID join  (SELECT  custom.ID AS customId,custom.AccountName AS customName,org.ID AS orgId,org.OrgName AS orgName,orgLevel.NAME AS orgLevelName,orgType.NAME AS orgTypeName FROM  pl_organization org  INNER JOIN pl_openaccount custom ON org.OpenAccountID = custom.ID    LEFT JOIN (SELECT item.Code AS CODE,item.Name AS NAME,category.OpenAccountID AS CUSTOMID  FROM  pl_codeitems item INNER JOIN pl_codecategory category ON category.ID = item.CategoryID  AND category.CategoryCode = 'hierarchyType'  AND item.IsVisible = 'T') orgLevel ON org.Type= orgLevel.CODE  AND org.OpenAccountID = orgLevel.CUSTOMID LEFT JOIN (SELECT  item.Code AS CODE,item.Name AS NAME,category.OpenAccountID AS CUSTOMID  FROM  pl_codeitems item INNER JOIN pl_codecategory category ON category.ID = item.CategoryID  AND category.CategoryCode = 'organizationType'  AND item.IsVisible = 'T') orgType ON org.OrgType= orgType.CODE  AND org.OpenAccountID = orgType.CUSTOMID WHERE  org.Status != - 1)	 orgdetail  on   org.ID=orgdetail.orgId   and  		org.OpenAccountID=orgdetail.customId LEFT JOIN   pl_userinfo cKMan   ON cKMan.ID = task.CheckManId	 LEFT JOIN     pl_userinfo ResMan ON ResMan.ID = task.ResponsibleManId			INNER JOIN        inspect_task_detail detail ON detail.TaskId = task.Id	and detail.IsDelete=0	INNER JOIN        inspect_templet_detail tem_detail ON tem_detail.Id = detail.TempletDetailId   LEFT JOIN        inspect_report report ON report.TaskId = task.Id	LEFT JOIN       inspect_task_detail_rectification rect		on rect.TaskDetailId=detail.Id and rect.IsDelete=0  	LEFT JOIN (SELECT	MIN(detail1.Status) AS detail_Status,task1.Id AS task_id	FROM	      inspect_task task1  INNER JOIN        inspect_task_detail detail1 ON detail1.TaskId = task1.Id	 AND detail1.Status  IN (2,3,4)      AND detail1.IsDelete = 0	INNER JOIN        inspect_templet_detail tem_detail1 ON tem_detail1.Id = detail1.TempletDetailId	AND tem_detail1.HasSon = 0		WHERE	       task1.IsDelete = 0	GROUP BY	task1.Id)  v_status   ON   v_status.task_id = task.Id	WHERE	task.IsDelete = 0  and   task.CreateTime>= \'${lastdate}\' and   task.CreateTime<\'${currentdate}\' ")
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/fact_serviceinspect")


    //hive
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/fact_serviceinspect/part*")
      .registerTempTable("fact_serviceinspecttemp")

    hiveContext.sql("use datawarehouse")
    //
    hiveContext.sql("drop table if  exists   fact_serviceinspect")
    hiveContext.sql("create table if not exists  fact_serviceinspect as  select  '0014' AS sourceflag,* from   fact_serviceinspecttemp")
    //
    //val data=hiveContext.sql("select  * from   fact_serviceinspecttemp")
    //data.write.mode("append").saveAsTable("fact_serviceinspect")



    sc.stop

  }
}
