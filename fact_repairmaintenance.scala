package datawarehouse
/*

 */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import java.time.LocalDate
object fact_repairmaintenance {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("fact_repairmaintenance").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    //sparkConf.set("spark.driver.maxResultSize", "8g")
    //sc.addJar("/usr/spark/lib/mysql-connector-java-5.1.35.jar")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val url2 = "jdbc:mysql://192.168.11.25:3306/cloudalarm"
    val prop2 = new java.util.Properties
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "ty123456")
    val url1 = "jdbc:mysql://192.168.11.25:3306/koalaplatform"
    val prop1 = new java.util.Properties
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "ty123456")
    val url = "jdbc:mysql://192.168.11.25:3306/tycloud2"
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "ty123456")
   // prop.setProperty("")
    val Koala_Repairs = sqlContext.read.jdbc(url, "Koala_Repairs", prop)
    val Koala_Maintenance = sqlContext.read.jdbc(url, "Koala_Maintenance", prop)
    //val ts_custominfo = sqlContext.read.jdbc(url, "ts_custominfo", prop)
    //val ts_organizationinfo = sqlContext.read.jdbc(url, "ts_organizationinfo", prop1)
    //val ts_companyinfo = sqlContext.read.jdbc(url, "ts_companyinfo",prop)
    //val global_userinfo = sqlContext.read.jdbc(url, "global_userinfo",prop)
    val Koala_Maintenance_Device = sqlContext.read.jdbc(url, "Koala_Maintenance_Device",prop)
    //val TS_DeviceBrandManager = sqlContext.read.jdbc(url, "TS_DeviceBrandManager",prop)
    //val TS_KoalaBrandDevice = sqlContext.read.jdbc(url, "TS_KoalaBrandDevice",prop)
    //val TS_BrandInfo = sqlContext.read.jdbc(url, "TS_BrandInfo",prop)
    //val Koala_Device_Location = sqlContext.read.jdbc(url, "Koala_Device_Location",prop)
    //val Koala_Device_Location_Type = sqlContext.read.jdbc(url, "Koala_Device_Location_Type",prop)
    val koala_maintenance_action_status = sqlContext.read.jdbc(url, "koala_maintenance_action_status",prop)
    //val TS_CustomRepairInfo = sqlContext.read.jdbc(url, "TS_CustomRepairInfo",prop)
    //val TS_MaintenceInfo = sqlContext.read.jdbc(url, "TS_MaintenceInfo",prop)
    //val TS_ServiceTechnician=sqlContext.read.jdbc(url, "TS_ServiceTechnician",prop)
    val  TS_MaintenceDevice=sqlContext.read.jdbc(url, "TS_MaintenceDevice",prop)
    //val   TS_TechnicianAreaRelation=sqlContext.read.jdbc(url, "TS_TechnicianAreaRelation",prop)
    //val   TS_CompanyOrganization=sqlContext.read.jdbc(url, "TS_CompanyOrganization",prop)
    val  TS_CustomCategory=sqlContext.read.jdbc(url, "TS_CustomCategory",prop)
    val  TS_MaintenanceProperty_rel=sqlContext.read.jdbc(url, "TS_MaintenanceProperty_rel",prop)
    val   TS_MaintenanceProperty=sqlContext.read.jdbc(url, "TS_MaintenanceProperty",prop)
    val  TS_SatisfactionInfo=sqlContext.read.jdbc(url, "TS_SatisfactionInfo",prop)
    val  koala_repairs_statistics=sqlContext.read.jdbc(url, "koala_repairs_statistics",prop)
    //TS_CompanyOrganization.registerTempTable("TS_CompanyOrganization")
    TS_CustomCategory.registerTempTable("TS_CustomCategory")
    TS_MaintenanceProperty_rel.registerTempTable("TS_MaintenanceProperty_rel")
    TS_MaintenanceProperty.registerTempTable("TS_MaintenanceProperty")
    TS_SatisfactionInfo.registerTempTable("TS_SatisfactionInfo")
    koala_repairs_statistics.registerTempTable("koala_repairs_statistics")
    Koala_Repairs.registerTempTable("Koala_Repairs")
    Koala_Maintenance.registerTempTable("Koala_Maintenance")
    //ts_custominfo.registerTempTable("ts_custominfo")
    //ts_organizationinfo.registerTempTable("ts_organizationinfo")
    //ts_companyinfo.registerTempTable("ts_companyinfo")
    //global_userinfo.registerTempTable("global_userinfo")
    Koala_Maintenance_Device.registerTempTable("Koala_Maintenance_Device")
    //TS_DeviceBrandManager.registerTempTable("TS_DeviceBrandManager")
    //TS_KoalaBrandDevice.registerTempTable("TS_KoalaBrandDevice")
    //TS_BrandInfo.registerTempTable("TS_BrandInfo")
    //Koala_Device_Location.registerTempTable("Koala_Device_Location")
    //Koala_Device_Location_Type.registerTempTable("Koala_Device_Location_Type")
    koala_maintenance_action_status.registerTempTable("koala_maintenance_action_status")
    //TS_CustomRepairInfo.registerTempTable("TS_CustomRepairInfo")
    //TS_MaintenceInfo.registerTempTable("TS_MaintenceInfo")
    //TS_ServiceTechnician.registerTempTable("TS_ServiceTechnician")
    TS_MaintenceDevice.registerTempTable("TS_MaintenceDevice")
    //TS_TechnicianAreaRelation.registerTempTable("TS_TechnicianAreaRelation")
    //基础信息
    val currentdate = LocalDate.now()
    //初次全量  左闭右开  //where  minfo.LASTMODIFY_DATE<\'${currentdate}\' ")
    // , orinfo.OI_ID_INT  as  orgrootid,     orinfo.OI_ORGANIZATIONNAME_VARC  as  orgrootname,     porinfo.OI_ID_INT  as  orgid,     porinfo.OI_ORGANIZATIONNAME_VARC as    orgname,porinfo.IsCustom,    cinfo.CI_CUSTOMCODE_VARC  as  branchnetcode,     CI_CUSTOMTYPE_INT  as  branchnettype,      cinfo.CI_CUSTOMNAME_VARC as  branchnetname,
    //    cinfo.CI_ID_INT  as  branchnetid,   cinfo.PLID as branchnetplid,   companyInfo.COMPANY_ID   as  companyid,  companyInfo.COMPANY_NAME  as  companyname, minfo.MI_REPAIRORDERSTATUS_INT as  originalOrderStatus, md.MD_TERMINALNAME_VARC    AS TerminalName,  md.MD_DEVICENAME_VARC  as  MaintenceDeviceName,  newLocation.LocationName as  LocationName,  newLocationType.TypeName  as  TypeName,  newManager.DBM_Location_Detail_VarC   as LocationDetail,  newManager.DBM_Serial_Number_VarC   as SerialNumber,     newBrandInfo.BI_BRANDNAME_VARC  as  BRANDNAME,  newBrandDevice.KBD_DEVICENAME_VARC  as  BrandDEVICENAME,  	oldManager.DBM_Serial_Number_VarC   AS oldSerialNumber,  oldBrandInfo.BI_BRANDNAME_VARC   AS oldBRANDNAME,  oldBrandDevice.KBD_DEVICENAME_VARC as  oldDEVICENAME
    //"	left join TS_DeviceBrandManager newManager on  newManager.DBM_ID_INT = md.MD_DeviceBrandManager_Int  	left join TS_KoalaBrandDevice newBrandDevice on  newBrandDevice.KBD_PLID_INT = newManager.DBM_BRANDDEVICE_INT  	left join TS_BrandInfo newBrandInfo on  newBrandInfo.BI_BRANDID_INT = newBrandDevice.KBD_BRANDID_INT  	left join Koala_Device_Location_Type newLocationType on newLocationType.ID = newManager.DBM_Location_Type_Int  	left join Koala_Device_Location newLocation	on newLocationType.DeviceLocationId = newLocation.ID  	left join TS_DeviceBrandManager oldManager on  oldManager.DBM_ID_INT = md.MD_Old_DeviceBrandManager_Int  	left join TS_KoalaBrandDevice oldBrandDevice on  oldBrandDevice.KBD_PLID_INT = oldManager.DBM_BRANDDEVICE_INT  	left join TS_BrandInfo oldBrandInfo on  oldBrandInfo.BI_BRANDID_INT = oldBrandDevice.KBD_BRANDID_INT
    //    crinfo.CR_ID_INT AS repairid,minfo.MI_ID_INT  as  MaintenanceId  ,sa.PLID  as  companyorgplid
    //val df = sqlContext.sql(s"	select   	  sfinfo.SI_SERVICELEVEL_INT  AS  Speed,  sfinfo.SI_SERVICELEVEL2_INT AS  Effect,  sfinfo.SI_SERVICELEVEL3_INT as  Attitude,sfinfo.SI_ADVICE_VARC  as  Advice,   '0014' AS sourceflag,        CI_Level_VarC      as  branchnetlevel,   CI_CUSTOMFULLNAME_VARC  as  branchnetfullname,   crinfo.CR_REPAIRORDERNO_VARC  as  RepairOrderNo,  CR_ORDERDATE_DATE    as  OrderTime,   minfo.MI_RESERVATIONTIME  as  ReservationTime,  minfo.MI_ACCEPTTIME_DATE   as  ReceiveTime,  minfo.MI_DEPARTURETIME_DATE  as  DepartTime,  minfo.MI_ARRIVETIME_DATE  as  ArriveTime,  minfo.MI_ENDTIME_DATE  as  RepairTime,  minfo.MI_CLOSETIME_DATE  as  CloseTime,   	  (CASE minfo.MI_REPAIRORDERSTATUS_INT                      	  WHEN 1 THEN '待接单'                     	  WHEN 2 THEN '待出发'                     	  WHEN 3 THEN '待到达'                     	  WHEN 4 THEN '待修复'                     	  WHEN 5 THEN '已完工'            	  WHEN 8 THEN '已关闭'                    	  WHEN 9 THEN '异常关闭'                         else '' END)  as     OrderStatus,  dutyMan.UI_CONTACTNAME_VARC AS DutyEngineer,  acceptMan.UI_CONTACTNAME_VARC AS AcceptMan,  md.MD_MALFUNCTIONTYPE_VARC   AS MalfunctionType,sa.CO_LEVEL as companyorglevel 	 from   ts_custominfo cinfo     	inner join  TS_CustomRepairInfo  crinfo    ON crinfo.CR_CUSTOMID_INT=cinfo.CI_ID_INT   and cinfo.ISDELETE_BIT=0   	inner join  TS_MaintenceInfo minfo    on crinfo.CR_REPAIRORDERNO_VARC = minfo.MI_REPAIRORDERNO_VARC and minfo.ISDELETE_BIT=0   	left join   TS_ServiceTechnician st   on st.ST_ID_INT=minfo.MI_ACCEPTMANID_INT and st.ISDELETE_BIT=0  	left join  ts_organizationinfo orinfo   on cinfo.CI_ROOTORGANIZATIONID_INT =orinfo.OI_ID_INT   left join  ts_organizationinfo porinfo   on cinfo.CI_ORGANIZATIONID_INT =porinfo.OI_ID_INT  left join  global_userinfo dutyMan   on dutyMan.UI_ID_INT=crinfo.CR_ORDERMANID_INT  	left join  global_userinfo acceptMan   on acceptMan.UI_ID_INT=crinfo.CR_OPERATORMANID_INT  	left join  TS_MaintenceDevice  md     on  md.MD_MAINTENCEID_INT = minfo.MI_ID_INT AND md.ISDELETE_BIT = 0  left  join TS_TechnicianAreaRelation  TAR ON TAR.TAR_TECHNICIANID_INT = minfo.MI_ACCEPTMANID_INT  	left  join TS_CompanyOrganization    sa on sa.CO_ID = TAR.TAR_AREAID_INT and sa.ISDELETE_BIT = 0   	 	left  join ts_companyinfo companyInfo on companyInfo.COMPANY_ID =  crinfo.COMPANY_ID     left join  TS_SatisfactionInfo sfinfo   on minfo.MI_ID_INT=sfinfo.SI_MATINCEID_INT")
    // , orinfo.OI_ID_INT  as  orgrootid,        orinfo.OI_ORGANIZATIONNAME_VARC  as  orgrootname,        porinfo.OI_ID_INT  as  orgid,        porinfo.OI_ORGANIZATIONNAME_VARC as    orgname, porinfo.IsCustom,        cinfo.CI_CUSTOMCODE_VARC  as  branchnetcode,        CI_CUSTOMTYPE_INT  as  branchnettype,        cinfo.CI_CUSTOMNAME_VARC as  branchnetname,
    //        cinfo.CI_ID_INT  as  branchnetid, cinfo.PLID as branchnetplid,         companyInfo.COMPANY_ID     as   companyid,     	companyInfo.COMPANY_NAME   as  companyname,  maintence.StatusId  as  originalOrderStatus,  mDevice.TerminalName AS TerminalName,     	mDevice.DeviceName    as  MaintenceDeviceName,     	newLocation.LocationName as LocationName,     	newLocationType.TypeName  as  TypeName,     	newManager.DBM_Location_Detail_VarC  as LocationDetail,     	newManager.DBM_Serial_Number_VarC as SerialNumber,     	newBrandInfo.BI_BRANDNAME_VARC  as  BRANDNAME,     	newBrandDevice.KBD_DEVICENAME_VARC  as  BrandDEVICENAME,     	oldManager.DBM_Serial_Number_VarC AS oldSerialNumber,     	oldBrandInfo.BI_BRANDNAME_VARC AS oldBRANDNAME,     	oldBrandDevice.KBD_DEVICENAME_VARC AS oldDEVICENAME,
    //   	repairs.ID AS repairid,	repairs.MaintenanceId, sa.PLID  as  companyorgplid
   // val df = sqlContext.sql(s" SELECT     	    RepairSpeed as	Speed,   RepairEffect  	as  Effect,   RepairAttitude  as   	Attitude,RepairAdvice  as	Advice, '0014' AS sourceflag,          CI_Level_VarC      as  branchnetlevel,       CI_CUSTOMFULLNAME_VARC  as  branchnetfullname,          	repairs.RepairOrderNo,     	repairs.OrderTime,       	maintence.ReservationTime,     	maintence.ReceiveTime,     	maintence.DepartTime,     	maintence.ArriveTime,     	maintence.RepairTime,     	maintence.CloseTime,          actionstatus.StatusName  as  OrderStatus,      	dutyMan.UI_CONTACTNAME_VARC AS DutyEngineer,     	acceptMan.UI_CONTACTNAME_VARC AS AcceptMan,     	mDevice.MalfunctionType AS MalfunctionType,sa.CO_LEVEL as companyorglevel   FROM     	Koala_Repairs repairs     left JOIN Koala_Maintenance maintence ON maintence.ID = repairs.MaintenanceId       and  repairs.IsDelete = 0     INNER JOIN  koala_maintenance_action_status actionstatus  on  maintence.StatusId=actionstatus.ID      and  actionstatus.IsDelete =0       INNER JOIN ts_custominfo cinfo ON cinfo.CI_ID_INT = repairs.CustomId     INNER JOIN ts_organizationinfo orinfo ON orinfo.OI_ID_INT = cinfo.CI_ROOTORGANIZATIONID_INT      left join  ts_organizationinfo porinfo   on cinfo.CI_ORGANIZATIONID_INT =porinfo.OI_ID_INT        LEFT JOIN global_userinfo dutyMan ON dutyMan.UI_ID_INT = repairs.DutyEngineerId     LEFT JOIN global_userinfo acceptMan ON acceptMan.UI_ID_INT = maintence.AcceptManId     LEFT JOIN Koala_Maintenance_Device mDevice ON mDevice.MaintenanceId = maintence.ID     AND mDevice.IsDelete = 0      left  join TS_TechnicianAreaRelation  TAR ON TAR.TAR_TECHNICIANID_INT = maintence.AcceptManId  	left  join TS_CompanyOrganization    sa on sa.CO_ID = TAR.TAR_AREAID_INT and sa.ISDELETE_BIT = 0   	 	left  join ts_companyinfo companyInfo on companyInfo.COMPANY_ID = repairs.CompanyId      where repairs.Updatetime<\'${currentdate}\' ")
    val df = sqlContext.sql(s" SELECT    	RepairSpeed AS Speed,    	RepairEffect AS Effect,    	RepairAttitude AS Attitude,    	RepairAdvice AS Advice,    	'0014' AS sourceflag,      repairs.CustomId  as servicebranchnetid,      repairs.DutyEngineerId  as  servicedutyuserid,      repairs.OrderManId   as  serviceorderuserid,    	repairs.RepairOrderNo,    	repairs.OrderTime,      repairs.CompanyId  as  servicecompanyid,      maintence.AcceptManId  as  serviceacceptmanuserid,    	maintence.ReservationTime,    	maintence.ReceiveTime,    	maintence.DepartTime,    	maintence.ArriveTime,    	maintence.RepairTime,    	maintence.CloseTime,    	actionstatus.StatusName AS OrderStatus,    " +
      s"  mDevice.MaintenanceType  as  maintenanceoriginaltype,(case mDevice.MaintenanceType when  1  then  '设备故障'  when  2  then  '线路故障'  when  3  then  '其它服务'  end)  as    MaintenanceType,   " +
      s"	mDevice.MalfunctionType AS MalfunctionType,      statis.RiskMinute,        statis.OrderResponseMinute,     statis.DepartResponseMinute,     statis.ArriveResponseMinute,    statis.RepairedResponseMinute  " +
      s"  FROM    	Koala_Repairs repairs    LEFT JOIN Koala_Maintenance maintence ON maintence.ID = repairs.MaintenanceId    AND repairs.IsDelete = 0    INNER JOIN koala_maintenance_action_status actionstatus ON maintence.StatusId = actionstatus.ID    AND actionstatus.IsDelete = 0  " +
      s"   LEFT JOIN Koala_Maintenance_Device mDevice ON mDevice.MaintenanceId = maintence.ID    AND mDevice.IsDelete = 0    left join   koala_repairs_statistics statis    on  repairs.ID=statis.RepairId "+
      s"   where repairs.Updatetime<\'${currentdate}\' ")
    // LEFT JOIN TS_DeviceBrandManager newManager ON newManager.DBM_ID_INT = mDevice.DeviceBrandManagerId     LEFT JOIN TS_KoalaBrandDevice newBrandDevice ON newBrandDevice.KBD_PLID_INT = newManager.DBM_BRANDDEVICE_INT     LEFT JOIN TS_BrandInfo newBrandInfo ON newBrandInfo.BI_BRANDID_INT = newBrandDevice.KBD_BRANDID_INT     LEFT JOIN Koala_Device_Location_Type newLocationType ON newLocationType.ID = newManager.DBM_Location_Type_Int     LEFT JOIN Koala_Device_Location newLocation ON newLocationType.DeviceLocationId = newLocation.ID     LEFT JOIN TS_DeviceBrandManager oldManager ON oldManager.DBM_ID_INT = mDevice.OldDeviceBrandManagerId     LEFT JOIN TS_KoalaBrandDevice oldBrandDevice ON oldBrandDevice.KBD_PLID_INT = oldManager.DBM_BRANDDEVICE_INT     LEFT JOIN TS_BrandInfo oldBrandInfo ON oldBrandInfo.BI_BRANDID_INT = oldBrandDevice.KBD_BRANDID_INT
    //.collect.foreach(println)
    //增量     左闭右开
    val lastdate = currentdate.minusDays(1)
    //val df =sqlContext.sql(s"SELECT ID as actionid,USERID,ACTTYPE,MEMO  from  t_useractionlog  where  LASTMODIFY>=\'${lastdate}\'  and     LASTMODIFY<\'${currentdate}\' ")
    df.write.mode(SaveMode.Overwrite).json("hdfs://192.168.11.21:8020/hivetemp/fact_repairmaintenance")
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.read.json("hdfs://192.168.11.21:8020/hivetemp/fact_repairmaintenance/part*")
      .registerTempTable("fact_repairmaintenancetemp")


    hiveContext.sql("use datawarehouse")
    //初始化  全量
    hiveContext.sql("drop table   if  exists  fact_repairmaintenance")
    hiveContext.sql("create table if not exists  fact_repairmaintenance as  select  * from   fact_repairmaintenancetemp")

    //增量
    //val data=hiveContext.sql("select  * from   fact_repairmaintenancetemp")
    //data.write.mode("append").saveAsTable("fact_repairmaintenance")



    sc.stop


  }

}
