package datawarehouse
/*
fact
权限
资源
角色相关的数据ETL
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
object privilege {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HiveSpark").setMaster("spark://192.168.11.21:7077")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    import hiveContext._
    sql("use  tydatawarehouse")
    sql("drop   table  if  exists  datawarehouse.fact_userpushgroup")
    sql("create table  if not exists  datawarehouse.fact_userpushgroup as  select distinct emp.id as  userid,SID,emp.NAME as  username,emp.PASSWORD,IMAGE_FILE,\n MOBILE_PHONE,org.name as orgname,POST,\nemp.CREATE_ID,emp.CREATE_DATE,emp.UPDATE_ID,emp.UPDATE_DATE,custom.CUSTOMNAME,SEX,DEVICETOKEN,\n (case  emp.STATUS  when  0  then '在职' when  1 then '离职' end) as status,\n APPTYPE, CID, DEVICETYPE,email,\nabstract,title,STATUS_DATE,emp.PLID,\nUserType,Cornet,emp.CustomType\n, (case  push.GROUPTYPE   when 1  then  '报警默认组'\n when  2  then '报警邀请组'    when 3  then '布防信息推送组'\n when 4  then '布防统计信息推送群组' end)  as  GROUPTYPE,type.ALARMTYPECODE,type.CODENAME,'0012'  sourceflag  from   t_sys_customrolerelation   role2  join   t_sys_employee   emp  on   role2.USERID=emp.id and  role2.CUSTOMID=emp.CUSTOMID\n   join  t_pushgroupinfo  push\non  emp.id=push.USERID\n  join   t_alarm_alarmtyperelation  type\n  on type.ALARMTYPECODE=push.ALARMTYPEID\nleft  join  t_sys_organization  org  on  emp.ORGANIZATION_ID=org.id\njoin t_sys_custominfo  custom  on    org.CUSTOMID=custom.id ")

/*
    sql("drop table if  exists  datawarehouse.fact_authority_resource")
    sql("create table if not exists datawarehouse.fact_authority_resource as  select   distinct AUTHORITY_NAME,AUTHORITY_DESC,b.ISENABLED  as  AUTHORITY_ISENABLED,\nISDEFAULT,d.CUSTOMNAME,b.PLID  as  authPlid\n,c.ID  as  resourceid,RESOURCE_NAME,RESOURCE_DESC,RESOURCE_TYPE,RESOURCE_STRING,c.ISENABLED as  RESOURCE_ISENABLED, \nSORT_INDEX, SHOW_NAV,PARENT_ID as resource_parentid,c.PLID as resourcePlid,'0012' as  sourceflag  from  t_sys_authority_resource a   join  t_sys_authority   b  \non a.AUTHORITY_ID=b.id  join  t_sys_resource c  on  a.RESOURCE_ID=c.id\njoin  t_sys_custominfo d  on   b.customid=d.id ")

    sql("drop   table if  exists  datawarehouse.fact_role_authority")
    sql("create table if not exists  datawarehouse.fact_role_authority as  \nselect distinct AUTHORITY_NAME,AUTHORITY_DESC,b.ISENABLED as  authority_ISENABLED,\nb.ISDEFAULT  as authority_ISDEFAULT,d.CUSTOMNAME,b.PLID as  authority_PLID\n,c.ISDEFAULT as  Role_ISDEFAULT,ROLE_NAME,SORT_INDEX as  role_SORT_INDEX,c.PLID  as  role_PLID,'0012' as  sourceflag  from  t_sys_roles_authority  a  join  t_sys_authority  b\non a.AUTHORITY_ID=b.id  join  t_sys_roles c \non  a.ROLE_ID=c.id \njoin  t_sys_custominfo d on  b.CUSTOMID=d.id\nand  c.CUSTOMID=d.id")


    sql("drop table if exists  datawarehouse.fact_role_resource")
    sql("create  table if not exists  datawarehouse.fact_role_resource as  select  distinct b.id as RESOURCE_ID,RESOURCE_NAME,RESOURCE_DESC,RESOURCE_TYPE,RESOURCE_STRING,\nb.ISENABLED as  RESOURCE_ISENABLED,b.SORT_INDEX as RESOURCE_SORT_INDEX,b.SHOW_NAV  as  RESOURCE_ShowNAV,\nb.PARENT_ID as  RESOURCE_ParentID,b.PLID as RESOURCE_PLID,\nc.ISDEFAULT as Role_ISDEFAULT, ROLE_NAME, c.SORT_INDEX as Role_SORT_INDEX,CUSTOMNAME,c.PLID as Role_Plid,'0012' as  sourceflag  from  t_sys_roles_resource a  join  t_sys_resource  b \non a.RESOURCE_ID=b.id  join  t_sys_roles  c  on a.ROLE_ID=c.id\njoin t_sys_custominfo d  on  c.CUSTOMID=d.id")



    sql("drop table if exists datawarehouse.fact_pl_role_resource")
    sql("create table if not exists  datawarehouse.fact_pl_role_resource as  select distinct b.ID as ResourceId,ResourceName,ResourceDesc,ResourceType,\nResourceURL,b.Isenabled  as  Resource_Isenabled,\nb.SortIndex  as  Resource_SortIndex,\nShowNav  as  Resource_ShowNav,b.ParentID as Resource_ParentID,c.id  as roleid,RoleName,c.SortIndex as  Role_SortIndex, c.IsDefaultRole as  Role_IsDefaultRole,'0011'  as  sourceflag from  pl_role_resource_rel  a join   pl_resource  b  on  a.ResourceID=b.id\n join  pl_roles  c\non  a.RoleID=c.id   where c.IsDelete=0 ")


    sql("drop table if exists datawarehouse.fact_pl_user_role")
    sql("create table if not exists datawarehouse.fact_pl_user_role as select distinct  b.ID as  userid, SID, b.LoginName, b.PassWord, Name, NickName, Sex, ContactTel, QQ, Cornet, b.Image, Email, UserType,d.OrgName, PostID, PostName, e.AccountName, b.Type,\n(case  b.Status  when  1 then  '?ú?§'  else  '????'   end)  as  status,StatusDate,b.InsertUserId, b.InsertUser, \nb.CreateTime, b.UpdateUserId, b.UpdateUser,b.Updatetime,b.ProductValue, isAdmin, IsMaintenance, MaintenancePost, Job_Type, Brief, Birthday, B.Address, PropertyCompany, PropertyContact, PropertyTel, WeChat, AliPay, mobileSystemName, mobileSystemVersion, mobileManufacturer, mobileModel, appVersion, appProduct, lastAppAccessTime,c.id as RoleID,\nRoleName, c.SortIndex as  Role_SortIndex, c.IsDefaultRole as  Role_IsDefaultRole,'0011' as sourceflag   from  pl_user_role_rel\na  join  pl_userinfo  b  on  a.UserID=b.id\njoin  pl_roles  c  on  a.RoleID=c.id\njoin pl_organization  d  on  b.OrgID=d.id\njoin pl_openaccount   e  on  b.OpenAccountID=e.id and  d.OpenAccountID=e.id ")

*/
    sc.stop()
  }
}
