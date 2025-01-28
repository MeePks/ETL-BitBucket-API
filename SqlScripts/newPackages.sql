select sp.AuditName,Cast(sp.numofmaps-sds.numofmaps as int) NewMaps,Cast(Getdate() as Date) as DateReportCreated
from dbo.___ssisDeployedServers sds
inner join 
 (SELECT auditname,count(*) numofmaps 
  FROM [dbo].[___SSISpackages]
  group by auditname) sp on sp.AuditName=sds.Client