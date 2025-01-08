Insert into [dbo].[___SSISpackages_clean]
select * 
from [dbo].[___SSISpackages] sp
where  not exists (
select 1 from [dbo].[___SSISpackages_clean] spc
where sp.PackageName=spc.PackageName
and sp.AuditName=spc.AuditName)

Update sds set NumOfMaps=sp.numofmaps
from ___ssisDeployedServers sds
inner join 
 (SELECT auditname,count(*) numofmaps--select * 
  FROM [dbo].[___SSISpackages]
  group by auditname) sp on sp.AuditName=sds.Client