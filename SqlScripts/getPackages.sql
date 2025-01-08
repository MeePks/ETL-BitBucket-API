select @@servername DeployedServer,
		f.name as AuditName,
		prj.Name,
		deployed_by_name DeployedBy,
		last_deployed_time LastDeployedTime,
		prj.created_time CreatedTime,
		p.name as PackageName,
		GetDate() Date
	from ssisdb.catalog.packages p
		inner join 
			SSISDB.catalog.projects prj
				on p.project_id=prj.project_id
		inner join 
		SSISDB.catalog.folders f
				on f.folder_id=prj.folder_id
	order by f.name