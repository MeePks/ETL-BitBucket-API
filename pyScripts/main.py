import pandas as pd
import sqlFunctions as sfn
import configparser as cp

#reading config file
config=cp.ConfigParser()
config.read('config.ini')
ssis_server_list=config['sqlconn']['ssis_deployed_list_server']
ssis_server_database=config['sqlconn']['ssis_deployed_list_database']
ssis_server_table=config['sqlconn']['ssis_deployed_list_table']

#reading sql query from sql scripts
with open ('.\SqlScripts\getPackages.sql','r') as file:
    ssis_server_query=file.read()

with open(r'.\SqlScripts\newPackages.sql','r') as file:
    new_map_query=file.read()

#getting list of servers
cntrl_sql_connection=sfn.open_sql_connection(ssis_server_list,ssis_server_database)
server_list=pd.read_sql_query(f'select distinct server from {ssis_server_table}',cntrl_sql_connection)

#iterating through servers
df_packages_details=pd.DataFrame()
for index,rows in server_list.iterrows():
    ssis_servers=sfn.open_sql_connection(rows.server,'SSISDB')
    df_packages_detail=pd.read_sql_query(ssis_server_query,ssis_servers)
    df_packages_details=pd.concat([df_packages_details,df_packages_detail])
    sfn.close_connnection(ssis_servers)

#inserting all the fetched ssis pacakges into the temp table
df_packages_details.to_sql('___SSISpackages',cntrl_sql_connection,schema='dbo',if_exists='replace',index=False)

#getting new maps
df_new_maps=pd.read_sql_query(new_map_query,cntrl_sql_connection)
print(df_new_maps)