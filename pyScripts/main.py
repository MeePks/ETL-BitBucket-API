import pandas as pd
import sqlFunctions as sfn
import configparser as cp

#reading config file


#reading sql query from sql script
with open ('.\SqlScripts\getPackages.sql','r') as file:
    ssis_server_query=file.read()

pd.read_sql_query(ssis_server_query,)