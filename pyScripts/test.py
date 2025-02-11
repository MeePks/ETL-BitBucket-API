import os
import sqlFunctions as sfn
import pandas as pd
import shutil
destination_dir=rf'\\ccaintranet.com\DFS-FLD-01\Imaging\Client\Out\Lowes\Emails_LiveFeed\testfiles\test'

testtable=sfn.open_sql_connection('lowes.stg.sql.ccaintranet.com','Lowesdataemaillivefeed')
df_tabledata=pd.read_sql_table('test10',testtable,index_col=None)
for index,rows in df_tabledata.iterrows():
    shutil.move(rows.NewPath, destination_dir)
    print(rows.NewPath)





