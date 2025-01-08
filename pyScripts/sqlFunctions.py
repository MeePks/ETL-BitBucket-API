from sqlalchemy import create_engine

def open_sql_connection(server,database):
    try:
        conn_string=f'mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+server&Trusted_connection=yes'
        engine=create_engine(conn_string)
        print(f"Connected to {server}:{database}")
        return engine
    except:
        print(f'Error connecting to the Server:{server}')

def close_connnection(conn):
    try:
        conn.dispose()
    except:
        print('Error disconnecting from the server')