#!/usr/bin/env python
import snowflake.connector
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd
import time

def snowflake_connector(tableLists):
    url = URL(
            account = 'account',
            user = 'user',
            password = 'password',
            database = 'database_name',
            schema = 'schema_name',
            warehouse = 'wh_name',
            role='role_name'
        )
    engine = create_engine(url)
    connection = engine.connect()
    tableDict = {}
    for tablename in tableLists:
        query = ''' select * from {};'''.format(tablename)
        df = pd.read_sql(query, connection)
        df.columns = map(str.upper, df.columns)
        tableDict[tablename] = df
    connection.close()
    engine.dispose()
    return tableDict


if __name__ == "__main__":
    start = time.time()
    tableDict = snowflake_connector(tableNameList)
    for tablename in tableDict:
        print("{} : , row : {}， col: {}".format(tablename, tableDict[tablename].shape[0], tableDict[tablename].shape[1]))
        print(tableDict[tablename].head(10))
    end = time.time()
    print("SNOWFALKE CONNECTION TIME(s): {:.2f}".format(end-start))
