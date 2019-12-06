#!/usr/bin/env python
import snowflake.connector
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd
import time
import pickle
import os, traceback

def snowflake_connector(tableLists):
    # connect to snowflake database
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


class cacheSFTables:
    def __init__(self):
        self.path = "pickleData"
        self.filename = self.path + "/tableDict.pickle"
        self.tableLists = ["table1"]


    def cacheTables(self):    
        '''SAVE SNOWFLAKE TABLES INTO A PICKE FILE'''
        try: 
            if not os.path.exists(self.path):
                os.makedirs(self.path)
            tableDict = snowflake_connector(self.tableLists)
            with open(self.filename, 'wb') as handle:
                pickle.dump(tableDict, handle)
            if not os.path.exists(self.filename):
                return "ERROR: picke didn't succesfully save the snowflake data to 'pickleData/tableDict.pickle'."
        except:
            return "ERORR: cacheTables() within cacheSFTables.py does not work properly"
        return "SUCCESSFULLY CACHED SNOWFALKE TABLES INTO A PICKE FILE: '{}'".format(self.filename)


    def getTablesFromCache(self):
        '''ACCESS PICKE FILE CONTAINING ALL TABLES'''
        try:
            with open(self.filename, 'rb') as handle:
                tableDict = pickle.load(handle)
            for tablename in tableDict:
                df = tableDict[tablename]
                df.columns = map(str.upper, df.columns)
                tableDict[tablename] = df
            return tableDict
        except FileNotFoundError:
            return {"hasError": True, 
                    "errorCode:": "301", 
                    "errorMsg:": "FileNotFoundError: {} is not found.".format(self.filename)}
      

        
if __name__ == "__main__":
    # start = time.time()
    # tableDict = snowflake_connector(tableNameList)
    # for tablename in tableDict:
    #     print("{} : , row : {}， col: {}".format(tablename, tableDict[tablename].shape[0], tableDict[tablename].shape[1]))
    #     print(tableDict[tablename].head(10))
    # end = time.time()
    # print("SNOWFALKE CONNECTION TIME(s): {:.2f}".format(end-start))
    cache =  cacheSFTables()
    print(cache.cacheTables())
