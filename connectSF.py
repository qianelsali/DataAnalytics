from snowflake_connector import snowflake_connector
import pickle
import os, traceback

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
        '''ACCESS PICKE FILE CONTAINING ALL TABLES FOR CARS CALCULATOR'''
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
   cache =  cacheSFTables()
   print(cache.cacheTables())
