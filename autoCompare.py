'''
Created on Sun Feb 23 14:45:00 2020
THIS TOOL IS TO COMPARE STATS OF TWO COLUMNS FROM TWO FILES
@author: Qian Li, qianelsali@gmail.com
'''
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats
import datetime
import os

params = {"dataPath": "rawData",
          "fileA_name": "fileA.csv", 
          "fileB_name": "fileB.csv",
          "labelA": "fileA",
          "labelB": "fileB",
          "method": "mean",
          "fileA_filterDict": None, 
          "fileB_filterDict": {"H": "Y", "G": "GREEN"},
          "fileA_sampleIdCol": ["B"], 
          "fileA_varCol": ["R"],
          "fileB_sampleIdCol" : ["A"], 
          "fileB_varCol": ["Y"],          
          "figPath": "figs",
          "dropSampeId": ['19B175'],
           }
    
def ImportSourceFile(*args):    
    fileList = []
    for filename in args:
        filePath = "{}/{}".format(params["dataPath"],filename)
        if "csv" in filename:
            fileList.append(pd.read_csv(filePath))
        elif "xlsx" in filename: 
            fileList.append(pd.read_excel(filePath))
    return fileList

def MakeStringCapital(df):
    cols = [col for col, dt in df.dtypes.items() if dt == object]
    for col in cols:
        df[col] = df[col].map(lambda x: x.upper())
    return df

def ConvertCol2Num(col_str):
    """ Convert base26 column string to number. """
    expn = 0
    col_num = 0
    for char in reversed(col_str):
        col_num += (ord(char) - ord('A') + 1) * (26 ** expn)
        expn += 1
    return col_num

def ConvertCol2Name(df, col_strs):
    '''cols_strs: 
        eg1 : ["A", "b", "cd"]   non-sensitive to capital
        eg2: "A"
        '''
    if type(col_strs)==list:        
        colIxs = [ConvertCol2Num(col_str) - 1 for col_str in col_strs]
        colNames = [df.columns[ix] for ix in colIxs]
    if type(col_strs)==str:
        idIx = ConvertCol2Num(col_strs)-1
        colNames = df.columns[idIx]         
    return colNames

def ApplyFilter(df, filterDict=None):
    '''
    filterDict = {"A", "YES"}
    '''
    colNames = df.columns    
    if type(filterDict)==dict:
        for col in filterDict:
            val = filterDict[col].upper()
            colIx = ConvertCol2Num(col) - 1
            colName = colNames[colIx] 
            if val not in df[colName].unique():
                raise ('ALERT: <{}> DOES NOT EXIST IN COLUMN <{}>'.format(val, colName))
            df = df[df[colName]==val]
    return df

def PivotTable(df=None, sampleIdCol=None, cols=None, method=None):
    '''example input: 
        id_col = "A"                      # sample id col
        cols = ["B", "C", "D"]            # cols to be pivoted
        method:  mean, std, min, max, count
    '''
    if not cols:
        raise('NO COLUMN IS GIVEN.')
    if not sampleIdCol:
        raise('SAMPLE ID COLUMN NAME IS NOT GIVEN.')
    colNames = ConvertCol2Name(df, cols)
    idColName = ConvertCol2Name(df, sampleIdCol)
    tb = df[colNames + idColName].groupby(idColName).agg(method)
    return tb


def FilterByIndex(df, indexlist):
    return df.loc[df.index.isin(indexlist)]

def FilterByOverlapSampleId(dfs=[None]):   
    df_ix_ls = [set(df.index) for df in dfs]
    sampleIDs = set.intersection(*df_ix_ls)
    return [FilterByIndex(df, sampleIDs) for df in dfs]

def DropSampleId (df, sampleIdList):
    return df.loc[~df.index.isin(sampleIdList)]

def PlotFigure(merged,tb1_std, tb2_std, fileAColName,fileBColName, lab1, lab2):
    x,y = merged[lab1].values, merged[lab2].values
    slope, intercept, r_value, p_value, std_err = stats.linregress(x,y)
    plt.figure(figsize=(10,10))
    plt.style.use('seaborn-poster')
    plt.scatter(x,y,c='r')
    plt.plot(x, intercept + slope*x, 'y', label="y={:.4f}x+{:.4f} \n\nR2={:.4f} \n\np={:.4f}\n\nNum_samples = {}".format(slope,intercept,r_value**2,p_value,len(x)))
    plt.errorbar(x, y,yerr= tb2_std.values, xerr= tb1_std.values, ecolor = "r", fmt='o')
    plt.xlabel("{} \n {}".format(lab1, fileAColName))
    plt.ylabel("{} \n {}".format(lab2, fileBColName))
    plt.ylim(-8,4)
    plt.xlim(-8,4)
    plt.title("{}".format(params["method"].upper()))
    plt.legend(loc=2)
    plt.savefig("{}/{}_{}_{}.jpg".format(params["figPath"],
                                         fileBColName,
                                         params["method"],
                                         str(datetime.date.today()),
                                        ))

def main():
    if not os.path.exists(params["figPath"]):
        os.makedirs(params["figPath"])
    # load data and filter
    [df_A, df_B] = ImportSourceFile(params["fileA_name"],params["fileB_name"] )
    [df_A, df_B] = map(MakeStringCapital,[df_A, df_B])
    df_A_filter = ApplyFilter(df=df_A, filterDict=params["fileA_filterDict"]) 
    df_B_filter = ApplyFilter(df=df_B, filterDict=params["fileB_filterDict"])    
    # get pivot tables
    tb1 = PivotTableMTF(df=df_A_filter,sampleIdCol=params["fileA_sampleIdCol"], cols=params["fileA_varCol"],method=params['method'])
    tb2 = PivotTableMTF(df=df_B_filter,sampleIdCol=params["fileB_sampleIdCol"],cols=params["fileB_varCol"],method=params['method'])  
    tb1_std = PivotTableMTF(df=df_A_filter,sampleIdCol=params["fileA_sampleIdCol"], cols=params["fileA_varCol"], method="std")    
    tb2_std =  PivotTableMTF(df=df_B_filter,sampleIdCol=params["fileB_sampleIdCol"],cols=params["fileB_varCol"], method="std")  
    # keep shared sample ids
    [tb1, tb2, tb1_std, tb2_std] = FilterByOverlapSampleId(dfs=[tb1,tb2, tb1_std, tb2_std])
    fileAColName = ConvertCol2Name(df_A, params["fileA_varCol"])[0]
    fileBColName = ConvertCol2Name(df_B, params["fileB_varCol"])[0]
    lab1, lab2 = params["labelA"], params["labelB"]
    # rename column names
    tb1.columns, tb2.columns = [lab1], [lab2]
    merged = tb1.merge(tb2, left_index=True, right_index=True).sort_values(by=[lab1, lab2])  
    [merged, tb1_std, tb2_std] = [DropSampleId(df, params["dropSampeId"]) for df in [merged, tb1_std, tb2_std]]
    # plot
    PlotFigure(merged,tb1_std, tb2_std, fileAColName,fileBColName,lab1,lab2)

if __name__ == "__main__":
    main()
