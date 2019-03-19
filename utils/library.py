# -*- coding: utf-8 -*-
"""
Created on Wed Mar  8 13:38:44 2017

@author: AustinPeel
"""

import pandas as pd
import re , sqlite3, os
from pandas.io.json import json_normalize
from utils import version3



def getLastResponse(file):
    lastDF = pd.read_csv(file,encoding="ISO-8859-1")
    last = lastDF.tail(1)
    lastResponse = str(last['ResponseID'].values)
    lastResponse = re.sub('\[' ,"",lastResponse)
    lastResponse = re.sub('\]' ,"",lastResponse)
    lastResponse = re.sub("\'" ,"",lastResponse) 
    return lastResponse

def getLastEndDate(file):
    lastDF = pd.read_csv(file,encoding="ISO-8859-1")
    last = lastDF.tail(1)
    lastResponse = str(last['EndDate'].values)
    lastResponse = re.sub('\[' ,"",lastResponse)
    lastResponse = re.sub('\]' ,"",lastResponse)
    lastResponse = re.sub("\'" ,"",lastResponse) 
    return lastResponse

def getFirstEndDate(file):
    lastDF = pd.read_csv(file,encoding="ISO-8859-1")
    first = lastDF.head(3)
    last = first.tail(1)
    lastResponse = str(last['EndDate'].values)
    lastResponse = re.sub('\[' ,"",lastResponse)
    lastResponse = re.sub('\]' ,"",lastResponse)
    lastResponse = re.sub("\'" ,"",lastResponse) 
    return lastResponse

def getSurveyCounts(file):
    df = pd.read_csv(file,encoding="ISO-8859-1")
    count = len(df.index)
    return count
    
def getDataFrame(location,fileName):
    name= location + '/' + fileName    
    df = pd.read_csv(name)
    df =df.drop(df.index[[0,1]])
    df = df.rename(columns= {list(df)[0]:'ID'})
    wide = pd.melt(df, id_vars="ID")
    #wide = wide[wide['variable'].str.startswith('Q')]
    return wide

def getQuestionLookup(surveyJSON):
    df2 = pd.DataFrame()
    for key in surveyJSON['result']['questions'].keys():
        df = json_normalize(surveyJSON['result']['questions'][key])
        df['QID'] = key
        df2 =df2.append(df)
    cols=['questionText','questionLabel','QID']
    df2 = df2[cols]
    return df2

def getColumnMappings(surveyJSON):
    df2 = pd.DataFrame()
    for key in surveyJSON['result']['exportColumnMap'].keys():
            df3 = json_normalize(surveyJSON['result']['exportColumnMap'][key])
            df3['mapping'] = key
            df2 =df2.append(df3)
    df2 = df2.rename(columns={'question': 'QID'}) 
    return df2

def getQuestionChoices(surveyJSON):
    df2 = pd.DataFrame()
    for key in surveyJSON['result']['questions'].keys():
            df = json_normalize(surveyJSON['result']['questions'][key])
            df['QID'] = key
            df2 =df2.append(df)
    cols=['questionText','questionLabel','QID']
    wide = pd.melt(df2, id_vars=cols)
    wide2 =  wide.dropna(subset=['value']) 
    wide2 = wide2[wide2['variable'].str.endswith('Text')]
    wide2['variable'] = wide2['variable'].str.replace('.choiceText', '')
    wide2['choices'] = wide2['QID'] + "." + wide2['variable']
    cols = ['choices','value']
    final = wide2[cols]
    return final


def getColumnInfo(surveyJSON):
    df = getQuestionLookup(surveyJSON)
    df2 = getColumnMappings(surveyJSON)
    df = pd.merge(df,df2,on="QID",how='left')
    df = df.fillna('')
    df['choices'] =df['choice'] +df['subQuestion']
    del df['choice']
    del df['subQuestion']
    del df['textEntry']
    df2 = getQuestionChoices(surveyJSON)
    df = pd.merge(df,df2,on='choices',how='left')
    return df

def getSurveyInfo(surveyJSON):
    surveyName = surveyJSON['result']['name']
    df = json_normalize(surveyJSON['result']['responseCounts'])
    size = len(surveyJSON['result']['questions'])
    df['size'] = size
    df['surveyName'] = surveyName
    return df 

def surveyToSqlite(sqlDB,folderLocation,token,survey):
    conn = sqlite3.connect(folderLocation + "/" + sqlDB + '.db.sqlite')
    surveyJSON = version3.qualtrics(token,survey).getSurveyInfo()
    df  = getSurveyInfo(surveyJSON)
    try:
        sendSQLite(df,'surveyInfo',conn)
    except sqlite3.OperationalError:
        creatTableLite(df,'surveyInfo',conn)
        sendSQLite(df,'surveyInfo',conn)
        
def getSurveyName(sqlDB,folderLocation): 
    conn = sqlite3.connect(folderLocation + "/" + sqlDB + '.db.sqlite')
    sql = 'SELECT surveyName FROM surveyInfo ORDER BY ROWID ASC LIMIT 1'
    name = pullSqlite(sql,conn)
    return str(name[0][0])

def getSurveyDownloadData(file,timeStamp):
    last = getLastResponse(file)
    count = getSurveyCounts(file)
    firstDate = getFirstEndDate(file)
    lastDate = getLastEndDate(file)
    d = {'lastResponse': last, 'responseCount': count,'fromDate':firstDate,'toDate':lastDate,'timeStamp':timeStamp}
    df = pd.DataFrame(data=d,index=[timeStamp])
    return df

def surveyDownloadsToSqlite(sqlDB,file,folderLocation,timeStamp):
    conn = sqlite3.connect(folderLocation + "/" + sqlDB + '.db.sqlite')
    df  = getSurveyDownloadData(file,timeStamp)
    try:
        sendSQLite(df,'surveyDownload',conn)
    except sqlite3.OperationalError:
        creatTableLite(df,'surveyDownload',conn)
        conn = sqlite3.connect(folderLocation + "/" + sqlDB + '.db.sqlite')
        sendSQLite(df,'surveyDownload',conn)

def getLastResonseSqlite(sqlDB,folderLocation): 
    conn = sqlite3.connect(folderLocation + "/" + sqlDB + '.db.sqlite')
    sql = 'SELECT lastResponse FROM surveyDownload ORDER BY ROWID DESC LIMIT 1'
    name = pullSqlite(sql,conn)
    return str(name[0][0])

def getLastTimeStampSqlite(sqlDB,folderLocation): 
    conn = sqlite3.connect(folderLocation + "/" + sqlDB + '.db.sqlite')
    sql = 'SELECT timeStamp FROM surveyDownload ORDER BY ROWID DESC LIMIT 1'
    name = pullSqlite(sql,conn)
    return str(name[0][0])

def saveMetaDataToCSV(sqlDB,tableName):
    conn =  sqlite3.connect(sqlDB)
    df =  pd.read_sql_query("select * from " + tableName, conn)
    df.to_csv(os.path.dirname(sqlDB)+"/"+tableName+".csv")
    print("csv saved, thank you")
    conn.close()

def getTuples(df):
    for r in df.columns.values:
        df[r] = df[r].map(str)
        df[r] = df[r].map(str.strip)   
    tuples = [tuple(x) for x in df.values]
    return tuples
    
    

def remove_wrong_nulls(x,tuples):
    for r in range(len(x)):
        for i,e in enumerate(tuples):
            for j,k in enumerate(e):
                if k == x[r]:
                    temp=list(tuples[i])
                    temp[j]=None
                    tuples[i]=tuple(temp)
    return tuples

               
def chunks(l, n):
    n = max(1, n)
    return [l[i:i + n] for i in range(0, len(l), n)]


def getListByChunks(tuples):
    string_list = ['NaT', 'nan', 'NaN', 'None']
    tuples2 = remove_wrong_nulls(string_list,tuples)
    new_list = chunks(tuples2, 1000)
    return new_list

def getQuery(df,datasource):
    a = list(df.columns)
    b =','.join("{0}".format(x) for x in a)
    records=[]
    for i in a:
        records.append("?")
    c=','.join("{0}".format(x) for x in records)
    query = """insert into """ + datasource +""" (""" + b +""") values("""+c+""")"""   
    return query


def getQueryCreate(df,datasource):
    a = list(df.columns)
    b =' VARCHAR(max),'.join("{0}".format(x) for x in a)
    b = b + ' VARCHAR(max)'
    query = ''"CREATE TABLE """ + datasource +""" (""" + b +""")""" 
    return query


    
def getQuery2(df,datasource):
    columns =[]
    for c in df:
        print(c)
        if df[c].dtype in ['float32','int64','float64','int8']: 
            b = {'column':c,'size': "",'type':'FLOAT'}  
        elif df[c].dtype in ['object']:
                b = {'column':c,'size': df[c].astype(str).dropna().map(len).max(),'type':'VARCHAR'}
        elif df[c].dtype == 'bool':
            b = {'column':c,'size': "",'type':'bit'}           
        else:
            b = {'column':c,'size': "MAX",'type':'VARCHAR'}
        columns.append(b)
    a= ""        
    for c in columns:
        try:
            if c['type'] == "VARCHAR":
                string = str(c['column']) + " VARCHAR(" +str(int(c['size']))+ ")," 
            elif c['type'] == "FLOAT":
                string = str(c['column']) + " FLOAT," 
            elif c['type'] == "BOOLEAN":              
                string = str(c['column']) + " bit," 
            else:
                string = str(c['column']) + " VARCHAR(MAX)," 
        except:
                string = str(c['column']) + " VARCHAR(MAX),"
        a = a + string                
    a = a[:-1]
    query = ''"CREATE TABLE """ + datasource +""" (""" + a +""")"""
    return query

def sendSQLite(df,datasource,conn):
    tuples = getTuples(df)
    new_list = getListByChunks(tuples)
    query = getQuery(df,datasource)
    #conn = sqlite3.connect(connection)
    c =conn.cursor()    
    for i in range(len(new_list)):
        c.executemany(query, new_list[i])
    conn.commit()
    print("data sent")
    print("Connection Closed")

def creatTableLite(df,datasource,conn):
    q = getQuery2(df,datasource)
    #conn = sqlite3.connect(connection)
    cursor=conn.cursor()    
    cursor.execute(q)
    print("Table Created")
    conn.commit()
    
    
def pullSqlite(sqlCode,connection):
    cursor = connection.cursor()
    cursor.execute(sqlCode)
    rows = cursor.fetchall()
    return rows

def dropTableLite(table,conn):            
    sql = "drop table "+ table
    #conn = sqlite3.connect(connection)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    print(table," dropped from SQL database")
    
    
def dropRowsLite(table,row,condition,conn):
    sql = "delete  FROM " + table + " WHERE " + row + " = '"+condition+"';"          
    #conn = sqlite3.connect(connection)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
     

def createAddDrop(df,table,conn,dropFirst=False):
    if dropFirst == True:
        try:
            #conn = sqlite3.connect(connection)
            dropTableLite(table,conn)
        except:
            print("no tables to drop")
    try:
        #conn = sqlite3.connect(connection)
        creatTableLite(df,table,conn)
    except:
        print("table already created, appending data")
    #conn = sqlite3.connect(connection)
    sendSQLite(df,table,conn) 