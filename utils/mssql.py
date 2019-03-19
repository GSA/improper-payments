# -*- coding: utf-8 -*-
"""
Created on Tue Nov 15 14:34:24 2016

@author: AustinPeel
"""
import urllib, pyodbc
import sqlalchemy as sl
import pandas as pd
#from config import serverName , userName ,password ,database 
from config import sql_enviorn 

serverName = sql_enviorn['serverName']
password =  sql_enviorn['password']
database= sql_enviorn['database']
userName =   sql_enviorn['userName']

class pull():
    
    def __init__(self,tableName,schema="dbo"):
         self.connection =  pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=" + serverName + ";DATABASE="+ database +";UID="+userName+";PWD=" +password)
         self.schema = schema
         self.table = tableName
		 
    def columns(self):            
        y = self._pullSQLData("SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('"+self.schema+"."+ self.table+"')")
        df = pd.DataFrame(y)
        columns = list(df[1])
        return columns
    
    def data(self,cols=None):
        if cols is not None:
            query = self._getQuery(cols)
            df = pd.DataFrame(self._pullSQLData(query))
            df.columns =  cols
        else:
            df = pd.DataFrame(self._pullSQLData("SELECT * FROM ["+ database+"].["+self.schema+"].["+ self.table+"]"))
            names = self.columns()
            df.columns =  names
        return df
    
    def _pullSQLData(self,sqlCode):
        sql = sqlCode
        cursor = self.connection.cursor()
        rows = cursor.execute(sql).fetchall()
        return rows    

    def _getQuery(self,columns):
        b =','.join("{0}".format(x) for x in columns)
        query = """select """ +b + " FROM ["+ database+"].["+self.schema+"].["+ self.table+"]"
        return query

# actionType can take 3 arguments append, drop, or create. 
class send():  
    def __init__(self,df,tableName,schema="dbo",actionType = "append"):  
         self.params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=" + serverName + ";DATABASE="+ database +";UID="+userName+";PWD=" +password)
         self.engine = sl.create_engine("mssql+pyodbc:///?odbc_connect=%s" % self.params, module=pyodbc)
         self.metadata = sl.MetaData(self.engine)
         self.connection =  pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=" + serverName + ";DATABASE="+ database +";UID="+userName+";PWD=" +password)
         self.schema = schema
         self.table = tableName
         self.df = df
         self.actionType = actionType


#send a dataframe to SQL database. Be carefull! send.data() will delete previous data if not actionType != append

    def data(self):
      if self.actionType == "append":
        try:
            self._send()
        except:
            print("error:could not append data")
        print("data appended to"+ self.table)
      elif self.actionType == "drop":
        try:
            self._drop()
            print("previous table dropped.")
        except:
            print("no data to drop")
        self._create()
        self._send()
      elif self.actionType == "create":
        self._create()  
      elif self.actionType == "truncate":
         try:
           self._truncate()
         except:
             print("no table to truncate, trying to create table")
             self._create()
         try:
             self._send()
         except:
             print("can not send data.")
      else: 
         print("error: something happened")
             
    def _truncate(self):            
        sql = "truncate table ["+database+"].["+self.schema+"].["+self.table+"]"
        cursor = self.connection.cursor()
        cursor.execute(sql)
        self.connection.commit()
        print(self.table," truncated from database no data left")
    
    def _drop(self):            
        sql = "drop table ["+database+"].["+self.schema+"].["+self.table+"]"
        cursor = self.connection.cursor()
        cursor.execute(sql)
        self.connection.commit()
        print(self.table," dropped from database no data left")
        
    def _getTuples(self):
        for r in self.df.columns.values:
            self.df[r] = self.df[r].map(str)
            self.df[r] = self.df[r].map(str.strip)   
        tuples = [tuple(x) for x in self.df.values]
        return tuples
      

    def _remove_wrong_nulls(self,x,tuples):
        for r in range(len(x)):
            for i,e in enumerate(tuples):
                for j,k in enumerate(e):
                    if k == x[r]:
                        temp=list(tuples[i])
                        temp[j]=None
                        tuples[i]=tuple(temp)
        return tuples

               
    def _chunks(self,l, n):
        n = max(1, n)
        return [l[i:i + n] for i in range(0, len(l), n)]


    def _getListByChunks(self,tuples):
        string_list = ['NaT', 'nan', 'NaN', 'None']
        tuples2 = self._remove_wrong_nulls(string_list,tuples)
        new_list = self._chunks(tuples2, 1000)
        return new_list
    
    def _getQuery(self):
        a = list(self.df.columns)
        b =','.join("{0}".format(x) for x in a)
        records=[]
        for i in a:
            records.append("?")
        c=','.join("{0}".format(x) for x in records)
        query = """insert into """ + self.table +""" (""" + b +""") values("""+c+""")"""   
        return query
    
    def _send(self):
        tuples = self._getTuples()
        new_list = self._getListByChunks(tuples)
        query = self._getQuery()
        cursor=self.connection.cursor()    
        for i in range(len(new_list)):
            cursor.executemany(query, new_list[i])
        self.connection.commit()
        print("data sent to:" + self.table)

    
    def _getQueryCreate(self):
        a = list(self.df.columns)
        b =' VARCHAR(max),'.join("{0}".format(x) for x in a)
        b = b + ' VARCHAR(max)'
        query = ''"CREATE TABLE """ + self.table +""" (""" + b +""")""" 
        return query
    
    def _create(self):
        q = self._getQuery2()
        cursor=self.connection.cursor()    
        cursor.execute(q)
        print(self.table +" Table Created")
        self.connection.commit()
        
    def _getQuery2(self):
        columns =[]
        for c in self.df:
            if self.df[c].dtype in ['float32','int64','float64','int8']: 
                b = {'column':c,'size': "",'type':'FLOAT'}  
            elif self.df[c].dtype in ['object']:
                    b = {'column':c,'size': self.df[c].astype(str).dropna().map(len).max(),'type':'VARCHAR'}
            elif self.df[c].dtype == 'bool':
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
        query = ''"CREATE TABLE """ + self.table +""" (""" + a +""")"""
        return query




