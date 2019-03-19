# -*- coding: utf-8 -*-
"""
Created on Tue Feb 12 11:53:04 2019

@author: AustinPeel
"""
import urllib
from sqlalchemy import create_engine,event
import pyodbc
import pandas as pd

from config import sql_enviorn 

serverName = sql_enviorn['serverName']
password =  sql_enviorn['password']
database= sql_enviorn['database']
userName =   sql_enviorn['userName']

params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=" + serverName + ";DATABASE="+ database +";UID="+userName+";PWD=" +password)
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, module=pyodbc)

@event.listens_for(engine, 'before_cursor_execute') 
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany): 
    print("FUNC call") 
    if executemany: 
        cursor.fast_executemany = True


def send_data(df,sqlDB):
    df.to_sql(sqlDB,engine,if_exists='append',chunksize= None)
    
