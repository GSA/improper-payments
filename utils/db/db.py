# -*- coding: utf-8 -*-
"""
Created on Tue Feb 12 11:53:04 2019

@author: AustinPeel
"""
import urllib
from sqlalchemy import create_engine,event
import pypyodbc
import pandas as pd

from config import serverName , userName ,password ,database ,sqlDB

params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=" + serverName + ";DATABASE="+ database +";UID="+userName+";PWD=" +password)
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, module=pypyodbc)

@event.listens_for(engine, 'before_cursor_execute') 
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany): 
    print("FUNC call") 
    if executemany: 
        cursor.fast_executemany = True


def send_data(df):
    df.to_sql(sqlDB,engine,if_exists='append',chunksize= None)
    