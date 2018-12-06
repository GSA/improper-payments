# -*- coding: utf-8 -*-

import os ,datetime
wd = os.getcwd()


#from sqlConnnect import mssql
from config import sqlDB
from utils import library, version3, mssql

#getting timestamp to log results 
timeStamp = '{:%Y_%m_%d_%H_%M_%S}'.format(datetime.datetime.now())


#### survey characteristics
folderLocation = wd + "/data"

#get survey data and save to sqlite
library.surveyToSqlite(sqlDB,folderLocation)
surveyName = library.getSurveyName(sqlDB,folderLocation) 
fileName = folderLocation + "/" + surveyName + ".csv"
print("survey data retrieved")


#download data from api and save to csv

try:
    lastResponse = library.getLastResonseSqlite(sqlDB,folderLocation)
    version3.qualtrics().downloadExtractZip(lastResponseId=lastResponse,filePath=folderLocation)
except:
    version3.qualtrics().downloadExtractZip(filePath=folderLocation)
    
#save survey download data into sqlite
library.surveyDownloadsToSqlite(sqlDB,fileName,folderLocation,timeStamp)
print("saving meta data to sqllite database")

# reshape data for MS SQL intake
df=library.getDataFrame(folderLocation,surveyName+".csv")
df= df.dropna()
df = df.rename(columns={"ID":"personID"})

#process to send data directly to SQL in 10,000 chunks

#create database
try:
    mssql.send(df,sqlDB,actionType='create').data()
    print("starting to append  data into SQL.")
except:
    print("starting to append  data into SQL.")
 
    
def splitDataFrameIntoSmaller(df, chunkSize = 10000): 
    listOfDf = list()
    numberChunks = len(df) // chunkSize + 1
    for i in range(numberChunks):
        listOfDf.append(df[i*chunkSize:(i+1)*chunkSize])
    return listOfDf

#split data into chuncks
listDF = splitDataFrameIntoSmaller(df)

#send data in 10,000 bit chunks
for i in listDF:
    mssql.send(i,sqlDB).data()
    print("batch added to database" )
    

#save data
print("saving data to local drive")
df.to_csv(folderLocation  +"/" + sqlDB +".csv" ,index=False,encoding='utf-8-sig')
os.rename(folderLocation +"/" + surveyName + ".csv",folderLocation  +"/"+ surveyName +"_" + timeStamp + ".csv")
timeStamp = library.getLastTimeStampSqlite(sqlDB,folderLocation)
os.rename(folderLocation  +"/" + sqlDB +".csv",folderLocation  +"/" + sqlDB +timeStamp +".csv")







