#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb  1 16:44:35 2020

@author: xavier
"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import HiveContext

class runSQL:
   
     def __init__(self,csvFile, tableName, SQL, memoryAllocation):
        self.csvFile=csvFile
        self.tableName=tableName
        self.SQL = SQL
        self.memoryAllocation = memoryAllocation
        self.hc = self.LoadData(csvFile,tableName,memoryAllocation)
        self.queryResult = self.executeQuery(self.hc,SQL)
        
     def LoadData(self, csvFile, tableName, memoryAllocation):
        scSpark = SparkSession \
            .builder \
            .appName("runSQL") \
            .config("spark.executor.memory", memoryAllocation) \
            .getOrCreate()
        print("Spark Session created")
        hc = HiveContext(scSpark)
        sdfData = hc.read.csv(csvFile, header=True, sep=",")
        sdfData.createOrReplaceTempView(tableName)
        print(sdfData.head(5))
        return hc
    
     def executeQuery(self,hc, SQL):
        sdfData = hc.sql(SQL)
        return sdfData
    
     def getQueryResult(self):
        return self.QueryResult
    
    
    #def executeSQL(self, SQL, df):
test =runSQL("A.csv","test","select Low from test","2G") 
print("==========================================")
print(test.SQL) 
print(test.queryResult.show())
    
