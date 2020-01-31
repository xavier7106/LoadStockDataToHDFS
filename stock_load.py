#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: xavier
"""
import pandas as pd
import datetime
import pydoop.hdfs as hdfs
import pandas_datareader.data as web
from pandas import Series, DataFrame
import os

 
class stockYahooLoader():
    pd_total = {}

    def __init__(self,startDate, endDate, symbols_file, fileLocalOutput, hdfs_path):
        self.start_date=startDate
        self.end_date=endDate
        self.symbols = self.getSymbols(symbols_file)
        self.hdfsPath = hdfs_path
        self.fileLocalOutput = fileLocalOutput
        self.startLoad()
       
    
    def startLoad(self):
        start= datetime.datetime.strptime(self.start_date, '%Y%m%d').strftime('%Y, %m, %d')
        end= datetime.datetime.strptime(self.end_date, '%Y%m%d').strftime('%Y, %m, %d')

        df = {}
        counter = 0
        for x in self.symbols:
            print(x)
            try:
                df[counter] = web.DataReader(x, 'yahoo', start, end)
                #print(df[counter].count)
                print(self.fileLocalOutput)
                print("saving file")
                try:
                    self.saveStockFile(stock_name = x, data_frame = df[counter], hdfs_path=self.hdfsPath)
                except ValueError:
                    print(ValueError.__str__)
                    print("cannot save file")
                counter = counter + 1
                self.pd_total = pd.concat(df)
            except :
                pass
        #print(pd_total.count)
        
     
    def saveStockFile(self,stock_name, data_frame, hdfs_path):
        print("saving stock "+ stock_name)
        outFile =  self.fileLocalOutput+stock_name+".csv"
        export_csv = data_frame.to_csv (outFile, index = None, header=True) 
        print(outFile)
        from_path = outFile
        if (hdfs_path!=""):
            print(hdfs_path)
            #to_path ='hdfs://localhost:9000/user/xavier/US_Stocks/'+stock_name+'.csv'
            to_path = hdfs_path + stock_name + '.csv'
            print(from_path +"==>"+ to_path)
            hdfs.put(from_path, to_path)
            os.remove(outFile)
       
   
    def getSymbols(self,inputFile):
        f = open(inputFile, "r")
        symbol_list =  []
        for line in f.readlines():
            row = line.split(",")
            symbol = row[0]
            symbol_list.append(symbol)
        print(symbol_list)
        return symbol_list
        #USE_20191127.txt
        
 
stock_df =stockYahooLoader(startDate = "20190131",endDate = "20200131", 
                    symbols_file="/home/xavier/Downloads/USE_20191127.txt", fileLocalOutput="",
                    hdfs_path="hdfs://localhost:9000/user/xavier/US_Stocks/")        
        
    
#start = datetime.datetime(2019, 1, 31)
#end = datetime.datetime(2020, 1, 31)
#stock_list = {'A','AA','AAA','AAPL'}
#df={}
#counter = 0
#for x in stock_list:
#    print(x)
#    df[counter] = web.DataReader(x, 'yahoo', start, end)
#    print(df[counter].count)
#    counter = counter + 1
#result = pd.concat(df) 
#print(result.count)


