#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 14:46:41 2020

@author: xavier
"""

import datetime
import subprocess
from datetime import datetime

class HDFS_commands():
    
    def __init__(self, **kwargs):
        
        self.name =  kwargs.get("name")
        print(self.name)
        self.directory = kwargs.get("directory")
        self.file = kwargs.get("file")
        self.command =  kwargs.get("command")
        self.localdirectory= kwargs.get("localdirectory")
        print(self.localdirectory)
        
        if self.command == "delete":
            delete_file = self.delete_file(directory = self.directory)
            print(delete_file)
        if self.command == "query_file":
            query_file =  self.query_file(directory = self.directory,file=self.file)
            print(query_file)
        if self.command == "query_file_top_day":
            query_file =  self.query_file_top_day(directory=self.directory,file=self.file)
            print(query_file)
        if self.command == "get_file":
            get_file =  self.get_file(directory=self.directory,localdirectory=self.localdirectory,file=self.file)
            print(get_file)
        if self.command == "put_file":
            put_file =  self.put_file(directory=self.directory,localdirectory=self.localdirectory,file=self.file)
            print(put_file)
        
    def get_days(self, directory, file):    
        current_datetime = datetime.now()
        command = "hadoop fs -ls " + directory + file + " | tr -s ' ' | cut -d' ' -f6-7 | grep '^[0-9]' "
        (status, output) = subprocess.getstatusoutput(command)
        print(output)
        if status == 0:
            file_date = datetime.strptime(output, '%Y-%m-%d %H:%M')
            date_period = current_datetime - file_date
        return date_period.days
    
    def delete_file(self,directory):
    # =============================================================================
    #         testHDFS = HDFS_commands(name="test", 
    #         directory="hdfs://127.0.0.1:9000/user/xavier/US_Stocks/*.*",
    #         command="delete")
    # =============================================================================
        try:
            command = "hadoop fs -rm " + directory+ "  "
            print("run delete command: "+command)
            (status, output) = subprocess.getstatusoutput(command)
        except:
            print("error, delete_file request could not be completed")
        return output
    
    def query_file(self, directory, file):
    # =============================================================================
    #         testHDFS = HDFS_commands(name="test", 
    #         directory="hdfs://127.0.0.1:9000/user/xavier/US_Stocks/",
    #         file="*.*",
    #         command="query_file")
    # =============================================================================
        try:
            command = "hadoop fs -ls " + directory+file +" "
            print("run query command: "+command)
            (status, output) = subprocess.getstatusoutput(command)
            print("===>"+output)
        except:
            print("error,query_file request could not be completed")
    
        return output
    
    def query_file_top_day(self, directory, file):
    # =============================================================================
    #         testHDFS = HDFS_commands(name="test", 
    #         directory="hdfs://127.0.0.1:9000/user/xavier/US_Stocks/",
    #         file="*.*",
    #         command="query_file_topday")
    # =============================================================================
        try:
            command = "hadoop fs -ls " + directory+file +" "
            print("run query command: "+command)
            (status, output) = subprocess.getstatusoutput(command)
            print("===>"+output)
        except:
            print("error,query_file request could not be completed")
    
        return output
    
    def get_file(self, directory,localdirectory, file):
    # =============================================================================
    #         testHDFS = HDFS_commands(name="test", 
    #                          directory="hdfs://127.0.0.1:9000/user/xavier/US_Stocks/",
    #                          localdirectory="/home/xavier/Documents/WorkSpace",
    #                          file="*.*",
    #                          command="get_file")
    # =============================================================================
         try:
             command = "hadoop fs -get " + directory + file +" "+ localdirectory
             print("run get command: "+command)
             (status, output) = subprocess.getstatusoutput(command)
             print("===>"+output)
         except:
             output = "error, get_file request could not be completed"
             print("error, get_file request could not be completed")
     
         return output
     
    def put_file(self, directory,localdirectory, file):
    # =============================================================================
    #         testHDFS = HDFS_commands(name="test", 
    #                          directory="hdfs://127.0.0.1:9000/user/xavier/US_Stocks/",
    #                          localdirectory="/home/xavier/Documents/WorkSpace/",
    #                          file="*.*",
    #                          command="put_file")
    # =============================================================================
         try:
             command = "hadoop fs -put " + localdirectory + file +" "+ directory
             print("run put command: "+command)
             (status, output) = subprocess.getstatusoutput(command)
             print("===>"+output)
         except:
             output = "error, get_file request could not be completed"
             print("error, get_file request could not be completed")
     
         return output
