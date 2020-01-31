# LoadStockDataToHDFS

download stock historic data to HDFS

stock_df =stockYahooLoader(startDate = "20190131",endDate = "20200131", 
                    symbols_file="/home/xavier/Downloads/USE_20191127.txt", fileLocalOutput="",
                    hdfs_path="hdfs://localhost:9000/user/xavier/US_Stocks/")  
