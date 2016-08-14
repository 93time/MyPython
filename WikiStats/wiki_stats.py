from pyspark.sql import SparkSession
from pyspark import Row
import urllib
import os
import shutil


currentWorkPath = "/Users/Jason/Documents/Python/MyPython/WikiStats"
wikiRawDataPath = "/Users/Jason/Documents/Python/Data/WikiStats/wikiRawData"
parquetDataPath = "/Users/Jason/Documents/Python/Data/WikiStats"

spark = SparkSession.builder.master("local[*]").appName("wiki_stats").config("spark.sql.warehouse.dir", "/Users/Jason/Documents/Python/MyPython/").getOrCreate()
sc = spark.sparkContext


def loadData(logDate):
    wiki_rawdata = sc.textFile(wikiRawDataPath +"/pagecounts-"+ logDate +"-000000.gz")
    wiki_data = wiki_rawdata.map(lambda x: x.split(" ")).filter(lambda line: line[0] == "en" and len(line) > 3)
    #wiki_data.first()
    
    wiki_ds = wiki_data.map(lambda line: Row(project=line[0], url=urllib.unquote(line[1]).lower(), num_requests=int(line[2]), content_size=line[3]))
    # wiki_ds.count()
    
    wiki_df = spark.createDataFrame(wiki_ds)
    wiki_df.createOrReplaceTempView("wiki_view")
    
    result = spark.sql("SELECT '"+ logDate +"' as logDate, url, sum(num_requests) as total_requests FROM wiki_view GROUP BY url")
    
    result.write.parquet(parquetDataPath +"/wiki.parquet", mode="append", partitionBy="logDate")


loadData("20150101")
loadData("20150102")



parquet_df = spark.read.parquet(parquetDataPath +"/wiki.parquet")
parquet_df.createTempView("parquet_data")
parquet_df.count()

spark.sql("SELECT * FROM parquet_data WHERE url NOT LIKE '%special%' AND url NOT LIKE '%page%' AND url NOT LIKE '%test%' AND url NOT LIKE '%wiki%' ORDER BY total_requests DESC LIMIT 100").show()






