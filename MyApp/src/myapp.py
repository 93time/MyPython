# -*- coding: utf-8 -*-
"""
Created on Thu Aug 04 22:27:10 2016

@author: jason.yi
"""


from pyspark.sql import SparkSession
from pyspark import Row

spark = SparkSession.builder.master("local[*]").appName("Spark 2.0").config("spark.sql.warehouse.dir", "/Users/Jason/Documents/Python/MyPython/MyApp/src").getOrCreate()
sc = spark.sparkContext

input_data = sc.textFile("/Users/Jason/Documents/Python/MyPython/MyApp/src/mobile_event_logs.txt").map(lambda x: x.split("\t"))
input_data.first()


df = spark.read.json(input_data.map(lambda x: x[2]))

df.count()
df.createOrReplaceTempView("db_table")
df.printSchema()


spark.sql("SELECT * FROM db_table").show()


df = spark.read.format("jdbc").options(
    url="jdbc:sqlserver://10.63.25.100",
    user="BI_ETL",
    password="ueR5JpUZ3h29EkwA5oov",
    dbtable="dbo._staging_mobile_platform_event_logs"
    ).load()

df.printSchema()
df.createOrReplaceTempView("db_table")

df.count()

spark.sql("SELECT * FROM db_table where app_id = 4 LIMIT 100").show()
spark.sql("SELECT count(*) FROM db_table").show()

df_1 = spark.sql("SELECT * FROM db_table WHERE app_id = 4")

df_1.write.jdbc(
    url="jdbc:sqlserver://10.63.25.100;database=EMEDW;user=BI_ETL;password=ueR5JpUZ3h29EkwA5oov"
    ,table="_staging_mobile_platform_event_logs_tmp"
    ,mode="append"
    )

spark.dropTempView("db_table")