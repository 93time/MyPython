import sys
import os

data_path = "/Users/Jason/Documents/Python/Data/FireDepartmentCalls"
work_path = "/Users/Jason/Documents/Python/MyPython/FireDepartment"
sys.path.append(work_path)

from pyspark.sql import SparkSession
import data_schema


spark = SparkSession.builder.master("local[*]").appName("fire_department_calls").config("spark.sql.warehouse.dir", work_path).getOrCreate()

input_df = spark.read.load(data_path +"/Fire_Department_Calls_for_Service.csv", format="csv", header="true", schema=data_schema.getDataSchema())
input_df.printSchema()
input_df.createOrReplaceTempView("fireDepartmentCalls")
spark.sql("SELECT * FROM fireDepartmentCalls LIMIT 100").show()
spark.sql("SELECT callType, COUNT(*) FROM fireDepartmentCalls GROUP BY callType ORDER BY 2 DESC").show()
input_df.count()
input_df.limit(10).show()

spark.sql("SELECT DISTINCT callType FROM fireDepartmentCalls").show()

input_df.select("callType").distinct().show(50, False)


## To change String data types to date/time data types
from pyspark.sql.functions import *

from_pattern1 = "MM/DD/YY"
to_pattern1 = "yyyy-MM-dd"
from_pattern2 = "MM/dd/yyyy hh:mm:ss aa"
to_pattern2 = "MM/dd/yyyy hh:mm:ss aa"

input_df2 = input_df.withColumn("callDateTS", unix_timestamp(input_df["WatchDate"], from_pattern1).cast("timestamp")).drop("callDate").withColumn("ReceivedDtTmTS", unix_timestamp(input_df["ReceivedDtTm"], from_pattern2).cast("timestamp")).drop("ReceivedDtTm")
input_df2.printSchema()
input_df2.limit(10).show()
input_df2.createOrReplaceTempView("fireDepartmentCalls_2")

spark.sql("SELECT DISTINCT YEAR(callDateTS) from fireDepartmentCalls_2 ORDER BY 1").show()

spark.sql("SELECT CAST(unix_timestamp(responseDtTm, 'MM/dd/yyyy hh:mm:ss') as timestamp), * from fireDepartmentCalls LIMIT 10").show()
spark.sql("SELECT TO_DATE(CAST(unix_timestamp(responseDtTm, 'MM/dd/yyyy hh:mm:ss') as timestamp)), * from fireDepartmentCalls LIMIT 10").show()

spark.sql("SELECT DISTINCT YEAR(CAST(unix_timestamp(responseDtTm, 'MM/dd/yyyy hh:mm:ss') as timestamp)), COUNT(*) from fireDepartmentCalls GROUP BY 1 ORDER BY 1").show()

spark.sql("SELECT COUNT(*) FROM fireDepartmentCalls WHERE TO_DATE(CAST(unix_timestamp(responseDtTm, 'MM/dd/yyyy hh:mm:ss') as timestamp)) BETWEEN '2016-07-01' AND '2016-07-10'").show()



input_df = input_df.withColumn("responseYYMMDD", unix_timestamp(input_df["responseDtTm"], "MM/dd/yyyy").cast("timestamp"))
input_df.select(["responseDtTm", "responseYYMMDD"]).limit(10).show()

input_df.createOrReplaceTempView("fireDepartmentCalls")
spark.sql("SELECT * FROM fireDepartmentCalls LIMIT 100").show()

spark.sql("SELECT TO_DATE(responseYYMMDD) AS date, COUNT(*) FROM fireDepartmentCalls WHERE TO_DATE(responseYYMMDD) BETWEEN '2016-07-01' AND '2016-07-10' GROUP BY TO_DATE(responseYYMMDD) ORDER BY 1").show()


input_df.rdd.getNumPartitions()
# input_df.cache()

input_df.repartition(6).createOrReplaceTempView("fireDepartmentCalls")

partial_input_data = spark.sql("SELECT CallNumber, CallType, responseYYMMDD FROM fireDepartmentCalls")
partial_input_data.limit(10).show()

partial_input_data.rdd.getNumPartitions()

partial_input_data.createOrReplaceTempView("partial_input_data_view")

spark.catalog.cacheTable("partial_input_data_view")
spark.table("partial_input_data_view").count()
spark.sql("SELECT COUNT(*) FROM partial_input_data_view").show()
spark.sql("SELECT callType, COUNT(*) FROM partial_input_data_view GROUP BY callType ORDER BY 2 DESC").show()


