import sys
import os

data_path = "/Users/Jason/Documents/Python/Data/SparkCourses"
work_path = "/Users/Jason/Documents/Python/MyPython/SparkCourses"
sys.path.append(work_path)

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("friendsbyage").config("spark.sql.warehouse.dir", work_path).getOrCreate()

data_schema = StructType().add("id", "integer").add("name", "string").add("age", "integer").add("friend_count", "integer")

data_df = spark.read.load(data_path +"/fakefriends.csv", format="csv", schema=data_schema)
data_df.show()
data_df.createOrReplaceTempView("data_df")

spark.sql("SELECT age, COUNT(*) AS people_count, SUM(friend_count) AS total_friends FROM data_df GROUP BY age").createOrReplaceTempView("DataByAge")
spark.sql("SELECT age, CAST(total_friends / people_count AS INT) FROM dataByAge ORDER BY 1 DESC").show()

spark.sql("""
SELECT age, CAST(SUM(friend_count) / COUNT(*) AS INT) FROM data_df GROUP BY age
""").show()
