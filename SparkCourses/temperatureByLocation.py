import sys
import os

data_path = "/Users/Jason/Documents/Python/Data/SparkCourses"
work_path = "/Users/Jason/Documents/Python/MyPython/SparkCourses"
sys.path.append(work_path)

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("temperatureByLocation").config("spark.sql.warehouse.dir", work_path).getOrCreate()

data_schema = StructType().add("id", "string").add("location_id", "string").add("temp_type", "string").add("temp", "integer")

data_df = spark.read.load(data_path +"/1800.csv", format="csv", schema=data_schema)
data_df.show()
data_df.createOrReplaceTempView("data_df")

spark.sql("SELECT location_id, MIN(temp) AS minimum_temp FROM data_df WHERE temp_type = 'TMIN' GROUP BY location_id ORDER BY 2").show()
