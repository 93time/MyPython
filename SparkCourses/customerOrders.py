import sys
import os

data_path = "/Users/Jason/Documents/Python/Data/SparkCourses"
work_path = "/Users/Jason/Documents/Python/MyPython/SparkCourses"
sys.path.append(work_path)

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("customerOrders").config("spark.sql.warehouse.dir", work_path).getOrCreate()

data_schema = StructType().add("customer_id", "integer").add("order_id", "integer").add("dollar_spent", "decimal(10, 2)")

data_df = spark.read.load(data_path +"/customer-orders.csv", format="csv", schema=data_schema)
data_df.show()
data_df.createOrReplaceTempView("customer_orders")

spark.sql("SELECT customer_id, SUM(dollar_spent) AS total_spent_dollar FROM customer_orders GROUP BY customer_id ORDER BY 2 DESC").show()
