import sys
import os
from pyspark.streaming.context import StreamingContext

work_path = "/Users/Jason/Documents/Python/MyPython/Streaming"
sys.path.append(work_path)

# Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("streaming_data").config("spark.sql.warehouse.dir", work_path).getOrCreate()

# Spark Streaming
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark.conf.set("spark.sql.shuffle.partitions", "2")

inputPath = "/Users/Jason/Documents/Python/Data/StreamingData/"
jsonSchema = StructType([StructField("time", StringType(), True), StructField("action", StringType(), True)])

streamingInputDF = spark.readStream.option("maxFilesPerTrigger", 1).json(inputPath, schema=jsonSchema)
streamingInputDF.isStreaming

# Without aggregated values.
query = streamingInputDF.writeStream.format("memory").queryName("count_test").start()
spark.sql("select count(*) from count_test").show()


# With aggregated values
streamingCountsDF = streamingInputDF.groupBy(streamingInputDF.action).count()
query = streamingCountsDF.writeStream.format("memory").queryName("counts2").outputMode("complete").start()

spark.sql("select * from counts").show()
spark.sql("select * from counts2").show()



