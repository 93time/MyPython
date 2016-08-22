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

# Document: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html


#spark.conf.set("spark.sql.shuffle.partitions", "2")
spark.conf.set("spark.sql.shuffle.partitions", "13")

inputPath = "/Users/Jason/Documents/Python/Data/StreamingData/"
# jsonSchema = StructType([StructField("time", StringType(), True), StructField("action", StringType(), True)])
jsonSchema = StructType().add("time", "string").add("action", "string")

streamingInputDF = spark.readStream.option("maxFilesPerTrigger", 1).json(inputPath, schema=jsonSchema)
streamingInputDF.isStreaming


streamingInputDFResult = streamingInputDF.groupBy(streamingInputDF.action).count()
streamingInputDFResult.writeStream.format("console").queryName("count_console").outputMode("complete").start()

# checkpoint
streamingInputDFResult.writeStream.format("console").queryName("count_console2").outputMode("complete").option("checkpointLocation", work_path).start()


# Without aggregated values.
query = streamingInputDF.writeStream.format("memory").queryName("count_test").start()
spark.sql("select count(*) from count_test").show()


# With aggregated values
streamingCountsDF = streamingInputDF.groupBy(streamingInputDF.action).count()
query = streamingCountsDF.writeStream.format("memory").queryName("counts2").outputMode("complete").start()

spark.sql("select * from counts").show()
spark.sql("select * from counts2").show()


