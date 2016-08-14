from pyspark.sql import SparkSession
from pyspark import Row
import urllib

spark = SparkSession.builder.master("local[*]").appName("wiki_stats").config("spark.sql.warehouse.dir", "/Users/Jason/Documents/Python/MyPython/").getOrCreate()
sc = spark.sparkContext

wiki_rawdata = sc.textFile("/Users/Jason/Documents/Python/MyPython/WikiStats/wiki_data/pagecounts-20150101-000000.gz")
wiki_data = wiki_rawdata.map(lambda x: x.split(" ")).filter(lambda line: line[0] == "en" and len(line) > 3)
wiki_data.first()

wiki_ds = wiki_data.map(lambda line: Row(project=line[0], url=urllib.unquote(line[1]).lower(), num_requests=int(line[2]), content_size=line[3]))
wiki_ds.count()

wiki_df = spark.createDataFrame(wiki_ds)
wiki_df.createTempView("wiki_view")

spark.sql("SELECT * FROM wiki_view LIMIT 10").collect()

