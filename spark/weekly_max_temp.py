from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as _max

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("WeeklyMaxTempClean") \
    .getOrCreate()

# Read cleaned dataset
df = spark.read.parquet("hdfs://namenode:9000/data/clean/weather_clean")

result = df.groupBy("year", "month", "week").agg(
    _max(col("temperature_max")).alias("weekly_max_temp")
).orderBy("year", "month", "week")

result.show()
