from pyspark.sql import SparkSession
from pyspark.sql.functions import weekofyear, col, max as _max

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("WeeklyMaxTemp") \
    .getOrCreate()

df = spark.read.csv(
    "hdfs://namenode:9000/data/weather/weather/weatherData.csv",
    header=True,
    inferSchema=True
)

df2 = df.withColumn("week", weekofyear(col("date")))

result = df2.groupBy("year", "month", "week").agg(
    _max("temperature_2m_max").alias("weekly_max_temp")
).orderBy("year", "month", "week")

result.show()
