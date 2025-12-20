from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Radiation") \
    .getOrCreate()

df = spark.read.csv(
    "hdfs://namenode:9000/data/weather/weather/weatherData.csv",
    header=True,
    inferSchema=True
)

# Count shortwave > 15 MJ/m2
df2 = df.withColumn("is_high", (col("shortwave_radiation") > 15).cast("int"))

result = df2.groupBy("month").agg(
    (_sum("is_high") / _sum(col("shortwave_radiation").isNotNull().cast("int")) * 100)
    .alias("percentage_above_15")
)

result.show()
