from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("RadiationClean") \
    .getOrCreate()

# Read cleaned dataset
df = spark.read.parquet("hdfs://namenode:9000/data/clean/weather_clean")

# shortwave_radiation already numeric & cleaned
df2 = df.withColumn("is_high", (col("shortwave_radiation") > 15).cast("int"))

# percentage of rows where radiation > 15 MJ/m2
result = df2.groupBy("month").agg(
    (_sum("is_high") / _sum(col("shortwave_radiation").isNotNull().cast("int")) * 100)
    .alias("percentage_above_15")
)

result.show()
