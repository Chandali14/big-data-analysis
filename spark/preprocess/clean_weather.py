from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, year, month, weekofyear, dayofmonth
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("WeatherPreprocessingFixed") \
    .getOrCreate()

# 1. Define Correct Schema
schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("weather_code", IntegerType(), True),
    StructField("temperature_max", FloatType(), True),
    StructField("temperature_min", FloatType(), True),
    StructField("temperature_mean", FloatType(), True),
    StructField("apparent_temp_max", FloatType(), True),
    StructField("apparent_temp_min", FloatType(), True),
    StructField("apparent_temp_mean", FloatType(), True),
    StructField("daylight_duration", FloatType(), True),
    StructField("sunshine_duration", FloatType(), True),
    StructField("precipitation_sum", FloatType(), True),
    StructField("rain_sum", FloatType(), True),
    StructField("precipitation_hours", FloatType(), True),
    StructField("wind_speed_max", FloatType(), True),
    StructField("wind_gusts_max", FloatType(), True),
    StructField("wind_direction", IntegerType(), True),
    StructField("shortwave_radiation", FloatType(), True),
    StructField("evapotranspiration", FloatType(), True),
    StructField("sunrise", StringType(), True),
    StructField("sunset", StringType(), True)
])

# 2. Load weather CSV
path_weather = "hdfs://namenode:9000/data/weather/weather/weatherData.csv"

df_weather = spark.read.csv(path_weather, schema=schema, header=True)

# 3. FIX DATE PARSING
# Date format is M/d/yyyy  → Example: 1/5/2010
df_weather = df_weather.withColumn(
    "date",
    to_date(col("date"), "M/d/yyyy")
)

# 4. Extract year, month, week, day
df_weather = df_weather \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("week", weekofyear("date")) \
    .withColumn("day", dayofmonth("date"))

# 5. Load location CSV
path_loc = "hdfs://namenode:9000/data/weather/location/locationData.csv"

df_loc = spark.read.csv(path_loc, header=True, inferSchema=True)

# 6. Join weather + location data
df_joined = df_weather.join(df_loc, on="location_id", how="left")

# 7. Save Clean Dataset
output_path = "hdfs://namenode:9000/data/clean/weather_clean"

df_joined.write.mode("overwrite").parquet(output_path)

print("Preprocessing complete!")
print("Cleaned dataset stored at:", output_path)

spark.stop()
