from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, to_date, year, month, weekofyear, dayofmonth
)

# 1. CREATE SPARK SESSION
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("WeatherPreprocessing") \
    .getOrCreate()

# 2. READ RAW DATA FROM HDFS
weather_path = "hdfs://namenode:9000/data/weather/weather/weatherData.csv"
location_path = "hdfs://namenode:9000/data/weather/location/locationData.csv"

df_weather = spark.read.csv(weather_path, header=True, inferSchema=True)
df_loc = spark.read.csv(location_path, header=True, inferSchema=True)

# 3. RENAME COLUMNS (remove spaces, units)
rename_map = {
    "temperature_2m_max (°C)": "temperature_max",
    "temperature_2m_min (°C)": "temperature_min",
    "temperature_2m_mean (°C)": "temperature_mean",
    "apparent_temperature_max (°C)": "apparent_temp_max",
    "apparent_temperature_min (°C)": "apparent_temp_min",
    "apparent_temperature_mean (°C)": "apparent_temp_mean",
    "daylight_duration (s)": "daylight_duration",
    "sunshine_duration (s)": "sunshine_duration",
    "precipitation_sum (mm)": "precipitation_sum",
    "rain_sum (mm)": "rain_sum",
    "precipitation_hours (h)": "precipitation_hours",
    "wind_speed_10m_max (km/h)": "wind_speed_max",
    "wind_gusts_10m_max (km/h)": "wind_gusts_max",
    "wind_direction_10m_dominant (°)": "wind_direction",
    "shortwave_radiation_sum (MJ/m²)": "shortwave_radiation",
    "et0_fao_evapotranspiration (mm)": "evapotranspiration"
}

for old, new in rename_map.items():
    df_weather = df_weather.withColumnRenamed(old, new)

# 4. CLEAN NUMERIC COLUMNS
numeric_cols = [
    "temperature_max", "temperature_min", "temperature_mean",
    "apparent_temp_max", "apparent_temp_min", "apparent_temp_mean",
    "daylight_duration", "sunshine_duration", "precipitation_sum",
    "rain_sum", "precipitation_hours", "wind_speed_max",
    "wind_gusts_max", "shortwave_radiation", "evapotranspiration"
]

for col_name in numeric_cols:
    df_weather = df_weather.withColumn(
        col_name,
        regexp_replace(col(col_name), "[^0-9.\-]", "").cast("float")
    )

# 5. DATE PARSING
df_weather = df_weather.withColumn(
    "date",
    to_date(col("date"), "yyyy-MM-dd")
)

df_weather = df_weather.withColumn("year", year(col("date"))) \
                       .withColumn("month", month(col("date"))) \
                       .withColumn("week", weekofyear(col("date"))) \
                       .withColumn("day", dayofmonth(col("date")))

# 6. JOIN WEATHER + LOCATION
df_joined = df_weather.join(df_loc, on="location_id", how="left")

# 7. SAVE CLEAN DATASET TO HDFS (PARQUET)
output_path = "hdfs://namenode:9000/data/clean/weather_clean"

df_joined.write.mode("overwrite").parquet(output_path)

print("Preprocessing complete!")
print("Cleaned dataset stored at:", output_path)

spark.stop()
