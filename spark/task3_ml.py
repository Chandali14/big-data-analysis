from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.linalg import Vectors

# Start Spark Session
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("WeatherML_Task3") \
    .getOrCreate()

print("\n=== Spark Session Created ===\n")

# Load Clean Dataset from HDFS
clean_path = "hdfs://namenode:9000/data/clean/weather_clean"

print("Loading cleaned data from:", clean_path)

df = spark.read.parquet(clean_path)

print("Loaded dataset with rows:", df.count())
df.printSchema()

# Filter for Month = May (month = 5)
df_may = df.filter(col("month") == 5)

print("\n=== Filtered May Dataset ===")
print("Rows in May:", df_may.count())

# Select Required Columns & Clean Missing Values
features_cols = ["precipitation_hours", "sunshine_duration", "wind_speed_max"]

df_clean = df_may.select(
    *features_cols,
    "evapotranspiration"
).dropna()

df_clean.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/output/task3/may_clean_data"
)
print("Saved cleaned May data to HDFS: /output/task3/may_clean_data")
print("\n=== After Dropping Nulls ===")
print("Rows:", df_clean.count())

# Vector Assembler
assembler = VectorAssembler(
    inputCols=features_cols,
    outputCol="features"
)

data = assembler.transform(df_clean)

# Train-Test Split (80/20)
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

print("\nTraining Rows:", train_data.count())
print("Testing Rows:", test_data.count())

# Build Linear Regression Model
lr = LinearRegression(
    featuresCol="features",
    labelCol="evapotranspiration"
)

model = lr.fit(train_data)

print("\n=== Model Training Complete ===")
print("Coefficients:", model.coefficients)
print("Intercept:", model.intercept)

# Evaluate Model
predictions = model.transform(test_data)

predictions.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/output/task3/predictions"
)

evaluator_rmse = RegressionEvaluator(
    labelCol="evapotranspiration",
    predictionCol="prediction",
    metricName="rmse"
)

evaluator_r2 = RegressionEvaluator(
    labelCol="evapotranspiration",
    predictionCol="prediction",
    metricName="r2"
)

rmse = evaluator_rmse.evaluate(predictions)
r2 = evaluator_r2.evaluate(predictions)

print("\n=== Model Evaluation ===")
print("RMSE:", rmse)
print("R2:", r2)

# SAVE METRICS TO HDFS
metrics_df = spark.createDataFrame(
    [(float(rmse), float(r2))],
    ["RMSE", "R2"]
)
metrics_df.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/output/task3/evaluation_metrics"
)
print("Saved evaluation metrics to HDFS: /output/task3/evaluation_metrics")

# 9. Prediction Example for May 2026
print("\n=== Predicting for May 2026 ===")

# Example hypothetical values
example_features = Vectors.dense([10.0, 8.0, 15.0])

example_df = spark.createDataFrame(
    [(example_features,)],
    ["features"]
)

result = model.transform(example_df)
result.show()

single_pred_value = float(result.collect()[0]["prediction"])
print(f"\nPredicted evapotranspiration: {single_pred_value:.3f} mm")

# SAVE MAY 2026 PREDICTION
result.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/output/task3/may2026_prediction"
)
print("Saved May 2026 prediction to HDFS: /output/task3/may2026_prediction")


pred_value = result.collect()[0]["prediction"]
print(f"\nPredicted evapotranspiration: {pred_value:.3f} mm")

if pred_value < 1.5:
    print("\nThis combination will produce evapotranspiration lower than 1.5 mm.\n")
else:
    print("\nThis combination produces evapotranspiration HIGHER than 1.5 mm.\n")
    print("Try increasing precipitation_hours or decreasing sunshine_duration.\n")

# Save Model in HDFS (optional)
model_path = "hdfs://namenode:9000/models/evapo_model"

try:
    model.save(model_path)
    print("Model saved at:", model_path)
except:
    print("Model already exists. To overwrite, delete folder in HDFS.")

spark.stop()
print("\n=== Task 3 Completed Successfully ===\n")

# Save CSV
df_clean.write.mode("overwrite").csv(
    "hdfs://namenode:9000/output/task3/may_clean_data_csv",
    header=True
)
predictions.write.mode("overwrite").csv(
    "hdfs://namenode:9000/output/task3/predictions_csv",
    header=True
)
result.write.mode("overwrite").csv(
    "hdfs://namenode:9000/output/task3/may2026_prediction_csv",
    header=True
)
