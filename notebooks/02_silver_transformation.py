# Databricks notebook source
#Reading the bronze layer
df_bronze=spark.read.parquet("/Volumes/workspace/default/taxi_data/bronze/yellow_taxi_2023/")
print(f"Bronze record count: {df_bronze.count():,}")
df_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, min, max, avg, count, when

df_bronze.select(
    min("trip_distance").alias("min_distance"),
    max("trip_distance").alias("max_distance"),
    avg("trip_distance").alias("avg_distance"),
    min("fare_amount").alias("min_fare"),
    max("fare_amount").alias("max_fare"),
    avg("fare_amount").alias("avg_fare"),
    min("passenger_count").alias("min_passengers"),
    max("passenger_count").alias("max_passengers"),
    count("*").alias("total_rows")
).show()


# COMMAND ----------

df_bronze.columns

# COMMAND ----------

#date range
df_bronze.select(
    min("tpep_pickup_datetime").alias("Earliest Pickup"),
    max("tpep_dropoff_datetime").alias("Last Pickup")
).show()

# COMMAND ----------

df_silver = df_bronze \
    .filter(col("passenger_count") > 0) \
    .filter(col("passenger_count") <= 6) \
    .filter(col("trip_distance") > 0.1) \
    .filter(col("trip_distance") < 100) \
    .filter(col("fare_amount") > 0) \
    .filter(col("fare_amount") < 500) \
    .filter(col("tpep_pickup_datetime") >= "2025-12-31") \
    .filter(col("tpep_pickup_datetime") < "2026-03-01") \
    .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) \
    .filter(col("PULocationID").isNotNull()) \
    .filter(col("DOLocationID").isNotNull())

print(f"Records after cleaning: {df_silver.count():,}")
print(f"Records dropped: {df_bronze.count() - df_silver.count():,}")

# COMMAND ----------

from pyspark.sql.functions import (
    col, unix_timestamp, round as spark_round,
    hour, dayofweek, month, datediff, to_date
)

df_silver = df_silver.withColumn(
    "trip_duration_mins",
    spark_round(
        (unix_timestamp("tpep_dropoff_datetime") - 
         unix_timestamp("tpep_pickup_datetime")) / 60, 2
    )
) \
.withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
.withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime")) \
.withColumn("pickup_month", month("tpep_pickup_datetime")) \
.withColumn("pickup_date", to_date("tpep_pickup_datetime")) \
.withColumn(
    "cost_per_mile",
    spark_round(col("fare_amount") / col("trip_distance"), 2)
) \
.withColumn(
    "is_tipped",
    when(col("tip_amount") > 0, 1).otherwise(0)
)

display(df_silver.limit(5))

# COMMAND ----------

df_silver = df_silver.withColumn(
    "trip_duration_mins",
    spark_round(
        (unix_timestamp("tpep_dropoff_datetime") - 
         unix_timestamp("tpep_pickup_datetime")) / 60, 2
    )
) \
.withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
.withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime")) \
.withColumn("pickup_month", month("tpep_pickup_datetime")) \
.withColumn("pickup_date", to_date("tpep_pickup_datetime")) \
.withColumn(
    "cost_per_mile",
    spark_round(col("fare_amount") / col("trip_distance"), 2)
) \
.withColumn(
    "is_tipped",
    when(col("tip_amount") > 0, 1).otherwise(0)
)

display(df_silver.limit(5))

# COMMAND ----------

df_silver = df_silver \
    .filter(col("trip_duration_mins") > 1) \
    .filter(col("trip_duration_mins") < 180) \
    .filter(col("cost_per_mile") < 100)

print(f"Final Silver count: {df_silver.count():,}")

# COMMAND ----------

df_silver = df_silver.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pickup_date",
    "pickup_hour",
    "pickup_day_of_week",
    "pickup_month",
    "passenger_count",
    "trip_distance",
    "trip_duration_mins",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "cost_per_mile",
    "is_tipped",
    "payment_type",
    "ingestion_timestamp"
)

print(f"Columns in Silver: {len(df_silver.columns)}")
display(df_silver.limit(10))

# COMMAND ----------

df_silver.write.mode("overwrite").partitionBy("pickup_month").parquet("/Volumes/workspace/default/taxi_data/silver/yellow_taxi_2023/")

print("✅ Silver layer written successfully!")


# COMMAND ----------

df_silver.count()

# COMMAND ----------

