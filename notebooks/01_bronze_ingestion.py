# Databricks notebook source
df_raw=spark.read.parquet("/Volumes/workspace/default/taxi_data/yellow_tripdata_2026-01.parquet","/Volumes/workspace/default/taxi_data/yellow_tripdata_2026-02.parquet")

# COMMAND ----------

print(f"Total records: {df_raw.count():,}")

# COMMAND ----------

df_raw.printSchema()

# COMMAND ----------

display(df_raw.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum

null_counts = df_raw.select([
    spark_sum(col(c).isNull().cast("int")).alias(c) 
    for c in df_raw.columns
])
display(null_counts)


# COMMAND ----------

print(f"Total Rows:{df_raw.count():,}")
print(f"Total Columns:{len(df_raw.columns)}")
print(f"Column Names:{df_raw.columns}")


# COMMAND ----------

print(f"Rows: {df_raw.count():,}")
print(f"Columns: {len(df_raw.columns)}")
print(f"Column names: {df_raw.columns}")

# COMMAND ----------

# DBTITLE 1,Cell 8
#a safe practice to add timestamp although not needed here much 
from pyspark.sql.functions import current_timestamp

df_bronze = df_raw.withColumn("ingestion_timestamp", current_timestamp())

df_bronze.write \
    .mode("overwrite") \
    .parquet("/Volumes/workspace/default/taxi_data/bronze/yellow_taxi_2023")

print("✅ Bronze layer written successfully!")

# COMMAND ----------

df_verify=spark.read.parquet("/Volumes/workspace/default/taxi_data/bronze/yellow_taxi_2023/")
print(f"Bronze layer record count: {df_verify.count():,}")
display(df_verify.limit(5))

# COMMAND ----------

