# Databricks notebook source
df_silver = spark.read.parquet("/Volumes/workspace/default/taxi_data/silver/yellow_taxi_2026")
df_silver.createOrReplaceTempView("yellow_taxi")
print(f"Loaded: {df_silver.count():,} records")

# COMMAND ----------

import time

start = time.time()
df_silver.filter("pickup_hour = 8").count()
df_silver.filter("pickup_month = 1").count()
df_silver.groupBy("pickup_hour").count().collect()
print(f"Without optimization: {time.time() - start:.2f}s")

# COMMAND ----------

# DBTITLE 1,Cell 3
df_silver.createOrReplaceTempView("yellow_taxi_opt")

start = time.time()
spark.sql("SELECT * FROM yellow_taxi_opt WHERE pickup_hour = 8").count()
spark.sql("SELECT * FROM yellow_taxi_opt WHERE pickup_month = 1").count()
spark.sql("SELECT pickup_hour, COUNT(*) FROM yellow_taxi_opt GROUP BY pickup_hour").collect()
print(f"With temp view optimization: {time.time() - start:.2f}s")

# COMMAND ----------

spark.catalog.dropTempView("yellow_taxi_opt")
print("✅ Temp view dropped")

# COMMAND ----------

from pyspark.sql import Row

zone_data = [
    Row(LocationID=1,  Borough="EWR",       Zone="Newark Airport"),
    Row(LocationID=4,  Borough="Manhattan", Zone="Alphabet City"),
    Row(LocationID=12, Borough="Manhattan", Zone="Battery Park"),
    Row(LocationID=13, Borough="Manhattan", Zone="Battery Park City"),
    Row(LocationID=24, Borough="Manhattan", Zone="Bloomingdale"),
    Row(LocationID=41, Borough="Manhattan", Zone="Central Park"),
    Row(LocationID=43, Borough="Manhattan", Zone="Central Park"),
    Row(LocationID=45, Borough="Brooklyn",  Zone="DUMBO"),
    Row(LocationID=61, Borough="Brooklyn",  Zone="Crown Heights"),
    Row(LocationID=79, Borough="Queens",    Zone="JFK Airport"),
]

df_zones = spark.createDataFrame(zone_data)
df_zones.createOrReplaceTempView("zone_lookup")
print(f"Zone lookup size: {df_zones.count()} rows")

# COMMAND ----------

#normal join time note
start = time.time()

df_joined_regular = df_silver.join(
    df_zones,
    df_silver.PULocationID == df_zones.LocationID,
    "left"
)

df_joined_regular.groupBy("Borough").count().collect()
print(f"Regular join: {time.time() - start:.2f}s")

# COMMAND ----------

#broadcast join time note
from pyspark.sql.functions import broadcast

start = time.time()

df_joined_broadcast = df_silver.join(
    broadcast(df_zones),
    df_silver.PULocationID == df_zones.LocationID,
    "left"
)

df_joined_broadcast.groupBy("Borough").count().show()
print(f"Broadcast join: {time.time() - start:.2f}s")

# COMMAND ----------

#Simple explain
df_silver.filter("pickup_hour = 8") \
         .groupBy("PULocationID") \
         .count() \
         .explain()

# COMMAND ----------

#Extended explain
df_silver.filter("pickup_hour = 8") \
         .groupBy("PULocationID") \
         .count() \
         .explain(extended=True)

# COMMAND ----------

#Compare regular vs broadcast in explain
print("=== WITHOUT BROADCAST ===")
df_silver.join(
    df_zones,
    df_silver.PULocationID == df_zones.LocationID,
    "left"
).explain()

print("\n=== WITH BROADCAST ===")
df_silver.join(
    broadcast(df_zones),
    df_silver.PULocationID == df_zones.LocationID,
    "left"
).explain()

# COMMAND ----------

#Repartition before writing
df_silver.repartition(col("pickup_month")) \
         .write \
         .mode("overwrite") \
         .partitionBy("pickup_month") \
         .parquet("/Volumes/workspace/default/taxi_data/silver/yellow_taxi_optimized")

print("✅ Written with repartition optimization")

# COMMAND ----------

df_silver.repartition(col("pickup_month")) \
         .write \
         .mode("overwrite") \
         .partitionBy("pickup_month") \
         .parquet("/Volumes/workspace/default/taxi_data/silver/yellow_taxi_optimized")

print("✅ Written with repartition optimization")

# COMMAND ----------

df_silver.repartition(200) \
         .write \
         .mode("overwrite") \
         .parquet("/Volumes/workspace/default/taxi_data/silver/tiny_files_demo")

files = dbutils.fs.ls("/Volumes/workspace/default/taxi_data/silver/tiny_files_demo")
print(f"Number of files written: {len(files)}")

# COMMAND ----------

df_silver.coalesce(4) \
         .write \
         .mode("overwrite") \
         .parquet("/Volumes/workspace/default/taxi_data/silver/optimized_files_demo")

files = dbutils.fs.ls("/Volumes/workspace/default/taxi_data/silver/optimized_files_demo")
print(f"Number of files written: {len(files)}")

# COMMAND ----------

