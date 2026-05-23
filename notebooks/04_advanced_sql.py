# Databricks notebook source
df_silver=spark.read.parquet("/Volumes/workspace/default/taxi_data/silver/yellow_taxi_2026/")
df_silver.createOrReplaceTempView("yellow_taxi")
df_silver.printSchema()


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum, round

windowSpec = Window.orderBy("pickup_date")

df_daily = (df_silver.groupBy("pickup_date").agg(round(spark_sum("total_amount"), 2).alias("daily_revenue")))

df_running_revenue = (df_daily.withColumn("running_revenue",round(spark_sum("daily_revenue").over(windowSpec.rowsBetween(Window.unboundedPreceding,Window.currentRow)),2)))

display(df_running_revenue.limit(10))

# COMMAND ----------

df_running_revenue = spark.sql("""
    WITH daily AS (
        SELECT
            pickup_date,
            ROUND(SUM(total_amount), 2) AS daily_revenue
        FROM yellow_taxi
        GROUP BY pickup_date
    )
    SELECT
        pickup_date,
        daily_revenue,
        ROUND(SUM(daily_revenue) OVER (
            ORDER BY pickup_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2)                           AS running_total_revenue
    FROM daily
    ORDER BY pickup_date
""")

display(df_running_revenue.limit(10))

# COMMAND ----------

#Rank Pickup Locations by Revenue (per month)
df_ranked_locations = spark.sql("""
    WITH location_monthly AS (
        SELECT
            pickup_month,
            PULocationID,
            COUNT(*)                        AS total_trips,
            ROUND(SUM(total_amount), 2)     AS total_revenue
        FROM yellow_taxi
        GROUP BY pickup_month, PULocationID
    )
    SELECT
        pickup_month,
        PULocationID,
        total_trips,
        total_revenue,
        RANK() OVER (
            PARTITION BY pickup_month
            ORDER BY total_revenue DESC
        )                                   AS revenue_rank
    FROM location_monthly
    QUALIFY RANK() OVER (
        PARTITION BY pickup_month
        ORDER BY total_revenue DESC
    ) <= 5
""")

display(df_ranked_locations)

# COMMAND ----------

#7-Day Moving Average of Trip Volume

df_moving_avg = spark.sql("""
    WITH daily_trips AS (
        SELECT
            pickup_date,
            COUNT(*) AS total_trips
        FROM yellow_taxi
        GROUP BY pickup_date
    )
    SELECT
        pickup_date,
        total_trips,
        ROUND(AVG(total_trips) OVER (
            ORDER BY pickup_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 0)                           AS trips_7day_moving_avg
    FROM daily_trips
    ORDER BY pickup_date
""")

display(df_moving_avg)

# COMMAND ----------

#LAG: Day-over-Day Revenue Change
df_dod_revenue = spark.sql("""
    WITH daily AS (
        SELECT
            pickup_date,
            ROUND(SUM(total_amount), 2) AS daily_revenue
        FROM yellow_taxi
        GROUP BY pickup_date
    )
    SELECT
        pickup_date,
        daily_revenue,
        LAG(daily_revenue, 1) OVER (
            ORDER BY pickup_date
        )                               AS prev_day_revenue,
        ROUND(daily_revenue - LAG(daily_revenue, 1) 
            OVER (ORDER BY pickup_date), 2) AS revenue_change,
        ROUND((daily_revenue - LAG(daily_revenue, 1) 
            OVER (ORDER BY pickup_date)) * 100.0 
            / LAG(daily_revenue, 1) 
            OVER (ORDER BY pickup_date), 2) AS pct_change
    FROM daily
    ORDER BY pickup_date
""")

display(df_dod_revenue)

# COMMAND ----------

# DBTITLE 1,Cell 7
# NTILE: Bucket Trips into Fare Quartiles
df_fare_quartiles = spark.sql("""
    SELECT
        fare_quartile,
        COUNT(*)                        AS total_trips,
        ROUND(MIN(fare_amount), 2)      AS min_fare,
        ROUND(MAX(fare_amount), 2)      AS max_fare,
        ROUND(AVG(fare_amount), 2)      AS avg_fare,
        ROUND(AVG(tip_amount), 2)       AS avg_tip,
        ROUND(AVG(trip_distance), 2)    AS avg_distance
    FROM (
        SELECT
            fare_amount,
            tip_amount,
            trip_distance,
            NTILE(4) OVER (ORDER BY fare_amount) AS fare_quartile
        FROM yellow_taxi
    ) quartiles
    GROUP BY fare_quartile
    ORDER BY fare_quartile
""")

display(df_fare_quartiles)

# COMMAND ----------

#CTEs (Common Table Expressions) Multi-level CTE: High Value Hour + Location Combo
df_hvhl = spark.sql("""
    WITH hourly_location AS (
        SELECT
            pickup_hour,
            PULocationID,
            COUNT(*)                    AS total_trips,
            ROUND(AVG(total_amount), 2) AS avg_revenue
        FROM yellow_taxi
        GROUP BY pickup_hour, PULocationID
    ),
    ranked AS (
        SELECT
            pickup_hour,
            PULocationID,
            total_trips,
            avg_revenue,
            RANK() OVER (
                PARTITION BY pickup_hour
                ORDER BY avg_revenue DESC
            )                           AS loc_rank
        FROM hourly_location
    )
    SELECT
        pickup_hour,
        PULocationID,
        total_trips,
        avg_revenue,
        loc_rank
    FROM ranked
    WHERE loc_rank = 1
    ORDER BY pickup_hour
""")

display(df_hvhl)


# COMMAND ----------

#Write Silver partitioned two ways and compare
df_silver.write.mode("overwrite").partitionBy("pickup_month","pickup_hour").parquet("/Volumes/workspace/default/taxi_data/silver_partitioned/yellow_taxi_2026")
print("✅ Written with dual partitioning")

# COMMAND ----------

# Without partition filter — scans everything
import time

start = time.time()
df_silver.filter("pickup_month = 1").count()
print(f"Without partitioning awareness: {time.time() - start:.2f}s")

# With partition filter — only reads month=1 folder
start = time.time()
df_part = spark.read.parquet("/Volumes/workspace/default/taxi_data/silver_partitioned/yellow_taxi_2026/")
df_part.filter("pickup_month = 1 AND pickup_hour = 8").count()
print(f"With partition pruning: {time.time() - start:.2f}s")


# COMMAND ----------

advanced_tables = {
    "running_revenue"   : df_running_revenue,
    "ranked_locations"  : df_ranked_locations,
    "moving_avg_trips"  : df_moving_avg,
    "dod_revenue"       : df_dod_revenue,
    "fare_quartiles"    : df_fare_quartiles,
    "best_hour_location": df_hvhl
}

for name, df in advanced_tables.items():
    df.write.mode("overwrite").parquet(f"/Volumes/workspace/default/taxi_data/gold/{name}")
    print(f"✅ Written: {name}")

# COMMAND ----------

