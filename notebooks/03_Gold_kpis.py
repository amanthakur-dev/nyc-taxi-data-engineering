# Databricks notebook source
df_silver=spark.read.parquet("/Volumes/workspace/default/taxi_data/silver/yellow_taxi_2023/")
print("Silver Schema\n")
print(df_silver.printSchema())
df_silver.createOrReplaceTempView("yellow_taxi")
print(f"Silver records loaded: {df_silver.count():,}")

# COMMAND ----------

df_daily = spark.sql("""
    SELECT
        pickup_date,
        COUNT(*)                            AS total_trips,
        ROUND(SUM(fare_amount), 2)          AS total_fare,
        ROUND(SUM(total_amount), 2)         AS total_revenue,
        ROUND(AVG(fare_amount), 2)          AS avg_fare,
        ROUND(AVG(trip_distance), 2)        AS avg_distance,
        ROUND(AVG(trip_duration_mins), 2)   AS avg_duration_mins
    FROM yellow_taxi
    GROUP BY pickup_date
    ORDER BY pickup_date
""")

display(df_daily)

# COMMAND ----------

#KPI 2
df_hourly = spark.sql("""
    SELECT
        pickup_hour,
        COUNT(*)                        AS total_trips,
        ROUND(AVG(fare_amount), 2)      AS avg_fare,
        ROUND(AVG(trip_duration_mins), 2) AS avg_duration_mins,
        ROUND(SUM(total_amount), 2)     AS total_revenue
    FROM yellow_taxi
    GROUP BY pickup_hour
    ORDER BY pickup_hour
""")

display(df_hourly)

# COMMAND ----------

#Day of Week Analysis
df_dow = spark.sql("""
    SELECT
        CASE pickup_day_of_week
            WHEN 1 THEN '1_Sunday'
            WHEN 2 THEN '2_Monday'
            WHEN 3 THEN '3_Tuesday'
            WHEN 4 THEN '4_Wednesday'
            WHEN 5 THEN '5_Thursday'
            WHEN 6 THEN '6_Friday'
            WHEN 7 THEN '7_Saturday'
        END                             AS day_of_week,
        COUNT(*)                        AS total_trips,
        ROUND(AVG(fare_amount), 2)      AS avg_fare,
        ROUND(AVG(tip_amount), 2)       AS avg_tip,
        ROUND(SUM(total_amount), 2)     AS total_revenue
    FROM yellow_taxi
    GROUP BY pickup_day_of_week
    ORDER BY pickup_day_of_week
""")

display(df_dow)

# COMMAND ----------

df_top_pickups = spark.sql("""
    SELECT
        PULocationID,
        COUNT(*)                        AS total_pickups,
        ROUND(AVG(fare_amount), 2)      AS avg_fare,
        ROUND(AVG(trip_distance), 2)    AS avg_distance,
        ROUND(SUM(total_amount), 2)     AS total_revenue
    FROM yellow_taxi
    GROUP BY PULocationID
    ORDER BY total_pickups DESC
    LIMIT 10
""")

display(df_top_pickups)

# COMMAND ----------

#Tipping Behaviour by Hour
df_tipping = spark.sql("""
    SELECT
        pickup_hour,
        COUNT(*)                                AS total_trips,
        SUM(is_tipped)                          AS tipped_trips,
        ROUND(SUM(is_tipped) * 100.0 
              / COUNT(*), 2)                    AS tip_rate_pct,
        ROUND(AVG(CASE WHEN is_tipped = 1 
              THEN tip_amount END), 2)          AS avg_tip_when_tipped
    FROM yellow_taxi
    GROUP BY pickup_hour
    ORDER BY pickup_hour
""")

display(df_tipping)

# COMMAND ----------

#Payment Type Breakdown
df_payment = spark.sql("""
    SELECT
        CASE payment_type
            WHEN 1 THEN 'Credit Card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No Charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
        END                                     AS payment_method,
        COUNT(*)                                AS total_trips,
        ROUND(COUNT(*) * 100.0 
              / SUM(COUNT(*)) OVER(), 2)        AS pct_of_total,
        ROUND(AVG(fare_amount), 2)              AS avg_fare,
        ROUND(AVG(tip_amount), 2)               AS avg_tip
    FROM yellow_taxi
    GROUP BY payment_type
    ORDER BY total_trips DESC
""")

display(df_payment)

# COMMAND ----------

gold_tables = {
    "daily_kpis"      : df_daily,
    "hourly_demand"   : df_hourly,
    "day_of_week"     : df_dow,
    "top_pickups"     : df_top_pickups,
    "tipping_by_hour" : df_tipping,
    "payment_split"   : df_payment
}

for table_name, df in gold_tables.items():
    path = f"/Volumes/workspace/default/taxi_data/gold/{table_name}"
    df.write.mode("overwrite").parquet(path)
    print(f"✅ Written: {table_name}")

# COMMAND ----------

for table_name in gold_tables.keys():
    df_check = spark.read.parquet(f"/Volumes/workspace/default/taxi_data/gold/{table_name}")
    print(f"{table_name}: {df_check.count()} rows")

# COMMAND ----------

