# Databricks notebook source
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# Load all gold tables as Pandas for plotting
df_daily       = spark.read.parquet("/Volumes/workspace/default/taxi_data/gold/daily_kpis").toPandas()
df_hourly      = spark.read.parquet("/Volumes/workspace/default/taxi_data/gold/hourly_demand").toPandas()
df_dow         = spark.read.parquet("/Volumes/workspace/default/taxi_data/gold/day_of_week").toPandas()
df_payment     = spark.read.parquet("/Volumes/workspace/default/taxi_data/gold/payment_split").toPandas()
df_tipping     = spark.read.parquet("/Volumes/workspace/default/taxi_data/gold/tipping_by_hour").toPandas()
df_moving_avg  = spark.read.parquet("/Volumes/workspace/default/taxi_data/gold/moving_avg_trips").toPandas()
df_dod         = spark.read.parquet("/Volumes/workspace/default/taxi_data/gold/dod_revenue").toPandas()
df_fare_q      = spark.read.parquet("/Volumes/workspace/default/taxi_data/gold/fare_quartiles").toPandas()

print("✅ All Gold tables loaded into Pandas")

# COMMAND ----------

df_daily.head()


# COMMAND ----------

# DBTITLE 1,Cell 3
import os

# Ensure the charts directory exists
os.makedirs("/Volumes/workspace/default/taxi_data/charts", exist_ok=True)

df_daily_sorted = df_daily.sort_values("pickup_date")

fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(df_daily_sorted["pickup_date"], df_daily_sorted["total_revenue"],
        color="#2196F3", linewidth=2)
ax.fill_between(df_daily_sorted["pickup_date"],
                df_daily_sorted["total_revenue"],
                alpha=0.15, color="#2196F3")
ax.set_title("Daily Revenue — NYC Yellow Taxi (Jan–Feb 2026)", fontsize=14, fontweight="bold")
ax.set_xlabel("Date")
ax.set_ylabel("Total Revenue ($)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("/Volumes/workspace/default/taxi_data/charts/daily_revenue.png", dpi=150)
plt.show()
print("✅ Saved: daily_revenue.png")

# COMMAND ----------

# HOURLY DEMAND HEATMAP STYLE
df_hourly_sorted = df_hourly.sort_values("pickup_hour")

fig, ax = plt.subplots(figsize=(14, 5))
bars = ax.bar(df_hourly_sorted["pickup_hour"],
              df_hourly_sorted["total_trips"],
              color="#4CAF50", edgecolor="white", linewidth=0.5)

# Highlight peak hours
for i, bar in enumerate(bars):
    if df_hourly_sorted["total_trips"].iloc[i] >= df_hourly_sorted["total_trips"].quantile(0.80):
        bar.set_color("#F44336")

ax.set_title("Trip Demand by Hour of Day — Peak Hours in Red", fontsize=14, fontweight="bold")
ax.set_xlabel("Hour of Day (0 = Midnight)")
ax.set_ylabel("Total Trips")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
ax.set_xticks(range(0, 24))
plt.tight_layout()
plt.savefig("/Volumes/workspace/default/taxi_data/charts/hourly_demand.png", dpi=150)
plt.show()
print("✅ Saved: hourly_demand.png")

# COMMAND ----------

#DAY OF WEEK REVENUE
df_dow_sorted = df_dow.sort_values("day_of_week")

fig, ax = plt.subplots(figsize=(10, 5))
colors = ["#FF7043" if r == df_dow_sorted["total_revenue"].max()
          else "#90CAF9" for r in df_dow_sorted["total_revenue"]]
ax.bar(df_dow_sorted["day_of_week"].str[2:],
       df_dow_sorted["total_revenue"],
       color=colors, edgecolor="white")
ax.set_title("Total Revenue by Day of Week — Highest Day in Orange", fontsize=13, fontweight="bold")
ax.set_xlabel("Day of Week")
ax.set_ylabel("Total Revenue ($)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
plt.tight_layout()
plt.savefig("/Volumes/workspace/default/taxi_data/charts/dow_revenue.png", dpi=150)
plt.show()
print("✅ Saved: dow_revenue.png")

# COMMAND ----------

#PAYMENT METHOD PIE CHART
fig, ax = plt.subplots(figsize=(8, 8))
colors = ["#42A5F5", "#66BB6A", "#FFA726", "#EF5350", "#AB47BC"]
wedges, texts, autotexts = ax.pie(
    df_payment["total_trips"],
    labels=df_payment["payment_method"],
    autopct="%1.1f%%",
    colors=colors[:len(df_payment)],
    startangle=140,
    pctdistance=0.82
)
for text in autotexts:
    text.set_fontsize(11)
ax.set_title("Payment Method Distribution", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.savefig("/Volumes/workspace/default/taxi_data/charts/payment_split.png", dpi=150)
plt.show()
print("✅ Saved: payment_split.png")

# COMMAND ----------

# TIPPING RATE BY HOUR
df_tipping_sorted = df_tipping.sort_values("pickup_hour")

fig, ax1 = plt.subplots(figsize=(14, 5))

ax1.bar(df_tipping_sorted["pickup_hour"],
        df_tipping_sorted["total_trips"],
        color="#B0BEC5", alpha=0.6, label="Total Trips")
ax1.set_xlabel("Hour of Day")
ax1.set_ylabel("Total Trips", color="#607D8B")

ax2 = ax1.twinx()
ax2.plot(df_tipping_sorted["pickup_hour"],
         df_tipping_sorted["tip_rate_pct"],
         color="#E91E63", linewidth=2.5, marker="o", label="Tip Rate %")
ax2.set_ylabel("Tip Rate (%)", color="#E91E63")
ax2.set_ylim(0, 100)

ax1.set_title("Tipping Rate vs Trip Volume by Hour", fontsize=14, fontweight="bold")
ax1.set_xticks(range(0, 24))
fig.legend(loc="upper left", bbox_to_anchor=(0.1, 0.95))
plt.tight_layout()
plt.savefig("/Volumes/workspace/default/taxi_data/charts/tipping_by_hour.png", dpi=150)
plt.show()
print("✅ Saved: tipping_by_hour.png")

# COMMAND ----------

#7-DAY MOVING AVERAGE vs ACTUAL TRIPS
df_moving_avg_sorted = df_moving_avg.sort_values("pickup_date")

fig, ax = plt.subplots(figsize=(14, 5))
ax.bar(df_moving_avg_sorted["pickup_date"],
       df_moving_avg_sorted["total_trips"],
       color="#CFD8DC", label="Daily Trips")
ax.plot(df_moving_avg_sorted["pickup_date"],
        df_moving_avg_sorted["trips_7day_moving_avg"],
        color="#E53935", linewidth=2.5, label="7-Day Moving Avg")
ax.set_title("Daily Trip Volume with 7-Day Moving Average", fontsize=14, fontweight="bold")
ax.set_xlabel("Date")
ax.set_ylabel("Total Trips")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.savefig("/Volumes/workspace/default/taxi_data/charts/moving_avg_trips.png", dpi=150)
plt.show()
print("✅ Saved: moving_avg_trips.png")

# COMMAND ----------

#FARE QUARTILE INSIGHTS
df_fare_q_sorted = df_fare_q.sort_values("fare_quartile")
quartile_labels = ["Q1 (Lowest)", "Q2", "Q3", "Q4 (Highest)"]

fig, axes = plt.subplots(1, 3, figsize=(15, 5))
fig.suptitle("Fare Quartile Analysis", fontsize=14, fontweight="bold")

metrics = [
    ("avg_fare",     "Avg Fare ($)",     "#42A5F5"),
    ("avg_tip",      "Avg Tip ($)",      "#66BB6A"),
    ("avg_distance", "Avg Distance (mi)","#FFA726"),
]

for ax, (col, label, color) in zip(axes, metrics):
    ax.bar(quartile_labels, df_fare_q_sorted[col], color=color, edgecolor="white")
    ax.set_title(label)
    ax.set_ylabel(label)
    ax.tick_params(axis="x", rotation=15)

plt.tight_layout()
plt.savefig("/Volumes/workspace/default/taxi_data/charts/fare_quartiles.png", dpi=150)
plt.show()
print("✅ Saved: fare_quartiles.png")

# COMMAND ----------

#EXPORT SUMMARY TO CSV 
# Export key gold tables as CSV — easy to attach to GitHub repo

export_map = {
    "daily_kpis"     : df_daily,
    "hourly_demand"  : df_hourly,
    "day_of_week"    : df_dow,
    "payment_split"  : df_payment,
    "tipping_by_hour": df_tipping,
}

for name, df in export_map.items():
    path = f"/Volumes/workspace/default/taxi_data/exports/{name}.csv"
    df.to_csv(path, index=False)
    print(f"✅ Exported: {name}.csv")

# COMMAND ----------

