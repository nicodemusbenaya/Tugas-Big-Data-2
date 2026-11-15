# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
month = batch_month()
namespace, _ = settings()
silver = spark.table(f"{namespace}.yellow_taxi_silver").where(month_filter(month))
gold = (
    silver.withColumn("pickup_hour", F.date_trunc("hour", "pickup_at").cast("timestamp_ntz"))
    .groupBy("batch_month", "pickup_hour", "pickup_zone_id", "pickup_zone", "borough")
    .agg(
        F.count("*").alias("jumlah_penjemputan"),
        F.count("duration_minutes").alias("jumlah_durasi_valid"),
        F.coalesce(F.sum("duration_minutes"), F.lit(0.0)).alias("total_menit_durasi_valid"),
    )
    .withColumn("pickup_date", F.to_date("pickup_hour"))
)
table = f"{namespace}.yellow_taxi_gold_hourly_zone"
summary = gold.agg(F.count("*").alias("rows"), F.sum("jumlah_penjemputan").alias("pickups")).first()
input_rows = silver.count()
if summary.pickups != input_rows:
    raise ValueError(f"Jumlah pickup Gold ({summary.pickups}) berbeda dari Silver ({input_rows}).")

write_month(gold, table, month, summary.rows)
stored = spark.table(table).where(month_filter(month))
grain_columns = ("pickup_hour", "pickup_zone_id")
if stored.groupBy(*grain_columns).count().where(F.col("count") > 1).limit(1).count():
    raise ValueError("Gold memiliki kunci pickup_hour dan pickup_zone_id ganda.")

publish_metrics(layer="gold", batch_month=month, written=summary.rows, pickups=summary.pickups)
