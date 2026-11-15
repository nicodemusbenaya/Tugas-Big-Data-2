# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
namespace, _ = settings()
bronze = spark.table(f"{namespace}.yellow_taxi_bronze")
silver = spark.table(f"{namespace}.yellow_taxi_silver")
gold = spark.table(f"{namespace}.yellow_taxi_gold_hourly_zone")

totals = (
    bronze.groupBy("batch_month").agg(F.count("*").alias("bronze_rows"))
    .join(silver.groupBy("batch_month").agg(F.count("*").alias("silver_rows")), "batch_month", "full")
    .join(gold.groupBy("batch_month").agg(
        F.count("*").alias("gold_rows"),
        F.sum("jumlah_penjemputan").alias("gold_pickups"),
        F.sum("jumlah_durasi_valid").alias("gold_valid_durations"),
        F.round(F.sum("total_menit_durasi_valid"), 2).alias("gold_total_duration_minutes"),
    ), "batch_month", "full")
    .orderBy("batch_month")
)
display(totals)
for batch in totals.collect():
    if not (batch.bronze_rows >= batch.silver_rows == batch.gold_pickups):
        raise AssertionError(f"Rekonsiliasi gagal untuk {batch.batch_month}: {batch.asDict()}")

duplicate_keys = gold.groupBy("pickup_hour", "pickup_zone_id").count().where(F.col("count") > 1)
if duplicate_keys.limit(1).count():
    raise AssertionError("Grain Gold tidak unik.")

display(gold.orderBy("pickup_hour", "pickup_zone_id").limit(20))
display(bronze.select("batch_month", "source_url", "source_file", "ingested_at").distinct().orderBy("batch_month"))
