# Databricks notebook source
# MAGIC %md
# MAGIC # Eksplorasi NYC Yellow Taxi
# MAGIC Jalankan setelah Bronze memuat batch yang ingin diperiksa.

# COMMAND ----------
# MAGIC %run ./00_config

# COMMAND ----------
namespace, landing = settings()
month = batch_month()
raw = spark.table(f"{namespace}.yellow_taxi_bronze").where(month_filter(month))
raw.printSchema()
raw_columns = [name for name in raw.columns if name not in METADATA]

display(raw.agg(F.count("*").alias("baris"), *[
    F.count(F.when(F.col(name).isNull(), 1)).alias(f"null_{name}")
    for name in raw_columns
]))
display(raw.select("source_file", "source_url", "ingested_at").distinct())
print(f"Duplikat baris penuh: {raw.count() - raw.select(*raw_columns).distinct().count():,}")

# COMMAND ----------
raw.createOrReplaceTempView("yellow_taxi_exploration")

display(spark.sql("""
SELECT CAST(tpep_pickup_datetime AS DATE) AS tanggal, COUNT(*) AS penjemputan
FROM yellow_taxi_exploration
GROUP BY 1
ORDER BY 1
"""))

display(raw.groupBy("PULocationID").count().orderBy(F.desc("count")).limit(10))

display(spark.sql("""
WITH durations AS (
  SELECT timestampdiff(MICROSECOND, tpep_pickup_datetime, tpep_dropoff_datetime)
         / 60000000.0 AS menit
  FROM yellow_taxi_exploration
)
SELECT COUNT(menit) AS durasi_tersedia, MIN(menit) AS min_menit,
       MAX(menit) AS max_menit, AVG(menit) AS avg_menit
FROM durations
"""))
