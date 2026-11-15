# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
month = batch_month()
namespace, landing = settings()
bronze = spark.table(f"{namespace}.yellow_taxi_bronze").where(month_filter(month))
raw_columns = [name for name in bronze.columns if name not in METADATA]
input_rows = bronze.count()

unique = bronze.dropDuplicates(raw_columns).withColumns({
    "pickup_at": F.expr("TRY_CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ)"),
    "dropoff_at": F.expr("TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP_NTZ)"),
    "source_zone_id": F.expr("TRY_CAST(PULocationID AS INT)"),
    "record_hash": F.xxhash64(*raw_columns),
})

pickup_in_month = F.date_format("pickup_at", "yyyy-MM") == month
duration = F.expr("timestampdiff(MICROSECOND, pickup_at, dropoff_at)") / F.lit(60_000_000.0)
prepared = unique.withColumn("raw_duration_minutes", duration)
valid_duration = (F.col("raw_duration_minutes") > 0) & (F.col("raw_duration_minutes") <= MAX_MINUTES)

quality = prepared.agg(
    F.count("*").alias("distinct_rows"),
    F.count(F.when(F.col("pickup_at").isNull(), 1)).alias("null_pickup"),
    F.count(F.when(F.col("pickup_at").isNotNull() & ~pickup_in_month, 1)).alias("outside_batch"),
    F.count(F.when(pickup_in_month, 1)).alias("valid_pickup"),
    F.count(F.when(pickup_in_month & valid_duration, 1)).alias("valid_duration"),
).first().asDict()

zones = spark.read.option("header", True).csv(str(landing / "taxi_zone_lookup.csv")).select(
    F.col("LocationID").cast("int").alias("source_zone_id"), "Zone", "Borough"
)
joined = prepared.where(pickup_in_month).join(F.broadcast(zones), "source_zone_id", "left")
silver = joined.select(
    "batch_month", "record_hash", "pickup_at", "dropoff_at",
    F.when(F.col("Zone").isNotNull(), F.col("source_zone_id")).otherwise(0).alias("pickup_zone_id"),
    F.coalesce("Zone", F.lit("Tidak diketahui")).alias("pickup_zone"),
    F.coalesce("Borough", F.lit("Tidak diketahui")).alias("borough"),
    F.when(valid_duration, F.col("raw_duration_minutes")).alias("duration_minutes"),
)

table = f"{namespace}.yellow_taxi_silver"
write_month(silver, table, month, quality["valid_pickup"])
stored = spark.table(table).where(month_filter(month)).count()
if stored != quality["valid_pickup"]:
    raise ValueError("Jumlah baris Silver berbeda dari jumlah pickup valid.")

publish_metrics(layer="silver", batch_month=month, input_rows=input_rows, written=stored,
                duplicates=input_rows - quality["distinct_rows"], null_pickup=quality["null_pickup"],
                outside_batch=quality["outside_batch"], valid_duration=quality["valid_duration"],
                excluded_durations=stored - quality["valid_duration"])
