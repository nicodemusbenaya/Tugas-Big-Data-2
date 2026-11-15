# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
month = batch_month()
namespace, landing = settings()
mode = widget("source_mode", "https")
filename = f"yellow_tripdata_{month}.parquet"
source_url = widget("source_url", "") or f"{TLC_ROOT}/trip-data/{filename}"
bronze = f"{namespace}.yellow_taxi_bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {namespace}.tlc_landing")
stage_source(ZONE_URL, landing / "taxi_zone_lookup.csv", valid_zone_csv, mode)

if spark.catalog.tableExists(bronze):
    previous = spark.table(bronze).where(month_filter(month)).groupBy("source_url").count().collect()
    if previous:
        if len(previous) != 1 or previous[0].source_url != source_url:
            raise ValueError(f"Batch {month} sudah memakai URL sumber berbeda.")
        publish_metrics(layer="bronze", batch_month=month, status="skipped", rows=previous[0]["count"], written=0)

source_path = landing / filename
method = stage_source(source_url, source_path, has_parquet_footer, mode)
source = spark.read.parquet(str(source_path))
missing = REQUIRED - {name.lower() for name in source.columns}
if missing:
    raise ValueError(f"Kolom sumber hilang: {sorted(missing)}")
row_count = source.count()
if not row_count:
    raise ValueError(f"File {filename} tidak memiliki baris.")

raw = source.withColumns({
    "batch_month": F.lit(month),
    "source_file": F.lit(filename),
    "source_url": F.lit(source_url),
    "ingested_at": F.current_timestamp(),
})
raw.write.format("delta").mode("append").saveAsTable(bronze)
publish_metrics(layer="bronze", batch_month=month, status="inserted", rows=row_count,
                written=row_count, method=method, source_file=filename, bytes=source_path.stat().st_size)
