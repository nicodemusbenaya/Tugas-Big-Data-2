# Databricks notebook source
import csv
import json
import re
import shutil
import tempfile
from datetime import date
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from pyspark.sql import functions as F

TLC_ROOT = "https://d37ci6vzurychx.cloudfront.net"
ZONE_URL = f"{TLC_ROOT}/misc/taxi_zone_lookup.csv"
METADATA = ("batch_month", "source_file", "source_url", "ingested_at")
REQUIRED = {"tpep_pickup_datetime", "tpep_dropoff_datetime", "pulocationid"}
MAX_MINUTES = 180


def widget(name, default):
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name).strip()


def settings():
    catalog = widget("catalog", "") or spark.sql("SELECT current_catalog()").first()[0]
    schema = widget("schema", "nyc_yellow_taxi")
    for value in (catalog, schema):
        if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_-]*", value):
            raise ValueError(f"Nama catalog atau schema tidak valid: {value!r}")
    if catalog.lower() in {"hive_metastore", "system", "samples"}:
        raise ValueError("Pilih catalog Unity Catalog yang mengizinkan penulisan.")
    return f"`{catalog}`.`{schema}`", Path(f"/Volumes/{catalog}/{schema}/tlc_landing")


def batch_month():
    month = widget("batch_month", "2025-01")
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}", month):
        raise ValueError(f"batch_month harus berformat YYYY-MM: {month!r}")
    date.fromisoformat(f"{month}-01")
    return month


def has_parquet_footer(path):
    if not path.is_file() or path.stat().st_size < 12:
        return False
    with path.open("rb") as stream:
        if stream.read(4) != b"PAR1":
            return False
        stream.seek(-4, 2)
        return stream.read() == b"PAR1"


def valid_zone_csv(path):
    if not path.is_file():
        return False
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if not {"LocationID", "Borough", "Zone"}.issubset(reader.fieldnames or []):
            return False
        rows = list(reader)
    zone_ids = [row.get("LocationID") or "" for row in rows]
    return len(set(zone_ids)) == len(rows) and len(rows) >= 260 and all(
        zone_id.isdecimal() for zone_id in zone_ids
    )


def stage_source(url, destination, validator, mode):
    if validator(destination):
        return "volume"
    if mode == "volume":
        raise FileNotFoundError(f"Unggah file valid ke {destination} sebelum menjalankan Job.")
    if mode != "https":
        raise ValueError("source_mode harus https atau volume.")
    if urlparse(url).scheme != "https":
        raise ValueError("URL sumber harus menggunakan HTTPS.")

    with tempfile.TemporaryDirectory() as temp_dir:
        local_file = Path(temp_dir, destination.name)
        try:
            request = Request(url, headers={"User-Agent": "NYC-Yellow-Taxi-Big-Data/1.0"})
            with urlopen(request, timeout=60) as response, local_file.open("wb") as output:
                shutil.copyfileobj(response, output)
        except OSError as error:
            raise RuntimeError(
                f"Unduhan TLC gagal untuk {destination.name}. Periksa akses HTTPS "
                "serverless atau unggah file resmi dan gunakan source_mode=volume."
            ) from error
        if not validator(local_file):
            raise ValueError(f"Isi unduhan tidak valid: {destination.name}")
        shutil.copyfile(local_file, destination)
    return "https"


def month_filter(month):
    return F.col("batch_month") == month


def write_month(frame, table, month, row_count):
    if row_count <= 0:
        raise ValueError(f"Batch {month} kosong. Tabel {table} tidak ditulis.")
    writer = frame.write.format("delta")
    if spark.catalog.tableExists(table):
        writer.mode("overwrite").option("replaceWhere", f"batch_month = '{month}'").saveAsTable(table)
    else:
        writer.mode("append").saveAsTable(table)


def publish_metrics(**metrics):
    result = json.dumps(metrics, default=str, ensure_ascii=False, sort_keys=True)
    print(result)
    dbutils.notebook.exit(result)
