# Databricks notebook source
# MAGIC %md
# MAGIC # Pasang proyek NYC Yellow Taxi
# MAGIC Pilih compute Serverless lalu **Run all**. Notebook ini membuat folder proyek,
# MAGIC notebook, schema, volume, dan Job. Dengan `run_demo=true`, notebook juga
# MAGIC menjalankan uji ETL, eksperimen model, dan menerbitkan dashboard.
# MAGIC Gunakan `run_demo=model` untuk melanjutkan model dari tabel Silver yang sudah terisi.

# COMMAND ----------
import base64
import json
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, OperationFailed, ResourceDoesNotExist
from databricks.sdk.service import dashboards, jobs, serving, workspace

NOTEBOOKS = {'00_config': '# Databricks notebook source\n'
              'import csv\n'
              'import json\n'
              'import re\n'
              'import shutil\n'
              'import tempfile\n'
              'from datetime import date\n'
              'from pathlib import Path\n'
              'from urllib.parse import urlparse\n'
              'from urllib.request import Request, urlopen\n'
              '\n'
              'from pyspark.sql import functions as F\n'
              '\n'
              'TLC_ROOT = "https://d37ci6vzurychx.cloudfront.net"\n'
              'ZONE_URL = f"{TLC_ROOT}/misc/taxi_zone_lookup.csv"\n'
              'METADATA = ("batch_month", "source_file", "source_url", "ingested_at")\n'
              'REQUIRED = {"tpep_pickup_datetime", "tpep_dropoff_datetime", "pulocationid"}\n'
              'MAX_MINUTES = 180\n'
              '\n'
              '\n'
              'def widget(name, default):\n'
              '    dbutils.widgets.text(name, default)\n'
              '    return dbutils.widgets.get(name).strip()\n'
              '\n'
              '\n'
              'def settings():\n'
              '    catalog = widget("catalog", "") or spark.sql("SELECT current_catalog()").first()[0]\n'
              '    schema = widget("schema", "nyc_yellow_taxi")\n'
              '    for value in (catalog, schema):\n'
              '        if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_-]*", value):\n'
              '            raise ValueError(f"Nama catalog atau schema tidak valid: {value!r}")\n'
              '    if catalog.lower() in {"hive_metastore", "system", "samples"}:\n'
              '        raise ValueError("Pilih catalog Unity Catalog yang mengizinkan penulisan.")\n'
              '    return f"`{catalog}`.`{schema}`", Path(f"/Volumes/{catalog}/{schema}/tlc_landing")\n'
              '\n'
              '\n'
              'def batch_month():\n'
              '    month = widget("batch_month", "2025-01")\n'
              '    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}", month):\n'
              '        raise ValueError(f"batch_month harus berformat YYYY-MM: {month!r}")\n'
              '    date.fromisoformat(f"{month}-01")\n'
              '    return month\n'
              '\n'
              '\n'
              'def has_parquet_footer(path):\n'
              '    if not path.is_file() or path.stat().st_size < 12:\n'
              '        return False\n'
              '    with path.open("rb") as stream:\n'
              '        if stream.read(4) != b"PAR1":\n'
              '            return False\n'
              '        stream.seek(-4, 2)\n'
              '        return stream.read() == b"PAR1"\n'
              '\n'
              '\n'
              'def valid_zone_csv(path):\n'
              '    if not path.is_file():\n'
              '        return False\n'
              '    with path.open(encoding="utf-8-sig", newline="") as stream:\n'
              '        reader = csv.DictReader(stream)\n'
              '        if not {"LocationID", "Borough", "Zone"}.issubset(reader.fieldnames or []):\n'
              '            return False\n'
              '        rows = list(reader)\n'
              '    zone_ids = [row.get("LocationID") or "" for row in rows]\n'
              '    return len(set(zone_ids)) == len(rows) and len(rows) >= 260 and all(\n'
              '        zone_id.isdecimal() for zone_id in zone_ids\n'
              '    )\n'
              '\n'
              '\n'
              'def stage_source(url, destination, validator, mode):\n'
              '    if validator(destination):\n'
              '        return "volume"\n'
              '    if mode == "volume":\n'
              '        raise FileNotFoundError(f"Unggah file valid ke {destination} sebelum menjalankan '
              'Job.")\n'
              '    if mode != "https":\n'
              '        raise ValueError("source_mode harus https atau volume.")\n'
              '    if urlparse(url).scheme != "https":\n'
              '        raise ValueError("URL sumber harus menggunakan HTTPS.")\n'
              '\n'
              '    with tempfile.TemporaryDirectory() as temp_dir:\n'
              '        local_file = Path(temp_dir, destination.name)\n'
              '        try:\n'
              '            request = Request(url, headers={"User-Agent": "NYC-Yellow-Taxi-Big-Data/1.0"})\n'
              '            with urlopen(request, timeout=60) as response, local_file.open("wb") as output:\n'
              '                shutil.copyfileobj(response, output)\n'
              '        except OSError as error:\n'
              '            raise RuntimeError(\n'
              '                f"Unduhan TLC gagal untuk {destination.name}. Periksa akses HTTPS "\n'
              '                "serverless atau unggah file resmi dan gunakan source_mode=volume."\n'
              '            ) from error\n'
              '        if not validator(local_file):\n'
              '            raise ValueError(f"Isi unduhan tidak valid: {destination.name}")\n'
              '        shutil.copyfile(local_file, destination)\n'
              '    return "https"\n'
              '\n'
              '\n'
              'def month_filter(month):\n'
              '    return F.col("batch_month") == month\n'
              '\n'
              '\n'
              'def write_month(frame, table, month, row_count):\n'
              '    if row_count <= 0:\n'
              '        raise ValueError(f"Batch {month} kosong. Tabel {table} tidak ditulis.")\n'
              '    writer = frame.write.format("delta")\n'
              '    if spark.catalog.tableExists(table):\n'
              '        writer.mode("overwrite").option("replaceWhere", f"batch_month = '
              '\'{month}\'").saveAsTable(table)\n'
              '    else:\n'
              '        writer.mode("append").saveAsTable(table)\n'
              '\n'
              '\n'
              'def publish_metrics(**metrics):\n'
              '    result = json.dumps(metrics, default=str, ensure_ascii=False, sort_keys=True)\n'
              '    print(result)\n'
              '    dbutils.notebook.exit(result)\n',
 '01_eksplorasi': '# Databricks notebook source\n'
                  '# MAGIC %md\n'
                  '# MAGIC # Eksplorasi NYC Yellow Taxi\n'
                  '# MAGIC Jalankan setelah Bronze memuat batch yang ingin diperiksa.\n'
                  '\n'
                  '# COMMAND ----------\n'
                  '# MAGIC %run ./00_config\n'
                  '\n'
                  '# COMMAND ----------\n'
                  'namespace, landing = settings()\n'
                  'month = batch_month()\n'
                  'raw = spark.table(f"{namespace}.yellow_taxi_bronze").where(month_filter(month))\n'
                  'raw.printSchema()\n'
                  'raw_columns = [name for name in raw.columns if name not in METADATA]\n'
                  '\n'
                  'display(raw.agg(F.count("*").alias("baris"), *[\n'
                  '    F.count(F.when(F.col(name).isNull(), 1)).alias(f"null_{name}")\n'
                  '    for name in raw_columns\n'
                  ']))\n'
                  'display(raw.select("source_file", "source_url", "ingested_at").distinct())\n'
                  'print(f"Duplikat baris penuh: {raw.count() - '
                  'raw.select(*raw_columns).distinct().count():,}")\n'
                  '\n'
                  '# COMMAND ----------\n'
                  'raw.createOrReplaceTempView("yellow_taxi_exploration")\n'
                  '\n'
                  'display(spark.sql("""\n'
                  'SELECT CAST(tpep_pickup_datetime AS DATE) AS tanggal, COUNT(*) AS penjemputan\n'
                  'FROM yellow_taxi_exploration\n'
                  'GROUP BY 1\n'
                  'ORDER BY 1\n'
                  '"""))\n'
                  '\n'
                  'display(raw.groupBy("PULocationID").count().orderBy(F.desc("count")).limit(10))\n'
                  '\n'
                  'display(spark.sql("""\n'
                  'WITH durations AS (\n'
                  '  SELECT timestampdiff(MICROSECOND, tpep_pickup_datetime, tpep_dropoff_datetime)\n'
                  '         / 60000000.0 AS menit\n'
                  '  FROM yellow_taxi_exploration\n'
                  ')\n'
                  'SELECT COUNT(menit) AS durasi_tersedia, MIN(menit) AS min_menit,\n'
                  '       MAX(menit) AS max_menit, AVG(menit) AS avg_menit\n'
                  'FROM durations\n'
                  '"""))\n',
 '02_bronze': '# Databricks notebook source\n'
              '# MAGIC %run ./00_config\n'
              '\n'
              '# COMMAND ----------\n'
              'month = batch_month()\n'
              'namespace, landing = settings()\n'
              'mode = widget("source_mode", "https")\n'
              'filename = f"yellow_tripdata_{month}.parquet"\n'
              'source_url = widget("source_url", "") or f"{TLC_ROOT}/trip-data/{filename}"\n'
              'bronze = f"{namespace}.yellow_taxi_bronze"\n'
              '\n'
              'spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")\n'
              'spark.sql(f"CREATE VOLUME IF NOT EXISTS {namespace}.tlc_landing")\n'
              'stage_source(ZONE_URL, landing / "taxi_zone_lookup.csv", valid_zone_csv, mode)\n'
              '\n'
              'if spark.catalog.tableExists(bronze):\n'
              '    previous = '
              'spark.table(bronze).where(month_filter(month)).groupBy("source_url").count().collect()\n'
              '    if previous:\n'
              '        if len(previous) != 1 or previous[0].source_url != source_url:\n'
              '            raise ValueError(f"Batch {month} sudah memakai URL sumber berbeda.")\n'
              '        publish_metrics(layer="bronze", batch_month=month, status="skipped", '
              'rows=previous[0]["count"], written=0)\n'
              '\n'
              'source_path = landing / filename\n'
              'method = stage_source(source_url, source_path, has_parquet_footer, mode)\n'
              'source = spark.read.parquet(str(source_path))\n'
              'missing = REQUIRED - {name.lower() for name in source.columns}\n'
              'if missing:\n'
              '    raise ValueError(f"Kolom sumber hilang: {sorted(missing)}")\n'
              'row_count = source.count()\n'
              'if not row_count:\n'
              '    raise ValueError(f"File {filename} tidak memiliki baris.")\n'
              '\n'
              'raw = source.withColumns({\n'
              '    "batch_month": F.lit(month),\n'
              '    "source_file": F.lit(filename),\n'
              '    "source_url": F.lit(source_url),\n'
              '    "ingested_at": F.current_timestamp(),\n'
              '})\n'
              'raw.write.format("delta").mode("append").saveAsTable(bronze)\n'
              'publish_metrics(layer="bronze", batch_month=month, status="inserted", rows=row_count,\n'
              '                written=row_count, method=method, source_file=filename, '
              'bytes=source_path.stat().st_size)\n',
 '03_silver': '# Databricks notebook source\n'
              '# MAGIC %run ./00_config\n'
              '\n'
              '# COMMAND ----------\n'
              'month = batch_month()\n'
              'namespace, landing = settings()\n'
              'bronze = spark.table(f"{namespace}.yellow_taxi_bronze").where(month_filter(month))\n'
              'raw_columns = [name for name in bronze.columns if name not in METADATA]\n'
              'input_rows = bronze.count()\n'
              '\n'
              'unique = bronze.dropDuplicates(raw_columns).withColumns({\n'
              '    "pickup_at": F.expr("TRY_CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ)"),\n'
              '    "dropoff_at": F.expr("TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP_NTZ)"),\n'
              '    "source_zone_id": F.expr("TRY_CAST(PULocationID AS INT)"),\n'
              '    "record_hash": F.xxhash64(*raw_columns),\n'
              '})\n'
              '\n'
              'pickup_in_month = F.date_format("pickup_at", "yyyy-MM") == month\n'
              'duration = F.expr("timestampdiff(MICROSECOND, pickup_at, dropoff_at)") / F.lit(60_000_000.0)\n'
              'prepared = unique.withColumn("raw_duration_minutes", duration)\n'
              'valid_duration = (F.col("raw_duration_minutes") > 0) & (F.col("raw_duration_minutes") <= '
              'MAX_MINUTES)\n'
              '\n'
              'quality = prepared.agg(\n'
              '    F.count("*").alias("distinct_rows"),\n'
              '    F.count(F.when(F.col("pickup_at").isNull(), 1)).alias("null_pickup"),\n'
              '    F.count(F.when(F.col("pickup_at").isNotNull() & ~pickup_in_month, '
              '1)).alias("outside_batch"),\n'
              '    F.count(F.when(pickup_in_month, 1)).alias("valid_pickup"),\n'
              '    F.count(F.when(pickup_in_month & valid_duration, 1)).alias("valid_duration"),\n'
              ').first().asDict()\n'
              '\n'
              'zones = spark.read.option("header", True).csv(str(landing / "taxi_zone_lookup.csv")).select(\n'
              '    F.col("LocationID").cast("int").alias("source_zone_id"), "Zone", "Borough"\n'
              ')\n'
              'joined = prepared.where(pickup_in_month).join(F.broadcast(zones), "source_zone_id", "left")\n'
              'silver = joined.select(\n'
              '    "batch_month", "record_hash", "pickup_at", "dropoff_at",\n'
              '    F.when(F.col("Zone").isNotNull(), '
              'F.col("source_zone_id")).otherwise(0).alias("pickup_zone_id"),\n'
              '    F.coalesce("Zone", F.lit("Tidak diketahui")).alias("pickup_zone"),\n'
              '    F.coalesce("Borough", F.lit("Tidak diketahui")).alias("borough"),\n'
              '    F.when(valid_duration, F.col("raw_duration_minutes")).alias("duration_minutes"),\n'
              ')\n'
              '\n'
              'table = f"{namespace}.yellow_taxi_silver"\n'
              'write_month(silver, table, month, quality["valid_pickup"])\n'
              'stored = spark.table(table).where(month_filter(month)).count()\n'
              'if stored != quality["valid_pickup"]:\n'
              '    raise ValueError("Jumlah baris Silver berbeda dari jumlah pickup valid.")\n'
              '\n'
              'publish_metrics(layer="silver", batch_month=month, input_rows=input_rows, written=stored,\n'
              '                duplicates=input_rows - quality["distinct_rows"], '
              'null_pickup=quality["null_pickup"],\n'
              '                outside_batch=quality["outside_batch"], '
              'valid_duration=quality["valid_duration"],\n'
              '                excluded_durations=stored - quality["valid_duration"])\n',
 '04_gold': '# Databricks notebook source\n'
            '# MAGIC %run ./00_config\n'
            '\n'
            '# COMMAND ----------\n'
            'month = batch_month()\n'
            'namespace, _ = settings()\n'
            'silver = spark.table(f"{namespace}.yellow_taxi_silver").where(month_filter(month))\n'
            'gold = (\n'
            '    silver.withColumn("pickup_hour", F.date_trunc("hour", "pickup_at").cast("timestamp_ntz"))\n'
            '    .groupBy("batch_month", "pickup_hour", "pickup_zone_id", "pickup_zone", "borough")\n'
            '    .agg(\n'
            '        F.count("*").alias("jumlah_penjemputan"),\n'
            '        F.count("duration_minutes").alias("jumlah_durasi_valid"),\n'
            '        F.coalesce(F.sum("duration_minutes"), F.lit(0.0)).alias("total_menit_durasi_valid"),\n'
            '    )\n'
            '    .withColumn("pickup_date", F.to_date("pickup_hour"))\n'
            ')\n'
            'table = f"{namespace}.yellow_taxi_gold_hourly_zone"\n'
            'summary = gold.agg(F.count("*").alias("rows"), '
            'F.sum("jumlah_penjemputan").alias("pickups")).first()\n'
            'input_rows = silver.count()\n'
            'if summary.pickups != input_rows:\n'
            '    raise ValueError(f"Jumlah pickup Gold ({summary.pickups}) berbeda dari Silver '
            '({input_rows}).")\n'
            '\n'
            'write_month(gold, table, month, summary.rows)\n'
            'stored = spark.table(table).where(month_filter(month))\n'
            'grain_columns = ("pickup_hour", "pickup_zone_id")\n'
            'if stored.groupBy(*grain_columns).count().where(F.col("count") > 1).limit(1).count():\n'
            '    raise ValueError("Gold memiliki kunci pickup_hour dan pickup_zone_id ganda.")\n'
            '\n'
            'publish_metrics(layer="gold", batch_month=month, written=summary.rows, pickups=summary.pickups)\n',
 '05_model_durasi': '# Databricks notebook source\n'
                    '# MAGIC %run ./00_config\n'
                    '\n'
                    '# COMMAND ----------\n'
                    'from inspect import signature\n'
                    '\n'
                    'import cloudpickle\n'
                    'import mlflow\n'
                    'import mlflow.sklearn\n'
                    'import numpy as np\n'
                    'import pandas as pd\n'
                    'import scipy\n'
                    'import sklearn\n'
                    'from mlflow.models import infer_signature\n'
                    'from packaging.version import Version\n'
                    'from sklearn.dummy import DummyRegressor\n'
                    'from sklearn.linear_model import Ridge\n'
                    'from sklearn.metrics import mean_absolute_error, mean_squared_error\n'
                    'from sklearn.pipeline import make_pipeline\n'
                    'from sklearn.preprocessing import OneHotEncoder\n'
                    '\n'
                    'namespace, _ = settings()\n'
                    'mlflow.set_registry_uri("databricks-uc")\n'
                    'mlflow.set_tracking_uri("databricks")\n'
                    'experiment_path = widget("experiment_path", "")\n'
                    'if experiment_path:\n'
                    '    mlflow.set_experiment(experiment_path)\n'
                    'sample_limit = int(widget("model_rows_per_split", "200000"))\n'
                    'if not 1 <= sample_limit <= 500_000:\n'
                    '    raise ValueError("model_rows_per_split harus antara 1 dan 500000.")\n'
                    '\n'
                    'features = ["pickup_zone_id", "pickup_hour_of_day", "pickup_day_of_week"]\n'
                    'target = "duration_minutes"\n'
                    'seed = 2025\n'
                    'trips = '
                    'spark.table(f"{namespace}.yellow_taxi_silver").where(F.col(target).isNotNull()).select(\n'
                    '    "batch_month", "pickup_at", features[0],\n'
                    '    F.hour("pickup_at").alias(features[1]), F.dayofweek("pickup_at").alias(features[2]),\n'
                    '    target, "record_hash",\n'
                    ')\n'
                    '\n'
                    '\n'
                    'def collect_sample(condition):\n'
                    '    sample = (\n'
                    '        trips.where(condition)\n'
                    '        .orderBy(F.xxhash64("record_hash", F.lit(seed)), "record_hash", "pickup_at", '
                    '*features, target)\n'
                    '        .limit(sample_limit)\n'
                    '        .select(*features, target)\n'
                    '        .toPandas()\n'
                    '    )\n'
                    '    if sample.empty:\n'
                    '        raise ValueError("Split model kosong. Jalankan pipeline untuk Januari dan '
                    'Februari 2025.")\n'
                    '    return sample.astype({column: "int64" for column in features})\n'
                    '\n'
                    '\n'
                    'jan = month_filter("2025-01")\n'
                    'day = F.dayofmonth("pickup_at")\n'
                    'train = collect_sample(jan & (day <= 24))\n'
                    'validation = collect_sample(jan & (day >= 25))\n'
                    'full_jan = collect_sample(jan)\n'
                    'test = collect_sample(month_filter("2025-02"))\n'
                    '\n'
                    'models = {\n'
                    '    "median": lambda: DummyRegressor(strategy="median"),\n'
                    '    "ridge": lambda: make_pipeline(OneHotEncoder(handle_unknown="ignore"), '
                    'Ridge(alpha=1.0, solver="lsqr")),\n'
                    '}\n'
                    '\n'
                    '\n'
                    'def evaluate(model, frame):\n'
                    '    prediction = model.predict(frame[features])\n'
                    '    return {\n'
                    '        "mae": float(mean_absolute_error(frame[target], prediction)),\n'
                    '        "rmse": float(np.sqrt(mean_squared_error(frame[target], prediction))),\n'
                    '    }\n'
                    '\n'
                    '\n'
                    'validation_scores = {}\n'
                    'for name, build in models.items():\n'
                    '    candidate = build().fit(train[features], train[target])\n'
                    '    validation_scores[name] = evaluate(candidate, validation)\n'
                    'winner = min(validation_scores, key=lambda name: validation_scores[name]["mae"])\n'
                    '\n'
                    '# COMMAND ----------\n'
                    'model_requirements = [\n'
                    '    f"mlflow=={Version(mlflow.__version__).public}",\n'
                    '    f"cloudpickle=={cloudpickle.__version__}",\n'
                    '    f"numpy=={np.__version__}",\n'
                    '    f"pandas=={pd.__version__}",\n'
                    '    f"scipy=={scipy.__version__}",\n'
                    '    f"scikit-learn=={sklearn.__version__}",\n'
                    ']\n'
                    'model_log_options = {\n'
                    '    ("name" if "name" in signature(mlflow.sklearn.log_model).parameters else '
                    '"artifact_path"): "model",\n'
                    '    "pip_requirements": model_requirements,\n'
                    '    "serialization_format": mlflow.sklearn.SERIALIZATION_FORMAT_CLOUDPICKLE,\n'
                    '}\n'
                    '\n'
                    'rows = []\n'
                    'selected_uri = None\n'
                    'for name, build in models.items():\n'
                    '    model = build().fit(full_jan[features], full_jan[target])\n'
                    '    splits = {"train": evaluate(model, full_jan), "validation": validation_scores[name], '
                    '"test": evaluate(model, test)}\n'
                    '    example = full_jan[features].head(5)\n'
                    '    with mlflow.start_run(run_name=f"nyc_yellow_taxi_{name}") as run:\n'
                    '        mlflow.log_params({\n'
                    '            "model": name, "features": ",".join(features), "max_duration_minutes": '
                    'MAX_MINUTES,\n'
                    '            "sampling_seed": seed, "sample_limit": sample_limit, "fit_rows": len(train),\n'
                    '            "validation_rows": len(validation), "refit_rows": len(full_jan), "test_rows": '
                    'len(test),\n'
                    '            "validation_period": "2025-01-25/2025-01-31", "test_period": "2025-02",\n'
                    '            **({"alpha": 1.0, "solver": "lsqr"} if name == "ridge" else {"strategy": '
                    '"median"}),\n'
                    '        })\n'
                    '        mlflow.log_metrics({f"{split}_{metric}": value for split, scores in '
                    'splits.items() for metric, value in scores.items()})\n'
                    '        mlflow.set_tag("selected_by_validation_mae", str(name == winner).lower())\n'
                    '        logged = mlflow.sklearn.log_model(\n'
                    '            model,\n'
                    '            signature=infer_signature(example, model.predict(example)),\n'
                    '            input_example=example,\n'
                    '            **model_log_options,\n'
                    '        )\n'
                    '        if name == winner:\n'
                    '            selected_uri = logged.model_uri\n'
                    '        rows.append((name, name == winner, run.info.run_id, splits["validation"]["mae"],\n'
                    '                     splits["test"]["mae"], splits["test"]["rmse"]))\n'
                    '\n'
                    'display(spark.createDataFrame(rows, "model STRING, selected BOOLEAN, run_id STRING, '
                    'validation_mae DOUBLE, test_mae DOUBLE, test_rmse DOUBLE"))\n'
                    'print(f"Model dipilih dari MAE validasi: {winner}. URI MLflow: {selected_uri}")\n'
                    '\n'
                    '# COMMAND ----------\n'
                    'loaded = mlflow.pyfunc.load_model(selected_uri)\n'
                    'examples = test.head(5).copy()\n'
                    'examples["predicted_minutes"] = loaded.predict(examples[features])\n'
                    'examples["absolute_error_minutes"] = (examples[target] - '
                    'examples["predicted_minutes"]).abs()\n'
                    'display(examples)\n'
                    'publish_metrics(\n'
                    '    model=winner, model_uri=selected_uri,\n'
                    '    evaluations=[dict(zip(("model", "selected", "run_id", "validation_mae", "test_mae", '
                    '"test_rmse"), row)) for row in rows],\n'
                    '    prediction_examples=examples.to_dict(orient="records"), features=features,\n'
                    ')\n',
 '06_validasi_pipeline': '# Databricks notebook source\n'
                         '# MAGIC %run ./00_config\n'
                         '\n'
                         '# COMMAND ----------\n'
                         'namespace, _ = settings()\n'
                         'bronze = spark.table(f"{namespace}.yellow_taxi_bronze")\n'
                         'silver = spark.table(f"{namespace}.yellow_taxi_silver")\n'
                         'gold = spark.table(f"{namespace}.yellow_taxi_gold_hourly_zone")\n'
                         '\n'
                         'totals = (\n'
                         '    bronze.groupBy("batch_month").agg(F.count("*").alias("bronze_rows"))\n'
                         '    .join(silver.groupBy("batch_month").agg(F.count("*").alias("silver_rows")), '
                         '"batch_month", "full")\n'
                         '    .join(gold.groupBy("batch_month").agg(\n'
                         '        F.count("*").alias("gold_rows"),\n'
                         '        F.sum("jumlah_penjemputan").alias("gold_pickups"),\n'
                         '        F.sum("jumlah_durasi_valid").alias("gold_valid_durations"),\n'
                         '        F.round(F.sum("total_menit_durasi_valid"), '
                         '2).alias("gold_total_duration_minutes"),\n'
                         '    ), "batch_month", "full")\n'
                         '    .orderBy("batch_month")\n'
                         ')\n'
                         'display(totals)\n'
                         'for batch in totals.collect():\n'
                         '    if not (batch.bronze_rows >= batch.silver_rows == batch.gold_pickups):\n'
                         '        raise AssertionError(f"Rekonsiliasi gagal untuk {batch.batch_month}: '
                         '{batch.asDict()}")\n'
                         '\n'
                         'duplicate_keys = gold.groupBy("pickup_hour", '
                         '"pickup_zone_id").count().where(F.col("count") > 1)\n'
                         'if duplicate_keys.limit(1).count():\n'
                         '    raise AssertionError("Grain Gold tidak unik.")\n'
                         '\n'
                         'display(gold.orderBy("pickup_hour", "pickup_zone_id").limit(20))\n'
                         'display(bronze.select("batch_month", "source_url", "source_file", '
                         '"ingested_at").distinct().orderBy("batch_month"))\n'}

def gold_query(namespace):
    return f"""SELECT pickup_date, pickup_zone, borough,
       hour(pickup_hour) AS jam,
       element_at(array('7 Min','1 Sen','2 Sel','3 Rab','4 Kam','5 Jum','6 Sab'),
                  dayofweek(pickup_date)) AS hari,
       jumlah_penjemputan, jumlah_durasi_valid, total_menit_durasi_valid
FROM {namespace}.yellow_taxi_gold_hourly_zone"""


def dashboard_spec(namespace):
    dataset = "gold_taxi"
    pickups = "SUM(`jumlah_penjemputan`)"
    duration = "TRY_DIVIDE(SUM(`total_menit_durasi_valid`), SUM(`jumlah_durasi_valid`))"

    def place(widget, bounds):
        return {"widget": widget, "position": dict(zip(("x", "y", "width", "height"), bounds))}

    def query(fields):
        return [{"name": "main_query", "query": {
            "datasetName": dataset,
            "fields": [{"name": name, "expression": expression} for name, expression in fields.items()],
            "disaggregated": False,
        }}]

    def widget(name, title, kind, version, fields, encodings, bounds):
        return place({
            "name": name, "queries": query(fields),
            "spec": {"version": version, "widgetType": kind, "encodings": encodings,
                     "frame": {"showTitle": True, "title": title}},
        }, bounds)

    def axis(field, label, scale):
        return {"fieldName": field, "displayName": label, "scale": {"type": scale}}

    def filter_by(field, title, kind, bounds):
        return widget(f"filter_{field}", title, kind, 2, {field: f"`{field}`"},
                      {"fields": [{"fieldName": field, "queryName": "main_query"}]}, bounds)

    def counter(name, title, expression, decimals, bounds):
        return widget(name, title, "counter", 2, {"nilai": expression}, {"value": {
            "fieldName": "nilai", "displayName": title, "format": {
                "type": "number-plain", "decimalPlaces": {"type": "max", "places": decimals},
            },
        }}, bounds)

    def note(name, text, bounds):
        return place({"name": name, "multilineTextboxSpec": {"lines": text.splitlines(keepends=True)}}, bounds)

    busiest_zones = widget("zona", "Penjemputan menurut zona", "bar", 3,
                          {"zona": "`pickup_zone`", "pickup": pickups},
                          {"x": axis("pickup", "Penjemputan", "quantitative"),
                           "y": {**axis("zona", "Zona pickup", "categorical"),
                                 "scale": {"type": "categorical", "sort": {"by": "value"}}}}, (7, 7, 5, 6))
    busiest_zones["widget"]["queries"][0]["query"]["orders"] = [{"direction": "DESC", "expression": pickups}]
    layout = [
        note("judul", "# Operasi NYC Yellow Taxi\nJanuari–Februari 2025 · Waktu lokal New York", (0, 0, 12, 2)),
        filter_by("pickup_date", "Periode penjemputan", "filter-date-range-picker", (0, 2, 6, 2)),
        filter_by("borough", "Borough", "filter-multi-select", (6, 2, 6, 2)),
        counter("total_pickup", "Total penjemputan", pickups, 0, (0, 4, 6, 3)),
        counter("durasi_rata_rata", "Rata-rata durasi valid (menit)", duration, 1, (6, 4, 6, 3)),
        widget("tren", "Penjemputan per tanggal", "line", 3,
               {"tanggal": "`pickup_date`", "pickup": pickups},
               {"x": axis("tanggal", "Tanggal", "temporal"), "y": axis("pickup", "Penjemputan", "quantitative")},
               (0, 7, 7, 6)),
        busiest_zones,
        widget("jam_ramai", "Jam ramai menurut hari", "heatmap", 3,
               {"jam": "`jam`", "hari": "`hari`", "pickup": pickups},
               {"x": axis("jam", "Jam pickup", "categorical"), "y": axis("hari", "Hari", "categorical"),
                "color": axis("pickup", "Penjemputan", "quantitative")}, (0, 13, 6, 6)),
        widget("jam_durasi", "Rata-rata durasi menurut jam", "line", 3,
               {"jam": "`jam`", "menit": duration, "borough": "`borough`"},
               {"x": axis("jam", "Jam pickup", "quantitative"), "y": axis("menit", "Menit", "quantitative"),
                "color": axis("borough", "Borough", "categorical")}, (6, 13, 6, 6)),
        note("catatan", "Volume mencakup pickup valid. Perhitungan durasi memakai 0 < menit ≤ 180. Sumber NYC TLC.", (0, 19, 12, 2)),
    ]
    return {"datasets": [{"name": dataset, "displayName": "Gold · Pickup per jam dan zona",
                          "queryLines": gold_query(namespace).splitlines(keepends=True)}],
            "pages": [{"name": "operasi", "displayName": "Operasi Yellow Taxi",
                       "pageType": "PAGE_TYPE_CANVAS", "layout": layout}]}

# COMMAND ----------
dbutils.widgets.text("catalog", "")
dbutils.widgets.dropdown("source_mode", "https", ["https", "volume"])
dbutils.widgets.dropdown("run_demo", "true", ["true", "false", "model"])
dbutils.widgets.dropdown("try_serving", "true", ["true", "false"])
catalog = dbutils.widgets.get("catalog").strip() or spark.sql("SELECT current_catalog()").first()[0]
if catalog.lower() in {"hive_metastore", "system", "samples"}:
    raise ValueError("Isi widget catalog dengan nama Unity Catalog yang mengizinkan penulisan.")
quoted_catalog = "`" + catalog.replace("`", "``") + "`"
namespace = f"{quoted_catalog}.nyc_yellow_taxi"
mode = dbutils.widgets.get("source_mode")
run_demo = dbutils.widgets.get("run_demo")

client = WorkspaceClient()
current_user = client.current_user.me().user_name
if not current_user:
    raise RuntimeError("SDK Databricks tidak dapat menentukan user workspace aktif.")

if run_demo == "model":
    if not spark.catalog.tableExists(f"{namespace}.yellow_taxi_silver"):
        raise ValueError("Tabel Silver belum tersedia. Pilih catalog tempat Job ETL sebelumnya dijalankan.")
else:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {namespace}.tlc_landing")
    print(f"Volume sumber: /Volumes/{catalog}/nyc_yellow_taxi/tlc_landing")

project_dir = f"/Users/{current_user}/Tugas 2 NYC Yellow Taxi"
if run_demo != "model":
    client.workspace.mkdirs(path=project_dir)

notebook_names = ("05_model_durasi",) if run_demo == "model" else NOTEBOOKS
for name in notebook_names:
    client.workspace.import_(
        path=f"{project_dir}/{name}",
        format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,
        content=base64.b64encode(NOTEBOOKS[name].encode()).decode("ascii"),
        overwrite=True,
    )

host = client.config.host.rstrip("/")
project_object_id = client.workspace.get_status(path=project_dir).object_id
project_link = f"{host}/browse/folders/{project_object_id}"
print(f"Notebook: {project_link}")

# COMMAND ----------
def configure_etl_job():
    task_notebooks = {"bronze": "02_bronze", "silver": "03_silver", "gold": "04_gold"}
    task_specs = []
    previous = None
    for name, notebook in task_notebooks.items():
        task_specs.append(jobs.Task(
            task_key=name,
            notebook_task=jobs.NotebookTask(notebook_path=f"{project_dir}/{notebook}", source=jobs.Source.WORKSPACE),
            depends_on=[jobs.TaskDependency(task_key=previous)] if previous else None,
            max_retries=1, timeout_seconds=1800,
        ))
        previous = name

    job_name = "Tugas 2 NYC Yellow Taxi · ETL"
    job_params = {"catalog": catalog, "schema": "nyc_yellow_taxi", "batch_month": "2025-01",
                  "source_mode": mode, "source_url": ""}
    parameters = [jobs.JobParameterDefinition(name=key, default=value) for key, value in job_params.items()]
    matches = [job for job in client.jobs.list(name=job_name) if job.settings and job.settings.name == job_name]
    if len(matches) > 1:
        raise RuntimeError("Ditemukan lebih dari satu Job dengan nama proyek yang sama.")
    if matches:
        job_id = matches[0].job_id
        existing_tasks = client.jobs.get(job_id=job_id).settings.tasks or []
        if any(not task.notebook_task or not task.notebook_task.notebook_path.startswith(f"{project_dir}/") for task in existing_tasks):
            raise RuntimeError("Job bernama sama menggunakan notebook di luar folder proyek.")
        client.jobs.update(job_id=job_id, new_settings=jobs.JobSettings(
            tasks=task_specs, parameters=parameters, max_concurrent_runs=1,
        ))
        return job_id
    return client.jobs.create(name=job_name, tasks=task_specs, parameters=parameters, max_concurrent_runs=1).job_id


def get_result(waiter, timeout_minutes=30):
    try:
        return waiter.result(timeout=timedelta(minutes=timeout_minutes))
    except OperationFailed:
        return client.jobs.get_run(run_id=waiter.response.run_id)


def collect_run(run):
    outcome = {
        "run_id": run.run_id, "run_url": run.run_page_url,
        "state": run.state.result_state.value if run.state and run.state.result_state else None,
        "tasks": {},
    }
    for task in run.tasks or []:
        state = task.state.result_state.value if task.state and task.state.result_state else None
        detail = {"state": state, "duration_seconds": (task.execution_duration or 0) / 1000, "run_id": task.run_id}
        if state == "SUCCESS":
            output = client.jobs.get_run_output(run_id=task.run_id).notebook_output
            detail["metrics"] = json.loads(output.result) if output and output.result else {}
        elif state == "FAILED":
            detail["error"] = client.jobs.get_run_output(run_id=task.run_id).error
        outcome["tasks"][task.task_key] = detail
    print(json.dumps(outcome, ensure_ascii=False, default=str, indent=2))
    return outcome


def run_batch(month):
    waiter = client.jobs.run_now(job_id=job_id, job_parameters={"batch_month": month, "source_mode": mode})
    print(f"{month}: {job_link}/runs/{waiter.response.run_id}")
    return collect_run(get_result(waiter))


def snapshot_month():
    month = "batch_month = '2025-01'"
    return spark.sql(f"""
        SELECT (SELECT COUNT(*) FROM {namespace}.yellow_taxi_bronze WHERE {month}) AS bronze_rows,
               (SELECT COUNT(*) FROM {namespace}.yellow_taxi_silver WHERE {month}) AS silver_rows,
               COUNT(*) AS gold_rows, SUM(jumlah_penjemputan) AS gold_pickups,
               SUM(jumlah_durasi_valid) AS gold_valid_duration,
               ROUND(SUM(total_menit_durasi_valid), 3) AS gold_duration_minutes
        FROM {namespace}.yellow_taxi_gold_hourly_zone WHERE {month}
    """).first().asDict()


def verify_pipeline():
    runs = []
    snapshots = []
    for month in ("2025-01", "2025-01", "2025-02"):
        run = run_batch(month)
        if run["state"] != "SUCCESS":
            raise RuntimeError(f"Job gagal pada {month}. Detail task dan tautan run ditampilkan di atas.")
        runs.append(run)
        snapshots.append(snapshot_month())
    assert runs[1]["tasks"]["bronze"]["metrics"]["written"] == 0, "Bronze harus melewati Januari yang sudah ada."
    assert snapshots[0] == snapshots[1] == snapshots[2], "Data Januari berubah pada rerun atau penambahan Februari."
    negative = run_batch("2025-13")
    assert negative["state"] == "FAILED" and negative["tasks"]["bronze"]["state"] == "FAILED"
    assert all(negative["tasks"].get(name, {}).get("state") != "SUCCESS" for name in ("silver", "gold"))
    print(f"Snapshot Januari tetap sama: {snapshots[0]}")
    return runs + [negative]


def train_model():
    parameters = {"catalog": catalog, "schema": "nyc_yellow_taxi",
                  "experiment_path": f"{project_dir}/Regresi durasi"}
    model_task = jobs.SubmitTask(task_key="model", notebook_task=jobs.NotebookTask(
        notebook_path=f"{project_dir}/05_model_durasi", source=jobs.Source.WORKSPACE,
        base_parameters=parameters,
    ), timeout_seconds=1800)
    run = collect_run(get_result(client.jobs.submit(run_name="NYC Yellow Taxi · Regresi durasi", tasks=[model_task])))
    if run["state"] != "SUCCESS":
        raise RuntimeError("Pelatihan model gagal. Periksa output run.")
    return run["tasks"]["model"]["metrics"]


def publish_dashboard():
    spark.sql(gold_query(namespace)).limit(1).collect()
    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError("SQL Warehouse belum tersedia untuk dashboard.")
    warehouse = next((item for item in warehouses if item.enable_serverless_compute), warehouses[0])
    title = "Operasi NYC Yellow Taxi"
    existing = next((item for item in client.lakeview.list()
                     if item.display_name == title and item.parent_path == project_dir), None)
    settings = dashboards.Dashboard(display_name=title, warehouse_id=warehouse.id,
                                     serialized_dashboard=json.dumps(dashboard_spec(namespace), ensure_ascii=False))
    if existing:
        dashboard = client.lakeview.update(dashboard_id=existing.dashboard_id, dashboard=settings)
    else:
        settings.parent_path = project_dir
        dashboard = client.lakeview.create(dashboard=settings)
    client.lakeview.publish(dashboard_id=dashboard.dashboard_id, warehouse_id=warehouse.id, embed_credentials=False)
    url = f"{host}/dashboardsv3/{dashboard.dashboard_id}/published"
    print(f"Dashboard: {url}")
    return url

def test_model_serving(metrics):
    import mlflow
    from mlflow.exceptions import MlflowException

    name = "tugas2-nyc-yellow-taxi-duration"
    model_name = f"{catalog}.nyc_yellow_taxi.durasi_penjemputan"
    try:
        try:
            endpoint = client.serving_endpoints.get(name=name)
        except ResourceDoesNotExist:
            endpoint = None
        if endpoint and any(entity.entity_name != model_name for entity in (endpoint.config.served_entities or [])):
            raise RuntimeError("Endpoint bernama sama sudah digunakan oleh model lain.")

        mlflow.set_registry_uri("databricks-uc")
        model_version = mlflow.register_model(metrics["model_uri"], model_name)
        entity = serving.ServedEntityInput(entity_name=model_name, entity_version=str(model_version.version),
                                           workload_size="Small", scale_to_zero_enabled=True)
        if endpoint:
            waiter = client.serving_endpoints.update_config(name=name, served_entities=[entity])
        else:
            waiter = client.serving_endpoints.create(name=name, config=serving.EndpointCoreConfigInput(
                name=name, served_entities=[entity],
            ))
        print(f"Model Serving: {host}/ml/endpoints/{name}")
        waiter.result(timeout=timedelta(minutes=15))
        records = [{feature: row[feature] for feature in metrics["features"]} for row in metrics["prediction_examples"]]
        response = client.serving_endpoints.query(name=name, dataframe_records=records)
        print(json.dumps({"serving_status": "SUCCESS", "inputs": records, "outputs": response.predictions},
                         ensure_ascii=False, default=str, indent=2))
    except (DatabricksError, MlflowException, RuntimeError) as error:
        print(f"Model Serving belum terverifikasi: {error}. Gunakan bukti prediksi batch dari run MLflow.")


# COMMAND ----------
if run_demo != "model":
    job_id = configure_etl_job()
    job_link = f"{host}/jobs/{job_id}"
    print(f"Job: {job_link}")

if run_demo == "true":
    demo_runs = verify_pipeline()
    try:
        dashboard_url = publish_dashboard()
    except (RuntimeError, DatabricksError) as error:
        print(f"Dashboard belum terbit: {error}")
if run_demo in ("true", "model"):
    model_result = train_model()
    if dbutils.widgets.get("try_serving") == "true":
        test_model_serving(model_result)
    print("Pengujian MLflow selesai. Hasil prediksi batch tercantum pada run model.")
else:
    print("Setup selesai. Pilih run_demo=true untuk demo, atau jalankan Job manual lewat tautan di atas.")
