# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------
from inspect import signature

import cloudpickle
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import scipy
import sklearn
from mlflow.models import infer_signature
from packaging.version import Version
from sklearn.dummy import DummyRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OneHotEncoder

namespace, _ = settings()
mlflow.set_registry_uri("databricks-uc")
mlflow.set_tracking_uri("databricks")
experiment_path = widget("experiment_path", "")
if experiment_path:
    mlflow.set_experiment(experiment_path)
sample_limit = int(widget("model_rows_per_split", "200000"))
if not 1 <= sample_limit <= 500_000:
    raise ValueError("model_rows_per_split harus antara 1 dan 500000.")

features = ["pickup_zone_id", "pickup_hour_of_day", "pickup_day_of_week"]
target = "duration_minutes"
seed = 2025
trips = spark.table(f"{namespace}.yellow_taxi_silver").where(F.col(target).isNotNull()).select(
    "batch_month", "pickup_at", features[0],
    F.hour("pickup_at").alias(features[1]), F.dayofweek("pickup_at").alias(features[2]),
    target, "record_hash",
)


def collect_sample(condition):
    sample = (
        trips.where(condition)
        .orderBy(F.xxhash64("record_hash", F.lit(seed)), "record_hash", "pickup_at", *features, target)
        .limit(sample_limit)
        .select(*features, target)
        .toPandas()
    )
    if sample.empty:
        raise ValueError("Split model kosong. Jalankan pipeline untuk Januari dan Februari 2025.")
    return sample.astype({column: "int64" for column in features})


jan = month_filter("2025-01")
day = F.dayofmonth("pickup_at")
train = collect_sample(jan & (day <= 24))
validation = collect_sample(jan & (day >= 25))
full_jan = collect_sample(jan)
test = collect_sample(month_filter("2025-02"))

models = {
    "median": lambda: DummyRegressor(strategy="median"),
    "ridge": lambda: make_pipeline(OneHotEncoder(handle_unknown="ignore"), Ridge(alpha=1.0, solver="lsqr")),
}


def evaluate(model, frame):
    prediction = model.predict(frame[features])
    return {
        "mae": float(mean_absolute_error(frame[target], prediction)),
        "rmse": float(np.sqrt(mean_squared_error(frame[target], prediction))),
    }


validation_scores = {}
for name, build in models.items():
    candidate = build().fit(train[features], train[target])
    validation_scores[name] = evaluate(candidate, validation)
winner = min(validation_scores, key=lambda name: validation_scores[name]["mae"])

# COMMAND ----------
model_requirements = [
    f"mlflow=={Version(mlflow.__version__).public}",
    f"cloudpickle=={cloudpickle.__version__}",
    f"numpy=={np.__version__}",
    f"pandas=={pd.__version__}",
    f"scipy=={scipy.__version__}",
    f"scikit-learn=={sklearn.__version__}",
]
model_log_options = {
    ("name" if "name" in signature(mlflow.sklearn.log_model).parameters else "artifact_path"): "model",
    "pip_requirements": model_requirements,
    "serialization_format": mlflow.sklearn.SERIALIZATION_FORMAT_CLOUDPICKLE,
}

rows = []
selected_uri = None
for name, build in models.items():
    model = build().fit(full_jan[features], full_jan[target])
    splits = {"train": evaluate(model, full_jan), "validation": validation_scores[name], "test": evaluate(model, test)}
    example = full_jan[features].head(5)
    with mlflow.start_run(run_name=f"nyc_yellow_taxi_{name}") as run:
        mlflow.log_params({
            "model": name, "features": ",".join(features), "max_duration_minutes": MAX_MINUTES,
            "sampling_seed": seed, "sample_limit": sample_limit, "fit_rows": len(train),
            "validation_rows": len(validation), "refit_rows": len(full_jan), "test_rows": len(test),
            "validation_period": "2025-01-25/2025-01-31", "test_period": "2025-02",
            **({"alpha": 1.0, "solver": "lsqr"} if name == "ridge" else {"strategy": "median"}),
        })
        mlflow.log_metrics({f"{split}_{metric}": value for split, scores in splits.items() for metric, value in scores.items()})
        mlflow.set_tag("selected_by_validation_mae", str(name == winner).lower())
        logged = mlflow.sklearn.log_model(
            model,
            signature=infer_signature(example, model.predict(example)),
            input_example=example,
            **model_log_options,
        )
        if name == winner:
            selected_uri = logged.model_uri
        rows.append((name, name == winner, run.info.run_id, splits["validation"]["mae"],
                     splits["test"]["mae"], splits["test"]["rmse"]))

display(spark.createDataFrame(rows, "model STRING, selected BOOLEAN, run_id STRING, validation_mae DOUBLE, test_mae DOUBLE, test_rmse DOUBLE"))
print(f"Model dipilih dari MAE validasi: {winner}. URI MLflow: {selected_uri}")

# COMMAND ----------
loaded = mlflow.pyfunc.load_model(selected_uri)
examples = test.head(5).copy()
examples["predicted_minutes"] = loaded.predict(examples[features])
examples["absolute_error_minutes"] = (examples[target] - examples["predicted_minutes"]).abs()
display(examples)
publish_metrics(
    model=winner, model_uri=selected_uri,
    evaluations=[dict(zip(("model", "selected", "run_id", "validation_mae", "test_mae", "test_rmse"), row)) for row in rows],
    prediction_examples=examples.to_dict(orient="records"), features=features,
)
