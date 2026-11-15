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

# NOTEBOOK_PAYLOAD

# DASHBOARD_SOURCE

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
