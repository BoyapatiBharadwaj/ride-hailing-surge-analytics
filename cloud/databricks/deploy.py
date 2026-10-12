"""Feed and run the Databricks lakehouse (Bronze -> Silver -> Gold with Delta Live Tables).

    python -m cloud.databricks.deploy setup     # schema `ridehail`, Volume `landing`, DLT notebook + pipeline (once)
    python -m cloud.databricks.deploy events    # upload new stream files  /lake/landing/events -> Volume landing/events
    python -m cloud.databricks.deploy history   # upload the nightly export (trips + zones) -> landing/trips, landing/zones
    python -m cloud.databricks.deploy run       # run the DLT pipeline and wait until it has finished
    python -m cloud.databricks.deploy all       # setup + history + events + run
    python -m cloud.databricks.deploy reconcile # Kafka vs Bronze vs Silver vs Gold record counts
    python -m cloud.databricks.deploy reset     # delete the pipeline and the schema (start again with `all`)

Why a Volume as landing zone: Databricks Free Edition cannot read a private S3 bucket, so the files are uploaded
through the Files API into /Volumes/workspace/ridehail/landing/... and Auto Loader reads them from there.
Exit code 99 = nothing to do / Databricks not configured (Airflow shows the task as skipped).
"""
import base64
import os
import shutil
import sys
import time

from cloud.databricks.workspace import CATALOG, SCHEMA, VOLUME, client, query, require, table
from common.config import ARCHIVED_TOPICS, EXPORT, KAFKA_BOOTSTRAP, LANDING, SKIP

PIPELINE = "ridehail-medallion"
NOTEBOOK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dlt_pipeline.py")
DONE = "_UPLOADED"            # marker written into a local folder once its files are in Databricks
KEEP_LOCAL_HOURS = 48         # uploaded stream files are deleted locally after 2 days (Kafka keeps 7 days anyway)


# ------------------------------------------------------------------ setup
def find_pipeline():
    w = client()
    found = [p for p in w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE}'")]
    return found[0].pipeline_id if found else None


def setup() -> str:
    from databricks.sdk.service.catalog import VolumeType
    from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary
    from databricks.sdk.service.workspace import ImportFormat, Language
    w = client()
    try:
        print(f"[dbx] connected to {w.config.host} as {w.current_user.me().user_name}")
    except Exception as exc:  # noqa: BLE001
        print(f"[dbx] cannot log in: {type(exc).__name__}: {str(exc)[:160]}")
        print("[dbx] check DATABRICKS_HOST (https://dbc-xxxx.cloud.databricks.com) and DATABRICKS_TOKEN in .env")
        sys.exit(1)
    for create, what in ((lambda: w.schemas.create(name=SCHEMA, catalog_name=CATALOG), f"schema {CATALOG}.{SCHEMA}"),
                         (lambda: w.volumes.create(catalog_name=CATALOG, schema_name=SCHEMA, name="landing",
                                                   volume_type=VolumeType.MANAGED), f"volume {VOLUME}")):
        try:
            create()
            print(f"[dbx] {what} created")
        except Exception as exc:  # noqa: BLE001
            if "already exists" not in str(exc).lower():
                raise
    for folder in ("events", "trips", "zones"):          # Auto Loader / DLT read these folders
        w.files.create_directory(f"{VOLUME}/{folder}")
    path = f"/Users/{w.current_user.me().user_name}/ridehail/dlt_pipeline"
    w.workspace.mkdirs(os.path.dirname(path))
    w.workspace.import_(path=path, content=base64.b64encode(open(NOTEBOOK, "rb").read()).decode(),
                        format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=True)
    settings = dict(name=PIPELINE, catalog=CATALOG, target=SCHEMA, serverless=True, development=True, continuous=False,
                    libraries=[PipelineLibrary(notebook=NotebookLibrary(path=path))],
                    configuration={"landing_path": VOLUME})
    pid = find_pipeline()
    if pid:
        w.pipelines.update(pipeline_id=pid, **settings)
    else:
        pid = w.pipelines.create(**settings).pipeline_id
    print(f"[dbx] DLT pipeline {PIPELINE} ready ({pid}); notebook {path}")
    return pid


# ------------------------------------------------------------------ uploads
def parquet_files(folder: str):
    return sorted(f for f in os.listdir(folder) if f.endswith(".parquet"))


def upload_folder(local: str, remote: str, prefix: str) -> int:
    w = client()
    n = 0
    for f in parquet_files(local):
        with open(os.path.join(local, f), "rb") as fh:
            w.files.upload(f"{remote}/{prefix}{f}", fh, overwrite=True)
        n += 1
    open(os.path.join(local, DONE), "w").close()
    return n


def upload_events() -> int:
    """Every complete batch folder (_SUCCESS written by Spark) that is not uploaded yet. The remote name keeps Spark's
    unique part-file name, so a file is never replaced by another one with the same name."""
    root = f"{LANDING}/events"
    batches = sorted(os.listdir(root)) if os.path.isdir(root) else []
    files = 0
    for b in batches:
        d = os.path.join(root, b)
        if os.path.exists(os.path.join(d, "_SUCCESS")) and not os.path.exists(os.path.join(d, DONE)):
            files += upload_folder(d, f"{VOLUME}/events", f"{b}-")
    # housekeeping: uploaded folders older than 2 days are not needed locally any more
    old = time.time() - KEEP_LOCAL_HOURS * 3600
    for b in batches:
        d = os.path.join(root, b)
        if os.path.exists(os.path.join(d, DONE)) and os.path.getmtime(os.path.join(d, DONE)) < old:
            shutil.rmtree(d, ignore_errors=True)
    print(f"[dbx] events: {files} new files uploaded to {VOLUME}/events")
    return files


def clear_remote(folder: str) -> None:
    w = client()
    try:
        for f in w.files.list_directory_contents(f"{VOLUME}/{folder}"):
            if not f.is_directory:
                w.files.delete(f.path)
    except Exception as exc:  # noqa: BLE001
        if "not found" not in str(exc).lower() and "does not exist" not in str(exc).lower():
            raise


def upload_history() -> int:
    """The nightly export replaces the trips and zones snapshot in the Volume (only when it changed)."""
    n = 0
    for local, remote in ((f"{EXPORT}/trips_raw", "trips"), (f"{EXPORT}/dim_zone", "zones")):
        if not os.path.isdir(local):
            print(f"[dbx] {remote}: {local} does not exist yet - run the nightly batch DAG first")
            continue
        if os.path.exists(os.path.join(local, DONE)):
            print(f"[dbx] {remote}: already uploaded")
            continue
        clear_remote(remote)
        n += upload_folder(local, f"{VOLUME}/{remote}", "")
        print(f"[dbx] {remote}: snapshot uploaded")
    return n
