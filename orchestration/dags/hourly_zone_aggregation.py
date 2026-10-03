"""Hourly - the STREAM into the lakehouse.

1. upload the new raw-event files (/lake/landing/events, written every minute by the raw archiver) to Databricks
2. run the DLT pipeline: Bronze -> Silver (quality expectations) -> Gold (zone KPIs, utilisation, surge history)
3. score the demand forecast for the next 30 minutes of every zone -> Gold demand_forecast
Step 1 ends as "skipped" when no new files arrived; steps 2 and 3 are then skipped too.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from dag_settings import DEFAULT_ARGS, SKIP, TOOLS

with DAG("hourly_zone_aggregation", default_args=DEFAULT_ARGS, schedule="@hourly",
         start_date=datetime(2025, 1, 1), catchup=False, max_active_runs=1, tags=["ridehail", "hourly"]) as dag:
    upload = BashOperator(task_id="upload_new_stream_files", skip_on_exit_code=SKIP,
                          bash_command=f"{TOOLS} python -m cloud.databricks.deploy events ")
    medallion = BashOperator(task_id="run_dlt_bronze_silver_gold", skip_on_exit_code=SKIP,
                             bash_command=f"{TOOLS} python -m cloud.databricks.deploy run ")
    forecast = BashOperator(task_id="score_demand_forecast", skip_on_exit_code=SKIP,
                            bash_command=f"{TOOLS} python -m ml.score ")
    upload >> medallion >> forecast
