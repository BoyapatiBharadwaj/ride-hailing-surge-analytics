"""Nightly batch pipeline, 00:30 UTC - the trip HISTORY.

MySQL --Sqoop--> HDFS --Spark Core--> cleansed trips + peak hours / cancellation reasons
      --> Hive star schema --> export --> AWS:        S3 (Parquet + CSV) -> Glue catalog -> Athena benchmark
                                      --> Databricks: trips + zones snapshot -> DLT pipeline (Bronze -> Silver -> Gold)
                                      --> ML:         train the demand forecast (Linear Regression vs Random Forest)
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from dag_settings import DEFAULT_ARGS, SKIP, SPARK, SPARK_POOL, SQOOP, TOOLS


def task(task_id: str, cmd: str, **kw) -> BashOperator:
    # the trailing space stops Airflow from treating a command ending in .sh as a template file
    if "spark-submit" in cmd:
        kw.setdefault("pool", SPARK_POOL)
    return BashOperator(task_id=task_id, bash_command=cmd + " ", skip_on_exit_code=SKIP, **kw)


with DAG("nightly_batch_pipeline", default_args=DEFAULT_ARGS, schedule="30 0 * * *",
         start_date=datetime(2025, 1, 1), catchup=False, max_active_runs=1, tags=["ridehail", "batch"]) as dag:
    # Sqoop is the import tool; if it fails, the Spark JDBC import writes the same files (backup)
    sqoop = task("sqoop_import_mysql_to_hdfs",
                 f"{SQOOP} bash /scripts/import_tables.sh || "
                 f"{{ echo 'Sqoop failed -> Spark JDBC backup import'; {SPARK} /app/ingestion/sqoop/spark_jdbc_import.py; }}",
                 pool=SPARK_POOL)
    core = task("spark_core_cleanse_analyse", f"{SPARK} /app/batch/spark_core_trips.py")
    hive = task("load_hive_star_schema", f"{SPARK} /app/batch/load_hive.py")
    export = task("export_for_cloud_and_ml", f"{SPARK} /app/batch/export.py")
    aws = task("aws_s3_glue_athena", f"{TOOLS} python -m cloud.aws all")
    dbx = task("databricks_upload_and_run", f"{TOOLS} python -m cloud.databricks.deploy all")
    # trains even when Databricks is not configured (dbx skipped); only the Gold hold-out write needs it
    ml = task("train_demand_forecast", f"{TOOLS} python -m ml.train", trigger_rule="none_failed")

    sqoop >> core >> hive >> export >> [aws, dbx]
    dbx >> ml          # training writes its hold-out predictions into the Databricks Gold schema
