"""Shared settings of the 3 DAGs. Every task runs one job inside a stack container with `docker exec`
(the Airflow image has the docker CLI and the docker socket), so Airflow only orchestrates - it does not need Spark,
Sqoop or the Python libraries itself."""
from datetime import timedelta

SPARK = "docker exec rh-spark /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g"
TOOLS = "docker exec rh-tools"
SQOOP = "docker exec rh-sqoop"

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email": ["ops@ridehail.local"],
    "email_on_failure": True,
    "email_on_retry": False,
}

SKIP = 99            # a job exits with 99 when there is nothing to do (e.g. cloud keys empty) -> task "skipped"
SPARK_POOL = "spark"  # one Spark job at a time: rh-spark has a 2.2 GB memory limit
