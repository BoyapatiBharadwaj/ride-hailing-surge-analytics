"""AWS part of the project: the trip HISTORY in S3, catalogued in Glue, queried with Athena.

    python -m cloud.aws upload      # /lake/export -> s3://<bucket>/curated/fact_trips (Parquet, by city/date) + raw_csv/
    python -m cloud.aws catalog     # Glue database `ridehail`: tables fact_trips (Parquet) and fact_trips_csv
    python -m cloud.aws benchmark   # Athena: 5 queries on CSV vs Parquet -> bytes scanned (KPI: >= 70 % less)
    python -m cloud.aws all         # the three steps above (nightly DAG)

Cost guards: S3 lifecycle (Athena results deleted after 7 days), Athena workgroup `ridehail` that cancels any query
scanning more than 1 GB, and a monthly AWS budget alert (US$5) when BUDGET_ALERT_EMAIL is set.
Exit code 99 = AWS keys not filled in (Airflow shows the task as skipped).
"""
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from common.config import EXPORT, REPORTS, SKIP, TARGET_ATHENA_REDUCTION, aws_configured, env

DB = env("GLUE_DATABASE", "ridehail")
WORKGROUP = env("ATHENA_WORKGROUP", "ridehail")
FACT_COLS = ("trip_id bigint, driver_key int, pickup_zone_key int, dropoff_zone_key int, time_key bigint, "
             "trip_status string, cancel_reason string, distance_km double, duration_min double, fare_amount double, "
             "surge_multiplier double, wait_time_min double")


def require() -> None:
    if not aws_configured():
        print("[aws] AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / S3_BUCKET are empty in .env - skipping this step")
        sys.exit(SKIP)


def client(name: str):
    import boto3
    return boto3.client(name, region_name=env("AWS_REGION", "ap-south-1"), aws_access_key_id=env("AWS_ACCESS_KEY_ID"),
                        aws_secret_access_key=env("AWS_SECRET_ACCESS_KEY"))


def bucket() -> str:
    return env("S3_BUCKET")


# ------------------------------------------------------------------ S3
def ensure_bucket() -> None:
    from botocore.exceptions import ClientError
    s3, b, region = client("s3"), bucket(), env("AWS_REGION", "ap-south-1")
    try:
        s3.head_bucket(Bucket=b)
    except ClientError:
        s3.create_bucket(Bucket=b, **({} if region == "us-east-1" else
                                      {"CreateBucketConfiguration": {"LocationConstraint": region}}))
        print(f"[aws] bucket {b} created")
    s3.put_public_access_block(Bucket=b, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
    s3.put_bucket_lifecycle_configuration(Bucket=b, LifecycleConfiguration={"Rules": [
        {"ID": "expire-athena-results", "Status": "Enabled", "Filter": {"Prefix": "athena-results/"},
         "Expiration": {"Days": 7}},
        {"ID": "abort-multipart", "Status": "Enabled", "Filter": {"Prefix": ""},
         "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 2}}]})


def mirror(local_root: str, prefix: str, suffix: str) -> None:
    """Make s3://bucket/prefix/ an exact copy of the local folder (new files uploaded, old part files deleted),
    so Athena never counts a trip twice."""
    from botocore.exceptions import ClientError
    s3, b = client("s3"), bucket()
    jobs = []
    for root, _, files in os.walk(local_root):
        for f in files:
            if f.endswith(suffix) and not f.startswith((".", "_")):
                path = os.path.join(root, f)
                jobs.append((path, f"{prefix}/{os.path.relpath(path, local_root)}".replace(os.sep, "/")))
    if not jobs:
        raise SystemExit(f"[aws] nothing to upload in {local_root} - run the nightly batch DAG first")

    def push(job) -> int:
        path, key = job
        try:
            if s3.head_object(Bucket=b, Key=key)["ContentLength"] == os.path.getsize(path):
                return 0
        except ClientError:
            pass
        s3.upload_file(path, b, key)
        return 1

    with ThreadPoolExecutor(8) as ex:
        uploaded = sum(ex.map(push, jobs))
    keep, stale = {k for _, k in jobs}, []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{prefix}/"):
        stale += [o["Key"] for o in page.get("Contents", []) if o["Key"] not in keep]
    for i in range(0, len(stale), 1000):
        s3.delete_objects(Bucket=b, Delete={"Objects": [{"Key": k} for k in stale[i:i + 1000]], "Quiet": True})
    print(f"[aws] s3://{b}/{prefix}: {len(jobs)} files, {uploaded} uploaded, {len(stale)} old ones deleted")


def upload() -> None:
    ensure_bucket()
    mirror(f"{EXPORT}/fact_trips", "curated/fact_trips", ".parquet")
    mirror(f"{EXPORT}/raw_csv/fact_trips", "raw_csv/fact_trips", ".csv")


# ------------------------------------------------------------------ cost guards
def ensure_workgroup() -> str:
    """Athena workgroup with a per-query scan limit (1 GB). Falls back to `primary` if the IAM user may not create one."""
    from botocore.exceptions import ClientError
    global WORKGROUP
    cfg = {"ResultConfiguration": {"OutputLocation": f"s3://{bucket()}/athena-results/"},
           "EnforceWorkGroupConfiguration": True, "PublishCloudWatchMetricsEnabled": True,
           "BytesScannedCutoffPerQuery": int(env("ATHENA_SCAN_LIMIT_MB", "1024")) * 1024 * 1024}
    ath = client("athena")
    try:
        try:
            ath.get_work_group(WorkGroup=WORKGROUP)
            ath.update_work_group(WorkGroup=WORKGROUP, ConfigurationUpdates={
                "ResultConfigurationUpdates": cfg["ResultConfiguration"], "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True, "BytesScannedCutoffPerQuery": cfg["BytesScannedCutoffPerQuery"]})
        except ClientError:
            ath.create_work_group(Name=WORKGROUP, Configuration=cfg, Description="ridehail - 1 GB scan limit per query")
            print(f"[aws] Athena workgroup {WORKGROUP} created (scan limit 1 GB per query)")
    except ClientError as exc:
        print(f"[aws] cannot manage Athena workgroups ({exc.response['Error']['Code']}) - using 'primary'")
        WORKGROUP = "primary"
    return WORKGROUP


def ensure_budget() -> None:
    from botocore.exceptions import ClientError
    email = env("BUDGET_ALERT_EMAIL")
    if not email:
        return
    amount, name = env("AWS_BUDGET_USD", "5"), "ridehail-capstone-monthly"
    try:
        account = client("sts").get_caller_identity()["Account"]
        bud = client("budgets")
        budget = {"BudgetName": name, "BudgetType": "COST", "TimeUnit": "MONTHLY",
                  "BudgetLimit": {"Amount": str(amount), "Unit": "USD"}}
        try:
            bud.describe_budget(AccountId=account, BudgetName=name)
            bud.update_budget(AccountId=account, NewBudget=budget)
        except ClientError:
            subs = [{"SubscriptionType": "EMAIL", "Address": email}]
            notes = [{"NotificationType": "ACTUAL", "ComparisonOperator": "GREATER_THAN", "Threshold": 80.0,
                      "ThresholdType": "PERCENTAGE"},
                     {"NotificationType": "FORECASTED", "ComparisonOperator": "GREATER_THAN", "Threshold": 100.0,
                      "ThresholdType": "PERCENTAGE"}]
            bud.create_budget(AccountId=account, Budget=budget,
                              NotificationsWithSubscribers=[{"Notification": n, "Subscribers": subs} for n in notes])
            print(f"[aws] budget alert {name} created ({amount} USD/month -> {email})")
    except ClientError as exc:
        print(f"[aws] budget alert skipped ({exc.response['Error']['Code']})")


# ------------------------------------------------------------------ Athena / Glue
def athena(sql: str, timeout: int = 600) -> dict:
    """Run one Athena statement and wait; returns bytes scanned and run time."""
    ath = client("athena")
    qid = ath.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP, QueryExecutionContext={"Database": DB},
                                    ResultConfiguration={"OutputLocation": f"s3://{bucket()}/athena-results/"}
                                    )["QueryExecutionId"]
    t0 = time.time()
    while True:
        qe = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = qe["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() - t0 > timeout:
            raise TimeoutError(sql[:80])
        time.sleep(1.5)
    if state != "SUCCEEDED":
        raise RuntimeError(f"Athena {state}: {qe['Status'].get('StateChangeReason')} :: {sql[:120]}")
    return {"bytes": qe["Statistics"].get("DataScannedInBytes", 0), "ms": qe["Statistics"].get("TotalExecutionTimeInMillis", 0)}


def catalog() -> None:
    """Register the S3 data in the Glue Data Catalog (Athena DDL writes into Glue) and load the partitions."""
    ensure_workgroup()
    ensure_budget()
    try:
        client("glue").create_database(DatabaseInput={"Name": DB, "Description": "Ride-hailing trip history (S3)"})
    except Exception as exc:  # noqa: BLE001
        if "AlreadyExists" not in str(exc):
            raise
    athena(f"CREATE EXTERNAL TABLE IF NOT EXISTS {DB}.fact_trips ({FACT_COLS}) "
           "PARTITIONED BY (city string, trip_date date) STORED AS PARQUET "
           f"LOCATION 's3://{bucket()}/curated/fact_trips/'")
    athena(f"CREATE EXTERNAL TABLE IF NOT EXISTS {DB}.fact_trips_csv ({FACT_COLS}, city string, trip_date string) "
           "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE "
           f"LOCATION 's3://{bucket()}/raw_csv/fact_trips/' TBLPROPERTIES ('skip.header.line.count'='1')")
    athena(f"MSCK REPAIR TABLE {DB}.fact_trips")
    print(f"[aws] Glue database {DB}: tables fact_trips (Parquet, partitioned) and fact_trips_csv (raw CSV)")


QUERIES = {
    "q1_status_week": "SELECT trip_status, COUNT(*) c FROM {T} WHERE city='NYC' AND trip_date BETWEEN {D1} AND {D2} GROUP BY 1",
    "q2_fare_by_zone_3days": "SELECT pickup_zone_key, AVG(fare_amount) f, AVG(distance_km) k FROM {T} WHERE city='NYC' AND trip_date BETWEEN {D3} AND {D4} GROUP BY 1",
    "q3_hourly_demand_week": "SELECT substr(cast(time_key AS varchar),9,2) h, COUNT(*) c FROM {T} WHERE city='NYC' AND trip_date BETWEEN {D1} AND {D2} GROUP BY 1",
    "q4_cancel_reasons_day": "SELECT cancel_reason, COUNT(*) c FROM {T} WHERE city='NYC' AND trip_date = {D5} AND trip_status='CANCELLED' GROUP BY 1",
    "q5_wait_weekend": "SELECT pickup_zone_key, AVG(wait_time_min) w FROM {T} WHERE city='NYC' AND trip_date BETWEEN {D6} AND {D7} GROUP BY 1",
}


def benchmark() -> None:
    """Same 5 questions on the raw CSV table and on the partitioned Parquet table; Athena bills per byte scanned."""
    ensure_workgroup()
    month = env("TLC_MONTH", "2025-01")
    days = [f"{month}-{n:02d}" for n in (5, 11, 12, 14, 15, 11, 12)]
    results, tot_csv, tot_pq = [], 0, 0
    for name, sql in QUERIES.items():
        def render(tbl: str, parquet: bool) -> str:
            lit = (lambda d: f"DATE '{d}'") if parquet else (lambda d: f"'{d}'")
            return sql.format(T=f"{DB}.{tbl}", **{f"D{i + 1}": lit(d) for i, d in enumerate(days)})
        c, p = athena(render("fact_trips_csv", False)), athena(render("fact_trips", True))
        tot_csv, tot_pq = tot_csv + c["bytes"], tot_pq + p["bytes"]
        red = round(1 - p["bytes"] / c["bytes"], 4) if c["bytes"] else None
        results.append({"query": name, "csv_bytes": c["bytes"], "parquet_bytes": p["bytes"], "reduction": red})
        print(f"[athena] {name:24s} csv {c['bytes']:>12,} B   parquet {p['bytes']:>10,} B   -> "
              + (f"{red:.1%} less" if red is not None else "n/a"))
    reduction = round(1 - tot_pq / tot_csv, 4) if tot_csv else 0.0
    report = {"queries": results, "total_csv_bytes": tot_csv, "total_parquet_bytes": tot_pq, "reduction": reduction,
              "target": TARGET_ATHENA_REDUCTION, "pass": reduction >= TARGET_ATHENA_REDUCTION}
    os.makedirs(REPORTS, exist_ok=True)
    json.dump(report, open(f"{REPORTS}/athena_benchmark.json", "w"), indent=2)
    print(f"[athena] TOTAL {reduction:.1%} less data scanned (target >= {TARGET_ATHENA_REDUCTION:.0%}) -> "
          f"{'PASS' if report['pass'] else 'FAIL'}")


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    require()
    steps = {"upload": [upload], "catalog": [catalog], "benchmark": [benchmark], "all": [upload, catalog, benchmark]}
    if cmd not in steps:
        print(__doc__)
        sys.exit(2)
    for step in steps[cmd]:
        step()


if __name__ == "__main__":
    main()
