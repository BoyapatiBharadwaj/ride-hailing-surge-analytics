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
