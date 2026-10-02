"""Spark Core batch job: cleanse the historical trips and analyse them with pair RDDs.

Input : hdfs:///raw/mysql/trips_csv        (Sqoop text import, or the Spark JDBC backup import)
Output: hdfs:///raw/mysql/trips            typed Parquet copy of the raw import
        hdfs:///curated/trips_clean        cleansed trips (Parquet)
        hdfs:///reports/peak_hours*        busiest hours (overall + per zone/day type)
        hdfs:///reports/cancel_reasons*    ranked cancellation reasons (overall + per zone/hour)
        /lake/reports/spark_core_report.json   same results + cleansing counters, small enough to read
"""
import csv
import json
import os
from collections import namedtuple
from datetime import datetime

from pyspark.sql import types as T

from common.config import HDFS_URL, REPORTS
from common.spark import get_spark

FIELDS = ["trip_id", "driver_id", "rider_id", "city", "pickup_zone_id", "dropoff_zone_id", "request_time",
          "pickup_time", "dropoff_time", "distance_km", "fare", "status", "cancel_reason", "surge_multiplier",
          "last_modified"]
Trip = namedtuple("Trip", FIELDS)
NULLS = {"", "\\N", "\\\\N", "null", "NULL", "None"}


def _s(v):
    return None if v is None or v in NULLS else v


def _ts(v):
    v = _s(v)
    return None if v is None else datetime.fromisoformat(v[:19].replace("T", " "))


def _i(v):
    v = _s(v)
    return None if v is None else int(float(v))


def _f(v):
    v = _s(v)
    return None if v is None else float(v)


def parse(line):
    """CSV line -> Trip, or None when the line is unreadable."""
    try:
        r = next(csv.reader([line]))
        if len(r) != len(FIELDS):
            return None
        return Trip(_i(r[0]), _i(r[1]), _i(r[2]), _s(r[3]), _i(r[4]), _i(r[5]), _ts(r[6]), _ts(r[7]), _ts(r[8]),
                    _f(r[9]), _f(r[10]), _s(r[11]), _s(r[12]), _f(r[13]), _ts(r[14]))
    except Exception:  # noqa: BLE001
        return None


def invalid_reason(t):
    """None when the record is valid, otherwise the cleansing rule it violates.
    Rules: missing zone, fare <= 0 (a cancelled trip has fare 0 and stays; a negative fare is always invalid),
    drop-off before pickup."""
    if t.pickup_zone_id is None or t.dropoff_zone_id is None:
        return "null_zone"
    if t.fare is not None and t.fare < 0:
        return "negative_fare"
    if t.request_time is None:
        return "null_request_time"
    if t.status == "COMPLETED":
        if t.fare is None or t.fare <= 0:
            return "non_positive_fare"
        if t.pickup_time is None or t.dropoff_time is None:
            return "completed_without_times"
        if t.dropoff_time < t.pickup_time:
            return "dropoff_before_pickup"
    return None


def day_type(dt):
    return "weekend" if dt.weekday() >= 5 else "weekday"


SCHEMA = T.StructType([
    T.StructField("trip_id", T.LongType()), T.StructField("driver_id", T.IntegerType()),
    T.StructField("rider_id", T.IntegerType()), T.StructField("city", T.StringType()),
    T.StructField("pickup_zone_id", T.IntegerType()), T.StructField("dropoff_zone_id", T.IntegerType()),
    T.StructField("request_time", T.TimestampType()), T.StructField("pickup_time", T.TimestampType()),
    T.StructField("dropoff_time", T.TimestampType()), T.StructField("distance_km", T.DoubleType()),
    T.StructField("fare", T.DoubleType()), T.StructField("status", T.StringType()),
    T.StructField("cancel_reason", T.StringType()), T.StructField("surge_multiplier", T.DoubleType()),
    T.StructField("last_modified", T.TimestampType()),
])


def save_text(spark, rdd, path: str) -> None:
    """saveAsTextFile that can be re-run nightly (removes the previous output first)."""
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    p.getFileSystem(spark._jsc.hadoopConfiguration()).delete(p, True)
    rdd.saveAsTextFile(path)


def main() -> None:
    spark = get_spark("spark-core-trips")
    sc = spark.sparkContext
    src = f"{HDFS_URL}/raw/mysql/trips_csv"

    parsed = sc.textFile(src).map(parse).filter(lambda t: t is not None).cache()
    total = parsed.count()
    spark.createDataFrame(parsed, SCHEMA).write.mode("overwrite").parquet(f"{HDFS_URL}/raw/mysql/trips")

    causes = dict(parsed.map(invalid_reason).filter(lambda c: c is not None).countByValue())
    trips = parsed.filter(lambda t: invalid_reason(t) is None).cache()
    valid = trips.count()
    spark.createDataFrame(trips, SCHEMA).write.mode("overwrite").parquet(f"{HDFS_URL}/curated/trips_clean")

    # ---- peak hours: (hour_of_day, 1) pairs -> reduceByKey -> sort (PDF snippet)
    peak_hours = (trips.filter(lambda t: t.request_time is not None)
                       .map(lambda t: (t.request_time.hour, 1))
                       .reduceByKey(lambda a, b: a + b)
                       .sortBy(lambda kv: -kv[1]))
    # busiest 3 hours per zone and day type
    peak_by_zone = (trips.map(lambda t: ((t.pickup_zone_id, day_type(t.request_time), t.request_time.hour), 1))
                         .reduceByKey(lambda a, b: a + b)
                         .map(lambda kv: ((kv[0][0], kv[0][1]), (kv[0][2], kv[1])))
                         .groupByKey()
                         .mapValues(lambda it: sorted(it, key=lambda x: -x[1])[:3]))
    # ---- cancellation reasons: (cancel_reason, 1) pairs reduced and sorted
    cancelled = trips.filter(lambda t: t.status == "CANCELLED")
    cancel_reasons = (cancelled.map(lambda t: (t.cancel_reason or "UNKNOWN", 1))
                               .reduceByKey(lambda a, b: a + b)
                               .sortBy(lambda kv: -kv[1]))
    cancel_by_zone_hour = (cancelled.map(lambda t: ((t.pickup_zone_id, t.request_time.hour, t.cancel_reason or "UNKNOWN"), 1))
                                    .reduceByKey(lambda a, b: a + b)
                                    .sortBy(lambda kv: -kv[1]))

    ph = peak_hours.collect()
    cr = cancel_reasons.collect()
    report = {
        "rows_parsed": total, "rows_valid": valid, "rows_removed": total - valid, "removed_by_rule": causes,
        "peak_hours": [{"hour": h, "trips": n} for h, n in ph],
        "cancellation_reasons": [{"reason": r, "count": n} for r, n in cr],
        "peak_hours_by_zone_sample": [{"zone": k[0], "day_type": k[1], "top_hours": v}
                                      for k, v in peak_by_zone.take(30)],
        "cancellations_by_zone_hour_top20": [{"zone": k[0], "hour": k[1], "reason": k[2], "count": n}
                                             for k, n in cancel_by_zone_hour.take(20)],
    }
    save_text(spark, peak_hours.map(lambda kv: f"{kv[0]},{kv[1]}"), f"{HDFS_URL}/reports/peak_hours")
    save_text(spark, peak_by_zone.map(lambda kv: json.dumps({"zone": kv[0][0], "day_type": kv[0][1], "top_hours": kv[1]})),
              f"{HDFS_URL}/reports/peak_hours_by_zone")
    save_text(spark, cancel_reasons.map(lambda kv: f"{kv[0]},{kv[1]}"), f"{HDFS_URL}/reports/cancel_reasons")
    save_text(spark, cancel_by_zone_hour.map(lambda kv: f"{kv[0][0]},{kv[0][1]},{kv[0][2]},{kv[1]}"),
              f"{HDFS_URL}/reports/cancel_reasons_by_zone_hour")

    os.makedirs(REPORTS, exist_ok=True)
    with open(f"{REPORTS}/spark_core_report.json", "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(json.dumps({k: report[k] for k in ("rows_parsed", "rows_valid", "rows_removed", "removed_by_rule")}))
    spark.stop()


if __name__ == "__main__":
    main()
