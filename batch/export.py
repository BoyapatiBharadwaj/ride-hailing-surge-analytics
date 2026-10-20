"""Nightly export of the Hive / HDFS data to the shared /lake volume, ready for the cloud uploads and the ML job.

    docker exec rh-spark spark-submit /app/batch/export.py

  /lake/export/fact_trips/city=NYC/trip_date=YYYY-MM-DD/*.parquet   partitioned Parquet     -> S3 curated/ (Athena)
  /lake/export/raw_csv/fact_trips/*.csv                             same rows as plain CSV  -> S3 raw_csv/ (Athena baseline)
  /lake/export/trips_raw/*.parquet                                  the MySQL trips import  -> Databricks Bronze trips
  /lake/export/dim_zone/*.parquet                                   taxi zones + centres    -> Databricks dim_zone
  /lake/export/demand_30min/*.parquet                               requests per zone per 30 minutes (ML training data)
"""
from pyspark.sql import functions as F

from common.config import EXPORT, HDFS_URL
from common.spark import get_spark, lp


def main() -> None:
    spark = get_spark("export", hive=True)
    fact = spark.table("ridehail.fact_trips").select(
        "trip_id", "driver_key", "pickup_zone_key", "dropoff_zone_key", "time_key", "trip_status", "cancel_reason",
        "distance_km", "duration_min", "fare_amount", "surge_multiplier", "wait_time_min", "city", "trip_date")
    (fact.repartition("city", "trip_date").write.mode("overwrite").partitionBy("city", "trip_date")
         .parquet(lp(f"{EXPORT}/fact_trips")))
    fact.coalesce(4).write.mode("overwrite").option("header", True).csv(lp(f"{EXPORT}/raw_csv/fact_trips"))
    print("[export] fact_trips rows:", fact.count())

    spark.read.parquet(f"{HDFS_URL}/raw/mysql/trips").coalesce(4).write.mode("overwrite").parquet(lp(f"{EXPORT}/trips_raw"))
    spark.table("ridehail.dim_zone").coalesce(1).write.mode("overwrite").parquet(lp(f"{EXPORT}/dim_zone"))

    # 30-minute demand per zone from the cleansed trips (every request counts as demand)
    clean = spark.read.parquet(f"{HDFS_URL}/curated/trips_clean")
    slots = (clean.where("pickup_zone_id IS NOT NULL")
                  .withColumn("slot_ts", F.to_timestamp(F.floor(F.unix_timestamp("request_time") / 1800) * 1800))
                  .groupBy(F.col("pickup_zone_id").alias("zone_id"), "slot_ts").agg(F.count("*").alias("demand")))
    slots.coalesce(1).write.mode("overwrite").parquet(lp(f"{EXPORT}/demand_30min"))
    print("[export] demand slots:", slots.count())
    spark.stop()


if __name__ == "__main__":
    main()
