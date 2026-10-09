# Databricks notebook source
# Delta Live Tables pipeline `ridehail-medallion`: the ONLY place where Bronze, Silver and Gold are built.
#
#   landing/events  (raw Kafka events, every hour)  -> bronze_events  -> silver_requests / pings / weather / matches / surge
#   landing/trips   (MySQL trips snapshot, nightly) -> bronze_trips   -> silver_trips
#   landing/zones   (taxi zones, nightly)                              -> dim_zone
#   Silver -> Gold: zone_minute_surge, zone_hourly_kpis, driver_daily_utilisation, zone_daily_utilisation,
#                   revenue_per_km, reconciliation_check
#   (Gold demand_forecast and driver_payouts are written by the ML and payout jobs, not by this pipeline.)
#
# Uploaded and started by:  python -m cloud.databricks.deploy all
# Time convention: event_time is UTC; every *_ts / date column in Silver and Gold is New York local time.
import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

LANDING = spark.conf.get("landing_path", "/Volumes/workspace/ridehail/landing")  # noqa: F821  (spark is given by DLT)
NY = "America/New_York"

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()), T.StructField("event_time", T.StringType()),
    T.StructField("zone_id", T.IntegerType()), T.StructField("dropoff_zone_id", T.IntegerType()),
    T.StructField("trip_miles", T.DoubleType()), T.StructField("trip_time_sec", T.IntegerType()),
    T.StructField("fare", T.DoubleType()), T.StructField("driver_id", T.IntegerType()),
    T.StructField("lat", T.DoubleType()), T.StructField("lon", T.DoubleType()),
    T.StructField("status", T.StringType()), T.StructField("source", T.StringType()),
    T.StructField("station_id", T.StringType()), T.StructField("free_bikes", T.IntegerType()),
    T.StructField("temperature_2m", T.DoubleType()), T.StructField("precipitation", T.DoubleType()),
    T.StructField("rain", T.DoubleType()), T.StructField("weather_code", T.IntegerType()),
    T.StructField("trip_id", T.StringType()), T.StructField("pickup_zone_id", T.IntegerType()),
    T.StructField("eta_min", T.DoubleType()), T.StructField("distance_km", T.DoubleType()),
    T.StructField("surge_multiplier", T.DoubleType()), T.StructField("city", T.StringType()),
    # surge_updates fields
    T.StructField("multiplier", T.DoubleType()), T.StructField("demand", T.LongType()),
    T.StructField("supply", T.LongType()), T.StructField("ratio", T.DoubleType()),
    T.StructField("rain_mm", T.DoubleType()), T.StructField("temp_c", T.DoubleType()),
    T.StructField("minute_key", T.StringType()), T.StructField("decayed", T.BooleanType()),
    T.StructField("window_end", T.StringType()),
])


# Schemas of the landing files (given explicitly, so an empty folder on the first run is not an error)
RAW_EVENT = T.StructType([
    T.StructField("topic", T.StringType()), T.StructField("kafka_partition", T.IntegerType()),
    T.StructField("kafka_offset", T.LongType()), T.StructField("key", T.StringType()),
    T.StructField("value", T.StringType()), T.StructField("kafka_ts", T.TimestampType()),
    T.StructField("ingested_at", T.TimestampType()),
])
RAW_TRIP = T.StructType([
    T.StructField("trip_id", T.LongType()), T.StructField("driver_id", T.IntegerType()),
    T.StructField("rider_id", T.IntegerType()), T.StructField("city", T.StringType()),
    T.StructField("pickup_zone_id", T.IntegerType()), T.StructField("dropoff_zone_id", T.IntegerType()),
    T.StructField("request_time", T.TimestampType()), T.StructField("pickup_time", T.TimestampType()),
    T.StructField("dropoff_time", T.TimestampType()), T.StructField("distance_km", T.DoubleType()),
    T.StructField("fare", T.DoubleType()), T.StructField("status", T.StringType()),
    T.StructField("cancel_reason", T.StringType()), T.StructField("surge_multiplier", T.DoubleType()),
    T.StructField("last_modified", T.TimestampType()),
])
ZONE = T.StructType([
    T.StructField("zone_key", T.IntegerType()), T.StructField("zone_id", T.IntegerType()),
    T.StructField("zone_name", T.StringType()), T.StructField("borough", T.StringType()),
    T.StructField("service_zone", T.StringType()), T.StructField("centroid_lat", T.DoubleType()),
    T.StructField("centroid_lon", T.DoubleType()),
])


# ============================================================ BRONZE (as received, no rules)
@dlt.table(name="bronze_events", comment="Every Kafka event as received (topic, partition, offset, JSON value)")
def bronze_events():
    return (spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet")  # noqa: F821
            .schema(RAW_EVENT).load(f"{LANDING}/events"))


@dlt.table(name="bronze_trips", comment="Historical MySQL trips (Sqoop import), latest nightly snapshot")
def bronze_trips():
    return spark.read.schema(RAW_TRIP).parquet(f"{LANDING}/trips")  # noqa: F821


@dlt.table(name="dim_zone", comment="NYC taxi zones with the zone centre (map position for Power BI)")
def dim_zone():
    return spark.read.schema(ZONE).parquet(f"{LANDING}/zones").select(  # noqa: F821
        "zone_id", F.col("borough").alias("Borough"), F.col("zone_name").alias("Zone"), "service_zone",
        "centroid_lat", "centroid_lon")


def parse(topic: str):
    return (dlt.read_stream("bronze_events").where(F.col("topic") == topic)
            .select("topic", "kafka_partition", "kafka_offset", F.from_json("value", EVENT_SCHEMA).alias("j"))
            .select("topic", "kafka_partition", "kafka_offset", "j.*")
            .withColumn("event_time", F.col("event_time").cast("timestamp"))
            .withColumn("local_ts", F.from_utc_timestamp("event_time", NY))
            .withWatermark("event_time", "10 minutes")
            .dropDuplicatesWithinWatermark(["event_id"]))
