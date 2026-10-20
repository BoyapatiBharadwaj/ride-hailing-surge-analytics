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


# ============================================================ SILVER (cleaned, typed, no duplicates)
@dlt.table(name="silver_requests", comment="Cleaned trip requests")
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dlt.expect_or_drop("valid_pickup_zone", "zone_id BETWEEN 1 AND 263")
@dlt.expect_or_drop("valid_dropoff_zone", "dropoff_zone_id BETWEEN 1 AND 263")
@dlt.expect_or_drop("positive_fare", "fare > 0")
@dlt.expect_or_drop("valid_distance", "trip_miles >= 0")
def silver_requests():
    return parse("trip_requests")


@dlt.table(name="silver_pings", comment="Cleaned driver GPS pings and CityBikes readings")
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dlt.expect_or_drop("valid_gps", "lat BETWEEN 40.4 AND 41.0 AND lon BETWEEN -74.3 AND -73.6")
@dlt.expect_or_drop("valid_status", "status IN ('available','on_trip')")
@dlt.expect_or_drop("has_subject", "(source = 'citybikes' AND free_bikes >= 0) OR (source = 'driver' AND driver_id IS NOT NULL)")
@dlt.expect_or_drop("mapped_zone", "zone_id > 0")
def silver_pings():
    return parse("driver_gps_pings")


@dlt.table(name="silver_weather", comment="Cleaned live weather")
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dlt.expect_or_drop("valid_temperature", "temperature_2m BETWEEN -40 AND 55")
@dlt.expect_or_drop("valid_rain", "rain >= 0")
def silver_weather():
    return parse("weather")


@dlt.table(name="silver_matches", comment="Dispatch results: driver found or not, rider ETA, fare")
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dlt.expect_or_drop("has_trip", "trip_id IS NOT NULL")
@dlt.expect_or_drop("valid_status", "status IN ('MATCHED','UNMATCHED')")
@dlt.expect_or_drop("valid_eta", "eta_min IS NULL OR eta_min >= 0")
def silver_matches():
    return parse("trip_matches")


@dlt.table(name="silver_surge", comment="Surge multipliers published by the streaming engine")
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dlt.expect_or_drop("valid_zone", "zone_id BETWEEN 1 AND 263")
@dlt.expect_or_drop("valid_multiplier", "multiplier BETWEEN 1.0 AND 3.0")
def silver_surge():
    return (parse("surge_updates")
            .withColumn("minute_ts", F.date_trunc("minute", F.from_utc_timestamp(F.col("window_end").cast("timestamp"), NY))))


@dlt.table(name="silver_trips", comment="Cleaned historical trips")
@dlt.expect_or_drop("non_null_zone", "pickup_zone_id IS NOT NULL AND dropoff_zone_id IS NOT NULL")
@dlt.expect_or_drop("positive_fare", "status != 'COMPLETED' OR fare > 0")
@dlt.expect_or_drop("pickup_after_request", "status != 'COMPLETED' OR pickup_time >= request_time")
@dlt.expect_or_drop("dropoff_after_pickup", "status != 'COMPLETED' OR dropoff_time >= pickup_time")
def silver_trips():
    return (dlt.read("bronze_trips").dropDuplicates(["trip_id"])
            .withColumn("wait_min", (F.unix_timestamp("pickup_time") - F.unix_timestamp("request_time")) / 60.0)
            .withColumn("hour_ts", F.date_trunc("hour", "request_time")))


# ============================================================ GOLD (business tables)
@dlt.table(name="zone_minute_surge", comment="Surge multiplier, demand and supply per zone and minute")
def zone_minute_surge():
    w = Window.partitionBy("zone_id", "minute_ts").orderBy(F.col("event_time").desc())
    return (dlt.read("silver_surge").withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
            .select("zone_id", "minute_ts", "demand", "supply", "ratio", "multiplier", "rain_mm", "temp_c",
                    F.col("local_ts").alias("updated_at"), "decayed"))


@dlt.table(name="zone_hourly_kpis", comment="Hourly KPIs per zone: history (MySQL trips) + live (stream)")
def zone_hourly_kpis():
    done = F.col("status") == "COMPLETED"
    hist = (dlt.read("silver_trips").groupBy(F.col("pickup_zone_id").alias("zone_id"), "hour_ts").agg(
        F.count("*").alias("requests"), F.sum(F.when(done, 1).otherwise(0)).alias("completed_trips"),
        F.sum(F.when(~done, 1).otherwise(0)).alias("cancelled_trips"), F.avg(F.when(done, F.col("wait_min"))).alias("avg_eta_min"),
        F.sum(F.when(done, F.col("fare")).otherwise(0.0)).alias("revenue"),
        F.sum(F.when(done, F.col("distance_km")).otherwise(0.0)).alias("distance_km"),
        F.avg("surge_multiplier").alias("avg_surge"), F.lit(None).cast("double").alias("avg_supply"))
        .withColumn("source", F.lit("history")))
    req = (dlt.read("silver_requests").groupBy("zone_id", F.date_trunc("hour", "local_ts").alias("hour_ts"))
           .agg(F.count("*").alias("requests")))
    mm = (dlt.read("silver_matches").groupBy(F.col("pickup_zone_id").alias("zone_id"), F.date_trunc("hour", "local_ts").alias("hour_ts"))
          .agg(F.sum(F.when(F.col("status") == "MATCHED", 1).otherwise(0)).alias("completed_trips"),
               F.sum(F.when(F.col("status") == "UNMATCHED", 1).otherwise(0)).alias("cancelled_trips"),
               F.avg("eta_min").alias("avg_eta_min"), F.sum(F.coalesce("fare", F.lit(0.0))).alias("revenue"),
               F.sum(F.when(F.col("status") == "MATCHED", F.col("distance_km")).otherwise(0.0)).alias("distance_km")))
    sg = (dlt.read("zone_minute_surge").groupBy("zone_id", F.date_trunc("hour", "minute_ts").alias("hour_ts"))
          .agg(F.avg("multiplier").alias("avg_surge"), F.avg("supply").alias("avg_supply")))
    live = (req.join(mm, ["zone_id", "hour_ts"], "left").join(sg, ["zone_id", "hour_ts"], "left")
            .withColumn("source", F.lit("live")))
    longs = ("requests", "completed_trips", "cancelled_trips")
    doubles = ("avg_eta_min", "revenue", "distance_km", "avg_surge", "avg_supply")
    cols = ["zone_id", "hour_ts", "source", *longs, *doubles]

    def cast(df):
        return df.select(*[F.col(c).cast("long") if c in longs else F.col(c).cast("double") if c in doubles
                           else F.col(c) for c in cols])
    return cast(hist).unionByName(cast(live))


@dlt.table(name="driver_daily_utilisation", comment="Per driver and day: online time, time on trips, trips, revenue")
def driver_daily_utilisation():
    ping_sec = 22.5                                    # pings arrive every 15-30 s
    p = (dlt.read("silver_pings").where("source = 'driver'").groupBy("driver_id", F.to_date("local_ts").alias("date"))
         .agg(F.count("*").alias("pings"), F.sum(F.when(F.col("status") == "on_trip", 1).otherwise(0)).alias("on_trip_pings"))
         .withColumn("online_min", F.round(F.col("pings") * ping_sec / 60.0, 2))
         .withColumn("on_trip_min", F.round(F.col("on_trip_pings") * ping_sec / 60.0, 2))
         .withColumn("utilisation", F.round(F.col("on_trip_pings") / F.col("pings"), 4)))
    m = (dlt.read("silver_matches").where("status = 'MATCHED'").groupBy("driver_id", F.to_date("local_ts").alias("date"))
         .agg(F.count("*").alias("trips"), F.round(F.sum("fare"), 2).alias("revenue"),
              F.sum(F.when(F.col("surge_multiplier") >= 1.5, 1).otherwise(0)).alias("surge_trips")))
    return (p.join(m, ["driver_id", "date"], "left").fillna({"trips": 0, "revenue": 0.0, "surge_trips": 0})
             .select("driver_id", "date", "pings", "on_trip_pings", "online_min", "on_trip_min", "utilisation",
                     F.col("trips").cast("long"), F.col("revenue").cast("double"), F.col("surge_trips").cast("long")))


@dlt.table(name="zone_daily_utilisation", comment="Driver utilisation per zone and day")
def zone_daily_utilisation():
    return (dlt.read("silver_pings").where("source = 'driver'").groupBy("zone_id", F.to_date("local_ts").alias("date"))
            .agg(F.count("*").alias("pings"), F.sum(F.when(F.col("status") == "on_trip", 1).otherwise(0)).alias("on_trip_pings"))
            .withColumn("utilisation", F.round(F.col("on_trip_pings") / F.col("pings"), 4)))


@dlt.table(name="revenue_per_km", comment="Revenue per km by zone, hour of day and weather")
def revenue_per_km():
    def bucket(rain, temp):
        return F.when(rain >= 0.5, "rain").when(temp < 5, "cold").when(temp > 30, "hot").otherwise("clear")
    h = (dlt.read("silver_trips").where("status = 'COMPLETED' AND distance_km > 0")
         .groupBy(F.col("pickup_zone_id").alias("zone_id"), F.hour("request_time").alias("hour_of_day"))
         .agg(F.count("*").alias("trips"), F.sum("fare").alias("revenue"), F.sum("distance_km").alias("distance_km"))
         .withColumn("weather_bucket", F.lit("unknown")).withColumn("source", F.lit("history")))
    w = (dlt.read("silver_weather").groupBy(F.date_trunc("hour", "local_ts").alias("hour_ts"))
         .agg(F.avg("rain").alias("hour_rain_mm"), F.avg("temperature_2m").alias("hour_temp_c")))
    m = (dlt.read("silver_matches").where("status = 'MATCHED' AND distance_km > 0")
         .withColumn("hour_ts", F.date_trunc("hour", "local_ts")).join(w, "hour_ts", "left")
         .withColumn("weather_bucket", F.coalesce(bucket(F.col("hour_rain_mm"), F.col("hour_temp_c")), F.lit("unknown")))
         .groupBy(F.col("pickup_zone_id").alias("zone_id"), F.hour("local_ts").alias("hour_of_day"), "weather_bucket")
         .agg(F.count("*").alias("trips"), F.sum("fare").alias("revenue"), F.sum("distance_km").alias("distance_km"))
         .withColumn("source", F.lit("live")))
    cols = ["zone_id", "hour_of_day", "weather_bucket", "source", "trips", "revenue", "distance_km"]
    return (h.select(*cols).unionByName(m.select(*cols))
            .withColumn("revenue_per_km", F.round(F.col("revenue") / F.col("distance_km"), 4))
            .withColumn("trips", F.col("trips").cast("long")))


@dlt.table(name="reconciliation_check", comment="Gold must reconcile with Silver: live requests counted once")
@dlt.expect("gold_matches_silver", "reconciled = true")
def reconciliation_check():
    silver = dlt.read("silver_requests").agg(F.count("*").alias("silver_requests"))
    gold = dlt.read("zone_hourly_kpis").where("source = 'live'").agg(F.coalesce(F.sum("requests"), F.lit(0)).alias("gold_requests"))
    return (silver.crossJoin(gold).withColumn("reconciled", F.col("silver_requests") == F.col("gold_requests"))
            .withColumn("checked_at", F.current_timestamp()))
