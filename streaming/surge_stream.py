"""Surge engine (Spark Structured Streaming) - the speed layer.

Kafka (trip_requests, driver_gps_pings, weather)
  -> parse JSON, map GPS -> zone_id (pre-computed point-in-polygon grid)
  -> 10 min watermark + dropDuplicates(event_id)
  -> window(event_time, "5 minutes", "1 minute") per zone: demand, supply (+ CityBikes proxy), weather
  -> every 60 s: ratio -> surge multiplier -> HBase (current price) + Kafka surge_updates

Why one aggregation and not reduceByKeyAndWindow: that function belongs to the old DStream API; the
Structured Streaming form gives watermarks, exactly-once checkpointing and Kafka offset tracking.
Weather is aggregated under the pseudo zone 0 in the same window and joined to every zone in the sink.
"""
import logging
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.config import (HDFS_URL, KAFKA_BOOTSTRAP, TOPIC_GPS, TOPIC_TRIPS, TOPIC_WEATHER)
from common.zones import zone_of
from common.spark import EVENT_SCHEMA, get_spark
from streaming import sinks

log = logging.getLogger("surge_stream")
CHECKPOINT = f"{HDFS_URL}/checkpoints/surge"
STATE = sinks.new_state()
_state_loaded = False


def build_stream(spark):
    zone_udf = F.udf(zone_of, T.IntegerType())
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", f"{TOPIC_TRIPS},{TOPIC_GPS},{TOPIC_WEATHER}")
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")
           .option("maxOffsetsPerTrigger", 400000)
           .load())
    ev = (raw.select(F.col("topic"), F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("j"))
             .select("topic", "j.*")
             .withColumn("event_time", F.col("event_time").cast("timestamp"))
             .where(F.col("event_time").isNotNull() & F.col("event_id").isNotNull()))
    ev = ev.withWatermark("event_time", "10 minutes").dropDuplicates(["event_id", "event_time"])
    zone = (F.when(F.col("topic") == TOPIC_TRIPS, F.col("zone_id"))
             .when(F.col("topic") == TOPIC_GPS, zone_udf(F.col("lat"), F.col("lon")))
             .otherwise(F.lit(0)))
    ev = ev.withColumn("zone", zone).where(F.col("zone").isNotNull() & ((F.col("zone") > 0) | (F.col("topic") == TOPIC_WEATHER)))

    is_trip = F.col("topic") == TOPIC_TRIPS
    is_driver = (F.col("topic") == TOPIC_GPS) & (F.col("source") == "driver") & (F.col("status") == "available")
    is_bike = (F.col("topic") == TOPIC_GPS) & (F.col("source") == "citybikes")
    is_wx = F.col("topic") == TOPIC_WEATHER
    return (ev.groupBy(F.window("event_time", "5 minutes", "1 minute"), "zone")
              .agg(F.count(F.when(is_trip, 1)).alias("demand"),
                   F.approx_count_distinct(F.when(is_driver, F.col("driver_id"))).alias("supply_drivers"),
                   F.sum(F.when(is_bike, F.col("free_bikes"))).alias("bikes_sum"),
                   F.approx_count_distinct(F.when(is_bike, F.date_trunc("minute", F.col("event_time")))).alias("bike_polls"),
                   F.max(F.when(is_wx, F.col("rain"))).alias("rain_mm"),
                   F.max(F.when(is_wx, F.col("temperature_2m"))).alias("temp_c")))


def process_batch(batch_df, batch_id: int) -> None:
    global _state_loaded
    if not _state_loaded:
        STATE["prev"].update(sinks.load_prev_multipliers())
        _state_loaded = True
    rows = [r.asDict() for r in batch_df.select(
        F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"), "zone",
        "demand", "supply_drivers", "bikes_sum", "bike_polls", "rain_mm", "temp_c").collect()]
    if not rows:
        return
    recs = sinks.build_surge_records(rows, STATE, batch_id, datetime.now(timezone.utc))
    if not recs:
        return
    sinks.write_hbase(recs)                       # idempotent via deterministic row key
    sinks.publish_kafka(recs)
    top = max(recs, key=lambda r: r["multiplier"])
    log.warning("batch %d: %d zones published, max surge %.2fx in zone %d", batch_id, len(recs), top["multiplier"], top["zone_id"])


def main() -> None:
    spark = get_spark("surge-engine", shuffle_partitions=4)
    agg = build_stream(spark)
    query = (agg.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", CHECKPOINT)
             .trigger(processingTime="60 seconds")
             .outputMode("update")
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
