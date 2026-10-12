"""Raw event archiver (Spark Structured Streaming): keeps EVERY Kafka message, exactly as received.

    Kafka (trip_requests, driver_gps_pings, weather, trip_matches, surge_updates)
      -> every 60 s one Parquet file in /lake/landing/events/<batch id>/
      -> the hourly Airflow DAG uploads new files to Databricks (landing Volume) -> DLT Bronze -> Silver -> Gold

Columns: topic, kafka_partition, kafka_offset, key, value (the JSON event), kafka_ts, ingested_at.
Partition + offset are stored so the Kafka -> Bronze -> Silver -> Gold counts can be reconciled.

Exactly-once: the Kafka offsets live in the Spark checkpoint on HDFS. A batch that is repeated after a crash is written
to the SAME folder again (overwrite), so an event is never stored twice; Silver also drops duplicate event ids.
"""
import logging

from pyspark.sql import functions as F

from common.config import ARCHIVED_TOPICS, HDFS_URL, KAFKA_BOOTSTRAP, LANDING
from common.spark import get_spark, lp

log = logging.getLogger("raw_archiver")
CHECKPOINT = f"{HDFS_URL}/checkpoints/raw_archiver"


def archive_batch(batch_df, batch_id: int) -> None:
    rows = (batch_df.select(
        "topic", F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"),
        F.col("key").cast("string").alias("key"), F.col("value").cast("string").alias("value"),
        F.col("timestamp").alias("kafka_ts"))
        .withColumn("ingested_at", F.current_timestamp()))
    if rows.isEmpty():
        return
    # one file per batch; Spark writes _SUCCESS last, so the uploader only picks up complete folders
    rows.coalesce(1).write.mode("overwrite").parquet(lp(f"{LANDING}/events/{batch_id:09d}"))
    log.warning("archived batch %d", batch_id)


def main() -> None:
    spark = get_spark("raw-archiver", shuffle_partitions=2)
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", ",".join(ARCHIVED_TOPICS))
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .option("maxOffsetsPerTrigger", 300000)
           .load())
    (raw.writeStream.foreachBatch(archive_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="60 seconds")
        .start()
        .awaitTermination())


if __name__ == "__main__":
    main()
