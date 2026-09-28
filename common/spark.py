"""Spark session helper + the JSON schema of the Kafka events (used by the streaming and batch jobs)."""
from pyspark.sql import SparkSession
from pyspark.sql import types as T


def lp(path: str) -> str:
    """Local-filesystem path for Spark. The default filesystem is HDFS; files for the cloud uploads are written to
    the shared /lake volume instead, where the Python tools container can read them."""
    return path if "://" in path else "file://" + path


def get_spark(app: str, hive: bool = False, shuffle_partitions: int = 8, utc: bool = True) -> SparkSession:
    """Defaults (HDFS, Hive metastore, UTC, microsecond Parquet timestamps) come from docker/spark/spark-defaults.conf."""
    b = SparkSession.builder.appName(app).config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    if hive:
        b = b.enableHiveSupport()
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# One permissive schema for every Kafka topic (the `topic` column tells them apart)
EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("zone_id", T.IntegerType()),
    T.StructField("dropoff_zone_id", T.IntegerType()),
    T.StructField("trip_miles", T.DoubleType()),
    T.StructField("trip_time_sec", T.IntegerType()),
    T.StructField("fare", T.DoubleType()),
    T.StructField("driver_id", T.IntegerType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("status", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("station_id", T.StringType()),
    T.StructField("free_bikes", T.IntegerType()),
    T.StructField("temperature_2m", T.DoubleType()),
    T.StructField("precipitation", T.DoubleType()),
    T.StructField("rain", T.DoubleType()),
    T.StructField("weather_code", T.IntegerType()),
    T.StructField("trip_id", T.StringType()),
    T.StructField("pickup_zone_id", T.IntegerType()),
    T.StructField("eta_min", T.DoubleType()),
    T.StructField("distance_km", T.DoubleType()),
    T.StructField("surge_multiplier", T.DoubleType()),
    T.StructField("city", T.StringType()),
])
