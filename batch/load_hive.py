"""Hive star schema: creates the tables (batch/hive_ddl.sql) and loads them from the cleansed trips.

    docker exec rh-spark spark-submit /app/batch/load_hive.py
Reads  hdfs:///curated/trips_clean, hdfs:///raw/mysql/{city_zones,drivers}_csv
Writes hdfs:///warehouse/{dim_zone,dim_driver,dim_time,fact_trips}  (fact partitioned by city and trip_date)
"""
import os
import re

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.config import HDFS_URL
from common.spark import get_spark

W = f"{HDFS_URL}/warehouse"
DDL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hive_ddl.sql")


def create_tables(spark) -> None:
    """Run every statement of hive_ddl.sql (CREATE ... IF NOT EXISTS, so it is safe every night)."""
    text = re.sub(r"--[^\n]*", "", open(DDL).read())
    for stmt in (s.strip() for s in text.split(";")):
        if stmt:
            spark.sql(stmt)


def main() -> None:
    spark = get_spark("load-hive", hive=True)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    create_tables(spark)
    trips = spark.read.parquet(f"{HDFS_URL}/curated/trips_clean")

    # ---------------- dim_zone
    zones = (spark.read.csv(f"{HDFS_URL}/raw/mysql/city_zones_csv", nullValue="\\N", schema=
             "zone_id INT, zone_name STRING, borough STRING, service_zone STRING, city STRING, "
             "centroid_lat DOUBLE, centroid_lon DOUBLE"))
    dim_zone = zones.withColumn("zone_key", F.row_number().over(Window.orderBy("zone_id")).cast("int")) \
                    .select("zone_key", "zone_id", "zone_name", "borough", "service_zone", "centroid_lat", "centroid_lon")
    dim_zone.write.mode("overwrite").parquet(f"{W}/dim_zone")

    # ---------------- dim_driver (names are synthetic and stored hashed)
    drivers = spark.read.csv(f"{HDFS_URL}/raw/mysql/drivers_csv", nullValue="\\N", schema=
                             "id INT, name STRING, vehicle_type STRING, joined_date DATE, home_city STRING, rating DOUBLE")
    dim_driver = drivers.select(F.col("id").alias("driver_key"), F.col("id").alias("driver_id"),
                                F.sha2("name", 256).alias("name_hash"), "vehicle_type", "joined_date", "home_city", "rating")
    dim_driver.write.mode("overwrite").parquet(f"{W}/dim_driver")

    # ---------------- dim_time (minute grain over the trip period)
    lo, hi = trips.agg(F.min("request_time"), F.max("request_time")).first()
    if lo is None:
        raise SystemExit("[hive] /curated/trips_clean has no trips - the Sqoop import is empty or incomplete; re-import: "
                         "hdfs dfs -rm -r -f /raw/mysql/_state /raw/mysql/trips_csv, then run the nightly DAG once")
    import holidays
    years = list(range(lo.year, hi.year + 1))
    hol = [(d,) for d in sorted(holidays.US(years=years).keys())]
    hol_df = spark.createDataFrame(hol, T.StructType([T.StructField("hdate", T.DateType())]))
    mins = (spark.range(1).select(F.explode(F.sequence(F.date_trunc("minute", F.lit(lo)), F.date_trunc("minute", F.lit(hi)),
                                                       F.expr("interval 1 minute"))).alias("full_ts")))
    dim_time = (mins.withColumn("date", F.to_date("full_ts")).withColumn("hour", F.hour("full_ts"))
                .withColumn("minute", F.minute("full_ts")).withColumn("day_of_week", F.dayofweek("full_ts") - 1)
                .withColumn("is_weekend", F.col("day_of_week").isin(0, 6))
                .join(F.broadcast(hol_df), F.col("date") == F.col("hdate"), "left")
                .withColumn("is_holiday", F.col("hdate").isNotNull())
                .withColumn("peak_flag", (~F.col("is_weekend")) & (F.col("hour").between(7, 9) | F.col("hour").between(16, 19)))
                .withColumn("time_key", F.date_format("full_ts", "yyyyMMddHHmm").cast("long"))
                .select("time_key", "full_ts", "date", "hour", "minute", "day_of_week", "is_weekend", "is_holiday", "peak_flag"))
    dim_time.write.mode("overwrite").parquet(f"{W}/dim_time")

    # ---------------- fact_trips (partitioned by city, trip_date)
    zk = dim_zone.select(F.col("zone_id").alias("zid"), F.col("zone_key").alias("zkey"))
    fact = (trips.join(F.broadcast(zk.alias("p")), trips.pickup_zone_id == F.col("p.zid"), "left")
                 .withColumnRenamed("zkey", "pickup_zone_key").drop("zid")
                 .join(F.broadcast(zk.alias("d")), trips.dropoff_zone_id == F.col("d.zid"), "left")
                 .withColumnRenamed("zkey", "dropoff_zone_key").drop("zid")
                 .select(F.col("trip_id"), F.col("driver_id").cast("int").alias("driver_key"),
                         F.col("pickup_zone_key").cast("int"), F.col("dropoff_zone_key").cast("int"),
                         F.date_format("request_time", "yyyyMMddHHmm").cast("long").alias("time_key"),
                         F.col("status").alias("trip_status"), "cancel_reason", "distance_km",
                         ((F.unix_timestamp("dropoff_time") - F.unix_timestamp("pickup_time")) / 60.0).alias("duration_min"),
                         F.col("fare").alias("fare_amount"), "surge_multiplier",
                         ((F.unix_timestamp("pickup_time") - F.unix_timestamp("request_time")) / 60.0).alias("wait_time_min"),
                         F.coalesce(F.col("city"), F.lit("NYC")).alias("city"),
                         F.to_date("request_time").alias("trip_date")))
    fact.write.mode("overwrite").partitionBy("city", "trip_date").parquet(f"{W}/fact_trips")
    spark.sql("MSCK REPAIR TABLE ridehail.fact_trips")

    print("[hive] fact_trips rows:", spark.table("ridehail.fact_trips").count())
    spark.sql("""SELECT d.hour, COUNT(*) trips FROM ridehail.fact_trips f JOIN ridehail.dim_time d ON f.time_key=d.time_key
                 GROUP BY d.hour ORDER BY trips DESC LIMIT 5""").show()
    spark.stop()


if __name__ == "__main__":
    main()
