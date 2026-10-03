-- Hive star schema: one fact + three dimensions, Parquet on HDFS, fact partitioned by city and date.
-- Run by batch/load_hive.py through Spark SQL with the Hive metastore (plain HiveQL).
CREATE DATABASE IF NOT EXISTS ridehail;
USE ridehail;

CREATE EXTERNAL TABLE IF NOT EXISTS dim_zone (
  zone_key INT, zone_id INT, zone_name STRING, borough STRING, service_zone STRING,
  centroid_lat DOUBLE, centroid_lon DOUBLE)
STORED AS PARQUET LOCATION 'hdfs:///warehouse/dim_zone';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_driver (
  driver_key INT, driver_id INT, name_hash STRING, vehicle_type STRING, joined_date DATE,
  home_city STRING, rating DOUBLE)
STORED AS PARQUET LOCATION 'hdfs:///warehouse/dim_driver';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_time (
  time_key BIGINT, full_ts TIMESTAMP, `date` DATE, `hour` INT, `minute` INT, day_of_week INT,
  is_weekend BOOLEAN, is_holiday BOOLEAN, peak_flag BOOLEAN)
STORED AS PARQUET LOCATION 'hdfs:///warehouse/dim_time';

CREATE EXTERNAL TABLE IF NOT EXISTS fact_trips (
  trip_id BIGINT, driver_key INT, pickup_zone_key INT, dropoff_zone_key INT,
  time_key BIGINT, trip_status STRING, cancel_reason STRING,
  distance_km DOUBLE, duration_min DOUBLE, fare_amount DOUBLE,
  surge_multiplier DOUBLE, wait_time_min DOUBLE)
PARTITIONED BY (city STRING, trip_date DATE)
STORED AS PARQUET LOCATION 'hdfs:///warehouse/fact_trips';
