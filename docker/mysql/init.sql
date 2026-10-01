-- Operational source database (seeded by data/seed_mysql.py, imported by Sqoop)
CREATE DATABASE IF NOT EXISTS ridehail;
USE ridehail;

CREATE TABLE IF NOT EXISTS city_zones (
  zone_id        SMALLINT PRIMARY KEY,          -- TLC LocationID
  zone_name      VARCHAR(80),
  borough        VARCHAR(30),
  service_zone   VARCHAR(30),
  city           VARCHAR(10) NOT NULL DEFAULT 'NYC',
  centroid_lat   DOUBLE,
  centroid_lon   DOUBLE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS drivers (
  id             INT PRIMARY KEY,
  name           VARCHAR(80),                   -- synthetic (Faker), commas removed
  vehicle_type   VARCHAR(20),
  joined_date    DATE,
  home_city      VARCHAR(10),
  rating         DOUBLE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS riders (
  id             INT PRIMARY KEY,
  name           VARCHAR(80),
  joined_date    DATE,
  home_city      VARCHAR(10),
  rating         DOUBLE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS trips (
  trip_id           BIGINT PRIMARY KEY,
  driver_id         INT,
  rider_id          INT,
  city              VARCHAR(10) NOT NULL DEFAULT 'NYC',
  pickup_zone_id    SMALLINT,
  dropoff_zone_id   SMALLINT,
  request_time      DATETIME,
  pickup_time       DATETIME NULL,
  dropoff_time      DATETIME NULL,
  distance_km       DOUBLE,
  fare              DOUBLE,
  status            VARCHAR(12),                -- COMPLETED | CANCELLED
  cancel_reason     VARCHAR(32) NULL,
  surge_multiplier  DOUBLE DEFAULT 1.0,
  last_modified     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_last_modified (last_modified),
  KEY idx_request_time (request_time)
) ENGINE=InnoDB;

-- Sqoop user (password comes from MYSQL_PASSWORD through the image entrypoint)
