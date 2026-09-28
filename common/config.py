"""All settings in one place. Every value comes from .env (environment variables) with a safe default."""
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")


def env(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return default if v is None or v.strip() == "" else v.strip().strip('"').strip("'")


def env_int(name: str, default: int) -> int:
    return int(env(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(env(name, str(default)))


# ---------------- services (docker network names) ----------------
KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "kafka1:9092,kafka2:9092,kafka3:9092")
HDFS_URL = env("HDFS_URL", "hdfs://namenode:9000")
HBASE_HOST = env("HBASE_HOST", "hbase")
HBASE_PORT = env_int("HBASE_PORT", 9090)
HBASE_TABLE = "surge_multipliers"
MYSQL_HOST = env("MYSQL_HOST", "mysql")
MYSQL_DB = env("MYSQL_DATABASE", "ridehail")
MYSQL_USER = env("MYSQL_USER", "etl")
MYSQL_PASSWORD = env("MYSQL_PASSWORD", "etlpass")
MYSQL_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:3306/{MYSQL_DB}"
MYSQL_JDBC = f"jdbc:mysql://{MYSQL_HOST}:3306/{MYSQL_DB}?useSSL=false&allowPublicKeyRetrieval=true"

# ---------------- Kafka topics ----------------
TOPIC_TRIPS = "trip_requests"       # riders asking for a car
TOPIC_GPS = "driver_gps_pings"      # driver positions (+ CityBikes docks as extra supply)
TOPIC_WEATHER = "weather"           # Open-Meteo rain / temperature
TOPIC_MATCHES = "trip_matches"      # dispatch results (driver found or not, ETA, fare)
TOPIC_SURGE = "surge_updates"       # multipliers published by the surge engine
ARCHIVED_TOPICS = [TOPIC_TRIPS, TOPIC_GPS, TOPIC_WEATHER, TOPIC_MATCHES, TOPIC_SURGE]

# ---------------- local folders (shared Docker volume /lake) ----------------
LAKE = env("LAKE_DIR", "/lake")
LANDING = f"{LAKE}/landing"         # landing/events = raw Kafka events waiting for upload to Databricks
EXPORT = f"{LAKE}/export"           # nightly export of the Hive data (-> S3 and Databricks)
REPORTS = f"{LAKE}/reports"         # small JSON / CSV / HTML results
MODELS = f"{LAKE}/models"           # trained forecast model
DATA_DIR = env("DATA_DIR", "/app/data")
TLC_DIR = f"{DATA_DIR}/raw"
ZONE_GRID_PATH = f"{DATA_DIR}/zone_grid.json"

# ---------------- business rules and targets ----------------
CITY = env("CITY", "NYC")
TARGET_ETA_MIN = 4.5
TARGET_UTILISATION = 0.78
TARGET_R2 = 0.70
TARGET_ATHENA_REDUCTION = 0.70
PAYOUT_BONUS = 20.0                 # USD when utilisation >= 78 % and trips >= 12
PAYOUT_MIN_TRIPS = 12
PAYOUT_SURGE_EXTRA = 1.5            # USD per trip with surge >= 1.5x

SKIP = 99                           # exit code that Airflow shows as "skipped" (e.g. cloud keys not filled in)


def to_ny_naive(dt: datetime) -> datetime:
    """UTC datetime -> New York wall-clock time without time zone (the time shown in Gold, the page and the chat)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(NY_TZ).replace(tzinfo=None)


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def aws_configured() -> bool:
    return bool(env("AWS_ACCESS_KEY_ID") and env("AWS_SECRET_ACCESS_KEY") and env("S3_BUCKET"))


def databricks_configured() -> bool:
    return bool(env("DATABRICKS_HOST") and env("DATABRICKS_TOKEN"))
