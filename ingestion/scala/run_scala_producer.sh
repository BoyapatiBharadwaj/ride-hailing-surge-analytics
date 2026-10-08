#!/usr/bin/env bash
# Compiles (first run only) and runs the custom Scala 2.12 Kafka producer inside the rh-spark container.
#   docker exec rh-spark bash /app/ingestion/scala/run_scala_producer.sh [seconds]      (or: make scala-producer)
# The Scala compiler, Spark, kafka-clients and Jackson all come from /opt/spark/jars (Spark 3.5.3 / Scala 2.12).
set -euo pipefail
SECONDS_TO_RUN="${1:-300}"
SRC=/app/ingestion/scala/SurgeProducer.scala
OUT=/tmp/ridehail-scala
JAR="$OUT/surge-producer.jar"

# the same .env values the Python producers get (rh-spark itself has no env_file)
if [ -f /app/.env ]; then
  for v in KAFKA_BOOTSTRAP TLC_MONTH REPLAY_DAY REPLAY_SAMPLE_RATE CITY FLEET_SIZE GPS_PING_MIN_SEC GPS_PING_MAX_SEC SCALA_FLEET_SIZE; do
    val=$(grep -E "^${v}=" /app/.env | tail -1 | cut -d= -f2- | tr -d '"'"'"' \r' || true)
    if [ -n "$val" ]; then export "$v=$val"; fi
  done
fi

mkdir -p "$OUT"
if [ ! -f "$JAR" ] || [ "$SRC" -nt "$JAR" ]; then
  echo "[scala-producer] compiling $(basename "$SRC") (Scala $(ls /opt/spark/jars | sed -n 's/^scala-library-\(.*\)\.jar$/\1/p'))"
  java -Xmx512m -cp "/opt/spark/jars/*" scala.tools.nsc.Main -usejavacp -nowarn -d "$JAR" "$SRC"
fi
exec /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 768m \
  --conf spark.ui.enabled=false --class ridehail.SurgeProducer "$JAR" --seconds "$SECONDS_TO_RUN"
