#!/bin/sh
# Creates the Kafka topics (RF=3, min.insync.replicas=2). Runs inside the kafka1 container:
#   docker exec rh-kafka1 sh /scripts/create_topics.sh
BOOT="${BOOTSTRAP:-localhost:9092}"
KT=/opt/kafka/bin/kafka-topics.sh
RET="${RETENTION_MS:-604800000}"   # 7 days

create() {
  "$KT" --bootstrap-server "$BOOT" --create --if-not-exists --topic "$1" \
        --partitions "$2" --replication-factor 3 \
        --config min.insync.replicas=2 --config retention.ms="$RET"
}

create trip_requests     6
create driver_gps_pings  6
create weather           1
create surge_updates     6
create trip_matches      6      # dispatch results of the simulator (live ETA KPI)

"$KT" --bootstrap-server "$BOOT" --describe
