#!/usr/bin/env bash
# Apache Sqoop 1.4.7: MySQL -> HDFS raw landing zone (runs inside the rh-sqoop container).
#   docker exec rh-sqoop /scripts/import_tables.sh
# trips are imported incrementally on last_modified (merge-key trip_id) so reruns never duplicate data.
# Small dimension tables are re-imported in full. Output: /raw/mysql/<table>_csv  (text, comma separated).
set -euo pipefail

DB="${MYSQL_DATABASE:-ridehail}"
USER_="${MYSQL_USER:-etl}"
JDBC="jdbc:mysql://mysql:3306/${DB}?useSSL=false"
RAW=/raw/mysql
STATE="${RAW}/_state/trips.last_value"
PWFILE=/secrets/mysql.pwd

echo "[sqoop] waiting for HDFS"
for i in $(seq 1 40); do hdfs dfs -ls / >/dev/null 2>&1 && break || sleep 3; done
hdfs dfsadmin -safemode wait >/dev/null 2>&1 || true

hdfs dfs -mkdir -p /secrets "${RAW}/_state"
printf '%s' "${MYSQL_PASSWORD:-etlpass}" | hdfs dfs -put -f - "${PWFILE}"
hdfs dfs -chmod 400 "${PWFILE}"

COMMON=(--connect "${JDBC}" --username "${USER_}" --password-file "${PWFILE}"
        --as-textfile --fields-terminated-by ',' --optionally-enclosed-by '"'
        --null-string '\\N' --null-non-string '\\N')

for T in city_zones drivers riders; do
  echo "[sqoop] full import: ${T}"
  sqoop import "${COMMON[@]}" --table "${T}" --target-dir "${RAW}/${T}_csv" --delete-target-dir -m 1
done

LAST="$(hdfs dfs -cat "${STATE}" 2>/dev/null || echo '1970-01-01 00:00:00')"
hdfs dfs -test -d "${RAW}/trips_csv" || LAST='1970-01-01 00:00:00'   # nothing imported yet -> full import
NEXT="$(date -u -d '-10 seconds' '+%Y-%m-%d %H:%M:%S')"
echo "[sqoop] incremental import: trips (last_modified > '${LAST}')"
sqoop import "${COMMON[@]}" --table trips --target-dir "${RAW}/trips_csv" \
      --incremental lastmodified --check-column last_modified --last-value "${LAST}" \
      --merge-key trip_id -m 4
printf '%s' "${NEXT}" | hdfs dfs -put -f - "${STATE}"

echo "[sqoop] done"
hdfs dfs -du -h "${RAW}"
