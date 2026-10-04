"""Backup import used only when Sqoop fails (same output layout as Sqoop).

Writes the same layout as Sqoop: hdfs:///raw/mysql/<table>_csv  (text, no header, \\N for NULL).
    docker exec rh-spark spark-submit /app/ingestion/sqoop/spark_jdbc_import.py
"""
from common.config import HDFS_URL, MYSQL_JDBC, MYSQL_PASSWORD, MYSQL_USER
from common.spark import get_spark

OPTS = {"url": MYSQL_JDBC, "user": MYSQL_USER, "password": MYSQL_PASSWORD, "driver": "com.mysql.cj.jdbc.Driver"}


def dump(df, name: str) -> None:
    (df.write.mode("overwrite").option("nullValue", "\\N").option("emptyValue", "")
       .option("timestampFormat", "yyyy-MM-dd HH:mm:ss").option("dateFormat", "yyyy-MM-dd")
       .csv(f"{HDFS_URL}/raw/mysql/{name}_csv"))
    print(f"[jdbc-import] {name}_csv written")


def main() -> None:
    spark = get_spark("spark-jdbc-import")
    for t in ("city_zones", "drivers", "riders"):
        dump(spark.read.format("jdbc").options(**OPTS, dbtable=t).load(), t)

    bounds = (spark.read.format("jdbc").options(**OPTS, dbtable="(SELECT MIN(trip_id) lo, MAX(trip_id) hi FROM trips) b")
              .load().collect()[0])
    trips = (spark.read.format("jdbc").options(**OPTS, dbtable="trips", partitionColumn="trip_id",
                                               lowerBound=str(bounds["lo"] or 1), upperBound=str((bounds["hi"] or 1) + 1),
                                               numPartitions="4").load())
    dump(trips, "trips")
    spark.stop()


if __name__ == "__main__":
    main()
