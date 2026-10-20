# Ride-Hailing Surge Analytics (Problem Statement #03)

A cab company has too few drivers in some parts of the city at peak hours. This project watches ride requests,
driver positions and the weather **as they happen**, computes the demand/supply ratio of every New York taxi zone
**every 60 seconds** and sets a **surge price** for each zone. In the background it keeps the full history, builds
clean business tables, forecasts demand 30 minutes ahead, pays drivers a daily bonus and answers questions in plain
English.

Team: **Bharadwaj Boyapati** (ingestion + speed layer) · **Khuraijam Richard** (storage + lakehouse) · **Moirangthem Erudite** (orchestration + AI)

---

## 1. The flow in one picture

```
 LIVE (24/7)                                                         every 60 s
 TLC trip replay ─┐                                    ┌─> HBase  surge_multipliers  (current price per zone)
 GPS fleet sim  ──┤                                    │
 CityBikes      ──┼─> Kafka (3 brokers) ─> Spark Structured Streaming ─┴─> Kafka surge_updates (price announced)
 Open-Meteo     ──┤      │                  "surge engine"
 Scala producer ──┘      │
                         └─> raw archiver (Spark, every 60 s) ─> /lake/landing/events  (Parquet files)
                                                                       │ hourly DAG
                                                                       v
 HISTORY (nightly)                                     DATABRICKS  (Delta Live Tables pipeline)
 MySQL ─Sqoop─> HDFS ─Spark Core─> Hive star schema ──export──>  Bronze -> Silver -> Gold  ─┬─> GenAI assistant + Ops page
                                          │                     (trips + zones nightly,     ├─> Power BI dashboard
                                          │                      events hourly)            ├─> demand forecast (ML)
                                          └──export──> AWS: S3 (Parquet + CSV) -> Glue -> Athena    └─> driver payouts + e-mail
```

**Each store has exactly one job. Nothing is stored twice:**

| Store | What is in it | Written by | When | Read by |
|---|---|---|---|---|
| **Kafka** (3 brokers, 7 days) | every live event: `trip_requests`, `driver_gps_pings`, `weather`, `trip_matches`, `surge_updates` | producers, surge engine | continuously | surge engine, raw archiver, Ops page |
| **HBase** `surge_multipliers` | the **current** surge price of each zone (row key `zone_id#minute`, kept 7 days) | surge engine | every 60 s | a rider app / `make surge` |
| **MySQL** `ridehail` | the operational database: trips, drivers, riders, city_zones (one month of NYC trips) | seed container | once | Sqoop |
| **HDFS** | Sqoop import (`/raw/mysql`), cleansed trips (`/curated`), Spark Core reports, Hive tables (`/warehouse`), streaming checkpoints | Sqoop, Spark | nightly | Spark, Hive |
| **Hive** `ridehail` | star schema: `fact_trips` (by city + date), `dim_zone`, `dim_driver`, `dim_time` | Spark | nightly | export job |
| **/lake** (local folder) | **only a waiting area**: new stream files before upload (deleted after 2 days), the nightly export, the model, small reports | archiver, export, ML | - | uploads, ML |
| **AWS S3 + Glue + Athena** | the trip **history**: `curated/fact_trips` (Parquet, by city/date) and `raw_csv/` (same rows as CSV), catalogued in Glue | nightly DAG | nightly | Athena (cost benchmark) |
| **Databricks** `workspace.ridehail` | **the lakehouse: Bronze, Silver, Gold** (Delta tables) | DLT pipeline, ML job, payout job | hourly + nightly | assistant, Ops page, Power BI, payouts, ML scoring |

Why both AWS and Databricks? They answer different questions. **AWS** proves the cloud **cost** target: the same
history as CSV and as partitioned Parquet, and Athena reads 99 % less data from the Parquet copy. **Databricks** is
the **lakehouse**: live events + history turned into clean, quality-checked business tables.

---

## 2. What happens, and when

| When | What runs | Where the data goes |
|---|---|---|
| **all the time** | producers send events to Kafka; the surge engine counts demand and supply per zone in a 5-minute window sliding every minute, applies the surge rules and writes the price | Kafka → HBase + Kafka `surge_updates` |
| **every 60 s** | raw archiver copies every Kafka event, unchanged, into a Parquet file | Kafka → `/lake/landing/events` |
| **every hour** (DAG `hourly_zone_aggregation`) | upload new event files → run the DLT pipeline → forecast the next 30 minutes | `/lake` → Databricks Bronze → Silver → Gold → `demand_forecast` |
| **00:30 UTC / 06:00 IST** (DAG `nightly_batch_pipeline`) | Sqoop → Spark Core → Hive → export → AWS and Databricks → train the model | MySQL → HDFS → Hive → S3/Glue/Athena + Databricks |
| **01:00 UTC / 06:30 IST** (DAG `daily_driver_incentive_payout`) | payout per driver: USD 20 if utilisation ≥ 78 % and ≥ 12 trips, + USD 1.50 per trip with surge ≥ 1.5x → e-mail | Gold `driver_daily_utilisation` → Gold `driver_payouts` → e-mail |

**Surge rule** (`streaming/surge_rules.py`): demand/supply ratio < 1 → 1.0x, < 1.5 → 1.2x, < 2 → 1.5x, ≤ 3 → 2.0x,
above → 2.5x; +0.2x when rain ≥ 2.5 mm, +0.1x below 5 °C or above 35 °C; never above 3.0x and at most ±0.3x per minute.

**Medallion in Databricks** (`cloud/databricks/dlt_pipeline.py`):
* **Bronze** - exactly what arrived: `bronze_events` (Auto Loader on the uploaded files), `bronze_trips`, `dim_zone`.
* **Silver** - cleaned and de-duplicated, with **DLT expectations** that drop bad rows (valid GPS coordinates,
  fare > 0, valid zones, ...): `silver_requests`, `silver_pings`, `silver_weather`, `silver_matches`, `silver_surge`, `silver_trips`.
* **Gold** - business tables: `zone_minute_surge`, `zone_hourly_kpis`, `driver_daily_utilisation`,
  `zone_daily_utilisation`, `revenue_per_km`, `reconciliation_check`, plus `demand_forecast` (ML job) and
  `driver_payouts` (payout job).

**Exactly once:** producers use `acks=all` + idempotence and topics keep 3 copies (2 must confirm). The streaming
jobs keep their Kafka offsets in checkpoints on HDFS. HBase keys (`zone#minute`) are overwritten, not duplicated. A
repeated archiver batch rewrites the same folder, and Silver drops any repeated `event_id`.

**Times:** event times are UTC. Every time in Silver, Gold, the Ops page and the assistant is **New York local time**.

---

## 3. Folder structure

```
ride-hailing-surge-analytics/
├── start.sh               start everything (one command)
├── Makefile               every other command  (make help)
├── .env.example           settings and cloud keys (copied to .env)
├── docker/                docker-compose.yml + the 4 images we build (spark, sqoop, python tools, airflow)
├── data/                  download the NYC data, build the zone grid, fill MySQL
├── common/                settings (config.py), zone lookup, Spark session + event schema
├── ingestion/
│   ├── producers/         Python producers -> Kafka: TLC replay, GPS fleet + dispatch, CityBikes, weather; topic script
│   ├── scala/             custom Scala Kafka producer
│   └── sqoop/             Sqoop import MySQL -> HDFS (+ Spark JDBC backup)
├── streaming/             surge engine, surge rules, HBase + Kafka outputs, raw archiver, HBase table
├── batch/                 Spark Core analysis, Hive star schema, nightly export
├── cloud/
│   ├── aws.py             S3 upload, Glue catalog, Athena benchmark, cost guards
│   └── databricks/        DLT pipeline (Bronze/Silver/Gold), deploy/upload/run, SQL connection
├── ml/                    features, training (Linear Regression vs Random Forest), live scoring
├── orchestration/         Airflow DAGs (dags/) and the payout job
├── genai/                 GenAI assistant (5 read-only tools on Gold) + Ops page server + live Kafka feed
├── frontend/              React Ops page (built version in frontend/dist)
└── powerbi/               Power BI project + guide
```

---

## 4. Run it

**You need:** Ubuntu (or WSL 2 on Windows) with Docker, 16 GB RAM (8 GB + swap works but is slow), 30 GB free disk,
internet. Optional: an AWS account, a Databricks (Free Edition) workspace and a Groq API key.

```bash
./start.sh                 # first run 30-45 min: builds images, downloads one month of NYC trips, starts everything
```

`start.sh` creates `.env`, builds the images, runs `docker compose up -d` (one-time setup containers create the Kafka
topics, HDFS folders and the HBase table and fill MySQL; then the streaming jobs, producers, Airflow and the Ops page
start). On its first start Airflow runs each DAG once by itself.

**Cloud keys** - fill them in `.env`, then run `./start.sh` again (containers pick up the new values):

| Key | Where to get it |
|---|---|
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_BUCKET` | IAM user with S3, Glue, Athena (and Budgets) access; any new bucket name |
| `DATABRICKS_HOST`, `DATABRICKS_TOKEN` | workspace URL; *Settings → Developer → Access tokens* |
| `DATABRICKS_HTTP_PATH` | *SQL Warehouses → your warehouse → Connection details* |
| `GROQ_API_KEY` | https://console.groq.com/keys |

While a key is empty, the Airflow tasks for that cloud show as **skipped** (not failed).

**First time with Databricks:** `make dbx` (creates schema `ridehail`, Volume `landing`, the DLT pipeline, uploads
everything and runs it). After that the DAGs keep it up to date.

| Open | Address |
|---|---|
| Ops page: live trips, drivers, surge map + chat assistant | http://localhost:8501 |
| Airflow (admin / admin) | http://localhost:8080 |
| MailHog (payout e-mails) | http://localhost:8026 |
| HDFS / HBase web UI | http://localhost:9870 / http://localhost:16010 |

## 5. Commands

| Command | What it does |
|---|---|
| `make status` / `make logs` | containers / follow the streaming jobs and producers |
| `make batch` · `make hourly` · `make payout` | run a DAG now instead of waiting for its schedule |
| `make surge ZONE=132` | current surge of a zone, read from HBase |
| `make refresh-sla` | when the multipliers were last refreshed (target: every 60 s) |
| `make hive` | Hive tables and the `fact_trips` row count |
| `make ask Q="Which zones are surging right now?"` | ask the assistant from the terminal |
| `make dbx` · `make dbx-run` · `make dbx-reconcile` · `make dbx-reset` | Databricks: deploy + run · run only · count check · start over |
| `make aws` | S3 upload, Glue catalog, Athena benchmark |
| `make train` · `make score` | train the forecast · forecast the next 30 minutes |
| `make scala-producer` | run the custom Scala producer for 5 minutes (100 extra drivers + trip requests) |
| `make down` · `make reset` | stop (keeps data) · stop and delete all local data |

## 6. KPIs and where to see them

| KPI | Target | Where |
|---|---|---|
| Surge refresh | every 60 s | `make refresh-sla`, Ops page "Surge" tab |
| Average rider ETA | < 4.5 min | Ops page, Power BI "Rider ETA", assistant `kpi_summary` |
| Driver utilisation | ≥ 78 % | Ops page "Drivers", Power BI gauge, payout e-mail |
| Athena scan reduction | ≥ 70 % | `make aws` → `/lake/reports/athena_benchmark.json` |
| Forecast R² | ≥ 0.70 | `make train` → `/lake/reports/forecast_metrics.json`, Databricks *Experiments* |

## 7. Failure drills (by hand)

1. **Broker kill** - `docker stop rh-kafka2`; producers and the surge engine keep working (2 of 3 copies are enough);
   `make logs` shows no errors; `docker start rh-kafka2`. Then drill 3 shows nothing was lost.
2. **Spark restart** - `docker restart rh-surge-stream rh-raw-archiver`; both continue from their checkpoints;
   `make refresh-sla` is back under 60 s, and drill 3 shows no duplicates.
3. **Reconciliation** - stop the producers (`docker stop rh-tlc-replay rh-gps-simulator rh-citybikes rh-weather`),
   wait 5 minutes, run `make hourly` and wait for it in Airflow, then `make dbx-reconcile`: every topic has the same
   count in Kafka and Bronze, Silver = Bronze minus duplicates and rows dropped by the expectations, and Gold =
   Silver (`reconciliation_check`). Start the producers again with `./start.sh`.

## 8. Upgrading from the previous version

This version stores the lakehouse only in Databricks (the old local Delta copy is gone). Start clean once:
`make reset` (deletes local data), `./start.sh`, then `make dbx-reset` and `make dbx`.

## 9. If something goes wrong

* **A container keeps restarting:** `docker logs <name>` (for example `rh-surge-stream`).
* **Seeding seems stuck:** `docker logs -f rh-seed` (the download is about 500 MB).
* **Databricks pipeline failed:** Databricks → *Jobs & Pipelines* → `ridehail-medallion` shows the failing table and
  why; fix and `make dbx-run`.
* **`make hive` shows far fewer than ~1.9 million `fact_trips` / training says "only N day(s)":** the trips were
  imported while MySQL was still being filled. Re-import them:
  `docker exec rh-namenode hdfs dfs -rm -r -f /raw/mysql/_state /raw/mysql/trips_csv` and then `make batch`.
* **Not enough memory:** stop the producers you do not need, or add swap.
* **After a Windows/WSL restart:** just run `./start.sh` again.
