SHELL := /bin/bash
DC    = docker compose --env-file .env -f docker/docker-compose.yml
TOOLS = docker exec rh-tools
AF    = docker exec rh-airflow-scheduler airflow
SPARK = docker exec rh-spark /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g

.PHONY: help up down reset status logs batch hourly payout surge refresh-sla hive ask dbx dbx-run dbx-reconcile \
        dbx-reset aws train score scala-producer

help:            ## list every command
	@grep -E '^[a-z-]+:.*## ' Makefile | sed 's/:.*## /\t/' | expand -t 18

# ---------------------------------------------------------------- stack
up:              ## start everything (same as ./start.sh)
	./start.sh
down:            ## stop everything, keep all data
	$(DC) down
reset:           ## stop everything and DELETE all local data (MySQL, Kafka, HDFS, HBase, /lake, Airflow)
	@read -r -p "Delete all local data? type yes: " a; [ "$$a" = yes ] && $(DC) down -v
status:          ## containers and their state
	@docker ps -a --filter "name=rh-" --format 'table {{.Names}}\t{{.Status}}' | sort
logs:            ## follow the streaming jobs and producers
	$(DC) logs -f --tail=30 surge-stream raw-archiver tlc-replay gps-simulator

# ---------------------------------------------------------------- run a DAG now (instead of waiting for its schedule)
batch:           ## nightly batch pipeline now
	$(AF) dags trigger nightly_batch_pipeline
hourly:          ## hourly stream -> Databricks upload + DLT + forecast now
	$(AF) dags trigger hourly_zone_aggregation
payout:          ## daily driver payout + e-mail now
	$(AF) dags trigger daily_driver_incentive_payout

# ---------------------------------------------------------------- look at the data
surge:           ## current surge of one zone from HBase:  make surge ZONE=132
	$(TOOLS) python -m streaming.hbase_table --latest $(or $(ZONE),132)
refresh-sla:     ## when the surge multipliers were last refreshed (target: every 60 s)
	$(TOOLS) python -m streaming.hbase_table --refresh
hive:            ## Hive star schema: tables and row counts
	docker exec rh-spark /opt/spark/bin/spark-sql -e "SHOW TABLES IN ridehail; SELECT COUNT(*) AS fact_trips FROM ridehail.fact_trips;" 2>/dev/null
ask:             ## ask the GenAI assistant:  make ask Q="Which zones are surging right now?"
	$(TOOLS) python -m genai.assistant "$(or $(Q),Which 5 zones have the highest surge right now?)"

# ---------------------------------------------------------------- cloud + ML by hand (the DAGs run these too)
dbx:             ## Databricks: create schema/Volume/pipeline, upload trips + zones + stream files, run DLT
	$(TOOLS) python -m cloud.databricks.deploy all
dbx-run:         ## Databricks: run the DLT pipeline only
	$(TOOLS) python -m cloud.databricks.deploy run
dbx-reconcile:   ## record counts Kafka -> Bronze -> Silver -> Gold (reconciliation drill)
	$(TOOLS) python -m cloud.databricks.deploy reconcile
dbx-reset:       ## Databricks: delete the pipeline and the ridehail schema, then start again with: make dbx
	$(TOOLS) python -m cloud.databricks.deploy reset
aws:             ## AWS: S3 upload, Glue catalog, Athena CSV-vs-Parquet benchmark
	$(TOOLS) python -m cloud.aws all
train:           ## train the demand forecast (Linear Regression vs Random Forest)
	$(TOOLS) python -m ml.train
score:           ## forecast the next 30 minutes for every zone
	$(TOOLS) python -m ml.score
scala-producer:  ## custom Scala Kafka producer for 5 minutes (100 extra drivers + trip requests)
	docker exec rh-spark bash /app/ingestion/scala/run_scala_producer.sh $(or $(DURATION),300)
