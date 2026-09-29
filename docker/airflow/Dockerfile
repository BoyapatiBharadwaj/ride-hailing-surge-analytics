FROM apache/airflow:2.9.3-python3.11

USER root
# Static docker CLI: DAG tasks run Sqoop / Spark / Python jobs with `docker exec` into the stack containers
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.3.1.tgz \
    | tar -xz --strip-components=1 -C /usr/local/bin docker/docker

USER airflow
