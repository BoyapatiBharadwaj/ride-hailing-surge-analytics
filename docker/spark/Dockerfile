FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates procps python3-pip \
 && rm -rf /var/lib/apt/lists/*

# Connector jars baked into the image (no runtime --packages downloads needed)
ARG MVN=https://repo1.maven.org/maven2
RUN set -eux; cd /opt/spark/jars; \
    for u in \
      org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar ; \
    do curl -fsSLO "$MVN/$u"; done

RUN pip3 install --no-cache-dir \
      "numpy<2" pandas==2.2.2 pyarrow==16.1.0 happybase==1.2.0 thrift==0.20.0 \
      confluent-kafka==2.5.3 requests==2.32.3 holidays==0.55

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ENV PYSPARK_PYTHON=python3 \
    PYTHONPATH=/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    TZ=UTC
WORKDIR /app
