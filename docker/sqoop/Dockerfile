# Apache Sqoop 1.4.7 (retired project, so everything is pinned) + Hadoop 2.10 client.
# Sqoop runs MapReduce in LOCAL mode and writes straight to HDFS (hdfs://namenode:9000).
FROM eclipse-temurin:8-jdk

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates netcat-openbsd procps \
 && rm -rf /var/lib/apt/lists/*

ENV HADOOP_VERSION=2.10.2 SQOOP_VERSION=1.4.7
ENV HADOOP_HOME=/opt/hadoop SQOOP_HOME=/opt/sqoop
ENV HADOOP_COMMON_HOME=/opt/hadoop HADOOP_HDFS_HOME=/opt/hadoop HADOOP_MAPRED_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=$PATH:/opt/hadoop/bin:/opt/sqoop/bin
ENV ACCUMULO_HOME=/tmp/none HCAT_HOME=/tmp/none ZOOKEEPER_HOME=/tmp/none HADOOP_USER_NAME=root

# Download: fast Apache mirrors first, archive.apache.org (slow in some regions) last; a transfer below 50 KB/s for 30 s
# is abandoned and the next mirror tried (2 tries each). Saved to a file first, so a cut never leaves half a tree.
RUN set -eux; \
    fetch() { out="$1"; shift; \
      for u in "$@"; do \
        for i in 1 2; do \
          echo ">> try $i: $u"; \
          if curl -fL --connect-timeout 20 --speed-limit 51200 --speed-time 30 -o "$out" "$u" && tar -tzf "$out" >/dev/null 2>&1; then return 0; fi; \
          rm -f "$out"; sleep 3; \
        done; \
      done; echo "download failed for $out" >&2; return 1; }; \
    fetch /tmp/hadoop.tgz \
      https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
      https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
      https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz; \
    tar -xzf /tmp/hadoop.tgz -C /opt --exclude='hadoop-${HADOOP_VERSION}/share/doc' && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop && rm -f /tmp/hadoop.tgz; \
    fetch /tmp/sqoop.tgz \
      https://archive.apache.org/dist/sqoop/${SQOOP_VERSION}/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz \
      https://downloads.apache.org/sqoop/${SQOOP_VERSION}/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz; \
    tar -xzf /tmp/sqoop.tgz -C /opt && mv /opt/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0 /opt/sqoop && rm -f /tmp/sqoop.tgz; \
    curl -fsSL --retry 5 --retry-all-errors -o /opt/sqoop/lib/mysql-connector-java-5.1.49.jar https://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.49/mysql-connector-java-5.1.49.jar; \
    curl -fsSL --retry 5 --retry-all-errors -o /opt/sqoop/lib/commons-lang-2.6.jar https://repo1.maven.org/maven2/commons-lang/commons-lang/2.6/commons-lang-2.6.jar; \
    mkdir -p /tmp/none

COPY core-site.xml mapred-site.xml /opt/hadoop/etc/hadoop/
WORKDIR /opt/sqoop
