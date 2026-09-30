#!/usr/bin/env bash
# One-time HDFS setup (container rh-hdfs-init). Kept in a file because the Hadoop image starts its command with an
# unquoted `exec $@`, which would split an inline `bash -c "..."` script into separate words.
# 1. leave safe mode after an unclean shutdown
# 2. remove only CORRUPT checkpoint files (the stream jobs then restart from Kafka; Silver drops any duplicate)
# 3. create the HDFS folders
timeout 120 hdfs dfsadmin -safemode wait || hdfs dfsadmin -safemode leave
hdfs fsck /checkpoints -delete >/dev/null 2>&1 || true
hdfs dfs -mkdir -p /raw/mysql /curated /warehouse /reports /checkpoints /tmp /user/hive/warehouse
hdfs dfs -chmod -R 777 /raw /curated /warehouse /reports /checkpoints /tmp /user
echo "HDFS folders ready"
