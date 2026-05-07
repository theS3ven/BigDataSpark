#!/usr/bin/env bash
set -e

CH_JDBC_JAR=/opt/extra-jars/clickhouse-jdbc-0.2.6.jar,/opt/extra-jars/guava-31.1-jre.jar

echo "=== [1/2] Star Schema ETL: mock_data → PostgreSQL star schema ==="
docker exec spark_master /opt/spark/bin/spark-submit \
  --master "local[*]" \
  /opt/spark-jobs/etl_star_schema.py

echo ""
echo "=== [2/2] ClickHouse ETL: PostgreSQL star schema → ClickHouse reports ==="
docker exec spark_master /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --jars "$CH_JDBC_JAR" \
  /opt/spark-jobs/etl_clickhouse.py

echo ""
echo "=== Все ETL-джобы выполнены ==="
