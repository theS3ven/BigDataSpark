#!/usr/bin/env bash
set -e

echo "=== [1/2] Star Schema ETL: mock_data → PostgreSQL star schema ==="
docker exec spark_master /opt/spark/bin/spark-submit \
  --master "local[*]" \
  /opt/spark-jobs/etl_star_schema.py

echo ""
echo "=== [2/2] ClickHouse ETL: PostgreSQL star schema → ClickHouse reports ==="
docker exec spark_master /opt/spark/bin/spark-submit \
  --master "local[*]" \
  /opt/spark-jobs/etl_clickhouse.py

echo ""
echo "=== Все ETL-джобы выполнены ==="
