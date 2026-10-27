#!/usr/bin/env bash
# Apply every clickhouse/init/*.sql (in name order) to the running ClickHouse container.
# Safe on an existing database: all statements are CREATE/ALTER ... IF NOT EXISTS. Never removes volumes.
set -euo pipefail
cd "$(dirname "$0")/.."

for file in clickhouse/init/*.sql; do
  echo "Applying ${file}"
  docker compose exec -T clickhouse sh -c \
    'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' < "${file}"
done

docker compose exec -T clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SHOW TABLES FROM pipeline"'
