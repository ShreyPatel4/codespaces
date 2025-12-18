#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/load_csv_into_clickhouse.sh -s <source_dir> [--database <db>] [--insert-only]

Loads generated CSV files into ClickHouse using the HTTP interface.

Environment variables:
  CLICKHOUSE_URL       ClickHouse HTTP endpoint (default: http://localhost:8123)
  CLICKHOUSE_USER      Username (default: default)
  CLICKHOUSE_PASSWORD  Password (default: default)
EOF
}

SOURCE_DIR=""
TARGET_DB=""
INSERT_ONLY=false
CLICKHOUSE_URL=${CLICKHOUSE_URL:-http://localhost:8123}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-default}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-default}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -s|--source)
      SOURCE_DIR="$2"
      shift 2
      ;;
    -d|--database)
      TARGET_DB="$2"
      shift 2
      ;;
    --insert-only)
      INSERT_ONLY=true
      shift 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[load_csv_into_clickhouse] Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$SOURCE_DIR" ]]; then
  echo "[load_csv_into_clickhouse] Missing required --source/-s argument" >&2
  usage
  exit 1
fi

if [[ ! -d "$SOURCE_DIR" ]]; then
  echo "[load_csv_into_clickhouse] Source directory not found: $SOURCE_DIR" >&2
  exit 1
fi

run_sql() {
  local sql="$1"
  curl -sS -f -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" --data-binary "$sql" "$CLICKHOUSE_URL/" >/dev/null
}

qualify_table() {
  local table="$1"
  if [[ -n "$TARGET_DB" ]]; then
    echo "${TARGET_DB}.${table}"
  else
    echo "$table"
  fi
}

table_exists() {
  local qualified_table="$1"
  local exists
  exists="$(curl -sS -f -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" --data-binary "EXISTS TABLE ${qualified_table}" "$CLICKHOUSE_URL/")"
  [[ "${exists//$'\n'/}" == "1" ]]
}

create_table() {
  local table="$1"
  local qualified_table
  qualified_table="$(qualify_table "$table")"
  case "$table" in
    app_logs|fibersqs_app_logs)
      run_sql "CREATE TABLE IF NOT EXISTS ${qualified_table} (\
timestamp String,\
region LowCardinality(String),\
cluster LowCardinality(String),\
service LowCardinality(String),\
host LowCardinality(String),\
level LowCardinality(String),\
transaction_id String,\
trace_id String,\
span_id String,\
customer_id String,\
transaction_type LowCardinality(String),\
event LowCardinality(String),\
dependency_region LowCardinality(String),\
dependency_service LowCardinality(String),\
dependency_latency_ms String,\
end_to_end_latency_ms String,\
http_status LowCardinality(String),\
error_code LowCardinality(String),\
circuit_id String,\
message String,\
ts DateTime64(3) MATERIALIZED parseDateTimeBestEffort64Milli(timestamp),\
end_to_end_latency_ms_f64 Float64 MATERIALIZED toFloat64OrZero(end_to_end_latency_ms)\
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (region, transaction_type, event, ts, transaction_id)";
      ;;
    trace_spans)
      run_sql "CREATE TABLE IF NOT EXISTS ${qualified_table} (\
timestamp String,\
trace_id String,\
span_id String,\
parent_span_id String,\
transaction_id String,\
region LowCardinality(String),\
service LowCardinality(String),\
operation LowCardinality(String),\
duration_ms Float64,\
status LowCardinality(String),\
circuit_id String,\
ts DateTime64(3) MATERIALIZED parseDateTimeBestEffort64Milli(timestamp)\
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (ts, trace_id, span_id)";
      ;;
    network_circuit_metrics)
      run_sql "CREATE TABLE IF NOT EXISTS ${qualified_table} (\
timestamp String,\
src_region LowCardinality(String),\
dst_region LowCardinality(String),\
circuit_id String,\
rtt_ms Float64,\
packet_loss_pct Float64,\
retransmits_per_s Float64,\
throughput_mbps Float64,\
ts DateTime64(3) MATERIALIZED parseDateTimeBestEffort64Milli(timestamp)\
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (circuit_id, ts)";
      ;;
    infra_host_metrics)
      run_sql "CREATE TABLE IF NOT EXISTS ${qualified_table} (\
timestamp String,\
region LowCardinality(String),\
host LowCardinality(String),\
cpu_pct Float64,\
mem_pct Float64,\
disk_io_util_pct Float64,\
net_errs_per_s Float64,\
ts DateTime64(3) MATERIALIZED parseDateTimeBestEffort64Milli(timestamp)\
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (host, ts)";
      ;;
    tso_calls)
      run_sql "CREATE TABLE IF NOT EXISTS ${qualified_table} (\
call_id String,\
timestamp String,\
customer_id String,\
customer_region LowCardinality(String),\
issue_category LowCardinality(String),\
issue_description String,\
service_type LowCardinality(String),\
transaction_id String,\
resolution_time_minutes Int32,\
escalated LowCardinality(String),\
resolution_code LowCardinality(String),\
ts DateTime64(3) MATERIALIZED parseDateTimeBestEffort64Milli(timestamp)\
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (ts, call_id)";
      ;;
    service_metrics|fibersqs_service_metrics_1m)
      run_sql "CREATE TABLE IF NOT EXISTS ${qualified_table} (\
timestamp String,\
region LowCardinality(String),\
transaction_type LowCardinality(String),\
request_rate Float64,\
error_rate Float64,\
p95_latency_ms Float64,\
p99_latency_ms Float64,\
saturation Float64,\
ts DateTime64(3) MATERIALIZED parseDateTimeBestEffort64Milli(timestamp)\
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (region, transaction_type, ts)";
      ;;
    network_events|network_events_alarms)
      run_sql "CREATE TABLE IF NOT EXISTS ${qualified_table} (\
event_id String,\
timestamp String,\
event_type LowCardinality(String),\
src_region LowCardinality(String),\
dst_region LowCardinality(String),\
circuit_id String,\
severity LowCardinality(String),\
description String,\
ts DateTime64(3) MATERIALIZED parseDateTimeBestEffort64Milli(timestamp)\
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (ts, event_id)";
      ;;
    txn_facts)
      run_sql "CREATE TABLE IF NOT EXISTS ${qualified_table} (\
transaction_id String,\
customer_id String,\
origin_region LowCardinality(String),\
txn_type LowCardinality(String),\
start_ts String,\
end_ts String,\
success LowCardinality(String),\
error_code LowCardinality(String),\
end_to_end_latency_ms Float64,\
start_dt DateTime64(3) MATERIALIZED parseDateTimeBestEffort64Milli(start_ts),\
end_dt DateTime64(3) MATERIALIZED parseDateTimeBestEffort64Milli(end_ts)\
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(start_dt) ORDER BY (origin_region, txn_type, start_dt, transaction_id)";
      ;;
    *)
      echo "[load_csv_into_clickhouse] Unknown table: $table" >&2
      exit 1
      ;;
  esac
}

insert_file() {
  local table="$1"
  local filepath="$2"
  local qualified_table
  qualified_table="$(qualify_table "$table")"
  curl -sS -f -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
    --data-binary @"$filepath" \
    "$CLICKHOUSE_URL/?query=INSERT%20INTO%20$qualified_table%20FORMAT%20CSVWithNames" >/dev/null
}

echo "[load_csv_into_clickhouse] Loading CSVs from $SOURCE_DIR"

loaded_any=false
shopt -s nullglob
for filepath in "$SOURCE_DIR"/clickhouse-*.csv; do
  filename=$(basename "$filepath")
  table="${filename#clickhouse-}"
  table="${table%.csv}"
  echo "[load_csv_into_clickhouse] Preparing table $table"
  if [[ "$INSERT_ONLY" == true ]]; then
    qualified_table="$(qualify_table "$table")"
    if ! table_exists "$qualified_table"; then
      echo "[load_csv_into_clickhouse] Missing table (insert-only mode): $qualified_table" >&2
      exit 1
    fi
  else
    create_table "$table"
  fi
  echo "[load_csv_into_clickhouse] Inserting $(basename "$filepath")"
  insert_file "$table" "$filepath"
  loaded_any=true
done
shopt -u nullglob

if [[ "$loaded_any" == false ]]; then
  echo "[load_csv_into_clickhouse] No CSV files found under $SOURCE_DIR" >&2
  exit 1
fi

echo "[load_csv_into_clickhouse] Completed load"
