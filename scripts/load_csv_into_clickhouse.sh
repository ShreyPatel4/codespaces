#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/load_csv_into_clickhouse.sh -s <source_dir>

Loads generated CSV files into ClickHouse using the HTTP interface.

Environment variables:
  CLICKHOUSE_URL       ClickHouse HTTP endpoint (default: http://localhost:8123)
  CLICKHOUSE_USER      Username (default: default)
  CLICKHOUSE_PASSWORD  Password (default: default)
EOF
}

SOURCE_DIR=""
CLICKHOUSE_URL=${CLICKHOUSE_URL:-http://localhost:8123}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-default}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-default}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -s|--source)
      SOURCE_DIR="$2"
      shift 2
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

create_table() {
  local table="$1"
  case "$table" in
    fibersqs_app_logs)
      run_sql "CREATE TABLE IF NOT EXISTS fibersqs_app_logs (\
timestamp DateTime64(3),\
region String,\
cluster String,\
service String,\
host String,\
level String,\
transaction_id String,\
trace_id String,\
span_id String,\
customer_id String,\
transaction_type String,\
event String,\
dependency_region String,\
dependency_service String,\
dependency_latency_ms Nullable(Int32),\
end_to_end_latency_ms Nullable(Float64),\
http_status String,\
error_code String,\
circuit_id String,\
message String\
) ENGINE = MergeTree ORDER BY (timestamp, transaction_id)";
      ;;
    trace_spans)
      run_sql "CREATE TABLE IF NOT EXISTS trace_spans (\
timestamp DateTime64(3),\
trace_id String,\
span_id String,\
parent_span_id String,\
transaction_id String,\
region String,\
service String,\
operation String,\
duration_ms Float64,\
status String,\
circuit_id String\
) ENGINE = MergeTree ORDER BY (timestamp, trace_id, span_id)";
      ;;
    network_circuit_metrics)
      run_sql "CREATE TABLE IF NOT EXISTS network_circuit_metrics (\
timestamp DateTime,\
src_region String,\
dst_region String,\
circuit_id String,\
rtt_ms Float64,\
packet_loss_pct Float64,\
retransmits_per_s Float64,\
throughput_mbps Float64\
) ENGINE = MergeTree ORDER BY (timestamp, circuit_id)";
      ;;
    infra_host_metrics)
      run_sql "CREATE TABLE IF NOT EXISTS infra_host_metrics (\
timestamp DateTime,\
region String,\
host String,\
cpu_pct Float64,\
mem_pct Float64,\
disk_io_util_pct Float64,\
net_errs_per_s Float64\
) ENGINE = MergeTree ORDER BY (timestamp, host)";
      ;;
    tso_calls)
      run_sql "CREATE TABLE IF NOT EXISTS tso_calls (\
call_id String,\
timestamp DateTime64(3),\
customer_id String,\
customer_region String,\
issue_category String,\
issue_description String,\
service_type String,\
transaction_id String,\
resolution_time_minutes Int32,\
escalated String,\
resolution_code String\
) ENGINE = MergeTree ORDER BY (timestamp, call_id)";
      ;;
    fibersqs_service_metrics_1m)
      run_sql "CREATE TABLE IF NOT EXISTS fibersqs_service_metrics_1m (\
timestamp DateTime64(3),\
region String,\
transaction_type String,\
req_count Int32,\
p50_latency_ms Float64,\
p95_latency_ms Float64,\
timeout_rate Float64,\
retry_rate Float64,\
queue_depth Float64\
) ENGINE = MergeTree ORDER BY (timestamp, region, transaction_type)";
      ;;
    network_events_alarms)
      run_sql "CREATE TABLE IF NOT EXISTS network_events_alarms (\
event_id String,\
ts DateTime64(3),\
source String,\
region String,\
circuit_id String,\
severity String,\
alert_name String,\
fired String,\
cleared_ts DateTime64(3),\
predicted_cause String,\
linked_incident_id String\
) ENGINE = MergeTree ORDER BY (ts, event_id)";
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
  curl -sS -f -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
    --data-binary @"$filepath" \
    "$CLICKHOUSE_URL/?query=INSERT%20INTO%20$table%20FORMAT%20CSVWithNames" >/dev/null
}

echo "[load_csv_into_clickhouse] Loading CSVs from $SOURCE_DIR"

loaded_any=false
shopt -s nullglob
for filepath in "$SOURCE_DIR"/clickhouse-*.csv; do
  filename=$(basename "$filepath")
  table="${filename#clickhouse-}"
  table="${table%.csv}"
  echo "[load_csv_into_clickhouse] Preparing table $table"
  create_table "$table"
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
