#!/usr/bin/env bash
set -euo pipefail

OUTPUT_DIR="/tmp/fibersqs"
SEED=7
APP_LOG_ROWS=15000000
SCALE_LOGS=1.0
ZIP_FLAG="--zip"
ENABLE_TIER2=false

CLICKHOUSE_URL=${CLICKHOUSE_URL:-http://localhost:8123}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-default}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-default}

RAW_CLICKHOUSE_BASE="${CLICKHOUSE_URL%%\?*}"
RAW_CLICKHOUSE_BASE="${RAW_CLICKHOUSE_BASE%/}"
CLICKHOUSE_BASE="${RAW_CLICKHOUSE_BASE%/}"

export CLICKHOUSE_URL="$CLICKHOUSE_BASE"

if [[ -z "$CLICKHOUSE_BASE" ]]; then
  echo "[regen_fibersqs] Invalid CLICKHOUSE_URL: ${CLICKHOUSE_URL}" >&2
  exit 1
fi

REGIONS=("east" "west" "central" "north" "south")

log() {
  echo "[regen_fibersqs][$(date -Iseconds)] $*"
}

usage() {
  cat <<'EOF'
Usage: scripts-dev/regen_fibersqs.sh [options]

Options:
  --output_dir <path>    Destination for generated dataset (default: /tmp/fibersqs)
  --seed <int>           RNG seed for deterministic regeneration (default: 7)
  --app_log_rows <int>   Baseline app-log rows before scaling (default: 15000000)
  --scale_logs <float>   Scale factor applied to app_log_rows (default: 1.0)
  --enable_tier2         Generate optional tier2 tables (default: off)
  --zip                  Package the dataset zip (default)
  --no-zip               Skip packaging
  -h, --help             Show this message

Environment overrides:
  CLICKHOUSE_URL (default: http://localhost:8123)
  CLICKHOUSE_USER (default: default)
  CLICKHOUSE_PASSWORD (default: default)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output_dir)
      OUTPUT_DIR="$2"; shift 2 ;;
    --seed)
      SEED="$2"; shift 2 ;;
    --app_log_rows)
      APP_LOG_ROWS="$2"; shift 2 ;;
    --scale_logs)
      SCALE_LOGS="$2"; shift 2 ;;
    --enable_tier2)
      ENABLE_TIER2=true; shift 1 ;;
    --zip)
      ZIP_FLAG="--zip"; shift 1 ;;
    --no-zip)
      ZIP_FLAG="--no-zip"; shift 1 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "[regen_fibersqs] Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$OUTPUT_DIR" || "$OUTPUT_DIR" == "/" ]]; then
  echo "[regen_fibersqs] Refusing to operate on empty or root output_dir" >&2
  exit 1
fi

log "Checking ClickHouse availability at ${CLICKHOUSE_BASE}/"
ping_body="$(clickhouse_get "/ping")"
if [[ "${ping_body//$'\n'/}" != "Ok." ]]; then
  echo "[regen_fibersqs] ClickHouse ping failed or unexpected response: ${ping_body}" >&2
  exit 1
fi

# ClickHouse query helpers (HTTP POST only; fail fast on 4xx/5xx)
clickhouse_post() {
  local sql="$1"
  local path="${2:-/}"
  local tmp
  tmp="$(mktemp)"
  local status
  local curl_exit=0
  status=$(curl -sS -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
    --data-binary "$sql" \
    -o "$tmp" \
    -w '%{http_code}' \
    "${CLICKHOUSE_BASE}${path}") || curl_exit=$?
  if [[ $curl_exit -ne 0 ]]; then
    echo "[regen_fibersqs] curl failed (exit=${curl_exit}) for ${CLICKHOUSE_BASE}${path}" >&2
    if [[ -s "$tmp" ]]; then
      echo "[regen_fibersqs] Response body:" >&2
      cat "$tmp" >&2
    fi
    rm -f "$tmp"
    exit 1
  fi
  if [[ "$status" -lt 200 || "$status" -ge 300 ]]; then
    echo "[regen_fibersqs] ClickHouse request failed (status=${status}) for ${CLICKHOUSE_BASE}${path}" >&2
    if [[ -s "$tmp" ]]; then
      cat "$tmp" >&2
    fi
    rm -f "$tmp"
    exit 1
  fi
  cat "$tmp"
  rm -f "$tmp"
}

clickhouse_get() {
  local path="$1"
  local tmp
  tmp="$(mktemp)"
  local status
  local curl_exit=0
  status=$(curl -sS -X GET -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
    -o "$tmp" \
    -w '%{http_code}' \
    "${CLICKHOUSE_BASE}${path}") || curl_exit=$?
  if [[ $curl_exit -ne 0 ]]; then
    echo "[regen_fibersqs] curl failed (exit=${curl_exit}) for ${CLICKHOUSE_BASE}${path}" >&2
    if [[ -s "$tmp" ]]; then
      echo "[regen_fibersqs] Response body:" >&2
      cat "$tmp" >&2
    fi
    rm -f "$tmp"
    exit 1
  fi
  if [[ "$status" -lt 200 || "$status" -ge 300 ]]; then
    echo "[regen_fibersqs] ClickHouse GET failed (status=${status}) for ${CLICKHOUSE_BASE}${path}" >&2
    if [[ -s "$tmp" ]]; then
      cat "$tmp" >&2
    fi
    rm -f "$tmp"
    exit 1
  fi
  cat "$tmp"
  rm -f "$tmp"
}

run_sql() {
  local sql="$1"
  clickhouse_post "$sql" "/" >/dev/null
}

run_sql_raw() {
  local sql="$1"
  clickhouse_post "$sql" "/"
}

run_sql_json() {
  local sql="$1"
  run_sql_raw "$sql FORMAT JSON"
}

db_exists() {
  local db="$1"
  local out
  out="$(run_sql_raw "SELECT count() FROM system.databases WHERE name='${db}'")"
  out="${out//$'\n'/}"
  [[ "$out" != "0" ]]
}

detect_generator() {
  local script_dir dataset_root
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  dataset_root="$(cd "$script_dir/.." && pwd)"
  if [[ -f "$dataset_root/generate_dataset.py" ]]; then
    echo "$dataset_root/generate_dataset.py"
    return 0
  fi
  echo "[regen_fibersqs] Unable to find generate_dataset.py under $dataset_root" >&2
  exit 1
}

GENERATOR_SCRIPT="$(detect_generator)"
DATASET_ROOT="$(cd "$(dirname "$GENERATOR_SCRIPT")" && pwd)"
LOADER_SCRIPT="$DATASET_ROOT/scripts/load_csv_into_clickhouse.sh"

if [[ ! -x "$LOADER_SCRIPT" ]]; then
  echo "[regen_fibersqs] Loader script not executable: $LOADER_SCRIPT" >&2
  exit 1
fi

log "Cleaning output directory $OUTPUT_DIR"
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

COMPAT_DB="default"
if db_exists "logs"; then
  COMPAT_DB="logs"
fi

log "Dropping existing ClickHouse dataset objects"
run_sql "CREATE DATABASE IF NOT EXISTS fibersqs_prod"

# Drop compatibility views first (safe if absent).
for view in fibersqs_app_logs trace_spans network_circuit_metrics infra_host_metrics tso_calls; do
  run_sql "DROP VIEW IF EXISTS ${COMPAT_DB}.${view}"
done

# Drop ES-style views (region + daily shards) from older runs.
for region in "${REGIONS[@]}"; do
  run_sql "$(printf 'DROP VIEW IF EXISTS fibersqs_prod.`fibersqs-prod-%s-app-logs`' "$region")"
  run_sql "$(printf 'DROP VIEW IF EXISTS fibersqs_prod.fibersqs_prod_%s_app_logs' "$region")"
done
index_views_to_drop="$(run_sql_raw "SELECT name FROM system.tables WHERE database='fibersqs_prod' AND engine='View' AND (name LIKE 'fibersqs-prod-%-app-logs-%' OR name LIKE 'fibersqs_prod_%_app_logs_%') FORMAT TSVRaw")"
while IFS= read -r view_name; do
  [[ -z "$view_name" ]] && continue
  run_sql "$(printf 'DROP VIEW IF EXISTS fibersqs_prod.`%s`' "$view_name")"
done <<< "$index_views_to_drop"

# Drop canonical tables.
for table in app_logs trace_spans network_circuit_metrics infra_host_metrics tso_calls service_metrics network_events txn_facts; do
  run_sql "DROP TABLE IF EXISTS fibersqs_prod.${table}"
done

# Drop legacy dataset-specific tables in the working db (from older runs).
for table in fibersqs_service_metrics_1m network_events_alarms service_metrics network_events txn_facts app_logs; do
  run_sql "DROP TABLE IF EXISTS ${COMPAT_DB}.${table}"
done

create_app_logs_table() {
  local with_indexes
  with_indexes="CREATE TABLE fibersqs_prod.app_logs (\
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
end_to_end_latency_ms_f64 Float64 MATERIALIZED toFloat64OrZero(end_to_end_latency_ms),\
INDEX idx_transaction_id transaction_id TYPE bloom_filter(0.01) GRANULARITY 1,\
INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1\
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (region, transaction_type, event, ts, transaction_id)"

  if run_sql "$with_indexes"; then
    return 0
  fi

  log "Bloom filter indexes not supported; creating fibersqs_prod.app_logs without data-skipping indexes"
  run_sql "CREATE TABLE fibersqs_prod.app_logs (\
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
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (region, transaction_type, event, ts, transaction_id)"
}

log "Creating canonical ClickHouse tables (fibersqs_prod.*)"
create_app_logs_table
run_sql "CREATE TABLE fibersqs_prod.trace_spans (\
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
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (ts, trace_id, span_id)"

run_sql "CREATE TABLE fibersqs_prod.network_circuit_metrics (\
timestamp String,\
src_region LowCardinality(String),\
dst_region LowCardinality(String),\
circuit_id String,\
rtt_ms Float64,\
packet_loss_pct Float64,\
retransmits_per_s Float64,\
throughput_mbps Float64,\
ts DateTime64(3) MATERIALIZED parseDateTimeBestEffort64Milli(timestamp)\
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (circuit_id, ts)"

run_sql "CREATE TABLE fibersqs_prod.infra_host_metrics (\
timestamp String,\
region LowCardinality(String),\
host LowCardinality(String),\
cpu_pct Float64,\
mem_pct Float64,\
disk_io_util_pct Float64,\
net_errs_per_s Float64,\
ts DateTime64(3) MATERIALIZED parseDateTimeBestEffort64Milli(timestamp)\
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (host, ts)"

run_sql "CREATE TABLE fibersqs_prod.tso_calls (\
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
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (ts, call_id)"

if [[ "$ENABLE_TIER2" == true ]]; then
  run_sql "CREATE TABLE fibersqs_prod.service_metrics (\
timestamp String,\
region LowCardinality(String),\
transaction_type LowCardinality(String),\
request_rate Float64,\
error_rate Float64,\
p95_latency_ms Float64,\
p99_latency_ms Float64,\
saturation Float64,\
ts DateTime64(3) MATERIALIZED parseDateTimeBestEffort64Milli(timestamp)\
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (region, transaction_type, ts)"

  run_sql "CREATE TABLE fibersqs_prod.network_events (\
event_id String,\
timestamp String,\
event_type LowCardinality(String),\
src_region LowCardinality(String),\
dst_region LowCardinality(String),\
circuit_id String,\
severity LowCardinality(String),\
description String,\
ts DateTime64(3) MATERIALIZED parseDateTimeBestEffort64Milli(timestamp)\
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY (ts, event_id)"

  run_sql "CREATE TABLE fibersqs_prod.txn_facts (\
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
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(start_dt) ORDER BY (origin_region, txn_type, start_dt, transaction_id)"
fi

log "Generating dataset with $GENERATOR_SCRIPT"
GENERATOR_ARGS=()
if [[ "$ENABLE_TIER2" == true ]]; then
  GENERATOR_ARGS+=(--enable_tier2)
fi

python "$GENERATOR_SCRIPT" \
  --output_dir "$OUTPUT_DIR" \
  --seed "$SEED" \
  --app_log_rows "$APP_LOG_ROWS" \
  --scale_logs "$SCALE_LOGS" \
  "${GENERATOR_ARGS[@]}" \
  $ZIP_FLAG

log "Loading CSVs into ClickHouse (fibersqs_prod.*)"
"$LOADER_SCRIPT" -s "$OUTPUT_DIR/data" --database fibersqs_prod --insert-only

log "Creating compatibility views in ${COMPAT_DB} (backward compatible names)"
run_sql "CREATE OR REPLACE VIEW ${COMPAT_DB}.fibersqs_app_logs AS SELECT * FROM fibersqs_prod.app_logs"
run_sql "CREATE OR REPLACE VIEW ${COMPAT_DB}.trace_spans AS SELECT * FROM fibersqs_prod.trace_spans"
run_sql "CREATE OR REPLACE VIEW ${COMPAT_DB}.network_circuit_metrics AS SELECT * FROM fibersqs_prod.network_circuit_metrics"
run_sql "CREATE OR REPLACE VIEW ${COMPAT_DB}.infra_host_metrics AS SELECT * FROM fibersqs_prod.infra_host_metrics"
run_sql "CREATE OR REPLACE VIEW ${COMPAT_DB}.tso_calls AS SELECT * FROM fibersqs_prod.tso_calls"

APP_LOG_SOURCE="${COMPAT_DB}.fibersqs_app_logs"
TRACE_SOURCE="${COMPAT_DB}.trace_spans"
NETWORK_SOURCE="${COMPAT_DB}.network_circuit_metrics"
INFRA_SOURCE="${COMPAT_DB}.infra_host_metrics"
TSO_SOURCE="${COMPAT_DB}.tso_calls"

log "Creating ES-style app log views (region + daily shards)"
day_span_json="$(run_sql_json "SELECT toString(min(toDate(parseDateTimeBestEffort(timestamp)))) AS min_day, toString(max(toDate(parseDateTimeBestEffort(timestamp)))) AS max_day FROM ${APP_LOG_SOURCE}")"
export DAY_SPAN_JSON="$day_span_json"
read -r MIN_DAY MAX_DAY <<< "$(python - <<'PY'
import json, os
payload = os.environ.get("DAY_SPAN_JSON", "{}")
data = json.loads(payload).get("data", [{}])[0]
min_day = (data.get("min_day") or "").strip()
max_day = (data.get("max_day") or "").strip()
print(min_day, max_day)
PY
)"

if [[ -z "$MIN_DAY" || -z "$MAX_DAY" || "$MIN_DAY" == "\\N" || "$MAX_DAY" == "\\N" ]]; then
  echo "[regen_fibersqs] Unable to determine day span from ${APP_LOG_SOURCE}" >&2
  exit 1
fi

export MIN_DAY
export MAX_DAY

region_view_count=0
for region in "${REGIONS[@]}"; do
  run_sql "$(printf 'CREATE VIEW IF NOT EXISTS fibersqs_prod.fibersqs_prod_%s_app_logs AS SELECT * FROM %s WHERE region='\''%s'\''' "$region" "$APP_LOG_SOURCE" "$region")"
  ((region_view_count++))
done

day_pairs="$(python - <<'PY'
from __future__ import annotations

from datetime import datetime, timedelta
import os

start = datetime.strptime(os.environ["MIN_DAY"], "%Y-%m-%d").date()
end = datetime.strptime(os.environ["MAX_DAY"], "%Y-%m-%d").date()
current = start
while current <= end:
    print(current.strftime("%Y-%m-%d"), current.strftime("%Y%m%d"))
    current += timedelta(days=1)
PY
)"

daily_view_count=0
while read -r day_iso day_suffix; do
  [[ -z "$day_iso" ]] && continue
  for region in "${REGIONS[@]}"; do
    view_name="fibersqs_prod_${region}_app_logs_${day_suffix}"
    run_sql "$(printf 'CREATE VIEW IF NOT EXISTS fibersqs_prod.%s AS SELECT * FROM %s WHERE region='\''%s'\'' AND toDate(parseDateTimeBestEffort(timestamp))=toDate('\''%s'\'')' "$view_name" "$APP_LOG_SOURCE" "$region" "$day_iso")"
    ((daily_view_count++))
  done
done <<< "$day_pairs"

VIEW_TABLE_LISTING="$(run_sql_raw "SHOW TABLES FROM fibersqs_prod LIKE 'fibersqs_prod_%_app_logs%'")"
VIEW_COUNT_JSON="$(run_sql_json "SELECT
  countIf(match(name, '^fibersqs_prod_[a-z]+_app_logs$')) AS region_views,
  countIf(match(name, '^fibersqs_prod_[a-z]+_app_logs_[0-9]{8}$')) AS daily_views
FROM system.tables
WHERE database='fibersqs_prod' AND engine='View' AND name LIKE 'fibersqs_prod_%_app_logs%'")"

REGION_DISTRIBUTION_JSON="$(run_sql_json "SELECT region, count() AS rows FROM ${APP_LOG_SOURCE} GROUP BY region ORDER BY region")"

export DAY_SPAN_JSON="$day_span_json"
export VIEW_COUNT_JSON
export VIEW_TABLE_LISTING
export REGION_DISTRIBUTION_JSON
export REGION_VIEW_COUNT="$region_view_count"
export DAILY_VIEW_COUNT="$daily_view_count"

log "Collecting post-load validation metrics"

export OUTPUT_DIR

export IMPACTED_VOLUME_JSON="$(
  run_sql_json "WITH per_minute AS (
    SELECT
      toStartOfMinute(parseDateTimeBestEffort(timestamp)) AS minute,
      count() AS reqs
    FROM ${APP_LOG_SOURCE}
    WHERE event='received'
      AND region='central'
      AND transaction_type IN ('provision_fiber_sqs','modify_service_profile')
    GROUP BY minute
  )
  SELECT
    quantileTDigest(0.50)(reqs) AS median_rpm,
    quantileTDigest(0.95)(reqs) AS p95_rpm,
    max(reqs) AS max_rpm
  FROM per_minute"
)"

FIX_TIME="$(python - <<'PY'
import json, os
from pathlib import Path
output_dir = os.environ["OUTPUT_DIR"]
gt = Path(output_dir) / "ground_truth.json"
data = json.loads(gt.read_text(encoding="utf-8"))
print(data["primary_incident"]["fix_time"])
PY
)"
FIX_TIME="${FIX_TIME//$'\n'/}"

export FIX_VERIFICATION_JSON="$(
  run_sql_json "WITH toDateTime64('$FIX_TIME', 3) AS fix
  SELECT
    if(log_ts < fix, 'pre_fix', 'post_fix') AS window,
    quantileTDigest(0.95)(lat) AS p95_latency_ms,
    quantileTDigest(0.99)(lat) AS p99_latency_ms,
    round(countIf(event='timeout')/count(), 4) AS timeout_rate,
    count() AS n
  FROM (
    SELECT
      parseDateTimeBestEffort(timestamp) AS log_ts,
      toFloat64OrZero(end_to_end_latency_ms) AS lat,
      event
    FROM ${APP_LOG_SOURCE}
    WHERE event IN ('completed','timeout')
      AND region='central'
      AND transaction_type IN ('provision_fiber_sqs','modify_service_profile')
  )
  GROUP BY window
  ORDER BY window"
)"

export TSO_MISSING_JSON="$(
  run_sql_json "SELECT
    round(countIf(transaction_id='')/count(),4) AS missing_rate,
    count() AS total
  FROM ${TSO_SOURCE}"
)"

export TSO_NONEXISTENT_JSON="$(
  run_sql_json "WITH logs AS (
    SELECT DISTINCT transaction_id
    FROM ${APP_LOG_SOURCE}
    WHERE event='received'
  ),
  denom AS (SELECT countIf(transaction_id!='') AS calls_with_txn FROM ${TSO_SOURCE})
  SELECT
    round(
      if((SELECT calls_with_txn FROM denom)=0, 0,
        countIf(transaction_id!='' AND transaction_id NOT IN (SELECT transaction_id FROM logs)) /
        (SELECT calls_with_txn FROM denom)
      ), 4
    ) AS nonexistent_rate"
)"

export TSO_MISMATCH_JSON="$(
  run_sql_json "WITH log_txn AS (
    SELECT transaction_id, any(customer_id) AS log_customer_id
    FROM ${APP_LOG_SOURCE}
    WHERE event='received'
    GROUP BY transaction_id
  )
  SELECT
    round(countIf(t.customer_id != l.log_customer_id)/count(), 4) AS mismatch_rate
  FROM ${TSO_SOURCE} t
  INNER JOIN log_txn l USING (transaction_id)
  WHERE t.transaction_id!=''"
)"

export TSO_CONFUSION_JSON="$(
  run_sql_json "WITH log_txn AS (
    SELECT transaction_id, any(customer_id) AS log_customer_id
    FROM ${APP_LOG_SOURCE}
    WHERE event='received'
    GROUP BY transaction_id
  ),
  base AS (
    SELECT
      countIf(transaction_id='') AS fn,
      countIf(transaction_id!='' AND log_customer_id IS NULL) AS fp,
      countIf(transaction_id!='' AND log_customer_id IS NOT NULL AND customer_id != log_customer_id) AS wrong_link,
      countIf(transaction_id!='' AND log_customer_id IS NOT NULL AND customer_id = log_customer_id) AS tp,
      count() AS total_calls,
      countIf(transaction_id!='') AS non_empty_calls
    FROM (
      SELECT t.transaction_id, t.customer_id, l.log_customer_id
      FROM ${TSO_SOURCE} t
      LEFT JOIN log_txn l USING (transaction_id)
    )
  )
  SELECT
    tp AS tso_tp_count,
    fn AS tso_fn_count,
    fp AS tso_fp_count,
    wrong_link AS tso_wrong_link_count,
    round(fn / total_calls, 4) AS tso_fn_rate,
    round(if(non_empty_calls=0, 0, fp / non_empty_calls), 4) AS tso_fp_rate_non_empty,
    round(if((tp + wrong_link)=0, 0, wrong_link / (tp + wrong_link)), 4) AS tso_wrong_link_rate_joined,
    round(if(total_calls=0, 0, tp / total_calls), 4) AS tso_tp_rate
  FROM base"
)"

INCIDENT_START="$(python - <<'PY'
import json, os
from pathlib import Path
output_dir = os.environ["OUTPUT_DIR"]
gt = Path(output_dir) / "ground_truth.json"
data = json.loads(gt.read_text(encoding="utf-8"))
print(data["primary_incident"]["start"])
PY
)"
INCIDENT_START="${INCIDENT_START//$'\n'/}"

INCIDENT_END="$(python - <<'PY'
import json, os
from pathlib import Path
output_dir = os.environ["OUTPUT_DIR"]
gt = Path(output_dir) / "ground_truth.json"
data = json.loads(gt.read_text(encoding="utf-8"))
print(data["primary_incident"]["end"])
PY
)"
INCIDENT_END="${INCIDENT_END//$'\n'/}"

export INCIDENT_MULTIPLIER_JSON="$(
  run_sql_json "WITH
    toDateTime64('${INCIDENT_START}', 3) AS incident_start,
    toDateTime64('${INCIDENT_END}', 3) AS incident_end,
    base AS (
      SELECT
        region,
        quantileTDigestIf(0.95)(lat, log_ts < incident_start) AS p95_baseline,
        quantileTDigestIf(0.95)(lat, log_ts >= incident_start AND log_ts < incident_end) AS p95_incident
      FROM (
        SELECT
          region,
          parseDateTimeBestEffort(timestamp) AS log_ts,
          toFloat64OrZero(end_to_end_latency_ms) AS lat,
          event,
          transaction_type
        FROM ${APP_LOG_SOURCE}
      )
      WHERE event IN ('completed','timeout')
        AND transaction_type IN ('provision_fiber_sqs','modify_service_profile')
      GROUP BY region
    )
  SELECT
    region,
    p95_baseline,
    p95_incident,
    round(if(p95_baseline=0, 0, p95_incident / p95_baseline), 2) AS p95_multiplier
  FROM base
  ORDER BY region"
)"

export TOP_CIRCUIT_UPLIFT_JSON="$(
  run_sql_json "WITH
    toDateTime64('${INCIDENT_START}', 3) AS incident_start,
    toDateTime64('${INCIDENT_END}', 3) AS incident_end,
    base AS (
      SELECT
        circuit_id,
        any(src_region) AS src_region,
        any(dst_region) AS dst_region,
        quantileTDigestIf(0.95)(rtt_ms, parseDateTimeBestEffort(timestamp) < incident_start) AS p95_baseline,
        quantileTDigestIf(0.95)(rtt_ms, parseDateTimeBestEffort(timestamp) >= incident_start AND parseDateTimeBestEffort(timestamp) < incident_end) AS p95_incident
      FROM ${NETWORK_SOURCE}
      WHERE src_region='central' AND dst_region='east'
      GROUP BY circuit_id
    )
  SELECT
    circuit_id,
    src_region,
    dst_region,
    p95_baseline,
    p95_incident,
    round(if(p95_baseline=0, 0, p95_incident / p95_baseline), 2) AS p95_multiplier
  FROM base
  ORDER BY p95_multiplier DESC
  LIMIT 5"
)"

export TIER2_COUNTS_JSON="$(
  run_sql_json "SELECT table, present, rows FROM (
    SELECT
      'service_metrics' AS table,
      (SELECT count() FROM system.tables WHERE database='fibersqs_prod' AND name='service_metrics') AS present,
      (SELECT sum(rows) FROM system.parts WHERE database='fibersqs_prod' AND table='service_metrics') AS rows
    UNION ALL
    SELECT
      'network_events' AS table,
      (SELECT count() FROM system.tables WHERE database='fibersqs_prod' AND name='network_events') AS present,
      (SELECT sum(rows) FROM system.parts WHERE database='fibersqs_prod' AND table='network_events') AS rows
    UNION ALL
    SELECT
      'txn_facts' AS table,
      (SELECT count() FROM system.tables WHERE database='fibersqs_prod' AND name='txn_facts') AS present,
      (SELECT sum(rows) FROM system.parts WHERE database='fibersqs_prod' AND table='txn_facts') AS rows
  )"
)"

python - <<'PY'
import json, os
from pathlib import Path

def first_row(payload: str):
  data = json.loads(payload)
  return (data.get("data") or [{}])[0]

output_dir = Path(os.environ["OUTPUT_DIR"])
report_path = output_dir / "postload_validation.json"

confusion = first_row(os.environ["TSO_CONFUSION_JSON"])
region_distribution = json.loads(os.environ["REGION_DISTRIBUTION_JSON"]).get("data", [])
day_span = first_row(os.environ["DAY_SPAN_JSON"])
view_counts = first_row(os.environ["VIEW_COUNT_JSON"])
view_tables = [line for line in os.environ.get("VIEW_TABLE_LISTING", "").splitlines() if line.strip()]
tier2_counts = json.loads(os.environ["TIER2_COUNTS_JSON"]).get("data", [])

report = {
  "impacted_slice_volume": first_row(os.environ["IMPACTED_VOLUME_JSON"]),
  "dataset_bounds": day_span,
  "fix_verification": json.loads(os.environ["FIX_VERIFICATION_JSON"]).get("data", []),
  "incident_p95_multiplier": json.loads(os.environ["INCIDENT_MULTIPLIER_JSON"]).get("data", []),
  "top_central_to_east_circuit_uplift": json.loads(os.environ["TOP_CIRCUIT_UPLIFT_JSON"]).get("data", []),
  "app_log_region_distribution": region_distribution,
  "view_creation": {
    "expected_region_views": int(os.environ.get("REGION_VIEW_COUNT", "0")),
    "expected_daily_views": int(os.environ.get("DAILY_VIEW_COUNT", "0")),
    "actual_counts": view_counts,
    "tables_like": view_tables,
  },
  "tier2": {"tables": tier2_counts},
  "tso_postload": {
    "missing": first_row(os.environ["TSO_MISSING_JSON"]),
    "nonexistent": first_row(os.environ["TSO_NONEXISTENT_JSON"]),
    "customer_mismatch": first_row(os.environ["TSO_MISMATCH_JSON"]),
    "confusion_matrix": confusion,
  },
  **confusion,
}

report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
print(f"[regen_fibersqs] Wrote validation report to {report_path}")
PY

log "Regeneration workflow complete"
