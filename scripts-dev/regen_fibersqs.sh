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

# ClickHouse query helpers (HTTP POST only; fail fast on 4xx/5xx)
run_sql() {
  local sql="$1"
  curl -sS -f -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
    --data-binary "$sql" \
    "$CLICKHOUSE_URL/" >/dev/null
}

run_sql_raw() {
  local sql="$1"
  curl -sS -f -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
    --data-binary "$sql" \
    "$CLICKHOUSE_URL/"
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

# Drop ES-style views (region + daily shards).
for region in "${REGIONS[@]}"; do
  run_sql "$(printf 'DROP VIEW IF EXISTS fibersqs_prod.`fibersqs-prod-%s-app-logs`' "$region")"
done
daily_views="$(run_sql_raw "SELECT name FROM system.tables WHERE database='fibersqs_prod' AND engine='View' AND name LIKE 'fibersqs-prod-%-app-logs-%' FORMAT TSVRaw")"
while IFS= read -r view_name; do
  [[ -z "$view_name" ]] && continue
  run_sql "$(printf 'DROP VIEW IF EXISTS fibersqs_prod.`%s`' "$view_name")"
done <<< "$daily_views"

# Drop canonical tables.
for table in app_logs trace_spans network_circuit_metrics infra_host_metrics tso_calls service_metrics network_events txn_facts; do
  run_sql "DROP TABLE IF EXISTS fibersqs_prod.${table}"
done

# Drop legacy tables in the working db (from older runs).
for table in fibersqs_app_logs trace_spans network_circuit_metrics infra_host_metrics tso_calls fibersqs_service_metrics_1m network_events_alarms service_metrics network_events txn_facts app_logs; do
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
run_sql "CREATE VIEW ${COMPAT_DB}.fibersqs_app_logs AS SELECT * FROM fibersqs_prod.app_logs"
run_sql "CREATE VIEW ${COMPAT_DB}.trace_spans AS SELECT * FROM fibersqs_prod.trace_spans"
run_sql "CREATE VIEW ${COMPAT_DB}.network_circuit_metrics AS SELECT * FROM fibersqs_prod.network_circuit_metrics"
run_sql "CREATE VIEW ${COMPAT_DB}.infra_host_metrics AS SELECT * FROM fibersqs_prod.infra_host_metrics"
run_sql "CREATE VIEW ${COMPAT_DB}.tso_calls AS SELECT * FROM fibersqs_prod.tso_calls"

log "Creating ES-style app log views (region + daily shards)"
for region in "${REGIONS[@]}"; do
  run_sql "$(printf 'CREATE VIEW fibersqs_prod.`fibersqs-prod-%s-app-logs` AS SELECT * FROM fibersqs_prod.app_logs WHERE region='\''%s'\''' "$region" "$region")"
done

max_day="$(run_sql_raw "SELECT toString(max(toDate(ts))) FROM fibersqs_prod.app_logs")"
max_day="${max_day//$'\n'/}"
if [[ -z "$max_day" || "$max_day" == "\\N" ]]; then
  echo "[regen_fibersqs] Unable to determine max_day from fibersqs_prod.app_logs" >&2
  exit 1
fi
export MAX_DAY="$max_day"

daily_views="$(run_sql_raw "SELECT name FROM system.tables WHERE database='fibersqs_prod' AND engine='View' AND name LIKE 'fibersqs-prod-%-app-logs-%' FORMAT TSVRaw")"
while IFS= read -r view_name; do
  [[ -z "$view_name" ]] && continue
  run_sql "$(printf 'DROP VIEW IF EXISTS fibersqs_prod.`%s`' "$view_name")"
done <<< "$daily_views"

day_pairs="$(python - <<'PY'
from __future__ import annotations

from datetime import datetime, timedelta
import os

max_day = os.environ["MAX_DAY"].strip()
max_date = datetime.strptime(max_day, "%Y-%m-%d").date()
start_date = max_date - timedelta(days=6)
for offset in range(7):
    d = start_date + timedelta(days=offset)
    print(d.strftime("%Y-%m-%d"), d.strftime("%Y%m%d"))
PY
)"

while read -r day_iso day_suffix; do
  [[ -z "$day_iso" ]] && continue
  for region in "${REGIONS[@]}"; do
    view_name="fibersqs-prod-${region}-app-logs-${day_suffix}"
    run_sql "$(printf 'CREATE VIEW fibersqs_prod.`%s` AS SELECT * FROM fibersqs_prod.app_logs WHERE region='\''%s'\'' AND toDate(ts)=toDate('\''%s'\'')' "$view_name" "$region" "$day_iso")"
  done
done <<< "$day_pairs"

log "Collecting post-load validation metrics"

export OUTPUT_DIR

export IMPACTED_VOLUME_JSON="$(
  run_sql_json "WITH per_minute AS (
    SELECT
      toStartOfMinute(ts) AS minute,
      count() AS reqs
    FROM fibersqs_prod.app_logs
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
    if(ts < fix, 'pre_fix', 'post_fix') AS window,
    quantileTDigest(0.95)(lat) AS p95_latency_ms,
    quantileTDigest(0.99)(lat) AS p99_latency_ms,
    round(countIf(event='timeout')/count(), 4) AS timeout_rate,
    count() AS n
  FROM (
    SELECT
      ts,
      end_to_end_latency_ms_f64 AS lat,
      event
    FROM fibersqs_prod.app_logs
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
  FROM fibersqs_prod.tso_calls"
)"

export TSO_NONEXISTENT_JSON="$(
  run_sql_json "WITH logs AS (
    SELECT DISTINCT transaction_id
    FROM fibersqs_prod.app_logs
    WHERE event='received'
  ),
  denom AS (SELECT countIf(transaction_id!='') AS calls_with_txn FROM fibersqs_prod.tso_calls)
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
    FROM fibersqs_prod.app_logs
    WHERE event='received'
    GROUP BY transaction_id
  )
  SELECT
    round(countIf(t.customer_id != l.log_customer_id)/count(), 4) AS mismatch_rate
  FROM fibersqs_prod.tso_calls t
  INNER JOIN log_txn l USING (transaction_id)
  WHERE t.transaction_id!=''"
)"

export TSO_CONFUSION_JSON="$(
  run_sql_json "WITH log_txn AS (
    SELECT transaction_id, any(customer_id) AS log_customer_id
    FROM fibersqs_prod.app_logs
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
      FROM fibersqs_prod.tso_calls t
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
        quantileTDigestIf(0.95)(end_to_end_latency_ms_f64, ts < incident_start) AS p95_baseline,
        quantileTDigestIf(0.95)(end_to_end_latency_ms_f64, ts >= incident_start AND ts < incident_end) AS p95_incident
      FROM fibersqs_prod.app_logs
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
        quantileTDigestIf(0.95)(rtt_ms, ts < incident_start) AS p95_baseline,
        quantileTDigestIf(0.95)(rtt_ms, ts >= incident_start AND ts < incident_end) AS p95_incident
      FROM fibersqs_prod.network_circuit_metrics
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

python - <<'PY'
import json, os
from pathlib import Path

def first_row(payload: str):
  data = json.loads(payload)
  return (data.get("data") or [{}])[0]

output_dir = Path(os.environ["OUTPUT_DIR"])
report_path = output_dir / "postload_validation.json"

confusion = first_row(os.environ["TSO_CONFUSION_JSON"])

report = {
  "impacted_slice_volume": first_row(os.environ["IMPACTED_VOLUME_JSON"]),
  "fix_verification": json.loads(os.environ["FIX_VERIFICATION_JSON"]).get("data", []),
  "incident_p95_multiplier": json.loads(os.environ["INCIDENT_MULTIPLIER_JSON"]).get("data", []),
  "top_central_to_east_circuit_uplift": json.loads(os.environ["TOP_CIRCUIT_UPLIFT_JSON"]).get("data", []),
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
