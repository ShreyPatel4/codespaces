#!/usr/bin/env bash
set -euo pipefail

OUTPUT_DIR="/tmp/fibersqs"
SEED=7
APP_LOG_ROWS=15000000
SCALE_LOGS=1.0
ZIP_FLAG="--zip"

CLICKHOUSE_URL=${CLICKHOUSE_URL:-http://localhost:8123}
CLICKHOUSE_USER=${CLICKHOUSE_USER:-default}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-default}

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
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --seed)
      SEED="$2"
      shift 2
      ;;
    --app_log_rows)
      APP_LOG_ROWS="$2"
      shift 2
      ;;
    --scale_logs)
      SCALE_LOGS="$2"
      shift 2
      ;;
    --zip)
      ZIP_FLAG="--zip"
      shift 1
      ;;
    --no-zip)
      ZIP_FLAG="--no-zip"
      shift 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
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

run_sql() {
  local sql="$1"
  curl -sS -f -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" --data-binary "$sql" "$CLICKHOUSE_URL/" >/dev/null
}

run_sql_json() {
  local sql="$1"
  curl -sS -f -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" --data-binary "$sql FORMAT JSON" "$CLICKHOUSE_URL/"
}

detect_generator() {
  local candidates=(
    "dataset-gen/synthetic/fibersqs_cross_region_latency_tso/generate_dataset.py"
    "generate_dataset.py"
  )
  for candidate in "${candidates[@]}"; do
    if [[ -f "$candidate" ]]; then
      echo "$candidate"
      return 0
    fi
  done
  echo "[regen_fibersqs] Unable to find generate_dataset.py" >&2
  exit 1
}

GENERATOR_SCRIPT=$(detect_generator)
LOADER_SCRIPT="./scripts/load_csv_into_clickhouse.sh"

if [[ ! -x "$LOADER_SCRIPT" ]]; then
  echo "[regen_fibersqs] Loader script not executable: $LOADER_SCRIPT" >&2
  exit 1
fi

log "Cleaning output directory $OUTPUT_DIR"
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

log "Dropping existing ClickHouse dataset tables"
for table in fibersqs_app_logs infra_host_metrics network_circuit_metrics trace_spans tso_calls; do
  run_sql "DROP TABLE IF EXISTS $table"
done

log "Generating dataset with $GENERATOR_SCRIPT"
python "$GENERATOR_SCRIPT" \
  --output_dir "$OUTPUT_DIR" \
  --seed "$SEED" \
  --app_log_rows "$APP_LOG_ROWS" \
  --scale_logs "$SCALE_LOGS" \
  $ZIP_FLAG

log "Loading CSVs into ClickHouse"
"$LOADER_SCRIPT" -s "$OUTPUT_DIR/data"

log "Collecting validation metrics"
export OUTPUT_DIR
export IMPACTED_VOLUME_JSON="$(run_sql_json "WITH per_minute AS (SELECT toStartOfMinute(parseDateTimeBestEffort(timestamp)) AS minute, count() AS reqs FROM fibersqs_app_logs WHERE event='received' AND region='central' AND transaction_type IN ('provision_fiber_sqs','modify_service_profile') GROUP BY minute) SELECT quantile(0.5)(reqs) AS median_rpm, quantile(0.95)(reqs) AS p95_rpm, max(reqs) AS max_rpm FROM per_minute")"

FIX_TIME=$(python - <<'PY'
import json
import os
from pathlib import Path

output_dir = os.environ.get("OUTPUT_DIR")
ground_truth_path = Path(output_dir) / "ground_truth.json"
with open(ground_truth_path, "r", encoding="utf-8") as fh:
    data = json.load(fh)
print(data["primary_incident"]["fix_time"])
PY
)
FIX_TIME="${FIX_TIME//$'\n'/}"

export FIX_VERIFICATION_JSON="$(run_sql_json "WITH toDateTime64('$FIX_TIME', 3) AS fix SELECT if(timestamp < fix, 'pre_fix', 'post_fix') AS window, quantile(0.95)(end_to_end_latency_ms) AS p95_latency_ms, quantile(0.99)(end_to_end_latency_ms) AS p99_latency_ms, round(countIf(event='timeout')/count(),4) AS timeout_rate FROM fibersqs_app_logs WHERE event IN ('completed','timeout') AND region='central' AND transaction_type IN ('provision_fiber_sqs','modify_service_profile') GROUP BY window ORDER BY window")"

export TSO_MISSING_JSON="$(run_sql_json "SELECT round(countIf(transaction_id='')/count(),4) AS missing_rate, count() total FROM tso_calls")"
export TSO_NONEXISTENT_JSON="$(run_sql_json "WITH logs AS (SELECT DISTINCT transaction_id FROM fibersqs_app_logs) SELECT round(countIf(transaction_id!='' AND transaction_id NOT IN (SELECT transaction_id FROM logs)) / countIf(transaction_id!=''),4) AS nonexistent_rate FROM tso_calls")"
export TSO_MISMATCH_JSON="$(run_sql_json "WITH log_txn AS (SELECT transaction_id, any(customer_id) AS log_customer_id FROM fibersqs_app_logs WHERE transaction_id!='' GROUP BY transaction_id) SELECT round(countIf(t.customer_id != l.log_customer_id)/count(),4) AS mismatch_rate FROM tso_calls t INNER JOIN log_txn l USING(transaction_id) WHERE t.transaction_id!=''")"

python - <<'PY'
import json
import os
from pathlib import Path

def first_row(payload: str):
    data = json.loads(payload)
    return data.get("data", [{}])[0]

output_dir = Path(os.environ["OUTPUT_DIR"])
report_path = output_dir / "postload_validation.json"

report = {
    "impacted_slice_volume": first_row(os.environ["IMPACTED_VOLUME_JSON"]),
    "fix_verification": json.loads(os.environ["FIX_VERIFICATION_JSON"]).get("data", []),
    "tso_postload": {
        "missing": first_row(os.environ["TSO_MISSING_JSON"]),
        "nonexistent": first_row(os.environ["TSO_NONEXISTENT_JSON"]),
        "customer_mismatch": first_row(os.environ["TSO_MISMATCH_JSON"]),
    },
}

report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
print(f"[regen_fibersqs] Wrote validation report to {report_path}")
PY

log "Regeneration workflow complete"
