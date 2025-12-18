# Fiber SQS Cross-Region Latency Synthetic Dataset Generator

This repository builds a production-quality observability dataset for the Fiber SQS latency investigation exercise.

## Features
- 5 regions: `east`, `west`, `central`, `north`, `south` (stable default weights: central 0.35, east 0.25, west 0.20, north 0.10, south 0.10).
- Canonical transaction fact stream spanning 2025-12-01 through 2025-12-08 with diurnal traffic and heavy-tailed latency.
- Tier 1 tables for application logs, distributed traces, network circuits, infra metrics, and TSO call records.
- Optional Tier 2 tables for Prometheus-style service KPIs, discrete network events, and a canonical transaction fact table for clean joins.
- Built-in confounders (central CPU spike, west deployment blip) to encourage multi-signal correlation.
- Deterministic generation with configurable scaling and zip packaging.

## Usage
```bash
python generate_dataset.py \
  --output_dir /tmp/fibersqs_dataset \
  --seed 7 \
  --app_log_rows 15000000 \
  --scale_logs 0.02 \
  --enable_tier2 \
  --zip
```

Key CLI options:
- `--output_dir`: destination folder; will contain `data/`, README, ground_truth, and optional zip.
- `--seed`: controls deterministic randomness.
- `--app_log_rows`: approximate target app-log rows before scaling.
- `--scale_logs`: multiplier applied to `--app_log_rows` for local testing (e.g., `0.001`).
- `--enable_tier2`: include optional tables for alerting realism.
- `--zip` / `--no-zip`: toggle packaging into `simulated_fibersqs_cross_region_latency_tso.zip`.

Example quick sanity run:
```bash
python generate_dataset.py --output_dir out --app_log_rows 50000 --scale_logs 0.001 --seed 1 --no-zip
```

## Regeneration and ClickHouse loading

Use `scripts-dev/regen_fibersqs.sh` to regenerate the dataset, drop only the Fiber SQS tables in ClickHouse, load the CSVs, and emit a `postload_validation.json` report. Defaults target the local ClickHouse HTTP endpoint at `http://localhost:8123` with `default` credentials.

Example:
```bash
./scripts-dev/regen_fibersqs.sh \
  --output_dir /tmp/fibersqs \
  --app_log_rows 7500000 \
  --scale_logs 0.01 \
  --seed 11 \
  --no-zip
```

Key behaviors:
- Deletes the specified output directory before regeneration.
- Creates canonical storage in ClickHouse under `fibersqs_prod.*` before ingestion (MergeTree tables) and preflights the sanitized `CLICKHOUSE_URL` (query string stripped, trailing slash trimmed) via `GET /ping`.
- Loads CSVs into canonical tables using `scripts/load_csv_into_clickhouse.sh` in insert-only mode (HTTP POST with `--data-binary`).
- Creates compatibility views for old table names (`fibersqs_app_logs`, `trace_spans`, `network_circuit_metrics`, `infra_host_metrics`, `tso_calls`) in `logs` database if it exists, otherwise in `default`.
- Creates ES-style “index pattern” views in `fibersqs_prod` against the compatibility app logs:
  - Region views: `fibersqs_prod.fibersqs_prod_<region>_app_logs`
  - Daily views (one per generated day): `fibersqs_prod.fibersqs_prod_<region>_app_logs_<YYYYMMDD>`
- Invokes the generator with boolean flags (`--zip` / `--no-zip`).
- Saves ClickHouse validation results to `postload_validation.json` in the output directory (including TSO TP/FN/FP/Wrong-link counts + rates, min/max day span, and view counts).

The generator writes a validation summary (`validation_summary.json`) confirming referential integrity, incident coherence, and confounder separability.

## Project Structure
- `generate_dataset.py`: CLI entry point orchestrating generation, packaging, and validations.
- `utils.py`: shared RNG wrappers, CSV helpers, config dataclasses, and incident utilities.
- `generators/txn_facts.py`: canonical transaction stream plus incident/confounder definitions.
- `generators/app_logs.py`, `trace_spans.py`, `tso_calls.py`: table writers derived from txn facts.
- `generators/network_metrics.py`, `infra_metrics.py`: circuit + host telemetry, plus tier2 network/service modules.
- `validation.py`: lightweight checks and JSON summary output.

## Output Contents
- `data/clickhouse-*.csv` files ready for ClickHouse ingestion (`clickhouse-` prefix stripped for tables), including:
  - Tier 1: `clickhouse-app_logs.csv`, `clickhouse-trace_spans.csv`, `clickhouse-network_circuit_metrics.csv`, `clickhouse-infra_host_metrics.csv`, `clickhouse-tso_calls.csv`
  - Tier 2 (optional): `clickhouse-service_metrics.csv`, `clickhouse-network_events.csv`, `clickhouse-txn_facts.csv`
- `README.md`: scenario + loading instructions (no spoilers beyond narrative outline).
- `ground_truth.json`: authoritative incident metadata (root cause type, windows, confounders, row counts, seed).
- `validation_summary.json`: metrics verifying integrity and realism signals.
- `simulated_fibersqs_cross_region_latency_tso.zip`: packaged bundle (optional).

## ClickHouse model + example queries

Canonical tables live in the `fibersqs_prod` database. Existing queries using legacy names keep working via compatibility views.

Index-pattern-style views are exposed via `SHOW TABLES FROM fibersqs_prod LIKE 'fibersqs_prod_%_app_logs%'`. Region-level views match `fibersqs_prod.fibersqs_prod_<region>_app_logs`, while daily shards use `fibersqs_prod.fibersqs_prod_<region>_app_logs_<YYYYMMDD>` and cover the full generated date span.

Example: query canonical app logs directly:
```sql
SELECT
  region,
  quantileTDigest(0.95)(end_to_end_latency_ms_f64) AS p95_ms
FROM fibersqs_prod.app_logs
WHERE event IN ('completed','timeout')
  AND transaction_type IN ('provision_fiber_sqs','modify_service_profile')
GROUP BY region
ORDER BY region;
```

Example: query an ES-style daily view:
```sql
SELECT
  quantileTDigest(0.95)(end_to_end_latency_ms_f64) AS p95_ms,
  round(countIf(event='timeout')/count(), 4) AS timeout_rate
FROM fibersqs_prod.fibersqs_prod_central_app_logs_20251204
WHERE event IN ('completed','timeout')
  AND transaction_type IN ('provision_fiber_sqs','modify_service_profile');
```

In Grafana or ClickHouse notebooks, use `fibersqs_prod.fibersqs_prod_<region>_app_logs` for region slices or the daily suffixed views for time-bounded investigations; `SHOW TABLES LIKE 'fibersqs_prod_%_app_logs%'` lists the available shards. Regeneration is deterministic for a given `--seed`, preserving the 2025-12-01 through 2025-12-08 window and the Central-only incident even as you scale row counts.

Example: TSO correlation confusion matrix (post-load computable in SQL):
```sql
WITH log_txn AS (
  SELECT transaction_id, any(customer_id) AS log_customer_id
  FROM fibersqs_prod.app_logs
  WHERE event='received'
  GROUP BY transaction_id
)
SELECT
  countIf(t.transaction_id='') AS fn_missing_txn_id,
  countIf(t.transaction_id!='' AND l.log_customer_id IS NULL) AS fp_nonexistent_txn_id,
  countIf(t.transaction_id!='' AND l.log_customer_id IS NOT NULL AND t.customer_id != l.log_customer_id) AS wrong_link,
  countIf(t.transaction_id!='' AND l.log_customer_id IS NOT NULL AND t.customer_id = l.log_customer_id) AS tp
FROM fibersqs_prod.tso_calls t
LEFT JOIN log_txn l USING (transaction_id);
```

## Notes
- All timestamps fall within 2025-12-01T00:00:00Z – 2025-12-08T00:00:00Z.
- Impacted transaction types are limited to synchronous central→east provisioning flows; other types provide background.
- Incident diagnosis requires correlating TSO calls, cross-region traces, network circuit spikes, and infra confounders.
