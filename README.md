# Fiber SQS Cross-Region Latency Synthetic Dataset Generator

This repository builds a production-quality observability dataset for the Fiber SQS latency investigation exercise.

## Features
- Canonical transaction fact stream spanning 2025-12-01 through 2025-12-08 with diurnal traffic and heavy-tailed latency.
- Tier 1 tables for application logs, distributed traces, network circuits, infra metrics, and TSO call records.
- Optional Tier 2 tables for service-level aggregates and network alert noise to study alert precision/recall trade-offs.
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

The generator writes a validation summary (`validation_summary.json`) confirming referential integrity, incident coherence, confounder separability, and alert confusion-matrix counts.

## Project Structure
- `generate_dataset.py`: CLI entry point orchestrating generation, packaging, and validations.
- `utils.py`: shared RNG wrappers, CSV helpers, config dataclasses, and incident utilities.
- `generators/txn_facts.py`: canonical transaction stream plus incident/confounder definitions.
- `generators/app_logs.py`, `trace_spans.py`, `tso_calls.py`: table writers derived from txn facts.
- `generators/network_metrics.py`, `infra_metrics.py`: circuit + host telemetry, plus tier2 alerts/metrics modules.
- `validation.py`: lightweight checks and JSON summary output.

## Output Contents
- `data/clickhouse-*.csv` files ready for ClickHouse ingestion (`clickhouse-` prefix stripped for tables).
- `README.md`: scenario + loading instructions (no spoilers beyond narrative outline).
- `ground_truth.json`: authoritative incident metadata (root cause type, windows, confounders, row counts, seed).
- `validation_summary.json`: metrics verifying integrity and realism signals.
- `simulated_fibersqs_cross_region_latency_tso.zip`: packaged bundle (optional).

## Notes
- All timestamps fall within 2025-12-01T00:00:00Z – 2025-12-08T00:00:00Z.
- Impacted transaction types are limited to synchronous central→east provisioning flows; other types provide background.
- Incident diagnosis requires correlating TSO calls, cross-region traces, network circuit spikes, and infra confounders.
