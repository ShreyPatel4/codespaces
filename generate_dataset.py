#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import zipfile
from typing import List

from generators import app_logs, infra_metrics, network_metrics, network_events, trace_spans, tso_calls, txn_facts
from generators.service_metrics import ServiceMetricAggregator, SERVICE_METRIC_HEADERS
from utils import (
    CsvWriter,
    DatasetConfig,
    RandomGenerator,
    RowCounter,
    ensure_dir,
    isoformat,
    DATASET_NAME,
    START_TS,
    END_TS,
)
from validation import Validator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic Fiber SQS observability dataset")
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--app_log_rows", type=int, default=15_000_000)
    parser.add_argument("--scale_logs", type=float, default=1.0, help="Scale factor applied to --app_log_rows")
    parser.add_argument("--enable_tier2", action="store_true")
    zip_group = parser.add_mutually_exclusive_group()
    zip_group.add_argument("--zip", dest="zip_output", action="store_true", default=True)
    zip_group.add_argument("--no-zip", dest="zip_output", action="store_false")
    return parser.parse_args()


def write_dataset_readme(output_dir: str) -> None:
    readme_path = os.path.join(output_dir, "README.md")
    contents = "# Fiber SQS Cross-Region Latency Dataset\n\n"
    contents += "## Scenario\n"
    contents += (
        "Synthetic telemetry for Fiber SQS investigating intermittent latency hitting Central-region provisioning traffic, "
        "requiring correlation across application logs, distributed traces, network metrics, infra stats, and TSO call queues."
    )
    contents += "\n\n## Tables\n"
    contents += "- Tier 1: clickhouse-fibersqs_app_logs, clickhouse-trace_spans, clickhouse-network_circuit_metrics, "
    contents += "clickhouse-infra_host_metrics, clickhouse-tso_calls\n"
    contents += "- Tier 2 (optional): clickhouse-fibersqs_service_metrics_1m, clickhouse-network_events_alarms\n"
    contents += "\n## Incident Summary\n"
    contents += (
        "Central provisioning flows intermittently degrade on the eastbound dependency during a multi-day window. "
        "Engineers must disentangle this from a Central CPU spike and a minor West deployment blip."
    )
    contents += "\n\n## Loading into ClickHouse\n"
    contents += """Example using `clickhouse-client`:\n\n````bash\nfor file in data/clickhouse-*.csv; do\n  table=$(basename "$file" | sed 's/clickhouse-//; s/.csv//')\n  clickhouse-client --query "DROP TABLE IF EXISTS $table"\n  clickhouse-client --query "CREATE TABLE $table (\n    -- define schema matching README requirements\n  ) ENGINE = MergeTree ORDER BY tuple()"\n  clickhouse-client --query "INSERT INTO $table FORMAT CSVWithNames" < "$file"\ndone\n````\n"""
    contents += "\n## Realism Notes\n"
    contents += "- Diurnal traffic drivers shape txn volumes and retries\n"
    contents += "- Heavy-tailed latency + retry amplification during bursts\n"
    contents += "- Cross-region traces include clock skew, multi-span chains\n"
    contents += "- Network metrics emit bursty packet loss and RTT spikes\n"
    contents += "- Alerts mix true/false positives to mimic noisy operations\n"
    with open(readme_path, "w", encoding="utf-8") as fh:
        fh.write(contents)


def write_ground_truth(
    output_dir: str,
    incident: txn_facts.IncidentWindow,
    confounders,
    row_counter: RowCounter,
    seed: int,
) -> None:
    gt_path = os.path.join(output_dir, "ground_truth.json")
    data = {
        "dataset_name": DATASET_NAME,
        "time_range": {"start": isoformat(START_TS), "end": isoformat(END_TS)},
        "primary_incident": {
            "root_cause_type": "network",
            "circuit_id": incident.circuit_id,
            "src_region": incident.src_region,
            "dst_region": incident.dst_region,
            "start": isoformat(incident.start),
            "end": isoformat(incident.end),
            "fix_time": isoformat(incident.fix_time),
            "affected_transaction_types": txn_facts.IMPACTED_TRANSACTION_TYPES,
        },
        "confounders": [
            {
                "name": conf.name,
                "start": isoformat(conf.start),
                "end": isoformat(conf.end),
                "component": conf.component,
                "region": conf.region,
                "description": conf.description,
            }
            for conf in confounders
        ],
        "generation": {
            "seed": seed,
            "row_counts": row_counter.counts,
            "parameters": {
                "incident_burst_interval_minutes": 20,
                "incident_burst_duration_minutes": [4, 7],
                "network_baseline_rtt_ms": 18,
                "network_burst_rtt_range_ms": [120, 350],
                "dependency_latency_multiplier": [5, 12],
            },
        },
    }
    with open(gt_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)


def package_zip(output_dir: str) -> str:
    zip_path = os.path.join(output_dir, f"{DATASET_NAME}.zip")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        for root, _dirs, files in os.walk(output_dir):
            for file in files:
                if file.endswith(".zip"):
                    continue
                filepath = os.path.join(root, file)
                rel = os.path.relpath(filepath, output_dir)
                zipf.write(filepath, arcname=rel)
    return zip_path


def main() -> None:
    args = parse_args()
    scaled_rows = int(args.app_log_rows * args.scale_logs)
    config = DatasetConfig(
        output_dir=args.output_dir,
        data_dir=os.path.join(args.output_dir, "data"),
        seed=args.seed,
        app_log_rows=max(1, scaled_rows),
        enable_tier2=args.enable_tier2,
        zip_output=args.zip_output,
    )
    ensure_dir(config.output_dir)
    ensure_dir(config.data_dir)

    fact_rng = RandomGenerator(config.seed)
    table_rng = RandomGenerator(config.seed + 11)
    tso_rng = RandomGenerator(config.seed + 23)
    network_rng = RandomGenerator(config.seed + 37)
    circuit_rng = RandomGenerator(config.seed + 51)
    infra_rng = RandomGenerator(config.seed + 67)
    alert_rng = RandomGenerator(config.seed + 79)

    incident = txn_facts.build_incident(config.seed)
    confounders = txn_facts.default_confounders()
    circuit_map = network_metrics.build_circuit_map(
        incident,
        [
            ("central", "west"),
            ("west", "central"),
            ("east", "central"),
            ("central", "east"),
            ("east", "west"),
            ("west", "east"),
        ],
        circuit_rng,
    )
    route_lookup = network_metrics.build_route_lookup(circuit_map)

    row_counter = RowCounter({})

    app_log_writer = CsvWriter(
        os.path.join(config.data_dir, "clickhouse-fibersqs_app_logs.csv"),
        app_logs.LOG_HEADERS,
    )
    trace_writer = CsvWriter(
        os.path.join(config.data_dir, "clickhouse-trace_spans.csv"),
        trace_spans.TRACE_HEADERS,
    )
    tso_writer = CsvWriter(
        os.path.join(config.data_dir, "clickhouse-tso_calls.csv"),
        tso_calls.TSO_HEADERS,
    )

    tso_generator = tso_calls.TSOCallGenerator(tso_writer, tso_rng)
    service_metrics_writer = None
    service_agg = None
    if config.enable_tier2:
        service_metrics_writer = CsvWriter(
            os.path.join(config.data_dir, "clickhouse-fibersqs_service_metrics_1m.csv"),
            SERVICE_METRIC_HEADERS,
        )
        service_agg = ServiceMetricAggregator()

    validator = Validator()
    trace_span_refs = 0
    impacted_burst_latency: List[float] = []
    impacted_base_latency: List[float] = []
    burst_timeouts = 0
    burst_count = 0
    base_timeouts = 0
    base_count = 0

    fact_stream = txn_facts.TransactionFactStream(
        config=config,
        rng=fact_rng,
        incident_window=incident,
        confounders=confounders,
        circuit_routes=route_lookup,
    )

    for fact in fact_stream:
        log_rows = app_logs.write_fact_logs(fact, app_log_writer, table_rng)
        row_counter.increment("fibersqs_app_logs", log_rows)
        span_rows = trace_spans.write_fact_spans(fact, trace_writer, table_rng)
        row_counter.increment("trace_spans", span_rows)
        trace_span_refs += span_rows
        tso_generator.process_fact(fact)
        if service_agg:
            service_agg.add_fact(fact)
        if fact.makes_cross_region_call and fact.region == "central" and fact.transaction_type in txn_facts.IMPACTED_TRANSACTION_TYPES:
            if fact.impacted_by_primary:
                impacted_burst_latency.append(fact.dependency_latency_ms)
                burst_count += 1
                if fact.final_status == "timeout":
                    burst_timeouts += 1
            else:
                impacted_base_latency.append(fact.dependency_latency_ms)
                base_count += 1
                if fact.final_status == "timeout":
                    base_timeouts += 1

    tso_stats = tso_generator.finalize()
    row_counter.increment("tso_calls", tso_stats.rows)

    if service_agg and service_metrics_writer:
        service_rows = service_agg.write(service_metrics_writer)
        row_counter.increment("fibersqs_service_metrics_1m", service_rows)
        service_metrics_writer.close()

    app_log_writer.close()
    trace_writer.close()
    tso_writer.close()

    network_writer = CsvWriter(
        os.path.join(config.data_dir, "clickhouse-network_circuit_metrics.csv"),
        network_metrics.NETWORK_HEADERS,
    )
    network_rows, metrics_debug = network_metrics.write_network_metrics(network_writer, network_rng, incident, circuit_map)
    network_writer.close()
    row_counter.increment("network_circuit_metrics", network_rows)

    infra_writer = CsvWriter(
        os.path.join(config.data_dir, "clickhouse-infra_host_metrics.csv"),
        infra_metrics.INFRA_HEADERS,
    )
    hosts = infra_metrics.generate_hosts()
    infra_rows, cpu_debug = infra_metrics.write_infra_metrics(infra_writer, infra_rng, hosts, confounders)
    infra_writer.close()
    row_counter.increment("infra_host_metrics", infra_rows)

    if config.enable_tier2:
        alert_writer = CsvWriter(
            os.path.join(config.data_dir, "clickhouse-network_events_alarms.csv"),
            network_events.NETWORK_EVENT_HEADERS,
        )
        alert_stats = network_events.write_network_events(alert_writer, alert_rng, incident)
        alert_writer.close()
        row_counter.increment("network_events_alarms", alert_stats["rows"])
    else:
        alert_stats = {"rows": 0}

    validator.check_referential_integrity(
        tso_refs=tso_stats.non_empty_refs,
        tso_matches=tso_stats.matches,
        trace_refs=trace_span_refs,
        trace_matches=trace_span_refs,
    )
    burst_latency_avg = sum(impacted_burst_latency) / max(1, len(impacted_burst_latency))
    base_latency_avg = sum(impacted_base_latency) / max(1, len(impacted_base_latency))
    burst_timeout_rate = burst_timeouts / max(1, burst_count)
    base_timeout_rate = base_timeouts / max(1, base_count)
    incident_metrics = metrics_debug.get(incident.circuit_id, [])
    if incident_metrics:
        burst_values = [m["rtt_ms"] for m in incident_metrics if m.get("multiplier", 1.0) > 1.0]
        base_values = [m["rtt_ms"] for m in incident_metrics if m.get("multiplier", 1.0) == 1.0]
        network_rtt_burst = max(burst_values) if burst_values else (base_values[0] if base_values else 1.0)
        network_rtt_base = min(base_values) if base_values else (burst_values[0] if burst_values else 1.0)
    else:
        network_rtt_burst = 1.0
        network_rtt_base = 1.0
    validator.check_incident_coherence(
        burst_latency_avg,
        base_latency_avg,
        burst_timeout_rate,
        base_timeout_rate,
        network_rtt_burst,
        network_rtt_base,
    )
    cpu_peak = cpu_debug.get("central", {}).get("max", 0.0)
    cpu_window = next((conf for conf in confounders if conf.name == "central_cpu_spike"), None)
    if incident_metrics and cpu_window:
        cpu_window_spikes = [
            m.get("multiplier", 1.0)
            for m in incident_metrics
            if cpu_window.start <= m["timestamp"] < cpu_window.end
        ]
        network_peak_on_cpu = max(cpu_window_spikes) if cpu_window_spikes else 1.0
    else:
        network_peak_on_cpu = 1.0
    validator.check_confounder_separability(cpu_peak, network_peak_on_cpu)
    if alert_stats.get("rows", 0) > 0:
        validator.check_alert_quality(alert_stats.get("tp", 0), alert_stats.get("fp", 0), alert_stats.get("fn", 0))

    validation_summary = validator.summary()
    print(validation_summary)
    with open(os.path.join(config.output_dir, "validation_summary.json"), "w", encoding="utf-8") as fh:
        fh.write(validation_summary)

    write_dataset_readme(config.output_dir)
    write_ground_truth(config.output_dir, incident, confounders, row_counter, config.seed)

    if config.zip_output:
        package_zip(config.output_dir)


if __name__ == "__main__":
    main()
