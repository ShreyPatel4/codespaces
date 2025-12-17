from __future__ import annotations

from datetime import timedelta
from typing import List

from utils import CsvWriter, RandomGenerator, isoformat, jitter_timestamp
from .txn_facts import TransactionFact


LOG_HEADERS = [
    "timestamp",
    "region",
    "cluster",
    "service",
    "host",
    "level",
    "transaction_id",
    "trace_id",
    "span_id",
    "customer_id",
    "transaction_type",
    "event",
    "dependency_region",
    "dependency_service",
    "dependency_latency_ms",
    "end_to_end_latency_ms",
    "http_status",
    "error_code",
    "circuit_id",
    "message",
]


def _host_for_region(region: str, rng: RandomGenerator) -> str:
    return f"fibersqs-prod-{region}-host{rng.randint(1, 48):02d}"


def _cluster_for_region(region: str) -> str:
    return f"fibersqs-prod-{region}"


def _span_id(rng: RandomGenerator) -> str:
    return rng.hex_id("sp")


def write_fact_logs(fact: TransactionFact, writer: CsvWriter, rng: RandomGenerator) -> int:
    rows_written = 0
    base_ts = fact.start_ts + timedelta(milliseconds=fact.clock_skew_ms)
    cluster = _cluster_for_region(fact.region)
    host = _host_for_region(fact.region, rng)

    def emit(event: str, service: str, event_ts, level: str, dependency_latency=None, end_to_end=None, http_status="", error_code="", message=""):
        nonlocal rows_written
        writer.write_row(
            [
                isoformat(event_ts),
                fact.region,
                cluster,
                service,
                host,
                level,
                fact.transaction_id,
                fact.trace_id,
                _span_id(rng),
                fact.customer_id,
                fact.transaction_type,
                event,
                fact.dependency_region or "",
                fact.dependency_service or "",
                int(dependency_latency) if dependency_latency is not None else "",
                round(end_to_end, 2) if end_to_end is not None else "",
                http_status,
                error_code or "",
                fact.circuit_id or "",
                message,
            ]
        )
        rows_written += 1

    emit("received", "api", base_ts, "INFO", message="request received")
    emit("queued", "api", jitter_timestamp(base_ts + timedelta(milliseconds=10), rng), "INFO", message="queued for orchestrator")

    orchestration_ts = jitter_timestamp(base_ts + timedelta(milliseconds=40), rng)
    emit("orchestrated", "orchestrator", orchestration_ts, "INFO", message="routing transaction")

    for attempt in range(fact.attempt_count):
        attempt_ts = jitter_timestamp(orchestration_ts + timedelta(milliseconds=50 + attempt * 30), rng)
        dep_latency = None
        if fact.makes_cross_region_call:
            dep_latency = fact.dependency_latency_ms * (1.0 + rng.uniform(-0.15, 0.15))
            level = "WARN" if fact.impacted_by_primary else "INFO"
            emit(
                "dependency_call",
                "orchestrator",
                attempt_ts,
                level,
                dependency_latency=dep_latency,
                message="dependency call",
            )
        worker_service = "worker" if attempt == fact.retry_count else "worker-retry"
        worker_delay = dep_latency if dep_latency is not None else 80
        worker_ts = attempt_ts + timedelta(milliseconds=worker_delay)
        emit("worker_progress", worker_service, worker_ts, "INFO", message="worker progressing")

    completion_ts = fact.end_ts + timedelta(milliseconds=fact.clock_skew_ms)
    level = "ERROR" if fact.final_status == "timeout" else "INFO"
    message = "completed" if fact.final_status.startswith("completed") else fact.final_status
    emit(
        "completed" if fact.final_status != "timeout" else "timeout",
        "api",
        completion_ts,
        level,
        dependency_latency=fact.dependency_latency_ms if fact.makes_cross_region_call else None,
        end_to_end=fact.end_to_end_latency_ms,
        http_status=fact.http_status,
        error_code=fact.error_code,
        message=message,
    )

    if fact.final_status == "timeout":
        emit(
            "retry",
            "api",
            completion_ts + timedelta(milliseconds=5),
            "WARN",
            message="queued for manual retry",
            http_status="",
        )

    return rows_written


__all__ = ["LOG_HEADERS", "write_fact_logs"]
