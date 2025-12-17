from __future__ import annotations

from typing import Iterator

from utils import CsvWriter, isoformat, RandomGenerator
from .txn_facts import TransactionFact

TRACE_HEADERS = [
    "timestamp",
    "trace_id",
    "span_id",
    "parent_span_id",
    "transaction_id",
    "region",
    "service",
    "operation",
    "duration_ms",
    "status",
    "circuit_id",
]


def span_id(rng: RandomGenerator) -> str:
    return rng.hex_id("sp")


def write_fact_spans(fact: TransactionFact, writer: CsvWriter, rng: RandomGenerator) -> int:
    rows = 0
    root_span = span_id(rng)
    writer.write_row(
        [
            isoformat(fact.start_ts),
            fact.trace_id,
            root_span,
            "",
            fact.transaction_id,
            fact.region,
            "api",
            "POST /fiber/txn",
            round(fact.end_to_end_latency_ms, 2),
            "error" if fact.final_status == "timeout" else "ok",
            "",
        ]
    )
    rows += 1

    orch_span = span_id(rng)
    writer.write_row(
        [
            isoformat(fact.start_ts),
            fact.trace_id,
            orch_span,
            root_span,
            fact.transaction_id,
            fact.region,
            "orchestrator",
            "coordinate",
            round(fact.end_to_end_latency_ms * 0.4, 2),
            "ok",
            "",
        ]
    )
    rows += 1

    worker_span = span_id(rng)
    writer.write_row(
        [
            isoformat(fact.start_ts),
            fact.trace_id,
            worker_span,
            orch_span,
            fact.transaction_id,
            fact.region,
            "worker",
            "apply",
            round(fact.end_to_end_latency_ms * 0.5, 2),
            "error" if fact.final_status == "timeout" else "ok",
            "",
        ]
    )
    rows += 1

    if fact.makes_cross_region_call:
        dep_span = span_id(rng)
        writer.write_row(
            [
                isoformat(fact.start_ts),
                fact.trace_id,
                dep_span,
                orch_span,
                fact.transaction_id,
                fact.region,
                fact.dependency_service or "inventory-client",
                "HTTP POST",
                round(fact.dependency_latency_ms, 2),
                "error" if fact.final_status == "timeout" else "ok",
                fact.circuit_id or "",
            ]
        )
        rows += 1

    return rows


__all__ = ["TRACE_HEADERS", "write_fact_spans"]
