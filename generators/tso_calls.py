from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Dict

from utils import CsvWriter, RandomGenerator, isoformat, END_TS
from .txn_facts import TransactionFact, IMPACTED_TRANSACTION_TYPES

TSO_HEADERS = [
    "call_id",
    "timestamp",
    "customer_id",
    "customer_region",
    "issue_category",
    "issue_description",
    "service_type",
    "transaction_id",
    "resolution_time_minutes",
    "escalated",
    "resolution_code",
]

ISSUE_NOTES = [
    "customer experiencing slow provisioning",
    "reported stalled order",
    "timeout observed during modify",
    "customer claims service stuck",
    "tso triage indicates regional delay",
    "TSO call referencing long queue",
]


@dataclass
class TSOStats:
    rows: int = 0
    non_empty_refs: int = 0
    matches: int = 0
    incident_calls: int = 0


class TSOCallGenerator:
    def __init__(self, writer: CsvWriter, rng: RandomGenerator) -> None:
        self.writer = writer
        self.rng = rng
        self.stats = TSOStats()

    def process_fact(self, fact: TransactionFact) -> None:
        call_probability = 0.01
        if fact.transaction_type in IMPACTED_TRANSACTION_TYPES and fact.customer_region == "central":
            call_probability = 0.12 if fact.impacted_by_primary else 0.04
        elif fact.impacted_by_confounder:
            call_probability = 0.05
        elif fact.final_status == "timeout":
            call_probability = 0.03
        if self.rng.random() > call_probability:
            return
        delta_minutes = self.rng.randint(5, 120)
        call_ts = fact.end_ts + timedelta(minutes=delta_minutes)
        max_ts = END_TS - timedelta(minutes=5)
        if call_ts >= max_ts:
            call_ts = max_ts
        if call_ts <= fact.end_ts:
            call_ts = fact.end_ts + timedelta(minutes=5)
            if call_ts > max_ts:
                call_ts = max_ts
        call_id = f"TSO-{call_ts.strftime('%Y%m%d%H%M%S')}{self.rng.randint(100, 999)}"
        issue_category = self.rng.choice(["slow_provisioning", "timeout", "failure"])
        resolution_time = self.rng.randint(10, 180)
        escalated = fact.impacted_by_primary and self.rng.random() < 0.6
        resolution_code = self.rng.choice(["system_resolved", "manual_intervention", "customer_callback"])
        txn_ref = fact.transaction_id if self.rng.random() > 0.05 else ""
        self.writer.write_row(
            [
                call_id,
                isoformat(call_ts, ms=False),
                fact.customer_id,
                fact.customer_region,
                issue_category,
                self.rng.choice(ISSUE_NOTES),
                fact.service_type,
                txn_ref,
                resolution_time,
                str(escalated).lower(),
                resolution_code,
            ]
        )
        self.stats.rows += 1
        if txn_ref:
            self.stats.non_empty_refs += 1
            self.stats.matches += 1
        if fact.impacted_by_primary:
            self.stats.incident_calls += 1

    def finalize(self) -> TSOStats:
        return self.stats


__all__ = ["TSO_HEADERS", "TSOCallGenerator", "TSOStats"]
