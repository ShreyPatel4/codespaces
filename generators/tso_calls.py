from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Deque, Dict, List, Optional

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
    noise_counts: Dict[str, int] = field(default_factory=dict)
    call_records: List["TSOCallRecord"] = field(default_factory=list)


@dataclass
class TSOCallRecord:
    call_id: str
    true_transaction_id: str
    emitted_transaction_id: str
    noise_type: str
    delay_minutes: int


class TSOCallGenerator:
    def __init__(self, writer: CsvWriter, rng: RandomGenerator) -> None:
        self.writer = writer
        self.rng = rng
        self.stats = TSOStats()
        self.fact_buffer: Deque[Dict[str, object]] = deque()
        self.buffer_horizon = timedelta(hours=2)
        self.noise_targets = {
            "missing": (0.024, 0.03),
            "fabricated": (0.001, 0.002),
            "wrong_customer": (0.005, 0.007),
        }

    def _prune_buffer(self, reference_ts) -> None:
        cutoff = reference_ts - self.buffer_horizon
        while self.fact_buffer and self.fact_buffer[0]["end_ts"] < cutoff:
            self.fact_buffer.popleft()

    def _record_fact(self, fact: TransactionFact) -> None:
        self._prune_buffer(fact.end_ts)
        self.fact_buffer.append(
            {
                "end_ts": fact.end_ts,
                "region": fact.customer_region,
                "transaction_id": fact.transaction_id,
                "customer_id": fact.customer_id,
            }
        )

    def _should_apply_noise(self, key: str) -> bool:
        desired, cap = self.noise_targets[key]
        current_rows = max(1, self.stats.rows)
        current_rate = self.stats.noise_counts.get(key, 0) / current_rows
        if current_rate >= cap:
            return False
        return self.rng.random() < desired

    def _fabricated_transaction_id(self, call_ts) -> str:
        return f"FAKE-TX-{call_ts.strftime('%Y%m%d%H%M%S')}-{self.rng.randint(1000, 9999)}"

    def _find_decoy(self, call_ts, region: str, customer_id: str) -> Optional[Dict[str, object]]:
        self._prune_buffer(call_ts)
        candidates = [
            item
            for item in self.fact_buffer
            if item["region"] == region and item["customer_id"] != customer_id
        ]
        if not candidates:
            return None

        def within_window(item, min_minutes: int, max_minutes: int) -> bool:
            delta = abs((call_ts - item["end_ts"]).total_seconds()) / 60.0
            return min_minutes <= delta <= max_minutes

        preferred = [item for item in candidates if within_window(item, 30, 60)]
        pool = preferred or [item for item in candidates if within_window(item, 10, 90)]
        if not pool:
            return None
        return self.rng.choice(pool)

    def _record_noise(self, noise_type: str) -> None:
        self.stats.noise_counts[noise_type] = self.stats.noise_counts.get(noise_type, 0) + 1

    def process_fact(self, fact: TransactionFact) -> None:
        self._record_fact(fact)
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
        txn_ref = fact.transaction_id
        noise_type = "clean"
        if self._should_apply_noise("missing"):
            noise_type = "missing"
            txn_ref = ""
        elif self._should_apply_noise("fabricated"):
            noise_type = "fabricated"
            txn_ref = self._fabricated_transaction_id(call_ts)
        elif self._should_apply_noise("wrong_customer"):
            decoy = self._find_decoy(call_ts, fact.customer_region, fact.customer_id)
            if decoy:
                noise_type = "wrong_customer"
                txn_ref = decoy["transaction_id"]  # type: ignore[index]
            else:
                noise_type = "clean"
                txn_ref = fact.transaction_id

        self._record_noise(noise_type)
        delay_minutes = max(0, int((call_ts - fact.end_ts).total_seconds() // 60))
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
            if noise_type == "clean":
                self.stats.matches += 1
        if fact.impacted_by_primary:
            self.stats.incident_calls += 1
        self.stats.call_records.append(
            TSOCallRecord(
                call_id=call_id,
                true_transaction_id=fact.transaction_id,
                emitted_transaction_id=txn_ref,
                noise_type=noise_type,
                delay_minutes=delay_minutes,
            )
        )

    def finalize(self) -> TSOStats:
        return self.stats


__all__ = ["TSO_HEADERS", "TSOCallGenerator", "TSOStats", "TSOCallRecord"]
