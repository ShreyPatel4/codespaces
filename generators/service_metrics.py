from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple

from utils import CsvWriter, isoformat
from .txn_facts import TransactionFact

SERVICE_METRIC_HEADERS = [
    "timestamp",
    "region",
    "transaction_type",
    "request_rate",
    "error_rate",
    "p95_latency_ms",
    "p99_latency_ms",
    "saturation",
]


@dataclass
class MetricBucket:
    latencies: List[float]
    requests: int
    errors: int
    retries: int


class ServiceMetricAggregator:
    def __init__(self) -> None:
        self.buckets: Dict[Tuple[str, str, str], MetricBucket] = {}

    def add_fact(self, fact: TransactionFact) -> None:
        ts = fact.start_ts.replace(second=0, microsecond=0)
        key = (isoformat(ts, ms=False), fact.region, fact.transaction_type)
        bucket = self.buckets.get(key)
        if not bucket:
            bucket = MetricBucket([], 0, 0, 0)
            self.buckets[key] = bucket
        bucket.latencies.append(fact.end_to_end_latency_ms)
        bucket.requests += 1
        if fact.retry_count > 0:
            bucket.retries += 1
        if fact.final_status == "timeout":
            bucket.errors += 1

    def percentile(self, values: List[float], perc: float) -> float:
        if not values:
            return 0.0
        values = sorted(values)
        idx = int(len(values) * perc)
        idx = min(len(values) - 1, idx)
        return values[idx]

    def write(self, writer: CsvWriter) -> int:
        rows = 0
        for (ts, region, txn_type), bucket in self.buckets.items():
            req = max(1, bucket.requests)
            p95 = self.percentile(bucket.latencies, 0.95)
            p99 = self.percentile(bucket.latencies, 0.99)
            error_rate = bucket.errors / req
            retry_rate = bucket.retries / req
            saturation = min(1.0, 0.15 + 0.7 * retry_rate + 1.0 * error_rate + 0.25 * min(1.0, p95 / 2500.0))
            writer.write_row(
                [
                    ts,
                    region,
                    txn_type,
                    round(bucket.requests / 60.0, 4),
                    round(error_rate, 4),
                    round(p95, 2),
                    round(p99, 2),
                    round(saturation, 4),
                ]
            )
            rows += 1
        return rows


__all__ = ["SERVICE_METRIC_HEADERS", "ServiceMetricAggregator"]
