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
    "req_count",
    "p50_latency_ms",
    "p95_latency_ms",
    "timeout_rate",
    "retry_rate",
    "queue_depth",
]


@dataclass
class MetricBucket:
    latencies: List[float]
    retries: int
    timeouts: int
    requests: int
    queue_depth: float


class ServiceMetricAggregator:
    def __init__(self) -> None:
        self.buckets: Dict[Tuple[str, str, str], MetricBucket] = {}

    def add_fact(self, fact: TransactionFact) -> None:
        ts = fact.start_ts.replace(second=0, microsecond=0)
        key = (isoformat(ts), fact.region, fact.transaction_type)
        bucket = self.buckets.get(key)
        if not bucket:
            bucket = MetricBucket([], 0, 0, 0, 0.0)
            self.buckets[key] = bucket
        bucket.latencies.append(fact.end_to_end_latency_ms)
        bucket.requests += 1
        bucket.queue_depth += max(0, fact.retry_count - 1)
        if fact.retry_count > 0:
            bucket.retries += 1
        if fact.final_status == "timeout":
            bucket.timeouts += 1

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
            p50 = self.percentile(bucket.latencies, 0.5)
            p95 = self.percentile(bucket.latencies, 0.95)
            timeout_rate = bucket.timeouts / req
            retry_rate = bucket.retries / req
            queue = bucket.queue_depth / req
            writer.write_row(
                [
                    ts,
                    region,
                    txn_type,
                    bucket.requests,
                    round(p50, 2),
                    round(p95, 2),
                    round(timeout_rate, 4),
                    round(retry_rate, 4),
                    round(queue, 2),
                ]
            )
            rows += 1
        return rows


__all__ = ["SERVICE_METRIC_HEADERS", "ServiceMetricAggregator"]
