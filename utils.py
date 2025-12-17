from __future__ import annotations

import csv
import math
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
ISO_FORMAT_NO_MS = "%Y-%m-%dT%H:%M:%SZ"
DATASET_NAME = "simulated_fibersqs_cross_region_latency_tso"
START_TS = datetime(2025, 12, 1, tzinfo=timezone.utc)
END_TS = datetime(2025, 12, 8, tzinfo=timezone.utc)


class RandomGenerator:
    """Wrapper around random.Random to centralise deterministic behaviour."""

    def __init__(self, seed: int) -> None:
        self._rand = random.Random(seed)

    def random(self) -> float:
        return self._rand.random()

    def uniform(self, a: float, b: float) -> float:
        return self._rand.uniform(a, b)

    def randint(self, a: int, b: int) -> int:
        return self._rand.randint(a, b)

    def choice(self, seq: Sequence):
        return self._rand.choice(seq)

    def weighted_choice(self, weights: Dict[str, float]) -> str:
        total = sum(max(w, 0.0) for w in weights.values())
        if total <= 0:
            raise ValueError("Weights must sum to > 0")
        r = self._rand.uniform(0, total)
        upto = 0.0
        for key, weight in weights.items():
            upto += max(weight, 0.0)
            if upto >= r:
                return key
        return next(iter(weights))

    def expovariate(self, lambd: float) -> float:
        return self._rand.expovariate(lambd)

    def gauss(self, mu: float, sigma: float) -> float:
        return self._rand.gauss(mu, sigma)

    def hex_id(self, prefix: str, length: int = 12) -> str:
        value = ''.join(self._rand.choice('0123456789abcdef') for _ in range(length))
        return f"{prefix}{value}"


@dataclass
class IncidentWindow:
    start: datetime
    end: datetime
    fix_time: datetime
    bursts: List[Tuple[datetime, datetime]]
    circuit_id: str
    src_region: str
    dst_region: str


@dataclass
class ConfounderWindow:
    name: str
    start: datetime
    end: datetime
    region: str
    component: str
    description: str


def daterange_minutes(start: datetime, end: datetime) -> Iterator[datetime]:
    cursor = start
    while cursor < end:
        yield cursor
        cursor += timedelta(minutes=1)


def daterange_5m(start: datetime, end: datetime) -> Iterator[datetime]:
    cursor = start
    while cursor < end:
        yield cursor
        cursor += timedelta(minutes=5)


def isoformat(dt: datetime, ms: bool = True) -> str:
    if ms:
        return dt.strftime(ISO_FORMAT)
    return dt.strftime(ISO_FORMAT_NO_MS)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def diurnal_factor(dt: datetime) -> float:
    hour = dt.hour + dt.minute / 60.0
    base = 0.55 + 0.45 * math.sin((hour - 3) / 24.0 * 2 * math.pi)
    weekend = 0.85 if dt.weekday() >= 5 else 1.0
    return max(0.2, base) * weekend


def heavy_tail_latency(base_ms: float, rng: RandomGenerator, burst_multiplier: float = 1.0) -> float:
    samples = sum(rng.random() for _ in range(3)) / 3.0
    tail = (1.0 - math.log(max(1e-6, samples)))
    latency = base_ms * (0.7 + tail) * burst_multiplier
    return max(base_ms * 0.5, latency)


def generate_incident_bursts(
    window_start: datetime,
    window_end: datetime,
    rng: RandomGenerator,
    period_minutes: int = 20,
    min_duration: int = 4,
    max_duration: int = 7,
) -> List[Tuple[datetime, datetime]]:
    bursts: List[Tuple[datetime, datetime]] = []
    cursor = window_start
    while cursor < window_end:
        jitter = timedelta(minutes=rng.uniform(-2, 2))
        burst_start = cursor + jitter
        duration_min = rng.uniform(min_duration, max_duration)
        burst_end = burst_start + timedelta(minutes=duration_min)
        if burst_end > window_end:
            burst_end = window_end
        if burst_start < window_start:
            burst_start = window_start
        bursts.append((burst_start, burst_end))
        cursor += timedelta(minutes=period_minutes)
    return bursts


def in_any_window(ts: datetime, windows: Sequence[Tuple[datetime, datetime]]) -> bool:
    for start, end in windows:
        if start <= ts < end:
            return True
    return False


def minutes_between(start: datetime, end: datetime) -> int:
    return int((end - start).total_seconds() // 60)


def jitter_timestamp(ts: datetime, rng: RandomGenerator, span_seconds: float = 1.0) -> datetime:
    return ts + timedelta(seconds=rng.uniform(0, span_seconds))


class CsvWriter:
    """Simple CSV writer that always emits headers and supports streaming writes."""

    def __init__(self, filepath: str, headers: Sequence[str]) -> None:
        ensure_dir(os.path.dirname(filepath))
        self._fh = open(filepath, "w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._fh)
        self._writer.writerow(headers)

    def write_row(self, row: Sequence) -> None:
        self._writer.writerow(row)

    def close(self) -> None:
        self._fh.close()


@dataclass
class DatasetConfig:
    output_dir: str
    data_dir: str
    seed: int
    app_log_rows: int
    enable_tier2: bool
    zip_output: bool
    avg_logs_per_txn: int = 10
    min_transactions: int = 500

    @property
    def transaction_count(self) -> int:
        target = int(self.app_log_rows / max(1, self.avg_logs_per_txn))
        return max(self.min_transactions, target)


@dataclass
class RowCounter:
    counts: Dict[str, int]

    def increment(self, table: str, amount: int = 1) -> None:
        self.counts[table] = self.counts.get(table, 0) + amount

    def get(self, table: str) -> int:
        return self.counts.get(table, 0)


__all__ = [
    "RandomGenerator",
    "IncidentWindow",
    "ConfounderWindow",
    "daterange_minutes",
    "daterange_5m",
    "isoformat",
    "ensure_dir",
    "diurnal_factor",
    "heavy_tail_latency",
    "generate_incident_bursts",
    "in_any_window",
    "minutes_between",
    "CsvWriter",
    "DatasetConfig",
    "RowCounter",
    "START_TS",
    "END_TS",
    "DATASET_NAME",
    "jitter_timestamp",
]
