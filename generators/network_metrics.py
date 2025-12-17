from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Sequence, Tuple

from utils import (
    CsvWriter,
    IncidentWindow,
    RandomGenerator,
    START_TS,
    END_TS,
    daterange_minutes,
    isoformat,
)

NETWORK_HEADERS = [
    "timestamp",
    "src_region",
    "dst_region",
    "circuit_id",
    "rtt_ms",
    "packet_loss_pct",
    "retransmits_per_s",
    "throughput_mbps",
]


class CircuitSignal:
    def __init__(self, base_rtt: float, base_loss: float, base_retx: float, base_throughput: float) -> None:
        self.base_rtt = base_rtt
        self.base_loss = base_loss
        self.base_retx = base_retx
        self.base_throughput = base_throughput

    def sample(self, rng: RandomGenerator, multiplier: float = 1.0) -> Dict[str, float]:
        rtt = self.base_rtt * rng.uniform(0.9, 1.1) * multiplier
        loss = self.base_loss * rng.uniform(0.8, 1.2) * multiplier
        retx = self.base_retx * rng.uniform(0.9, 1.3) * multiplier
        throughput = self.base_throughput * rng.uniform(0.85, 1.1) / max(1.0, multiplier)
        return {
            "rtt_ms": round(rtt, 2),
            "packet_loss_pct": round(loss, 4),
            "retransmits_per_s": round(retx, 2),
            "throughput_mbps": round(throughput, 2),
        }


def build_circuit_catalog(facts: Iterable[str], rng: RandomGenerator) -> Dict[str, CircuitSignal]:
    catalog: Dict[str, CircuitSignal] = {}
    for cid in facts:
        if cid not in catalog and cid:
            catalog[cid] = CircuitSignal(
                base_rtt=rng.uniform(15, 40),
                base_loss=rng.uniform(0.0001, 0.0008),
                base_retx=rng.uniform(5, 18),
                base_throughput=rng.uniform(250, 600),
            )
    return catalog


def write_network_metrics(
    writer: CsvWriter,
    rng: RandomGenerator,
    incident: IncidentWindow,
    circuits: Dict[str, Tuple[str, str, CircuitSignal]],
) -> Tuple[int, Dict[str, List[Dict[str, float]]]]:
    rows = 0
    metrics_debug: Dict[str, List[Dict[str, float]]] = defaultdict(list)
    start = START_TS
    end = END_TS
    for ts in daterange_minutes(start, end):
        for circuit_id, (src_region, dst_region, signal) in circuits.items():
            multiplier = 1.0
            if circuit_id == incident.circuit_id and any(s <= ts < e for s, e in incident.bursts):
                multiplier = rng.uniform(6, 14)
            sample = signal.sample(rng, multiplier)
            writer.write_row(
                [
                    isoformat(ts, ms=False),
                    src_region,
                    dst_region,
                    circuit_id,
                    sample["rtt_ms"],
                    sample["packet_loss_pct"],
                    sample["retransmits_per_s"],
                    sample["throughput_mbps"],
                ]
            )
            rows += 1
            if circuit_id == incident.circuit_id:
                metrics_debug[circuit_id].append({"timestamp": ts, **sample, "multiplier": multiplier})
    return rows, metrics_debug


def build_circuit_map(
    primary_incident: IncidentWindow,
    additional_pairs: Sequence[Tuple[str, str]],
    rng: RandomGenerator,
) -> Dict[str, Tuple[str, str, CircuitSignal]]:
    circuit_map: Dict[str, Tuple[str, str, CircuitSignal]] = {}
    circuit_map[primary_incident.circuit_id] = (
        primary_incident.src_region,
        primary_incident.dst_region,
        CircuitSignal(18.0, 0.0002, 8.0, 360.0),
    )
    for src, dst in additional_pairs:
        cid = f"CKT-{src[:3].upper()}-{dst[:3].upper()}-{rng.randint(210, 298)}"
        circuit_map[cid] = (
            src,
            dst,
            CircuitSignal(
                base_rtt=rng.uniform(20, 45),
                base_loss=rng.uniform(0.00005, 0.0004),
                base_retx=rng.uniform(3, 12),
                base_throughput=rng.uniform(300, 650),
            ),
        )
    return circuit_map


def build_route_lookup(circuit_map: Dict[str, Tuple[str, str, CircuitSignal]]):
    routes: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for circuit_id, (src, dst, _signal) in circuit_map.items():
        routes[(src, dst)].append(circuit_id)
    return routes


__all__ = [
    "NETWORK_HEADERS",
    "CircuitSignal",
    "build_circuit_map",
    "write_network_metrics",
    "build_route_lookup",
]
