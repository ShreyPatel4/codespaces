from __future__ import annotations

from datetime import timedelta
from typing import Iterable, List, Sequence

from utils import CsvWriter, IncidentWindow, RandomGenerator, isoformat

NETWORK_EVENT_HEADERS = [
    "event_id",
    "ts",
    "source",
    "region",
    "circuit_id",
    "severity",
    "alert_name",
    "fired",
    "cleared_ts",
    "predicted_cause",
    "linked_incident_id",
]


SOURCES = ["slo", "network_nms", "synthetic_check"]


def write_network_events(writer: CsvWriter, rng: RandomGenerator, incident: IncidentWindow):
    rows = 0
    alert_id = 0
    tp = 0
    fp = 0
    fn = 0
    for burst_start, burst_end in incident.bursts:
        fired_ts = burst_start
        cleared_ts = burst_end + timedelta(minutes=2)
        writer.write_row(
            [
                f"ALERT-{alert_id:05d}",
                isoformat(fired_ts),
                "network_nms",
                incident.src_region,
                incident.circuit_id,
                "critical",
                "circuit_latency_spike",
                "true",
                isoformat(cleared_ts),
                "network",
                "INC-001",
            ]
        )
        rows += 1
        alert_id += 1
        tp += 1
    fn = max(1, len(incident.bursts) // 5)
    for idx in range(10):
        ts = incident.start - timedelta(hours=10) + timedelta(hours=idx)
        writer.write_row(
            [
                f"ALERT-{alert_id:05d}",
                isoformat(ts),
                rng.choice(SOURCES),
                rng.choice(["central", "east", "west"]),
                "",
                rng.choice(["warning", "critical"]),
                rng.choice(["slo_latency", "http_error", "synthetic_timeout"]),
                rng.choice(["true", "false"]),
                isoformat(ts + timedelta(minutes=rng.randint(5, 60))),
                rng.choice(["application", "network", "infra", "unknown"]),
                "" if rng.random() < 0.5 else "INC-00" + str(rng.randint(2, 5)),
            ]
        )
        rows += 1
        fp += 1
        alert_id += 1
    return {"rows": rows, "tp": tp, "fp": fp, "fn": fn}


__all__ = ["NETWORK_EVENT_HEADERS", "write_network_events"]
