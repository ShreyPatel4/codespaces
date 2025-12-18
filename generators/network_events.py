from __future__ import annotations

from datetime import timedelta
from typing import Dict, Tuple

from utils import CsvWriter, IncidentWindow, RandomGenerator, isoformat, START_TS, END_TS
from .network_metrics import CircuitSignal

NETWORK_EVENT_HEADERS = [
    "event_id",
    "timestamp",
    "event_type",
    "src_region",
    "dst_region",
    "circuit_id",
    "severity",
    "description",
]

EVENT_TYPES = ["flap", "maintenance", "reroute", "packet_loss_burst"]
SEVERITIES = ["info", "warning", "critical"]


def write_network_events(
    writer: CsvWriter,
    rng: RandomGenerator,
    incident: IncidentWindow,
    circuits: Dict[str, Tuple[str, str, CircuitSignal]],
) -> Dict[str, int]:
    rows = 0
    event_id = 0

    def emit(ts, event_type: str, src: str, dst: str, circuit_id: str, severity: str, description: str) -> None:
        nonlocal rows, event_id
        writer.write_row(
            [
                f"EVT-{event_id:06d}",
                isoformat(ts, ms=False),
                event_type,
                src,
                dst,
                circuit_id,
                severity,
                description,
            ]
        )
        rows += 1
        event_id += 1

    # Strong signal: degraded circuit at incident start, plus a "fix" event.
    emit(
        incident.start,
        "packet_loss_burst",
        incident.src_region,
        incident.dst_region,
        incident.circuit_id,
        "critical",
        "Packet loss burst and RTT elevation detected on primary circuit",
    )
    emit(
        incident.fix_time,
        "reroute",
        incident.src_region,
        incident.dst_region,
        incident.circuit_id,
        "info",
        "Traffic rerouted away from degraded circuit; service latency recovers",
    )

    # Low-noise background events across other circuits.
    non_incident_circuits = [
        (circuit_id, src, dst)
        for circuit_id, (src, dst, _sig) in circuits.items()
        if circuit_id != incident.circuit_id
    ]
    for _ in range(10):
        circuit_id, src, dst = rng.choice(non_incident_circuits)
        offset_minutes = rng.randint(0, int((END_TS - START_TS).total_seconds() // 60) - 1)
        ts = START_TS + timedelta(minutes=offset_minutes)
        event_type = rng.choice(EVENT_TYPES)
        if event_type == "maintenance":
            severity = "info"
            description = "Planned maintenance window on circuit"
        elif event_type == "reroute":
            severity = "warning"
            description = "Routing policy change applied; path flaps briefly"
        elif event_type == "flap":
            severity = "warning"
            description = "Intermittent circuit flap observed"
        else:
            severity = "warning" if rng.random() < 0.85 else "critical"
            description = "Short packet loss burst on circuit"
        # Avoid additional critical signals to keep the primary incident clean.
        if severity == "critical":
            severity = "warning"
        emit(ts, event_type, src, dst, circuit_id, severity, description)

    return {"rows": rows}


__all__ = ["NETWORK_EVENT_HEADERS", "write_network_events"]
