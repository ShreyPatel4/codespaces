from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class ValidationResult:
    referential_integrity: Dict[str, float] = field(default_factory=dict)
    incident_coherence: Dict[str, float] = field(default_factory=dict)
    confounder_separability: Dict[str, float] = field(default_factory=dict)
    alert_quality: Dict[str, int] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(
            {
                "referential_integrity": self.referential_integrity,
                "incident_coherence": self.incident_coherence,
                "confounder_separability": self.confounder_separability,
                "alert_quality": self.alert_quality,
            },
            indent=2,
        )


class Validator:
    def __init__(self) -> None:
        self.result = ValidationResult()

    def check_referential_integrity(
        self,
        tso_refs: int,
        tso_matches: int,
        trace_refs: int,
        trace_matches: int,
    ) -> None:
        tso_den = tso_refs if tso_refs > 0 else 1
        trace_den = trace_refs if trace_refs > 0 else 1
        self.result.referential_integrity = {
            "tso_match_pct": 100.0 if tso_refs == 0 else round(100.0 * tso_matches / tso_den, 3),
            "trace_match_pct": 100.0 if trace_refs == 0 else round(100.0 * trace_matches / trace_den, 3),
        }

    def check_incident_coherence(
        self,
        burst_latency_avg: float,
        baseline_latency_avg: float,
        burst_timeout_rate: float,
        baseline_timeout_rate: float,
        network_rtt_burst: float,
        network_rtt_base: float,
    ) -> None:
        self.result.incident_coherence = {
            "dependency_latency_multiplier": round(burst_latency_avg / max(1.0, baseline_latency_avg), 2),
            "timeout_rate_multiplier": round(burst_timeout_rate / max(0.01, baseline_timeout_rate), 2),
            "network_rtt_multiplier": round(network_rtt_burst / max(1.0, network_rtt_base), 2),
        }

    def check_confounder_separability(
        self,
        cpu_peak: float,
        network_peak_on_cpu_window: float,
    ) -> None:
        diff = max(0.0001, network_peak_on_cpu_window)
        self.result.confounder_separability = {
            "cpu_peak_pct": round(cpu_peak, 2),
            "network_peak_during_cpu": round(diff, 4),
        }

    def check_alert_quality(self, tp: int, fp: int, fn: int) -> None:
        self.result.alert_quality = {"tp": tp, "fp": fp, "fn": fn}

    def summary(self) -> str:
        return self.result.to_json()


__all__ = ["Validator", "ValidationResult"]
