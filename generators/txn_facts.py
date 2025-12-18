from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from utils import (
    ConfounderWindow,
    DatasetConfig,
    IncidentWindow,
    RandomGenerator,
    diurnal_factor,
    generate_incident_bursts,
    in_any_window,
    isoformat,
    minutes_between,
    START_TS,
    END_TS,
)


IMPACTED_TRANSACTION_TYPES = ["provision_fiber_sqs", "modify_service_profile"]
NON_IMPACTED_TYPES = [
    "cancel_subscription",
    "diagnostic_ping",
    "update_billing",
    "firmware_update",
    "service_health_check",
]
SERVICE_TYPE_MAP = {
    "provision_fiber_sqs": "fiber_sqs",
    "modify_service_profile": "fiber_sqs",
    "cancel_subscription": "fiber_tv",
    "diagnostic_ping": "fiber_sqs",
    "update_billing": "fiber_internet",
    "firmware_update": "fiber_internet",
    "service_health_check": "fiber_sqs",
}
REGIONS = ["east", "west", "central", "north", "south"]
REGION_WEIGHTS = {"central": 0.35, "east": 0.25, "west": 0.20, "north": 0.10, "south": 0.10}
SERVICES_CHAIN = ["api", "orchestrator", "worker"]
DEPENDENCY_SERVICE = "inventory-client"
TXN_FACT_HEADERS = [
    "transaction_id",
    "customer_id",
    "origin_region",
    "txn_type",
    "start_ts",
    "end_ts",
    "success",
    "error_code",
    "end_to_end_latency_ms",
]


@dataclass
class TransactionFact:
    transaction_id: str
    trace_id: str
    customer_id: str
    customer_region: str
    transaction_type: str
    service_type: str
    region: str
    start_ts: datetime
    end_ts: datetime
    dependency_region: Optional[str]
    dependency_service: Optional[str]
    circuit_id: Optional[str]
    dependency_latency_ms: float
    end_to_end_latency_ms: float
    base_latency_ms: float
    final_status: str
    retry_count: int
    makes_cross_region_call: bool
    impacted_by_primary: bool
    impacted_by_confounder: bool
    confounder_label: Optional[str]
    error_code: Optional[str]
    http_status: str
    clock_skew_ms: int
    services_chain: Sequence[str]

    @property
    def attempt_count(self) -> int:
        return max(1, self.retry_count + 1)


def txn_fact_row(fact: "TransactionFact") -> List[object]:
    return [
        fact.transaction_id,
        fact.customer_id,
        fact.region,
        fact.transaction_type,
        isoformat(fact.start_ts),
        isoformat(fact.end_ts),
        "true" if fact.final_status != "timeout" else "false",
        fact.error_code or "",
        round(fact.end_to_end_latency_ms, 2),
    ]


class TransactionFactStream:
    def __init__(
        self,
        config: DatasetConfig,
        rng: RandomGenerator,
        incident_window: IncidentWindow,
        confounders: Sequence[ConfounderWindow],
        circuit_routes: Dict[tuple, List[str]],
    ) -> None:
        self.config = config
        self.rng = rng
        self.incident = incident_window
        self.confounders = confounders
        self.circuit_routes = circuit_routes
        self.total_minutes = minutes_between(START_TS, END_TS)
        self.base_per_minute = config.transaction_count / max(1, self.total_minutes)
        self.minute_cursor = START_TS
        self.generated = 0
        self.carry = 0.0

    def __iter__(self) -> Iterator[TransactionFact]:
        for minute in self._minute_iterator():
            if self.generated >= self.config.transaction_count:
                break
            count = self._transactions_for_minute(minute)
            for _ in range(count):
                if self.generated >= self.config.transaction_count:
                    break
                ts = minute + timedelta(seconds=self.rng.uniform(0, 60))
                fact = self._build_fact(ts, self.generated)
                self.generated += 1
                yield fact
        while self.generated < self.config.transaction_count:
            minute = START_TS + timedelta(minutes=self.rng.randint(0, self.total_minutes - 1))
            ts = minute + timedelta(seconds=self.rng.uniform(0, 60))
            fact = self._build_fact(ts, self.generated)
            self.generated += 1
            yield fact

    def _minute_iterator(self) -> Iterator[datetime]:
        cursor = START_TS
        while cursor < END_TS:
            yield cursor
            cursor += timedelta(minutes=1)

    def _transactions_for_minute(self, minute: datetime) -> int:
        lam = self.base_per_minute * diurnal_factor(minute) * (0.9 + 0.2 * self.rng.random())
        deterministic = int(lam)
        self.carry += lam - deterministic
        if self.carry >= 1:
            deterministic += 1
            self.carry -= 1
        return deterministic

    def _select_region(self) -> str:
        r = self.rng.random()
        cumulative = 0.0
        for region in REGIONS:
            weight = REGION_WEIGHTS.get(region, 0.0)
            cumulative += weight
            if r <= cumulative:
                return region
        return "central"

    def _select_transaction_type(self, region: str) -> str:
        weights: Dict[str, float] = {
            "provision_fiber_sqs": 0.25 if region == "central" else 0.15,
            "modify_service_profile": 0.18 if region == "central" else 0.12,
            "cancel_subscription": 0.08,
            "diagnostic_ping": 0.17,
            "update_billing": 0.17,
            "firmware_update": 0.10,
            "service_health_check": 0.15,
        }
        return self.rng.weighted_choice(weights)

    def _build_fact(self, ts: datetime, seq: int) -> TransactionFact:
        region = self._select_region()
        transaction_type = self._select_transaction_type(region)
        service_type = SERVICE_TYPE_MAP[transaction_type]
        customer_id = f"CUST-{self.rng.randint(10000000, 99999999)}"
        transaction_id = f"TX-{ts.strftime('%Y%m%d%H%M%S')}-{seq:07d}"
        trace_id = self.rng.hex_id("tr")
        makes_cross_region_call = False
        dependency_region: Optional[str] = None
        dependency_service: Optional[str] = None
        circuit_id: Optional[str] = None
        base_latency = 420.0 if transaction_type in IMPACTED_TRANSACTION_TYPES else 280.0
        dependency_latency = 0.0
        impacted_by_primary = False
        if transaction_type in IMPACTED_TRANSACTION_TYPES and region == "central":
            makes_cross_region_call = True
            dependency_region = "east"
            dependency_service = DEPENDENCY_SERVICE
            circuit_id = self.incident.circuit_id
            if in_any_window(ts, self.incident.bursts):
                impacted_by_primary = True
                dependency_latency = base_latency * self.rng.uniform(5, 12)
            else:
                dependency_latency = base_latency * self.rng.uniform(0.9, 1.6)
        else:
            available_routes = [pair for pair in self.circuit_routes if pair[0] == region and pair[1] != region]
            if available_routes and self.rng.random() < 0.08:
                makes_cross_region_call = True
                route = self.rng.choice(available_routes)
                dependency_region = route[1]
                dependency_service = "inventory-client"
                circuits = [
                    cid
                    for cid in (self.circuit_routes.get(route) or [])
                    if cid != self.incident.circuit_id
                ]
                if not circuits:
                    circuits = self.circuit_routes.get(route) or []
                if not circuits:
                    circuits = [self.incident.circuit_id]
                circuit_id = self.rng.choice(circuits)
                dependency_latency = base_latency * self.rng.uniform(0.8, 1.8)

        confounder_label: Optional[str] = None
        impacted_by_confounder = False
        for conf in self.confounders:
            if conf.start <= ts < conf.end and (conf.region == region or conf.region == "*"):
                impacted_by_confounder = True
                confounder_label = conf.name
                break

        burst_multiplier = 1.0
        if impacted_by_primary:
            burst_multiplier = self.rng.uniform(2.5, 4.5)
        elif impacted_by_confounder:
            burst_multiplier = self.rng.uniform(1.2, 1.8)

        end_to_end = base_latency * (0.8 + self.rng.uniform(0.0, 0.4)) * burst_multiplier
        retry_count = 0
        if impacted_by_primary:
            retry_count = self.rng.randint(1, 3)
        elif impacted_by_confounder and self.rng.random() < 0.4:
            retry_count = 1

        status = "success"
        error_code: Optional[str] = None
        http_status = "200"
        failure_bias = 0.05
        if impacted_by_primary:
            failure_bias = 0.35
        elif impacted_by_confounder:
            failure_bias = 0.12
        if self.rng.random() < failure_bias:
            status = "timeout"
            http_status = "504"
            error_code = "DEP_TIMEOUT" if makes_cross_region_call else "ORCH_TIMEOUT"
        elif self.rng.random() < 0.08:
            status = "retry"
            http_status = "202"
        elif retry_count > 0:
            status = "completed_after_retry"

        clock_skew = self.rng.randint(-500, 500)
        end_ts = ts + timedelta(milliseconds=end_to_end)

        return TransactionFact(
            transaction_id=transaction_id,
            trace_id=trace_id,
            customer_id=customer_id,
            customer_region=region,
            transaction_type=transaction_type,
            service_type=service_type,
            region=region,
            start_ts=ts,
            end_ts=end_ts,
            dependency_region=dependency_region,
            dependency_service=dependency_service,
            circuit_id=circuit_id,
            dependency_latency_ms=dependency_latency,
            end_to_end_latency_ms=end_to_end,
            base_latency_ms=base_latency,
            final_status=status,
            retry_count=retry_count,
            makes_cross_region_call=makes_cross_region_call,
            impacted_by_primary=impacted_by_primary,
            impacted_by_confounder=impacted_by_confounder,
            confounder_label=confounder_label,
            error_code=error_code,
            http_status=http_status,
            clock_skew_ms=clock_skew,
            services_chain=SERVICES_CHAIN,
        )


def build_incident(seed: int, circuit_id: str = "CKT-CEN-EAS-003") -> IncidentWindow:
    rng = RandomGenerator(seed + 991)
    start = datetime(2025, 12, 3, 12, 20, tzinfo=START_TS.tzinfo)
    end = datetime(2025, 12, 5, 18, 10, tzinfo=START_TS.tzinfo)
    fix_time = datetime(2025, 12, 5, 18, 15, tzinfo=START_TS.tzinfo)
    bursts = generate_incident_bursts(start, end, rng)
    return IncidentWindow(
        start=start,
        end=end,
        fix_time=fix_time,
        bursts=bursts,
        circuit_id=circuit_id,
        src_region="central",
        dst_region="east",
    )


def default_confounders() -> List[ConfounderWindow]:
    return [
        ConfounderWindow(
            name="central_cpu_spike",
            start=datetime(2025, 12, 2, 9, 30, tzinfo=START_TS.tzinfo),
            end=datetime(2025, 12, 2, 11, 0, tzinfo=START_TS.tzinfo),
            region="central",
            component="infra",
            description="Short CPU saturation on central hosts",
        ),
        ConfounderWindow(
            name="west_deployment_blip",
            start=datetime(2025, 12, 6, 17, 0, tzinfo=START_TS.tzinfo),
            end=datetime(2025, 12, 6, 18, 0, tzinfo=START_TS.tzinfo),
            region="west",
            component="app",
            description="Deployment-induced latency bump in west",
        ),
    ]


__all__ = [
    "TransactionFact",
    "TransactionFactStream",
    "build_incident",
    "default_confounders",
    "IMPACTED_TRANSACTION_TYPES",
    "REGIONS",
    "TXN_FACT_HEADERS",
    "txn_fact_row",
]
