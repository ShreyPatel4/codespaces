from __future__ import annotations

from typing import Dict, Iterable, List, Tuple

from utils import (
    CsvWriter,
    RandomGenerator,
    daterange_5m,
    isoformat,
    START_TS,
    END_TS,
)
from .txn_facts import TransactionFact

INFRA_HEADERS = [
    "timestamp",
    "region",
    "host",
    "cpu_pct",
    "mem_pct",
    "disk_io_util_pct",
    "net_errs_per_s",
]


HOSTS_PER_REGION = 12


def generate_hosts() -> Dict[str, List[str]]:
    hosts = {}
    for region in ["central", "east", "west"]:
        hosts[region] = [f"fibersqs-{region}-infra{idx:02d}" for idx in range(1, HOSTS_PER_REGION + 1)]
    return hosts


def write_infra_metrics(
    writer: CsvWriter,
    rng: RandomGenerator,
    hosts: Dict[str, List[str]],
    confounder_windows,
) -> Tuple[int, Dict[str, Dict[str, float]]]:
    rows = 0
    cpu_debug: Dict[str, Dict[str, float]] = {"central": {"max": 0.0}, "west": {"max": 0.0}}
    for ts in daterange_5m(START_TS, END_TS):
        for region, region_hosts in hosts.items():
            for host in region_hosts:
                cpu = rng.uniform(18, 52)
                mem = rng.uniform(40, 70)
                disk = rng.uniform(20, 60)
                net_errs = rng.uniform(0.01, 0.08)
                for conf in confounder_windows:
                    if conf.region in (region, "*") and conf.start <= ts < conf.end:
                        if conf.name == "central_cpu_spike":
                            cpu = rng.uniform(75, 97)
                        if conf.name == "west_deployment_blip":
                            net_errs = rng.uniform(0.1, 0.4)
                writer.write_row(
                    [
                        isoformat(ts, ms=False),
                        region,
                        host,
                        round(cpu, 2),
                        round(mem, 2),
                        round(disk, 2),
                        round(net_errs, 3),
                    ]
                )
                rows += 1
                cpu_debug.setdefault(region, {"max": 0.0})
                cpu_debug[region]["max"] = max(cpu_debug[region]["max"], cpu)
    return rows, cpu_debug


__all__ = ["INFRA_HEADERS", "generate_hosts", "write_infra_metrics"]
