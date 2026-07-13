from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from math import ceil
from statistics import median
from typing import Any


def percentile(values: list[float], pct: int) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, ceil((pct / 100.0) * len(ordered)))
    return ordered[rank - 1]


@dataclass
class RoundResult:
    measure_seconds: int
    counts: dict[str, dict[str, dict[str, int]]] = field(
        default_factory=lambda: defaultdict(
            lambda: defaultdict(lambda: defaultdict(int))
        )
    )
    latencies: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    errors: dict[str, dict[str, dict[str, int]]] = field(
        default_factory=lambda: defaultdict(
            lambda: defaultdict(lambda: defaultdict(int))
        )
    )

    def record(
        self,
        phase: str,
        txn_type: str,
        outcome: str,
        latency_ms: float,
        error_detail: str | None = None,
    ) -> None:
        self.counts[phase][txn_type][outcome] += 1
        if outcome == "commit":
            self.latencies[txn_type].append(latency_ms)
        elif error_detail:
            self.errors[phase][txn_type][error_detail] += 1

    def record_batch(
        self, records: list[tuple[str, str, str, float, str | None]]
    ) -> None:
        """Merge worker-local records while the caller holds the result lock."""
        for phase, txn_type, outcome, latency_ms, error_detail in records:
            self.record(phase, txn_type, outcome, latency_ms, error_detail)

    def measured_committed_new_order(self) -> int:
        return self.counts["measure"]["new_order"]["commit"]

    def phase_counts(self, phase: str) -> dict[str, int]:
        committed = 0
        aborted = 0
        new_order_commit = 0
        new_order_abort = 0
        for txn_type, outcomes in self.counts[phase].items():
            for outcome, count in outcomes.items():
                if outcome == "commit":
                    committed += count
                    if txn_type == "new_order":
                        new_order_commit += count
                else:
                    aborted += count
                    if txn_type == "new_order":
                        new_order_abort += count
        total = committed + aborted
        return {
            "commits": committed,
            "aborts": aborted,
            "total": total,
            "new_order_commit": new_order_commit,
            "new_order_abort": new_order_abort,
        }

    def total_committed_new_order(self) -> int:
        return sum(
            self.counts[phase]["new_order"]["commit"]
            for phase in ("warmup", "measure", "drain")
        )

    def tpmc(self) -> float:
        return self.measured_committed_new_order() / (self.measure_seconds / 60.0)

    def abort_rate(self) -> float:
        committed = 0
        aborted = 0
        for txn_counts in self.counts.values():
            for outcomes in txn_counts.values():
                committed += outcomes.get("commit", 0)
                aborted += sum(
                    count for name, count in outcomes.items() if name != "commit"
                )
        total = committed + aborted
        return 0.0 if total == 0 else aborted / total

    def to_dict(self) -> dict[str, Any]:
        return {
            "measure_seconds": self.measure_seconds,
            "tpmc": self.tpmc(),
            "abort_rate": self.abort_rate(),
            "counts": {
                phase: {txn: dict(outcomes) for txn, outcomes in txns.items()}
                for phase, txns in self.counts.items()
            },
            "latency_ms": {
                txn_type: {
                    "p50": percentile(values, 50),
                    "p95": percentile(values, 95),
                    "p99": percentile(values, 99),
                    "max": max(values) if values else None,
                }
                for txn_type, values in self.latencies.items()
            },
            "errors": {
                phase: {txn: dict(details) for txn, details in txns.items()}
                for phase, txns in self.errors.items()
            },
        }


def median_tpmc(rounds: list[RoundResult]) -> float:
    return (
        0.0
        if not rounds
        else float(median(round_result.tpmc() for round_result in rounds))
    )


def format_progress_line(
    result: RoundResult,
    round_no: int,
    total_rounds: int,
    phase: str,
    elapsed_seconds: int,
    total_seconds: int,
) -> str:
    counts = result.phase_counts(phase)
    abort_rate = (
        0.0 if counts["total"] == 0 else counts["aborts"] / counts["total"] * 100.0
    )
    tpmc = 0.0
    if phase == "measure" and elapsed_seconds > 0:
        tpmc = counts["new_order_commit"] / (elapsed_seconds / 60.0)
    return (
        f"[round {round_no}/{total_rounds} {phase} {elapsed_seconds}/{total_seconds}s] "
        f"commits={counts['commits']} aborts={counts['aborts']} "
        f"new_order_commit={counts['new_order_commit']} new_order_abort={counts['new_order_abort']} "
        f"tpmC={tpmc:.2f} abort_rate={abort_rate:.2f}%"
    )
