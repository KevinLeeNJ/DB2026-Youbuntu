from __future__ import annotations

import threading
import time
from typing import Callable

from benchmark.tpcc.core.result import RoundResult, format_progress_line
from benchmark.tpcc.core.workload import inspect_dataset, worker_loop


def progress_monitor(
    result: RoundResult,
    round_no: int,
    total_rounds: int,
    warmup_seconds: int,
    measure_seconds: int,
    warmup_end: float,
    measure_end: float,
    interval_seconds: int,
    stop_event: threading.Event,
) -> None:
    last_phase = ""
    while not stop_event.wait(interval_seconds):
        now = time.monotonic()
        if now < warmup_end:
            phase = "warmup"
            elapsed = max(0, int(warmup_seconds - (warmup_end - now)))
            total = warmup_seconds
        elif now < measure_end:
            phase = "measure"
            elapsed = max(0, int(measure_seconds - (measure_end - now)))
            total = measure_seconds
        else:
            break
        if phase != last_phase:
            last_phase = phase
        print(format_progress_line(result, round_no, total_rounds, phase, elapsed, total), flush=True)


def run_round(
    backend_factory: Callable[[], object],
    warehouses: int,
    workers: int,
    warmup_seconds: int,
    measure_seconds: int,
    warehouse_policy: str,
    round_no: int,
    total_rounds: int,
    progress_interval: int,
) -> RoundResult:
    result = RoundResult(measure_seconds=measure_seconds)
    profile = inspect_dataset(backend_factory)
    warmup_end = time.monotonic() + warmup_seconds
    measure_end = warmup_end + measure_seconds
    print(format_progress_line(result, round_no, total_rounds, "warmup", 0, warmup_seconds), flush=True)
    stop_event = threading.Event()
    monitor = None
    if progress_interval > 0:
        monitor = threading.Thread(
            target=progress_monitor,
            args=(
                result,
                round_no,
                total_rounds,
                warmup_seconds,
                measure_seconds,
                warmup_end,
                measure_end,
                progress_interval,
                stop_event,
            ),
            daemon=True,
        )
        monitor.start()
    threads = [
        threading.Thread(
            target=worker_loop,
            args=(backend_factory, result, worker_id, profile, warehouse_policy, warmup_end, measure_end),
        )
        for worker_id in range(workers)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    stop_event.set()
    if monitor is not None:
        monitor.join()
    print(format_progress_line(result, round_no, total_rounds, "measure", measure_seconds, measure_seconds), flush=True)
    return result


def run_benchmark(
    backend_factory: Callable[[], object],
    warehouses: int,
    workers: int = 16,
    warmup_seconds: int = 30,
    measure_seconds: int = 360,
    rounds: int = 3,
    warehouse_policy: str = "terminal-home",
    progress_interval: int = 5,
) -> list[RoundResult]:
    return [
        run_round(
            backend_factory,
            warehouses,
            workers,
            warmup_seconds,
            measure_seconds,
            warehouse_policy,
            round_no,
            rounds,
            progress_interval,
        )
        for round_no in range(1, rounds + 1)
    ]
