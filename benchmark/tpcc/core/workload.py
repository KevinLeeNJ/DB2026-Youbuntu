from __future__ import annotations

import os
import random
import threading
import time
from dataclasses import dataclass
from typing import Callable

from benchmark.tpcc.core.backend import BackendAbort, BackendError
from benchmark.tpcc.core.parsing import scalar_int
from benchmark.tpcc.core.result import RoundResult
from benchmark.tpcc.core.transactions import (
    InvalidItemRollback,
    TxnContext,
    delivery,
    new_order,
    order_status,
    payment,
    stock_level,
)

TXN_FUNCS = {
    "new_order": new_order,
    "payment": payment,
    "order_status": order_status,
    "delivery": delivery,
    "stock_level": stock_level,
}

_RESULT_LOCK = threading.Lock()
_RESULT_BATCH_SIZE = 32


def _debug_knobs() -> tuple[float, bool]:
    """Optional timing knobs for reproducing transaction-lifecycle races."""
    try:
        think_ms = max(0.0, float(os.getenv("RMDB_BENCH_THINK_MS", "0")))
    except ValueError:
        think_ms = 0.0
    reconnect = os.getenv("RMDB_BENCH_RECONNECT_EACH_TXN", "0") == "1"
    return think_ms / 1000.0, reconnect


@dataclass
class DatasetProfile:
    warehouses: int
    districts_per_warehouse: int
    customers_per_district: int
    item_count: int


def inspect_dataset(backend_factory: Callable[[], object]) -> DatasetProfile:
    backend = backend_factory()
    try:
        return DatasetProfile(
            warehouses=max(
                1, scalar_int(backend.execute("select count(*) from warehouse;"), 1)
            ),
            districts_per_warehouse=max(
                1, scalar_int(backend.execute("select max(d_id) from district;"), 1)
            ),
            customers_per_district=max(
                1, scalar_int(backend.execute("select max(c_id) from customer;"), 1)
            ),
            item_count=max(
                1, scalar_int(backend.execute("select max(i_id) from item;"), 1)
            ),
        )
    finally:
        backend.close()


def choose_txn() -> str:
    return random.choices(
        ["new_order", "payment", "order_status", "delivery", "stock_level"],
        weights=[45, 43, 4, 4, 4],
        k=1,
    )[0]


def choose_context(profile: DatasetProfile, worker_id: int, policy: str) -> TxnContext:
    if policy == "terminal-home":
        w_id = (worker_id % profile.warehouses) + 1
    else:
        w_id = random.randint(1, profile.warehouses)
    return TxnContext(
        w_id=w_id,
        d_id=random.randint(1, profile.districts_per_warehouse),
        warehouses=profile.warehouses,
        item_count=profile.item_count,
        customers_per_district=profile.customers_per_district,
        districts_per_warehouse=profile.districts_per_warehouse,
    )


def worker_loop(
    backend_factory: Callable[[], object],
    result: RoundResult,
    worker_id: int,
    profile: DatasetProfile,
    policy: str,
    warmup_end: float,
    measure_end: float,
    record_queue=None,
) -> None:
    backend = backend_factory()
    think_seconds, reconnect_each_txn = _debug_knobs()
    pending_records: list[tuple[str, str, str, float, str | None]] = []

    def flush_records() -> None:
        if not pending_records:
            return
        if record_queue is not None:
            # Queue serialization happens on a feeder thread; copy before
            # clearing the worker-owned list.
            record_queue.put(list(pending_records))
        else:
            with _RESULT_LOCK:
                result.record_batch(pending_records)
        pending_records.clear()

    def reconnect_backend() -> None:
        nonlocal backend
        try:
            backend.close()
        except Exception:
            pass
        backend = backend_factory()

    try:
        while True:
            now = time.monotonic()
            if now < warmup_end:
                phase = "warmup"
            elif now < measure_end:
                phase = "measure"
            else:
                break
            txn_type = choose_txn()
            start = time.monotonic()
            try:
                TXN_FUNCS[txn_type](backend, choose_context(profile, worker_id, policy))
                outcome = "commit"
                error_detail = None
            except InvalidItemRollback:
                outcome = "invalid-item-rollback"
                error_detail = "invalid item rollback"
            except BackendAbort as exc:
                try:
                    backend.rollback()
                except Exception:
                    pass
                outcome = "server-abort"
                error_detail = str(exc)
            except BackendError as exc:
                try:
                    backend.rollback()
                except Exception:
                    pass
                reconnect_backend()
                outcome = "backend-error"
                error_detail = str(exc)
            except Exception as exc:
                try:
                    backend.rollback()
                except Exception:
                    pass
                outcome = "backend-error"
                error_detail = f"{type(exc).__name__}: {exc}"
            latency_ms = (time.monotonic() - start) * 1000.0
            pending_records.append(
                (phase, txn_type, outcome, latency_ms, error_detail)
            )
            if len(pending_records) >= _RESULT_BATCH_SIZE:
                flush_records()
            if reconnect_each_txn:
                reconnect_backend()
            if think_seconds > 0:
                time.sleep(think_seconds)
    finally:
        flush_records()
        backend.close()
        if record_queue is not None:
            record_queue.put(None)
