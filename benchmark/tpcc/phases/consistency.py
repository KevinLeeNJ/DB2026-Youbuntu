from __future__ import annotations

from benchmark.tpcc.core.backend import Backend
from benchmark.tpcc.core.constants import DISTRICTS_PER_WAREHOUSE
from benchmark.tpcc.core.parsing import scalar_int


def check_scalar(backend: Backend, sql: str, expected: int, name: str) -> list[str]:
    actual = scalar_int(backend.execute(sql), -1)
    if actual != expected:
        return [f"{name}: expected {expected}, got {actual}"]
    return []


def run_consistency(
    backend: Backend,
    baseline_warehouse_total: int,
    baseline_district_total: int,
    baseline_orders_total: int,
    total_committed_new_order: int,
) -> list[str]:
    failures: list[str] = []
    expected_orders = baseline_orders_total + total_committed_new_order
    failures.extend(check_scalar(backend, "select count(*) from warehouse;", baseline_warehouse_total, "warehouse count"))
    failures.extend(check_scalar(backend, "select count(*) from district;", baseline_district_total, "district count"))
    failures.extend(check_scalar(backend, "select count(*) from orders;", expected_orders, "orders total"))
    return failures


def run_district_diagnostics(backend: Backend, warehouses: int, districts_per_warehouse: int = DISTRICTS_PER_WAREHOUSE) -> list[str]:
    failures: list[str] = []
    for w_id in range(1, warehouses + 1):
        for d_id in range(1, districts_per_warehouse + 1):
            d_next = scalar_int(
                backend.execute(f"select d_next_o_id from district where d_w_id = {w_id} and d_id = {d_id};"),
                -1,
            )
            max_order = scalar_int(
                backend.execute(f"select max(o_id) from orders where o_w_id = {w_id} and o_d_id = {d_id};"),
                -1,
            )
            if d_next != max_order + 1:
                failures.append(f"district next id mismatch w={w_id} d={d_id}: d_next={d_next}, max_order={max_order}")
    return failures
