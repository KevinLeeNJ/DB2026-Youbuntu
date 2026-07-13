from __future__ import annotations

from benchmark.tpcc.core.backend import Backend
from benchmark.tpcc.core.constants import (
    DISTRICTS_PER_WAREHOUSE,
)
from benchmark.tpcc.core.parsing import scalar_float, scalar_int


def check_scalar(backend: Backend, sql: str, expected: int, name: str) -> list[str]:
    actual = scalar_int(backend.execute(sql), -1)
    if actual != expected:
        return [f"{name}: expected {expected}, got {actual}"]
    return []


def run_consistency(
    backend: Backend,
    baseline_warehouse_total: int,
    baseline_district_total: int,
) -> list[str]:
    """Validate TPC-C's committed database-state consistency conditions.

    Snapshot isolation can legitimately abort a transaction after the client has
    sent BEGIN, so client-side commit counters are not authoritative. These
    checks only use state that survived commit.
    """
    failures: list[str] = []
    failures.extend(
        check_scalar(
            backend,
            "select count(*) from warehouse;",
            baseline_warehouse_total,
            "warehouse count",
        )
    )
    failures.extend(
        check_scalar(
            backend,
            "select count(*) from district;",
            baseline_district_total,
            "district count",
        )
    )
    for warehouse_id in range(1, baseline_warehouse_total + 1):
        warehouse_ytd = scalar_float(
            backend.execute(f"select w_ytd from warehouse where w_id = {warehouse_id};"),
            0.0,
        )
        district_ytd = scalar_float(
            backend.execute(f"select sum(d_ytd) from district where d_w_id = {warehouse_id};"),
            0.0,
        )
        if abs(warehouse_ytd - district_ytd) > 0.01:
            failures.append(
                f"warehouse/district YTD mismatch w={warehouse_id}: "
                f"warehouse={warehouse_ytd:.2f}, districts={district_ytd:.2f}"
            )
    return failures


def run_district_diagnostics(
    backend: Backend,
    warehouses: int,
    districts_per_warehouse: int = DISTRICTS_PER_WAREHOUSE,
) -> list[str]:
    failures: list[str] = []
    for w_id in range(1, warehouses + 1):
        for d_id in range(1, districts_per_warehouse + 1):
            d_next = scalar_int(
                backend.execute(
                    f"select d_next_o_id from district where d_w_id = {w_id} and d_id = {d_id};"
                ),
                -1,
            )
            max_order = scalar_int(
                backend.execute(
                    f"select max(o_id) from orders where o_w_id = {w_id} and o_d_id = {d_id};"
                ),
                -1,
            )
            max_new_order = scalar_int(
                backend.execute(
                    f"select max(no_o_id) from new_orders where no_w_id = {w_id} and no_d_id = {d_id};"
                ),
                -1,
            )
            if d_next - 1 != max_order:
                failures.append(
                    f"district order id mismatch w={w_id} d={d_id}: "
                    f"d_next={d_next}, max_order={max_order}"
                )

            count_order = scalar_int(
                backend.execute(
                    f"select count(o_id) from orders where o_w_id = {w_id} and o_d_id = {d_id};"
                ),
                0,
            )
            min_order = scalar_int(
                backend.execute(
                    f"select min(o_id) from orders where o_w_id = {w_id} and o_d_id = {d_id};"
                ),
                -1,
            )
            if count_order > 0 and count_order != max_order - min_order + 1:
                failures.append(
                    f"orders gap w={w_id} d={d_id}: "
                    f"count={count_order}, min={min_order}, max={max_order}"
                )

            count_new_order = scalar_int(
                backend.execute(
                    f"select count(no_o_id) from new_orders where no_w_id = {w_id} and no_d_id = {d_id};"
                ),
                0,
            )
            min_new_order = scalar_int(
                backend.execute(
                    f"select min(no_o_id) from new_orders where no_w_id = {w_id} and no_d_id = {d_id};"
                ),
                -1,
            )
            if (
                count_new_order > 0
                and count_new_order != max_new_order - min_new_order + 1
            ):
                failures.append(
                    f"new_orders gap w={w_id} d={d_id}: "
                    f"count={count_new_order}, min={min_new_order}, max={max_new_order}"
                )
            if count_new_order > 0 and max_new_order != max_order:
                failures.append(
                    f"new_orders tail mismatch w={w_id} d={d_id}: "
                    f"max_new_order={max_new_order}, max_order={max_order}"
                )

            count_unassigned = scalar_int(
                backend.execute(
                    f"select count(o_id) from orders where o_w_id = {w_id} and o_d_id = {d_id} "
                    "and o_carrier_id = 0;"
                ),
                0,
            )
            if count_unassigned != count_new_order:
                failures.append(
                    f"pending order mismatch w={w_id} d={d_id}: "
                    f"carrier_zero={count_unassigned}, new_orders={count_new_order}"
                )

            sum_order_line_count = scalar_int(
                backend.execute(
                    f"select sum(o_ol_cnt) from orders where o_w_id = {w_id} and o_d_id = {d_id};"
                ),
                0,
            )
            count_order_line = scalar_int(
                backend.execute(
                    f"select count(ol_o_id) from order_line where ol_w_id = {w_id} and ol_d_id = {d_id};"
                ),
                0,
            )
            if sum_order_line_count != count_order_line:
                failures.append(
                    f"order_line count mismatch w={w_id} d={d_id}: "
                    f"sum_o_ol_cnt={sum_order_line_count}, count_ol_o_id={count_order_line}"
                )
    return failures
