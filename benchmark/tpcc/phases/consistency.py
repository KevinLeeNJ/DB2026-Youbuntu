from __future__ import annotations

from benchmark.tpcc.core.backend import Backend
from benchmark.tpcc.core.constants import (
    DISTRICTS_PER_WAREHOUSE,
    INITIAL_ORDERS_PER_DISTRICT,
)
from benchmark.tpcc.core.parsing import scalar_float, scalar_int


def check_scalar(backend: Backend, sql: str, expected: int, name: str) -> list[str]:
    actual = scalar_int(backend.execute(sql), -1)
    if actual != expected:
        return [f"{name}: expected {expected}, got {actual}"]
    return []


def check_equal_scalars(
    backend: Backend, left_sql: str, right_sql: str, name: str
) -> list[str]:
    left = scalar_int(backend.execute(left_sql), -1)
    right = scalar_int(backend.execute(right_sql), -1)
    if left != right:
        return [f"{name}: left={left}, right={right}"]
    return []


def check_equal_amounts(
    backend: Backend, left_sql: str, right_sql: str, name: str
) -> list[str]:
    left = scalar_float(backend.execute(left_sql), 0.0)
    right = scalar_float(backend.execute(right_sql), 0.0)
    if abs(left - right) > 0.01:
        return [f"{name}: left={left:.2f}, right={right:.2f}"]
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
    post_load_order_lines = (
        f"from order_line where ol_o_id > {INITIAL_ORDERS_PER_DISTRICT}"
    )
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
    failures.extend(
        check_scalar(
            backend, "select count(*) from orders;", expected_orders, "orders total"
        )
    )
    failures.extend(
        check_equal_amounts(
            backend,
            "select sum(w_ytd) from warehouse;",
            "select sum(d_ytd) from district;",
            "warehouse/district YTD mismatch",
        )
    )
    failures.extend(
        check_equal_amounts(
            backend,
            "select sum(w_ytd) from warehouse;",
            "select sum(h_amount) from history;",
            "warehouse/history YTD mismatch",
        )
    )
    failures.extend(
        check_equal_amounts(
            backend,
            "select sum(c_ytd_payment) from customer;",
            "select sum(h_amount) from history;",
            "customer/history payment amount mismatch",
        )
    )
    failures.extend(
        check_equal_scalars(
            backend,
            "select sum(c_payment_cnt) from customer;",
            "select count(*) from history;",
            "customer/history payment count mismatch",
        )
    )
    failures.extend(
        check_equal_amounts(
            backend,
            "select sum(s_ytd) from stock;",
            f"select sum(ol_quantity) {post_load_order_lines};",
            "stock YTD/new order-line quantity mismatch",
        )
    )
    failures.extend(
        check_equal_scalars(
            backend,
            "select sum(s_order_cnt) from stock;",
            f"select count(*) {post_load_order_lines};",
            "stock/new order-line count mismatch",
        )
    )
    failures.extend(
        check_equal_scalars(
            backend,
            "select sum(s_remote_cnt) from stock;",
            f"select count(*) {post_load_order_lines} and ol_supply_w_id != ol_w_id;",
            "stock/remote order-line count mismatch",
        )
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
