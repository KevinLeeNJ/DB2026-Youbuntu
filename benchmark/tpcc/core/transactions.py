from __future__ import annotations

import random
import time
from dataclasses import dataclass

from benchmark.tpcc.core.backend import Backend, BackendError
from benchmark.tpcc.core.constants import (
    DISTRICTS_PER_WAREHOUSE,
    INITIAL_ORDERS_PER_DISTRICT,
    surname,
)
from benchmark.tpcc.core.parsing import (
    parse_table_rows,
    scalar_float,
    scalar_int,
    scalar_text,
)


class InvalidItemRollback(RuntimeError):
    pass


@dataclass
class TxnContext:
    w_id: int
    d_id: int
    warehouses: int
    item_count: int
    customers_per_district: int
    districts_per_warehouse: int = DISTRICTS_PER_WAREHOUSE


def now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def new_order(backend: Backend, ctx: TxnContext) -> None:
    c_id = random.randint(1, ctx.customers_per_district)
    ol_cnt = random.randint(5, 15)
    backend.begin()
    try:
        backend.execute(
            f"select c_discount, c_last, c_credit, w_tax from customer, warehouse where w_id = {ctx.w_id} "
            f"and c_w_id = w_id and c_d_id = {ctx.d_id} and c_id = {c_id};"
        )
        # Acquire the district write lock before reading the assigned id. Under
        # RC, a SELECT followed by a constant UPDATE can allocate the same id
        # twice; the self-relative UPDATE serializes allocators instead.
        backend.execute(
            f"update district set d_next_o_id = d_next_o_id + 1 where d_id = {ctx.d_id} and d_w_id = {ctx.w_id};"
        )
        next_after_increment = scalar_int(
            backend.execute(
                f"select d_next_o_id, d_tax from district where d_id = {ctx.d_id} and d_w_id = {ctx.w_id};"
            ),
            -1,
        )
        if next_after_increment <= 1:
            raise BackendError("district next order id not found")
        d_next = next_after_increment - 1
        backend.execute(
            f"insert into orders values ({d_next}, {ctx.d_id}, {ctx.w_id}, {c_id}, '{now_text()}', 0, {ol_cnt}, 1);"
        )
        backend.execute(
            f"insert into new_orders values ({d_next}, {ctx.d_id}, {ctx.w_id});"
        )
        invalid = random.randint(1, 100) == 1
        all_local = True
        for number in range(1, ol_cnt + 1):
            item_id = (
                ctx.item_count + 1
                if invalid and number == ol_cnt
                else random.randint(1, ctx.item_count)
            )
            item_price_text = backend.execute(
                f"select i_price, i_name, i_data from item where i_id = {item_id};"
            )
            if scalar_text(item_price_text, "") == "":
                raise InvalidItemRollback()
            price = scalar_float(item_price_text, 1.0)
            qty = random.randint(1, 10)
            supply_w_id = ctx.w_id
            if ctx.warehouses > 1 and random.randint(1, 100) == 1:
                while supply_w_id == ctx.w_id:
                    supply_w_id = random.randint(1, ctx.warehouses)
                all_local = False
            remote_increment = 1 if supply_w_id != ctx.w_id else 0
            # Updating the counters first acquires the stock-row write lock.
            # The following quantity read/modify step is then protected at RC.
            backend.execute(
                f"update stock set s_ytd = s_ytd + {qty}, s_order_cnt = s_order_cnt + 1, "
                f"s_remote_cnt = s_remote_cnt + {remote_increment} where s_i_id = {item_id} "
                f"and s_w_id = {supply_w_id};"
            )
            stock_qty = scalar_int(
                backend.execute(
                    f"select s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, "
                    f"s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 from stock where s_i_id = {item_id} "
                    f"and s_w_id = {supply_w_id};"
                ),
                10,
            )
            quantity_delta = 91 - qty if stock_qty - qty < 10 else -qty
            quantity_op = "+" if quantity_delta >= 0 else "-"
            backend.execute(
                f"update stock set s_quantity = s_quantity {quantity_op} {abs(quantity_delta)} "
                f"where s_i_id = {item_id} and s_w_id = {supply_w_id};"
            )
            backend.execute(
                f"insert into order_line values ({d_next}, {ctx.d_id}, {ctx.w_id}, {number}, {item_id}, {supply_w_id}, "
                f"'{now_text()}', {qty}, {round(price * qty, 2)}, 'dist');"
            )
        if not all_local:
            backend.execute(
                f"update orders set o_all_local = 0 where o_id = {d_next} and o_d_id = {ctx.d_id} "
                f"and o_w_id = {ctx.w_id};"
            )
        backend.commit()
    except InvalidItemRollback:
        backend.rollback()
        raise


def payment(backend: Backend, ctx: TxnContext) -> None:
    c_id = random.randint(1, ctx.customers_per_district)
    amount = round(random.uniform(1.0, 5000.0), 2)
    backend.begin()
    backend.execute(
        f"update warehouse set w_ytd = w_ytd + {amount} where w_id = {ctx.w_id};"
    )
    backend.execute(
        f"select w_street_1, w_street_2, w_city, w_state, w_zip, w_name from warehouse where w_id = {ctx.w_id};"
    )
    backend.execute(
        f"update district set d_ytd = d_ytd + {amount} where d_w_id = {ctx.w_id} and d_id = {ctx.d_id};"
    )
    backend.execute(
        f"select d_street_1, d_street_2, d_city, d_state, d_zip, d_name from district where "
        f"d_w_id = {ctx.w_id} and d_id = {ctx.d_id};"
    )
    backend.execute(
        f"update customer set c_balance = c_balance - {amount}, "
        f"c_ytd_payment = c_ytd_payment + {amount}, c_payment_cnt = c_payment_cnt + 1 "
        f"where c_w_id = {ctx.w_id} and c_d_id = {ctx.d_id} and c_id = {c_id};"
    )
    backend.execute(
        f"select c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, "
        f"c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since from customer where "
        f"c_w_id = {ctx.w_id} and c_d_id = {ctx.d_id} and c_id = {c_id};"
    )
    backend.execute(
        f"insert into history values ({c_id}, {ctx.d_id}, {ctx.w_id}, {ctx.d_id}, {ctx.w_id}, '{now_text()}', "
        f"{amount}, 'payment');"
    )
    backend.commit()


def order_status(backend: Backend, ctx: TxnContext) -> None:
    backend.begin()
    if random.randint(1, 100) <= 60:
        c_last = surname(random.randint(0, 999))
        backend.execute(
            f"select count(c_id) as count_c_id from customer where c_w_id = {ctx.w_id} and c_d_id = {ctx.d_id} "
            f"and c_last = '{c_last}';"
        )
        backend.execute(
            f"select c_balance, c_first, c_middle, c_last from customer where c_w_id = {ctx.w_id} and "
            f"c_d_id = {ctx.d_id} and c_last = '{c_last}' order by c_first;"
        )
        c_id = random.randint(1, ctx.customers_per_district)
    else:
        c_id = random.randint(1, ctx.customers_per_district)
        backend.execute(
            f"select c_balance, c_first, c_middle, c_last from customer where c_w_id = {ctx.w_id} and "
            f"c_d_id = {ctx.d_id} and c_id = {c_id};"
        )
    o_id = random.randint(1, INITIAL_ORDERS_PER_DISTRICT)
    backend.execute(
        f"select o_id, o_entry_d, o_carrier_id from orders where o_w_id = {ctx.w_id} and o_d_id = {ctx.d_id} "
        f"and o_c_id = {c_id} and o_id = {o_id};"
    )
    backend.execute(
        f"select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from order_line "
        f"where ol_w_id = {ctx.w_id} and ol_d_id = {ctx.d_id} and ol_o_id = {o_id};"
    )
    backend.commit()


def delivery(backend: Backend, ctx: TxnContext) -> None:
    carrier_id = random.randint(1, 10)
    backend.begin()
    for d_id in range(1, ctx.districts_per_warehouse + 1):
        # Serialize Delivery and NewOrder for this district before choosing the
        # oldest pending order. This prevents two RC transactions from both
        # processing the same new_orders row.
        backend.execute(
            f"update district set d_next_o_id = d_next_o_id + 0 where d_w_id = {ctx.w_id} and d_id = {d_id};"
        )
        o_id = scalar_int(
            backend.execute(
                f"select min(no_o_id) from new_orders where no_w_id = {ctx.w_id} and no_d_id = {d_id};"
            ),
            0,
        )
        if o_id == 0:
            continue
        backend.execute(
            f"delete from new_orders where no_w_id = {ctx.w_id} and no_d_id = {d_id} and no_o_id = {o_id};"
        )
        customer_id = scalar_int(
            backend.execute(
                f"select o_c_id from orders where o_id = {o_id} and o_d_id = {d_id} and o_w_id = {ctx.w_id};"
            ),
            0,
        )
        backend.execute(
            f"update orders set o_carrier_id = {carrier_id} where o_id = {o_id} and o_d_id = {d_id} "
            f"and o_w_id = {ctx.w_id};"
        )
        backend.execute(
            f"update order_line set ol_delivery_d = '{now_text()}' where ol_o_id = {o_id} and ol_d_id = {d_id} "
            f"and ol_w_id = {ctx.w_id};"
        )
        amount = scalar_float(
            backend.execute(
                f"select sum(ol_amount) from order_line where ol_o_id = {o_id} and ol_d_id = {d_id} "
                f"and ol_w_id = {ctx.w_id};"
            ),
            0.0,
        )
        if customer_id > 0:
            backend.execute(
                f"update customer set c_balance = c_balance + {amount}, c_delivery_cnt = c_delivery_cnt + 1 "
                f"where c_id = {customer_id} and c_d_id = {d_id} and c_w_id = {ctx.w_id};"
            )
    backend.commit()


def stock_level(backend: Backend, ctx: TxnContext) -> None:
    threshold = random.randint(10, 20)
    backend.begin()
    d_next = scalar_int(
        backend.execute(
            f"select d_next_o_id from district where d_id = {ctx.d_id} and d_w_id = {ctx.w_id};"
        ),
        0,
    )
    item_text = backend.execute(
        f"select ol_i_id from order_line where ol_w_id = {ctx.w_id} and ol_d_id = {ctx.d_id} and "
        f"ol_o_id < {d_next} and ol_o_id >= {max(1, d_next - 20)};"
    )
    for row in parse_table_rows(item_text):
        if not row:
            continue
        item_id = scalar_int(row[0], 0)
        if item_id <= 0:
            continue
        backend.execute(
            f"select count(*) as count_stock from stock where s_w_id = {ctx.w_id} and s_i_id = {item_id} "
            f"and s_quantity < {threshold};"
        )
    backend.commit()


def count_rows(backend: Backend, table: str) -> int:
    return scalar_int(backend.execute(f"select count(*) from {table};"), 0)


def fetch_text(backend: Backend, sql: str, default: str = "") -> str:
    return scalar_text(backend.execute(sql), default)
