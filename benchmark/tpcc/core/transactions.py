from __future__ import annotations

import random
import time
from dataclasses import dataclass

from benchmark.tpcc.core.backend import Backend, BackendError
from benchmark.tpcc.core.constants import DISTRICTS_PER_WAREHOUSE
from benchmark.tpcc.core.parsing import scalar_float, scalar_int, scalar_text


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
            f"update district set d_next_o_id = d_next_o_id + 1 where d_w_id = {ctx.w_id} and d_id = {ctx.d_id};"
        )
        d_next_after = scalar_int(
            backend.execute(
                f"select d_next_o_id from district where d_w_id = {ctx.w_id} and d_id = {ctx.d_id};"
            ),
            -1,
        )
        d_next = d_next_after - 1
        if d_next <= 0:
            raise BackendError("district next order id not found")
        backend.execute(
            f"insert into orders values ({d_next}, {ctx.d_id}, {ctx.w_id}, {c_id}, '{now_text()}', 0, {ol_cnt}, 1);"
        )
        backend.execute(
            f"insert into new_orders values ({d_next}, {ctx.d_id}, {ctx.w_id});"
        )
        invalid = random.randint(1, 100) == 1
        for number in range(1, ol_cnt + 1):
            item_id = (
                ctx.item_count + 1
                if invalid and number == ol_cnt
                else random.randint(1, ctx.item_count)
            )
            item_price_text = backend.execute(
                f"select i_price from item where i_id = {item_id};"
            )
            if scalar_text(item_price_text, "") == "":
                raise InvalidItemRollback()
            price = scalar_float(item_price_text, 1.0)
            qty = random.randint(1, 10)
            stock_qty = scalar_int(
                backend.execute(
                    f"select s_quantity from stock where s_w_id = {ctx.w_id} and s_i_id = {item_id};"
                ),
                10,
            )
            new_qty = stock_qty + 91 - qty if stock_qty - qty < 10 else stock_qty - qty
            backend.execute(
                f"update stock set s_quantity = {new_qty}, s_ytd = s_ytd + {qty}, s_order_cnt = s_order_cnt + 1 "
                f"where s_w_id = {ctx.w_id} and s_i_id = {item_id};"
            )
            backend.execute(
                f"insert into order_line values ({d_next}, {ctx.d_id}, {ctx.w_id}, {number}, {item_id}, {ctx.w_id}, "
                f"'{now_text()}', {qty}, {round(price * qty, 2)}, 'dist');"
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
        f"update district set d_ytd = d_ytd + {amount} where d_w_id = {ctx.w_id} and d_id = {ctx.d_id};"
    )
    backend.execute(
        f"update customer set c_balance = c_balance - {amount}, c_ytd_payment = c_ytd_payment + {amount}, "
        f"c_payment_cnt = c_payment_cnt + 1 where c_w_id = {ctx.w_id} and c_d_id = {ctx.d_id} and c_id = {c_id};"
    )
    backend.execute(
        f"insert into history values ({c_id}, {ctx.d_id}, {ctx.w_id}, {ctx.d_id}, {ctx.w_id}, '{now_text()}', "
        f"{amount}, 'payment');"
    )
    backend.commit()


def order_status(backend: Backend, ctx: TxnContext) -> None:
    c_id = random.randint(1, ctx.customers_per_district)
    backend.begin()
    backend.execute(
        f"select c_balance, c_first, c_last from customer where c_w_id = {ctx.w_id} and c_d_id = {ctx.d_id} "
        f"and c_id = {c_id};"
    )
    last_o_id = scalar_int(
        backend.execute(
            f"select max(o_id) from orders where o_w_id = {ctx.w_id} and o_d_id = {ctx.d_id} and o_c_id = {c_id};"
        )
    )
    if last_o_id > 0:
        backend.execute(
            f"select ol_i_id, ol_quantity, ol_amount from order_line where ol_w_id = {ctx.w_id} and "
            f"ol_d_id = {ctx.d_id} and ol_o_id = {last_o_id};"
        )
    backend.commit()


def delivery(backend: Backend, ctx: TxnContext) -> None:
    carrier_id = random.randint(1, 10)
    backend.begin()
    for d_id in range(1, ctx.districts_per_warehouse + 1):
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
        backend.execute(
            f"update orders set o_carrier_id = {carrier_id} where o_w_id = {ctx.w_id} and o_d_id = {d_id} and o_id = {o_id};"
        )
        amount = scalar_float(
            backend.execute(
                f"select sum(ol_amount) from order_line where ol_w_id = {ctx.w_id} and ol_d_id = {d_id} and ol_o_id = {o_id};"
            ),
            0.0,
        )
        customer_id = scalar_int(
            backend.execute(
                f"select o_c_id from orders where o_w_id = {ctx.w_id} and o_d_id = {d_id} and o_id = {o_id};"
            ),
            0,
        )
        backend.execute(
            f"update order_line set ol_delivery_d = '{now_text()}' where ol_w_id = {ctx.w_id} and ol_d_id = {d_id} "
            f"and ol_o_id = {o_id};"
        )
        if customer_id > 0:
            backend.execute(
                f"update customer set c_balance = c_balance + {amount}, c_delivery_cnt = c_delivery_cnt + 1 "
                f"where c_w_id = {ctx.w_id} and c_d_id = {d_id} and c_id = {customer_id};"
            )
    backend.commit()


def stock_level(backend: Backend, ctx: TxnContext) -> None:
    threshold = random.randint(10, 20)
    backend.begin()
    d_next = scalar_int(
        backend.execute(
            f"select d_next_o_id from district where d_w_id = {ctx.w_id} and d_id = {ctx.d_id};"
        ),
        0,
    )
    backend.execute(
        f"select count(*) from stock, order_line where s_w_id = {ctx.w_id} and s_i_id = ol_i_id and "
        f"ol_w_id = {ctx.w_id} and ol_d_id = {ctx.d_id} and ol_o_id >= {max(1, d_next - 20)} and "
        f"ol_o_id < {d_next} and s_quantity < {threshold};"
    )
    backend.commit()


def count_rows(backend: Backend, table: str) -> int:
    return scalar_int(backend.execute(f"select count(*) from {table};"), 0)


def fetch_text(backend: Backend, sql: str, default: str = "") -> str:
    return scalar_text(backend.execute(sql), default)
