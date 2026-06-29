from __future__ import annotations

import csv
import random
import string
from pathlib import Path
from typing import Iterable, Iterator

from benchmark.tpcc.core.constants import (
    CUSTOMERS_PER_DISTRICT,
    DISTRICTS_PER_WAREHOUSE,
    INITIAL_NEW_ORDERS_PER_DISTRICT,
    INITIAL_ORDERS_PER_DISTRICT,
    ITEM_COUNT,
    surname,
)


def rand_str(length: int) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(length))


def ts_text() -> str:
    return "2026-06-29 00:00:00"


def write_rows(path: Path, rows: Iterable[list[object]]) -> None:
    with path.open("w", newline="") as file:
        writer = csv.writer(file)
        for row in rows:
            writer.writerow(row)


def ensure_empty_or_allowed(data_dir: Path, overwrite: bool) -> None:
    existing_csvs = list(data_dir.glob("*.csv")) if data_dir.exists() else []
    if existing_csvs and not overwrite:
        raise FileExistsError(f"refusing to overwrite existing CSV files in {data_dir}")


def warehouse_header() -> list[str]:
    return ["w_id", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip", "w_tax", "w_ytd"]


def district_header() -> list[str]:
    return ["d_id", "d_w_id", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip", "d_tax", "d_ytd", "d_next_o_id"]


def customer_header() -> list[str]:
    return [
        "c_id",
        "c_d_id",
        "c_w_id",
        "c_first",
        "c_middle",
        "c_last",
        "c_street_1",
        "c_street_2",
        "c_city",
        "c_state",
        "c_zip",
        "c_phone",
        "c_since",
        "c_credit",
        "c_credit_lim",
        "c_discount",
        "c_balance",
        "c_ytd_payment",
        "c_payment_cnt",
        "c_delivery_cnt",
        "c_data",
    ]


def history_header() -> list[str]:
    return ["h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data"]


def new_orders_header() -> list[str]:
    return ["no_o_id", "no_d_id", "no_w_id"]


def orders_header() -> list[str]:
    return ["o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_carrier_id", "o_ol_cnt", "o_all_local"]


def order_line_header() -> list[str]:
    return ["ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount", "ol_dist_info"]


def item_header() -> list[str]:
    return ["i_id", "i_im_id", "i_name", "i_price", "i_data"]


def stock_header() -> list[str]:
    return [
        "s_i_id",
        "s_w_id",
        "s_quantity",
        "s_dist_01",
        "s_dist_02",
        "s_dist_03",
        "s_dist_04",
        "s_dist_05",
        "s_dist_06",
        "s_dist_07",
        "s_dist_08",
        "s_dist_09",
        "s_dist_10",
        "s_ytd",
        "s_order_cnt",
        "s_remote_cnt",
        "s_data",
    ]


def warehouse_rows(warehouses: int) -> list[list[object]]:
    rows: list[list[object]] = [warehouse_header()]
    for w_id in range(1, warehouses + 1):
        rows.append([w_id, rand_str(10), rand_str(20), rand_str(20), rand_str(20), rand_str(2), rand_str(9), round(random.uniform(0, 0.2), 4), 300000.0])
    return rows


def district_rows(warehouses: int) -> list[list[object]]:
    rows: list[list[object]] = [district_header()]
    for w_id in range(1, warehouses + 1):
        for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
            rows.append([d_id, w_id, rand_str(10), rand_str(20), rand_str(20), rand_str(20), rand_str(2), rand_str(9), round(random.uniform(0, 0.2), 4), 30000.0, INITIAL_ORDERS_PER_DISTRICT + 1])
    return rows


def iter_customer_rows(warehouses: int) -> Iterator[list[object]]:
    yield customer_header()
    for w_id in range(1, warehouses + 1):
        for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
            for c_id in range(1, CUSTOMERS_PER_DISTRICT + 1):
                last = surname(c_id - 1) if c_id <= 1000 else surname(random.randint(0, 999))
                yield [
                    c_id,
                    d_id,
                    w_id,
                    rand_str(16),
                    "OE",
                    last,
                    rand_str(20),
                    rand_str(20),
                    rand_str(20),
                    rand_str(2),
                    rand_str(9),
                    "".join(random.choice(string.digits) for _ in range(16)),
                    ts_text(),
                    "BC" if c_id % 10 == 0 else "GC",
                    50000.0,
                    round(random.uniform(0, 0.5), 4),
                    -10.0,
                    10.0,
                    1,
                    0,
                    rand_str(40),
                ]


def iter_history_rows(warehouses: int) -> Iterator[list[object]]:
    yield history_header()
    for w_id in range(1, warehouses + 1):
        for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
            for c_id in range(1, CUSTOMERS_PER_DISTRICT + 1):
                yield [c_id, d_id, w_id, d_id, w_id, ts_text(), 10.0, rand_str(24)]


def iter_item_rows() -> Iterator[list[object]]:
    yield item_header()
    for i_id in range(1, ITEM_COUNT + 1):
        data = rand_str(44)
        if i_id % 10 == 0:
            data = "ORIGINAL" + data[:36]
        yield [i_id, random.randint(1, 10000), rand_str(24), round(random.uniform(1, 100), 2), data]


def iter_stock_rows(warehouses: int) -> Iterator[list[object]]:
    yield stock_header()
    for w_id in range(1, warehouses + 1):
        for i_id in range(1, ITEM_COUNT + 1):
            data = rand_str(44)
            if i_id % 10 == 0:
                data = "ORIGINAL" + data[:36]
            yield [
                i_id,
                w_id,
                random.randint(10, 100),
                rand_str(24),
                rand_str(24),
                rand_str(24),
                rand_str(24),
                rand_str(24),
                rand_str(24),
                rand_str(24),
                rand_str(24),
                rand_str(24),
                rand_str(24),
                0,
                0,
                0,
                data,
            ]


def iter_orders_rows(warehouses: int) -> Iterator[list[object]]:
    yield orders_header()
    for w_id in range(1, warehouses + 1):
        for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
            customer_ids = list(range(1, CUSTOMERS_PER_DISTRICT + 1))
            random.shuffle(customer_ids)
            for o_id, c_id in enumerate(customer_ids, start=1):
                yield [o_id, d_id, w_id, c_id, ts_text(), random.randint(1, 10) if o_id <= 2100 else 0, random.randint(5, 15), 1]


def iter_new_orders_rows(warehouses: int) -> Iterator[list[object]]:
    yield new_orders_header()
    start = INITIAL_ORDERS_PER_DISTRICT - INITIAL_NEW_ORDERS_PER_DISTRICT + 1
    for w_id in range(1, warehouses + 1):
        for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
            for o_id in range(start, INITIAL_ORDERS_PER_DISTRICT + 1):
                yield [o_id, d_id, w_id]


def iter_order_line_rows(warehouses: int) -> Iterator[list[object]]:
    yield order_line_header()
    for w_id in range(1, warehouses + 1):
        for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
            for o_id in range(1, INITIAL_ORDERS_PER_DISTRICT + 1):
                ol_cnt = 5 + ((o_id + d_id + w_id) % 11)
                for number in range(1, ol_cnt + 1):
                    yield [
                        o_id,
                        d_id,
                        w_id,
                        number,
                        ((o_id * 17 + number) % ITEM_COUNT) + 1,
                        w_id,
                        "" if o_id > 2100 else ts_text(),
                        5,
                        round(random.uniform(0.01, 999.99), 2) if o_id > 2100 else 0.0,
                        rand_str(24),
                    ]


def generate_all(warehouses: int, data_dir: Path, seed: int = 1, overwrite: bool = False) -> None:
    random.seed(seed)
    ensure_empty_or_allowed(data_dir, overwrite=overwrite)
    data_dir.mkdir(parents=True, exist_ok=True)
    write_rows(data_dir / "warehouse.csv", warehouse_rows(warehouses))
    write_rows(data_dir / "district.csv", district_rows(warehouses))
    write_rows(data_dir / "customer.csv", iter_customer_rows(warehouses))
    write_rows(data_dir / "history.csv", iter_history_rows(warehouses))
    write_rows(data_dir / "item.csv", iter_item_rows())
    write_rows(data_dir / "stock.csv", iter_stock_rows(warehouses))
    write_rows(data_dir / "orders.csv", iter_orders_rows(warehouses))
    write_rows(data_dir / "new_orders.csv", iter_new_orders_rows(warehouses))
    write_rows(data_dir / "order_line.csv", iter_order_line_rows(warehouses))
