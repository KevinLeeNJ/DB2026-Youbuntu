from pathlib import Path
import unittest
from unittest import mock

from benchmark.tpcc.core.backend import BackendError
from benchmark.tpcc.core.sqlite_backend import SqliteBackend
from benchmark.tpcc.core.transactions import (
    InvalidItemRollback,
    TxnContext,
    delivery,
    new_order,
    order_status,
    payment,
    stock_level,
)
from benchmark.tpcc.phases.load import execute_sql_file

EMPTY_PRICE_RESULT = (
    "+------------------+\n"
    "|          i_price |\n"
    "+------------------+\n"
    "+------------------+\n"
    "Total record(s): 0"
)


class FakeBackend:
    def __init__(self, responses: dict[str, str]):
        self.responses = responses
        self.statements: list[str] = []
        self.committed = False
        self.rolled_back = False

    def begin(self) -> None:
        self.statements.append("begin")

    def execute(self, sql: str) -> str:
        self.statements.append(sql)
        return self.responses.get(sql, "10")

    def commit(self) -> None:
        self.committed = True
        self.statements.append("commit")

    def rollback(self) -> None:
        self.rolled_back = True
        self.statements.append("rollback")


def deterministic_randint(low: int, high: int) -> int:
    if (low, high) == (5, 15):
        return 5
    if (low, high) == (1, 100):
        return 1
    return low


def deterministic_valid_randint(low: int, high: int) -> int:
    if (low, high) == (5, 15):
        return 5
    if (low, high) == (1, 100):
        return 2
    return low


class TransactionsTest(unittest.TestCase):
    def test_sqlite_payment_and_new_order_maintain_accounting_fields(self) -> None:
        backend = SqliteBackend(":memory:")
        schema = Path(__file__).parents[1] / "schema" / "sqlite_schema.sql"
        execute_sql_file(backend, schema)
        backend.execute(
            "insert into warehouse values (1, 'w', 's1', 's2', 'city', 'ST', 'zip', 0.1, 300000);"
        )
        backend.execute(
            "insert into district values (1, 1, 'd', 's1', 's2', 'city', 'ST', 'zip', 0.1, 30000, 3001);"
        )
        backend.execute(
            "insert into customer values (1, 1, 1, 'first', 'OE', 'last', 's1', 's2', 'city', 'ST', "
            "'zip', 'phone', '2026-01-01 00:00:00', 'GC', 50000, 0.1, -10, 10, 1, 0, 'data');"
        )
        backend.execute(
            "insert into history values (1, 1, 1, 1, 1, '2026-01-01 00:00:00', 10, 'initial');"
        )
        backend.execute("insert into item values (1, 1, 'item', 2.5, 'data');")
        backend.execute(
            "insert into stock values (1, 1, 50, '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', "
            "0, 0, 0, 'data');"
        )
        ctx = TxnContext(
            w_id=1, d_id=1, warehouses=1, item_count=1, customers_per_district=1
        )

        with mock.patch(
            "benchmark.tpcc.core.transactions.random.randint", return_value=1
        ):
            with mock.patch(
                "benchmark.tpcc.core.transactions.random.uniform", return_value=10.0
            ):
                payment(backend, ctx)
        with mock.patch(
            "benchmark.tpcc.core.transactions.random.randint",
            side_effect=deterministic_valid_randint,
        ):
            new_order(backend, ctx)

        self.assertEqual(
            backend.execute(
                "select c_balance, c_ytd_payment, c_payment_cnt from customer;"
            ),
            "-20.0|20.0|2",
        )
        self.assertEqual(
            backend.execute(
                "select s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock;"
            ),
            "45|5.0|5|0",
        )
        self.assertEqual(backend.execute("select d_next_o_id from district;"), "3002")
        self.assertEqual(backend.execute("select count(*) from orders;"), "1")
        self.assertEqual(backend.execute("select count(*) from order_line;"), "5")
        backend.close()

    def test_new_order_marks_remote_order_not_all_local(self) -> None:
        backend = FakeBackend(
            {
                "select d_next_o_id, d_tax from district where d_id = 1 and d_w_id = 1;": "3002",
                "select i_price, i_name, i_data from item where i_id = 1;": "2.50",
            }
        )
        ctx = TxnContext(
            w_id=1, d_id=1, warehouses=2, item_count=1, customers_per_district=1
        )
        percent_calls = 0

        def remote_once_randint(low: int, high: int) -> int:
            nonlocal percent_calls
            if (low, high) == (5, 15):
                return 5
            if (low, high) == (1, 100):
                percent_calls += 1
                return 1 if percent_calls == 2 else 2
            if (low, high) == (1, 2):
                return 2
            return low

        with mock.patch(
            "benchmark.tpcc.core.transactions.random.randint",
            side_effect=remote_once_randint,
        ):
            new_order(backend, ctx)

        self.assertIn(
            "update stock set s_ytd = s_ytd + 1, s_order_cnt = s_order_cnt + 1, "
            "s_remote_cnt = s_remote_cnt + 1 where s_i_id = 1 and s_w_id = 2;",
            backend.statements,
        )
        self.assertIn(
            "update orders set o_all_local = 0 where o_id = 3001 and o_d_id = 1 and o_w_id = 1;",
            backend.statements,
        )

    def test_new_order_uses_final_sql_shape_for_order_id_and_stock(self) -> None:
        backend = FakeBackend(
            {
                "select d_next_o_id, d_tax from district where d_id = 1 and d_w_id = 1;": "3002",
                "select i_price, i_name, i_data from item where i_id = 1;": "2.50",
                "select s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, "
                "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 from stock where s_i_id = 1 "
                "and s_w_id = 1;": "50",
            }
        )
        ctx = TxnContext(
            w_id=1, d_id=1, warehouses=1, item_count=1, customers_per_district=1
        )

        with mock.patch(
            "benchmark.tpcc.core.transactions.random.randint",
            side_effect=deterministic_valid_randint,
        ):
            new_order(backend, ctx)

        district_update = (
            "update district set d_next_o_id = d_next_o_id + 1 where d_id = 1 and d_w_id = 1;"
        )
        district_select = (
            "select d_next_o_id, d_tax from district where d_id = 1 and d_w_id = 1;"
        )
        self.assertIn(district_update, backend.statements)
        self.assertIn(district_select, backend.statements)
        self.assertLess(
            backend.statements.index(district_update),
            backend.statements.index(district_select),
        )
        self.assertTrue(
            any(
                statement.startswith("insert into orders values (3001, 1, 1")
                for statement in backend.statements
            )
        )
        self.assertIn("insert into new_orders values (3001, 1, 1);", backend.statements)
        self.assertIn(
            "select i_price, i_name, i_data from item where i_id = 1;",
            backend.statements,
        )
        self.assertIn(
            "select s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, "
            "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 from stock where s_i_id = 1 "
            "and s_w_id = 1;",
            backend.statements,
        )
        self.assertIn(
            "update stock set s_ytd = s_ytd + 1, s_order_cnt = s_order_cnt + 1, "
            "s_remote_cnt = s_remote_cnt + 0 where s_i_id = 1 and s_w_id = 1;",
            backend.statements,
        )
        self.assertIn(
            "update stock set s_quantity = s_quantity - 1 where s_i_id = 1 and s_w_id = 1;",
            backend.statements,
        )
        stock_lock = next(
            statement
            for statement in backend.statements
            if statement.startswith("update stock set s_ytd")
        )
        stock_select = next(
            statement
            for statement in backend.statements
            if statement.startswith("select s_quantity")
        )
        self.assertLess(
            backend.statements.index(stock_lock), backend.statements.index(stock_select)
        )
        self.assertTrue(backend.committed)

    def test_new_order_rolls_back_when_rmdb_empty_item_result_has_table_header(
        self,
    ) -> None:
        backend = FakeBackend(
            {
                "select d_next_o_id, d_tax from district where d_id = 1 and d_w_id = 1;": "3002",
                "select i_price, i_name, i_data from item where i_id = 2;": EMPTY_PRICE_RESULT,
            }
        )
        ctx = TxnContext(
            w_id=1, d_id=1, warehouses=1, item_count=1, customers_per_district=1
        )

        with mock.patch(
            "benchmark.tpcc.core.transactions.random.randint",
            side_effect=deterministic_randint,
        ):
            with self.assertRaises(InvalidItemRollback):
                new_order(backend, ctx)

        self.assertTrue(backend.rolled_back)
        self.assertFalse(backend.committed)

    def test_new_order_rejects_missing_district_next_id_before_insert(self) -> None:
        backend = FakeBackend(
            {
                "select d_next_o_id, d_tax from district where d_id = 1 and d_w_id = 1;": (
                    "+------------------+--------+\n"
                    "|      d_next_o_id |  d_tax |\n"
                    "+------------------+--------+\n"
                    "+------------------+--------+\n"
                    "Total record(s): 0"
                ),
            }
        )
        ctx = TxnContext(
            w_id=1, d_id=1, warehouses=1, item_count=1, customers_per_district=1
        )

        with mock.patch(
            "benchmark.tpcc.core.transactions.random.randint",
            side_effect=deterministic_randint,
        ):
            with self.assertRaises(BackendError):
                new_order(backend, ctx)

        self.assertFalse(
            any(
                statement.startswith("insert into orders")
                for statement in backend.statements
            )
        )

    def test_payment_uses_atomic_customer_accounting_update(self) -> None:
        customer_select = (
            "select c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, "
            "c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since from customer where "
            "c_w_id = 1 and c_d_id = 1 and c_id = 1;"
        )
        backend = FakeBackend(
            {
                customer_select: "first,OE,last,s1,s2,city,ST,zip,phone,GC,50000,0.1,100.0,since"
            }
        )
        ctx = TxnContext(
            w_id=1, d_id=1, warehouses=1, item_count=1, customers_per_district=1
        )

        with mock.patch(
            "benchmark.tpcc.core.transactions.random.randint", return_value=1
        ):
            with mock.patch(
                "benchmark.tpcc.core.transactions.random.uniform", return_value=10.0
            ):
                payment(backend, ctx)

        self.assertIn(
            "select w_street_1, w_street_2, w_city, w_state, w_zip, w_name from warehouse where w_id = 1;",
            backend.statements,
        )
        self.assertIn(
            "select d_street_1, d_street_2, d_city, d_state, d_zip, d_name from district where "
            "d_w_id = 1 and d_id = 1;",
            backend.statements,
        )
        self.assertIn(customer_select, backend.statements)
        customer_update = (
            "update customer set c_balance = c_balance - 10.0, "
            "c_ytd_payment = c_ytd_payment + 10.0, c_payment_cnt = c_payment_cnt + 1 "
            "where c_w_id = 1 and c_d_id = 1 and c_id = 1;"
        )
        self.assertIn(customer_update, backend.statements)
        self.assertLess(
            backend.statements.index(customer_update),
            backend.statements.index(customer_select),
        )

    def test_order_status_uses_final_last_name_sql_shape(self) -> None:
        backend = FakeBackend({})
        ctx = TxnContext(
            w_id=1, d_id=1, warehouses=1, item_count=1, customers_per_district=1
        )

        with mock.patch(
            "benchmark.tpcc.core.transactions.random.randint",
            side_effect=[50, 0, 1, 3001],
        ):
            order_status(backend, ctx)

        self.assertIn(
            "select count(c_id) as count_c_id from customer where c_w_id = 1 and c_d_id = 1 "
            "and c_last = 'BARBARBAR';",
            backend.statements,
        )
        self.assertIn(
            "select c_balance, c_first, c_middle, c_last from customer where c_w_id = 1 and "
            "c_d_id = 1 and c_last = 'BARBARBAR' order by c_first;",
            backend.statements,
        )
        self.assertIn(
            "select o_id, o_entry_d, o_carrier_id from orders where o_w_id = 1 and o_d_id = 1 "
            "and o_c_id = 1 and o_id = 3001;",
            backend.statements,
        )
        self.assertIn(
            "select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from order_line "
            "where ol_w_id = 1 and ol_d_id = 1 and ol_o_id = 3001;",
            backend.statements,
        )

    def test_order_status_id_path_reuses_selected_customer_id(self) -> None:
        backend = FakeBackend({})
        ctx = TxnContext(
            w_id=1, d_id=1, warehouses=1, item_count=1, customers_per_district=10
        )

        with mock.patch(
            "benchmark.tpcc.core.transactions.random.randint",
            side_effect=[80, 7, 99, 3001],
        ):
            order_status(backend, ctx)

        self.assertIn(
            "select c_balance, c_first, c_middle, c_last from customer where c_w_id = 1 and "
            "c_d_id = 1 and c_id = 7;",
            backend.statements,
        )
        self.assertIn(
            "select o_id, o_entry_d, o_carrier_id from orders where o_w_id = 1 and o_d_id = 1 "
            "and o_c_id = 7 and o_id = 99;",
            backend.statements,
        )

    def test_delivery_uses_final_sql_shape_for_sum_and_customer_update(self) -> None:
        backend = FakeBackend(
            {
                "select min(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1;": "3001",
                "select o_c_id from orders where o_id = 3001 and o_d_id = 1 and o_w_id = 1;": "7",
                "select sum(ol_amount) from order_line where ol_o_id = 3001 and ol_d_id = 1 and ol_w_id = 1;": "42.5",
            }
        )
        ctx = TxnContext(
            w_id=1,
            d_id=1,
            warehouses=1,
            item_count=1,
            customers_per_district=1,
            districts_per_warehouse=1,
        )

        with mock.patch(
            "benchmark.tpcc.core.transactions.random.randint", return_value=3
        ):
            delivery(backend, ctx)

        self.assertIn(
            "select o_c_id from orders where o_id = 3001 and o_d_id = 1 and o_w_id = 1;",
            backend.statements,
        )
        self.assertIn(
            "select sum(ol_amount) from order_line where ol_o_id = 3001 and ol_d_id = 1 and ol_w_id = 1;",
            backend.statements,
        )
        self.assertIn(
            "update district set d_next_o_id = d_next_o_id + 0 where d_w_id = 1 and d_id = 1;",
            backend.statements,
        )
        district_lock = (
            "update district set d_next_o_id = d_next_o_id + 0 where d_w_id = 1 and d_id = 1;"
        )
        pending_select = (
            "select min(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1;"
        )
        self.assertLess(
            backend.statements.index(district_lock),
            backend.statements.index(pending_select),
        )
        self.assertIn(
            "update customer set c_balance = c_balance + 42.5, c_delivery_cnt = c_delivery_cnt + 1 "
            "where c_id = 7 and c_d_id = 1 and c_w_id = 1;",
            backend.statements,
        )

    def test_stock_level_uses_split_order_line_and_stock_count_queries(self) -> None:
        order_line_select = (
            "select ol_i_id from order_line where ol_w_id = 1 and ol_d_id = 1 and "
            "ol_o_id < 3001 and ol_o_id >= 2981;"
        )
        backend = FakeBackend(
            {
                "select d_next_o_id from district where d_id = 1 and d_w_id = 1;": "3001",
                order_line_select: "42",
            }
        )
        ctx = TxnContext(
            w_id=1, d_id=1, warehouses=1, item_count=1, customers_per_district=1
        )

        with mock.patch(
            "benchmark.tpcc.core.transactions.random.randint", return_value=15
        ):
            stock_level(backend, ctx)

        self.assertIn(order_line_select, backend.statements)
        self.assertIn(
            "select count(*) as count_stock from stock where s_w_id = 1 and s_i_id = 42 and s_quantity < 15;",
            backend.statements,
        )


if __name__ == "__main__":
    unittest.main()
