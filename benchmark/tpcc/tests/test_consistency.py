import unittest

from benchmark.tpcc.phases.consistency import run_consistency, run_district_diagnostics


class FakeBackend:
    def __init__(self, values):
        self.values = values

    def execute(self, sql: str) -> str:
        return self.values[sql]


class ConsistencyTest(unittest.TestCase):
    def test_consistency_uses_baseline_order_count(self) -> None:
        backend = FakeBackend(
            {
                "select count(*) from warehouse;": "1",
                "select count(*) from district;": "3",
                "select count(*) from orders;": "32",
                "select sum(w_ytd) from warehouse;": "300000.00",
                "select sum(d_ytd) from district;": "300000.00",
                "select sum(h_amount) from history;": "300000.00",
                "select sum(c_ytd_payment) from customer;": "300000.00",
                "select sum(c_payment_cnt) from customer;": "30000",
                "select count(*) from history;": "30000",
                "select sum(s_ytd) from stock;": "12",
                "select sum(ol_quantity) from order_line where ol_o_id > 3000;": "12",
                "select sum(s_order_cnt) from stock;": "2",
                "select count(*) from order_line where ol_o_id > 3000;": "2",
                "select sum(s_remote_cnt) from stock;": "1",
                "select count(*) from order_line where ol_o_id > 3000 and ol_supply_w_id != ol_w_id;": "1",
            }
        )
        failures = run_consistency(
            backend,
            baseline_warehouse_total=1,
            baseline_district_total=3,
            baseline_orders_total=30,
            total_committed_new_order=2,
        )
        self.assertEqual(failures, [])

    def test_district_diagnostics_accepts_full_consistency_rules(self) -> None:
        backend = FakeBackend(
            {
                "select d_next_o_id from district where d_w_id = 1 and d_id = 1;": "3003",
                "select max(o_id) from orders where o_w_id = 1 and o_d_id = 1;": "3002",
                "select max(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1;": "3002",
                "select count(o_id) from orders where o_w_id = 1 and o_d_id = 1;": "3002",
                "select min(o_id) from orders where o_w_id = 1 and o_d_id = 1;": "1",
                "select count(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1;": "3",
                "select min(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1;": "3000",
                "select count(o_id) from orders where o_w_id = 1 and o_d_id = 1 and o_carrier_id = 0;": "3",
                "select sum(o_ol_cnt) from orders where o_w_id = 1 and o_d_id = 1;": "18",
                "select count(ol_o_id) from order_line where ol_w_id = 1 and ol_d_id = 1;": "18",
            }
        )

        self.assertEqual(
            run_district_diagnostics(backend, warehouses=1, districts_per_warehouse=1),
            [],
        )

    def test_district_diagnostics_reports_all_rule_violations(self) -> None:
        backend = FakeBackend(
            {
                "select d_next_o_id from district where d_w_id = 1 and d_id = 1;": "3003",
                "select max(o_id) from orders where o_w_id = 1 and o_d_id = 1;": "3002",
                "select max(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1;": "3001",
                "select count(o_id) from orders where o_w_id = 1 and o_d_id = 1;": "3002",
                "select min(o_id) from orders where o_w_id = 1 and o_d_id = 1;": "1",
                "select count(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1;": "2",
                "select min(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1;": "2999",
                "select count(o_id) from orders where o_w_id = 1 and o_d_id = 1 and o_carrier_id = 0;": "3",
                "select sum(o_ol_cnt) from orders where o_w_id = 1 and o_d_id = 1;": "18",
                "select count(ol_o_id) from order_line where ol_w_id = 1 and ol_d_id = 1;": "17",
            }
        )

        self.assertEqual(
            run_district_diagnostics(backend, warehouses=1, districts_per_warehouse=1),
            [
                "new_orders gap w=1 d=1: count=2, min=2999, max=3001",
                "new_orders tail mismatch w=1 d=1: max_new_order=3001, max_order=3002",
                "pending order mismatch w=1 d=1: carrier_zero=3, new_orders=2",
                "order_line count mismatch w=1 d=1: sum_o_ol_cnt=18, count_ol_o_id=17",
            ],
        )

    def test_consistency_reports_transaction_accounting_failures(self) -> None:
        backend = FakeBackend(
            {
                "select count(*) from warehouse;": "1",
                "select count(*) from district;": "10",
                "select count(*) from orders;": "30001",
                "select sum(w_ytd) from warehouse;": "300010",
                "select sum(d_ytd) from district;": "300010",
                "select sum(h_amount) from history;": "300010",
                "select sum(c_ytd_payment) from customer;": "300000",
                "select sum(c_payment_cnt) from customer;": "30000",
                "select count(*) from history;": "30001",
                "select sum(s_ytd) from stock;": "0",
                "select sum(ol_quantity) from order_line where ol_o_id > 3000;": "5",
                "select sum(s_order_cnt) from stock;": "0",
                "select count(*) from order_line where ol_o_id > 3000;": "1",
                "select sum(s_remote_cnt) from stock;": "0",
                "select count(*) from order_line where ol_o_id > 3000 and ol_supply_w_id != ol_w_id;": "0",
            }
        )

        failures = run_consistency(backend, 1, 10, 30000, 1)

        self.assertEqual(
            failures,
            [
                "customer/history payment amount mismatch: left=300000.00, right=300010.00",
                "customer/history payment count mismatch: left=30000, right=30001",
                "stock YTD/new order-line quantity mismatch: left=0.00, right=5.00",
                "stock/new order-line count mismatch: left=0, right=1",
            ],
        )


if __name__ == "__main__":
    unittest.main()
