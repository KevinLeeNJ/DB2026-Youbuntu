import unittest

from benchmark.tpcc.phases.consistency import run_consistency, run_district_diagnostics


class FakeBackend:
    def __init__(self, values):
        self.values = values

    def execute(self, sql: str) -> str:
        return self.values[sql]


class ConsistencyTest(unittest.TestCase):
    def test_consistency_uses_standard_committed_state_rules(self) -> None:
        backend = FakeBackend(
            {
                "select count(*) from warehouse;": "1",
                "select count(*) from district;": "3",
                "select w_ytd from warehouse where w_id = 1;": "300000.00",
                "select sum(d_ytd) from district where d_w_id = 1;": "300000.00",
            }
        )
        failures = run_consistency(
            backend,
            baseline_warehouse_total=1,
            baseline_district_total=3,
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

    def test_consistency_reports_per_warehouse_ytd_failure(self) -> None:
        backend = FakeBackend(
            {
                "select count(*) from warehouse;": "1",
                "select count(*) from district;": "10",
                "select w_ytd from warehouse where w_id = 1;": "300010",
                "select sum(d_ytd) from district where d_w_id = 1;": "300000",
            }
        )

        failures = run_consistency(backend, 1, 10)

        self.assertEqual(
            failures,
            [
                "warehouse/district YTD mismatch w=1: warehouse=300010.00, districts=300000.00",
            ],
        )


if __name__ == "__main__":
    unittest.main()
