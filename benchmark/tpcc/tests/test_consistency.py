import unittest

from benchmark.tpcc.phases.consistency import run_consistency


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


if __name__ == "__main__":
    unittest.main()
