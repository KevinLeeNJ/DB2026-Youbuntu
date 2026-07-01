import unittest
from unittest import mock

from benchmark.tpcc.core.backend import BackendError
from benchmark.tpcc.core.transactions import InvalidItemRollback, TxnContext, new_order

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


class TransactionsTest(unittest.TestCase):
    def test_new_order_rolls_back_when_rmdb_empty_item_result_has_table_header(
        self,
    ) -> None:
        backend = FakeBackend(
            {
                "select d_next_o_id from district where d_w_id = 1 and d_id = 1;": "3001",
                "select i_price from item where i_id = 2;": EMPTY_PRICE_RESULT,
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
                "select d_next_o_id from district where d_w_id = 1 and d_id = 1;": (
                    "+------------------+\n"
                    "|      d_next_o_id |\n"
                    "+------------------+\n"
                    "+------------------+\n"
                    "Total record(s): 0"
                )
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


if __name__ == "__main__":
    unittest.main()
