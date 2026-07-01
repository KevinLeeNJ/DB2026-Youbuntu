import time
import unittest
from unittest import mock

from benchmark.tpcc.core.backend import BackendError
from benchmark.tpcc.core.result import RoundResult
from benchmark.tpcc.core.workload import DatasetProfile, worker_loop


class ReconnectBackend:
    def __init__(self, idx: int):
        self.idx = idx
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def rollback(self) -> None:
        if self.idx == 0:
            raise BackendError("connection is broken")


class WorkloadTest(unittest.TestCase):
    def test_worker_reconnects_after_backend_error(self) -> None:
        backends: list[ReconnectBackend] = []
        calls: list[int] = []

        def backend_factory() -> ReconnectBackend:
            backend = ReconnectBackend(len(backends))
            backends.append(backend)
            return backend

        def txn_func(backend: ReconnectBackend, _ctx) -> None:
            calls.append(backend.idx)
            if backend.idx == 0:
                raise BackendError("response timed out")

        profile = DatasetProfile(
            warehouses=1,
            districts_per_warehouse=1,
            customers_per_district=1,
            item_count=1,
        )
        now = time.monotonic()
        with mock.patch(
            "benchmark.tpcc.core.workload.choose_txn", return_value="new_order"
        ), mock.patch.dict(
            "benchmark.tpcc.core.workload.TXN_FUNCS",
            {"new_order": txn_func},
            clear=True,
        ):
            worker_loop(
                backend_factory,
                RoundResult(measure_seconds=1),
                worker_id=0,
                profile=profile,
                policy="terminal-home",
                warmup_end=now,
                measure_end=now + 0.05,
            )

        self.assertGreaterEqual(len(backends), 2)
        self.assertTrue(backends[0].closed)
        self.assertIn(1, calls)


if __name__ == "__main__":
    unittest.main()
