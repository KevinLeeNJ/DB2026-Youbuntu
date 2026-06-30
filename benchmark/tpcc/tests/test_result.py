import unittest

from benchmark.tpcc.core.result import RoundResult, format_progress_line, percentile


class ResultTest(unittest.TestCase):
    def test_percentile_uses_nearest_rank(self) -> None:
        self.assertEqual(percentile([10, 20, 30, 40], 50), 20)
        self.assertEqual(percentile([10, 20, 30, 40], 95), 40)

    def test_tpmc_uses_measured_new_order_only(self) -> None:
        result = RoundResult(measure_seconds=360)
        result.record("warmup", "new_order", "commit", 1.0)
        result.record("measure", "new_order", "commit", 1.0)
        result.record("measure", "new_order", "commit", 1.0)
        result.record("drain", "new_order", "commit", 1.0)
        self.assertEqual(result.tpmc(), 2.0 / 6.0)
        self.assertEqual(result.total_committed_new_order(), 4)

    def test_format_progress_line_shows_live_counts(self) -> None:
        result = RoundResult(measure_seconds=360)
        result.record("measure", "new_order", "commit", 10.0)
        result.record("measure", "new_order", "server-abort", 10.0)
        result.record("measure", "payment", "commit", 10.0)

        line = format_progress_line(
            result,
            round_no=1,
            total_rounds=3,
            phase="measure",
            elapsed_seconds=12,
            total_seconds=360,
        )

        self.assertIn("[round 1/3 measure 12/360s]", line)
        self.assertIn("commits=2", line)
        self.assertIn("aborts=1", line)
        self.assertIn("new_order_commit=1", line)
        self.assertIn("new_order_abort=1", line)
        self.assertIn("abort_rate=33.33%", line)

    def test_error_details_are_counted_in_json_result(self) -> None:
        result = RoundResult(measure_seconds=60)
        result.record("measure", "payment", "backend-error", 1.0, "Error: Index entry already exists")
        result.record("measure", "payment", "backend-error", 1.0, "Error: Index entry already exists")

        self.assertEqual(
            result.to_dict()["errors"]["measure"]["payment"]["Error: Index entry already exists"],
            2,
        )


if __name__ == "__main__":
    unittest.main()
