import unittest

from benchmark.tpcc.core.parsing import (
    parse_table_rows,
    scalar_float,
    scalar_int,
    scalar_text,
)


class ParsingTest(unittest.TestCase):
    def test_parse_table_rows_skips_rmdb_header_and_total(self) -> None:
        text = (
            "+------------------+\n"
            "|      d_next_o_id |\n"
            "+------------------+\n"
            "|             3072 |\n"
            "+------------------+\n"
            "Total record(s): 1"
        )
        self.assertEqual(parse_table_rows(text), [["3072"]])
        self.assertEqual(scalar_int(text, -1), 3072)

    def test_scalar_helpers_ignore_empty_rmdb_result_count(self) -> None:
        text = (
            "+------------------+\n"
            "|          i_price |\n"
            "+------------------+\n"
            "+------------------+\n"
            "Total record(s): 0"
        )
        self.assertEqual(parse_table_rows(text), [])
        self.assertEqual(scalar_text(text, "missing"), "missing")
        self.assertEqual(scalar_int(text, -1), -1)
        self.assertEqual(scalar_float(text, -1.0), -1.0)

    def test_scalar_helpers_still_accept_plain_backend_output(self) -> None:
        self.assertEqual(parse_table_rows("42"), [["42"]])
        self.assertEqual(scalar_int("42", -1), 42)
        self.assertEqual(scalar_float("3.5", -1.0), 3.5)


if __name__ == "__main__":
    unittest.main()
