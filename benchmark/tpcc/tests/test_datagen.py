from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from benchmark.tpcc.phases.datagen import (
    complete_csv_set,
    customer_header,
    district_rows,
    ensure_empty_or_allowed,
    warehouse_header,
    warehouse_rows,
)


class DataGenTest(unittest.TestCase):
    def test_warehouse_rows_include_header_and_one_row(self) -> None:
        rows = warehouse_rows(1)
        self.assertEqual(rows[0], warehouse_header())
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1][0], 1)

    def test_district_rows_include_header_and_ten_rows_for_one_warehouse(self) -> None:
        rows = district_rows(1)
        self.assertEqual(rows[0][0], "d_id")
        self.assertEqual(len(rows), 11)

    def test_customer_header_contains_expected_columns(self) -> None:
        header = customer_header()
        self.assertEqual(header[:4], ["c_id", "c_d_id", "c_w_id", "c_first"])
        self.assertEqual(header[-1], "c_data")

    def test_existing_csv_requires_explicit_overwrite(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "warehouse.csv").write_text("x\n")
            with self.assertRaises(FileExistsError):
                ensure_empty_or_allowed(path, overwrite=False)
            ensure_empty_or_allowed(path, overwrite=True)

    def test_complete_csv_set_requires_every_table_file(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp)
            for table in ["warehouse", "district"]:
                (path / f"{table}.csv").write_text("header\n")
            self.assertTrue(complete_csv_set(path, ["warehouse", "district"]))
            self.assertFalse(complete_csv_set(path, ["warehouse", "district", "customer"]))


if __name__ == "__main__":
    unittest.main()
