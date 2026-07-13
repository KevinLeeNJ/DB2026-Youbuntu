import tempfile
import unittest
from pathlib import Path

from benchmark.tpcc.phases.load import TABLES, rmdb_load_path
from benchmark.tpcc.tpcc_run import import_csv_to_sqlite


class LoadTest(unittest.TestCase):
    def test_sqlite_import_batches_rows_before_building_indexes(self) -> None:
        source_dir = Path(__file__).parents[3] / "benchmark" / "tpcc" / "data"
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir) / "data"
            data_dir.mkdir()
            for table in TABLES:
                source_rows = source_dir.joinpath(f"{table}.csv").read_text().splitlines()
                data_dir.joinpath(f"{table}.csv").write_text(
                    "\n".join(source_rows[:2]) + "\n"
                )
            sqlite_path = Path(temp_dir) / "tpcc.sqlite"
            import_csv_to_sqlite(sqlite_path, data_dir)

            import sqlite3

            connection = sqlite3.connect(sqlite_path)
            try:
                self.assertEqual(
                    connection.execute("select count(*) from warehouse").fetchone()[0],
                    1,
                )
                indexes = {
                    row[1]
                    for row in connection.execute(
                        "select type, name from sqlite_master where type = 'index'"
                    )
                }
                self.assertIn("idx_stock_pk", indexes)
                self.assertIn("idx_order_line_pk", indexes)
            finally:
                connection.close()

    def test_rmdb_load_path_uses_absolute_path_without_db_dir_context(self) -> None:
        self.assertEqual(
            rmdb_load_path(Path("src/test/file.csv")),
            Path("src/test/file.csv").resolve().as_posix(),
        )

    def test_rmdb_load_path_resolves_explicit_relative_prefix_without_db_dir_context(
        self,
    ) -> None:
        self.assertEqual(
            rmdb_load_path(Path("./src/test/file.csv")),
            Path("./src/test/file.csv").resolve().as_posix(),
        )

    def test_rmdb_load_path_can_be_made_relative_to_db_dir(self) -> None:
        self.assertEqual(
            rmdb_load_path(
                Path("src/test/file.csv"), relative_to=Path("tpcc_smoke_db")
            ),
            "../src/test/file.csv",
        )


if __name__ == "__main__":
    unittest.main()
