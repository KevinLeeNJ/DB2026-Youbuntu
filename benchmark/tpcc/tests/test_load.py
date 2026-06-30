import unittest
from pathlib import Path

from benchmark.tpcc.phases.load import rmdb_load_path


class LoadTest(unittest.TestCase):
    def test_rmdb_load_path_adds_dot_slash_for_relative_path(self) -> None:
        self.assertEqual(rmdb_load_path(Path("src/test/file.csv")), "./src/test/file.csv")

    def test_rmdb_load_path_preserves_explicit_relative_prefix(self) -> None:
        self.assertEqual(rmdb_load_path(Path("./src/test/file.csv")), "./src/test/file.csv")

    def test_rmdb_load_path_can_be_made_relative_to_db_dir(self) -> None:
        self.assertEqual(
            rmdb_load_path(Path("src/test/file.csv"), relative_to=Path("tpcc_smoke_db")),
            "../src/test/file.csv",
        )


if __name__ == "__main__":
    unittest.main()
