from pathlib import Path
import unittest


class SchemaTest(unittest.TestCase):
    def test_history_table_has_no_benchmark_index(self) -> None:
        schema_dir = Path(__file__).parents[1] / "schema"
        for filename in ("rmdb_indexes.sql", "sqlite_indexes.sql"):
            with self.subTest(filename=filename):
                indexes = (schema_dir / filename).read_text().lower()
                self.assertNotIn("index history", indexes)
                self.assertNotIn("index idx_history", indexes)


if __name__ == "__main__":
    unittest.main()
