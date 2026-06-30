import unittest

from benchmark.tpcc.core.sqlite_backend import SqliteBackend


class SqliteBackendTest(unittest.TestCase):
    def test_execute_commit_and_fetch(self) -> None:
        backend = SqliteBackend(":memory:")
        backend.execute("create table t (id integer primary key, v text);")
        backend.begin()
        backend.execute("insert into t values (1, 'x');")
        backend.commit()
        backend.begin()
        backend.execute("insert into t values (2, 'y');")
        backend.commit()
        self.assertIn("x", backend.execute("select v from t where id = 1;"))
        self.assertIn("y", backend.execute("select v from t where id = 2;"))
        backend.close()


if __name__ == "__main__":
    unittest.main()
