from __future__ import annotations


class BackendError(RuntimeError):
    pass


class BackendAbort(BackendError):
    pass


class Backend:
    def begin(self) -> None:
        self.execute("begin;")

    def execute(self, sql: str) -> str:
        raise NotImplementedError

    def commit(self) -> None:
        self.execute("commit;")

    def rollback(self) -> None:
        self.execute("rollback;")

    def close(self) -> None:
        raise NotImplementedError
