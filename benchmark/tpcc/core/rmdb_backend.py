from __future__ import annotations

import socket

from benchmark.tpcc.core.backend import Backend, BackendAbort, BackendError


class RmdbBackend(Backend):
    def __init__(
        self,
        host: str,
        port: int,
        timeout: float = 30.0,
        isolation: str = "read-committed",
    ):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        if isolation == "snapshot-isolation":
            self.execute("set transaction isolation level snapshot isolation;")
        elif isolation != "read-committed":
            self.close()
            raise ValueError(f"unsupported rmdb isolation level: {isolation}")

    def execute(self, sql: str) -> str:
        if self.sock is None:
            raise BackendError("rmdb connection is closed")
        payload = sql.encode("utf-8") + b"\0"
        try:
            self.sock.sendall(payload)
            chunks: list[bytes] = []
            while True:
                chunk = self.sock.recv(65536)
                if not chunk:
                    self.close()
                    raise BackendError("connection closed by rmdb server")
                chunks.append(chunk)
                if b"\0" in chunk:
                    break
        except socket.timeout as exc:
            self.close()
            raise BackendError("rmdb response timed out") from exc
        except OSError as exc:
            self.close()
            raise BackendError(f"rmdb connection error: {exc}") from exc
        raw = b"".join(chunks).split(b"\0", 1)[0].decode("utf-8", errors="replace")
        text = raw.rstrip("\n")
        if text == "abort":
            raise BackendAbort("rmdb transaction aborted")
        if text.startswith("Error:") or text.startswith("Parser Error"):
            raise BackendError(text)
        return text

    def close(self) -> None:
        if self.sock is not None:
            self.sock.close()
            self.sock = None
