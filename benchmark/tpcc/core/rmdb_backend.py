from __future__ import annotations

import socket

from benchmark.tpcc.core.backend import Backend, BackendAbort, BackendError


class RmdbBackend(Backend):
    def __init__(self, host: str, port: int, timeout: float = 30.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)

    def execute(self, sql: str) -> str:
        payload = sql.encode("utf-8") + b"\0"
        self.sock.sendall(payload)
        chunks: list[bytes] = []
        while True:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise BackendError("connection closed by rmdb server")
            chunks.append(chunk)
            if b"\0" in chunk:
                break
        raw = b"".join(chunks).split(b"\0", 1)[0].decode("utf-8", errors="replace")
        text = raw.rstrip("\n")
        if text == "abort":
            raise BackendAbort("rmdb transaction aborted")
        if text.startswith("Error:") or text.startswith("Parser Error"):
            raise BackendError(text)
        return text

    def close(self) -> None:
        self.sock.close()

