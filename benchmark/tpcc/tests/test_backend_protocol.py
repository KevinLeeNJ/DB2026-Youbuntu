import socket
import threading
import time
import unittest

from benchmark.tpcc.core.backend import BackendAbort, BackendError
from benchmark.tpcc.core.rmdb_backend import RmdbBackend


def run_fake_server(response: bytes, captured: list[bytes]) -> int:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]

    def serve() -> None:
        conn, _ = server.accept()
        with conn:
            captured.append(conn.recv(4096))
            conn.sendall(response)
        server.close()

    threading.Thread(target=serve, daemon=True).start()
    return port


def run_slow_server(
    response: bytes, delay_seconds: float, captured: list[bytes]
) -> int:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]

    def serve() -> None:
        conn, _ = server.accept()
        with conn:
            captured.append(conn.recv(4096))
            time.sleep(delay_seconds)
            try:
                conn.sendall(response)
            except OSError:
                pass
        server.close()

    threading.Thread(target=serve, daemon=True).start()
    return port


class BackendProtocolTest(unittest.TestCase):
    def test_snapshot_isolation_is_set_before_workload_commands(self) -> None:
        captured: list[bytes] = []
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]

        def serve() -> None:
            conn, _ = server.accept()
            with conn:
                commands = []
                pending = b""
                while len(commands) < 2:
                    pending += conn.recv(4096)
                    while b"\0" in pending:
                        command, pending = pending.split(b"\0", 1)
                        commands.append(command)
                        conn.sendall(b"OK\0")
                        if len(commands) == 2:
                            break
                captured.extend(commands)
            server.close()

        threading.Thread(target=serve, daemon=True).start()
        backend = RmdbBackend("127.0.0.1", port, isolation="snapshot-isolation")
        self.assertEqual(backend.execute("select * from warehouse;"), "OK")
        backend.close()
        self.assertEqual(
            captured,
            [
                b"set transaction isolation level snapshot isolation;",
                b"select * from warehouse;",
            ],
        )

    def test_execute_sends_nul_terminated_single_command(self) -> None:
        captured: list[bytes] = []
        port = run_fake_server(b"OK\0", captured)
        backend = RmdbBackend("127.0.0.1", port)
        self.assertEqual(backend.execute("select * from warehouse;"), "OK")
        backend.close()
        self.assertEqual(captured, [b"select * from warehouse;\0"])

    def test_abort_response_raises_backend_abort(self) -> None:
        captured: list[bytes] = []
        port = run_fake_server(b"abort\n\0", captured)
        backend = RmdbBackend("127.0.0.1", port)
        with self.assertRaises(BackendAbort):
            backend.execute("commit;")
        backend.close()

    def test_error_response_raises_backend_error(self) -> None:
        captured: list[bytes] = []
        port = run_fake_server(b"Error: bad\n\0", captured)
        backend = RmdbBackend("127.0.0.1", port)
        with self.assertRaises(BackendError):
            backend.execute("bad sql;")
        backend.close()

    def test_timeout_closes_connection_before_late_response_can_desync_protocol(
        self,
    ) -> None:
        captured: list[bytes] = []
        port = run_slow_server(b"LATE\0", 0.2, captured)
        backend = RmdbBackend("127.0.0.1", port, timeout=0.05)
        with self.assertRaises(BackendError):
            backend.execute("slow sql;")
        with self.assertRaises(BackendError):
            backend.execute("select 1;")
        backend.close()
        self.assertEqual(captured, [b"slow sql;\0"])


if __name__ == "__main__":
    unittest.main()
