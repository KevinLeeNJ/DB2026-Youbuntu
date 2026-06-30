import socket
import threading
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


class BackendProtocolTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()

