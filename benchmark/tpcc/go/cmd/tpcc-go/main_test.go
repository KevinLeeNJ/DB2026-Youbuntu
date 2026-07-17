package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func runTestServer(t *testing.T, response []byte, received chan<- []byte) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer listener.Close()
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buffer := make([]byte, 4096)
		n, err := conn.Read(buffer)
		if err == nil {
			received <- buffer[:n]
		}
		_, _ = conn.Write(response)
	}()
	return listener.Addr().String()
}

func TestClientExecUsesNulProtocol(t *testing.T) {
	received := make(chan []byte, 1)
	address := runTestServer(t, []byte("OK\n\x00"), received)
	client, err := newClient(address, time.Second, "read-committed")
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	text, err := client.exec("select 1;")
	if err != nil {
		t.Fatal(err)
	}
	if text != "OK" {
		t.Fatalf("response = %q, want OK", text)
	}
	if got := string(<-received); got != "select 1;\x00" {
		t.Fatalf("request = %q", got)
	}
}

func TestClientExecClassifiesAbort(t *testing.T) {
	received := make(chan []byte, 1)
	address := runTestServer(t, []byte("abort\n\x00"), received)
	client, err := newClient(address, time.Second, "read-committed")
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	if _, err := client.exec("commit;"); err != errAbort {
		t.Fatalf("error = %v, want errAbort", err)
	}
}

func TestParseRowsDropsTableHeader(t *testing.T) {
	rows := parseRows("+---+\n| id |\n+---+\n| 42 |\n+---+\nTotal record(s): 1\n")
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] != "42" {
		t.Fatalf("rows = %#v", rows)
	}
}

func TestVerifyAtomicSumsAcceptsNegativeSum(t *testing.T) {
	response := "+---+\n| SUM(value) |\n+---+\n| %d |\n+---+\nTotal record(s): 1\n"
	if err := verifyAtomicSums(fmt.Sprintf(response, 175), fmt.Sprintf(response, -175)); err != nil {
		t.Fatalf("verifyAtomicSums() error = %v", err)
	}
}

func TestSurnameAcceptsTPCCTokenRange(t *testing.T) {
	if got := surname(0); got != "BARBARBAR" {
		t.Fatalf("surname(0) = %q", got)
	}
	if got := surname(999); got != "EINGEINGEING" {
		t.Fatalf("surname(999) = %q", got)
	}
}

func TestResultMergePreservesCountsAndLatencies(t *testing.T) {
	combined := newResult(60)
	first := newResult(60)
	first.record("measure", "new_order", "commit", 10, "")
	second := newResult(60)
	second.record("measure", "new_order", "commit", 20, "")
	second.record("measure", "payment", "server-abort", 5, "abort")
	combined.merge(first)
	combined.merge(second)
	combined.finalize()
	if combined.Counts["measure"]["new_order"]["commit"] != 2 {
		t.Fatalf("new_order commits = %d", combined.Counts["measure"]["new_order"]["commit"])
	}
	if combined.TPMC != 2 {
		t.Fatalf("tpmC = %v, want 2", combined.TPMC)
	}
	if combined.LatencyMS["new_order"].P50 != 10 || combined.LatencyMS["new_order"].Max != 20 {
		t.Fatalf("latency = %#v", combined.LatencyMS["new_order"])
	}
}

func TestResultFinalizeIncludesPerTransactionTPM(t *testing.T) {
	result := newResult(60)
	result.record("measure", "new_order", "commit", 1, "")
	result.record("measure", "new_order", "commit", 2, "")
	result.record("measure", "delivery", "commit", 3, "")
	result.finalize()
	if result.TxnTPM["new_order"] != 2 || result.TxnTPM["delivery"] != 1 {
		t.Fatalf("txn_tpm = %#v", result.TxnTPM)
	}
}

func TestSQLiteBackendRoundTrip(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tpcc.sqlite")
	backend, err := newSQLiteBackend(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.close()
	if _, err := backend.exec("create table t (id integer, value text);"); err != nil {
		t.Fatal(err)
	}
	if err := backend.begin(); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.exec("insert into t values (7, 'ok');"); err != nil {
		t.Fatal(err)
	}
	if err := backend.commit(); err != nil {
		t.Fatal(err)
	}
	text, err := backend.exec("select id, value from t;")
	if err != nil {
		t.Fatal(err)
	}
	if text != "7|ok\n" {
		t.Fatalf("SQLite result = %q, want %q", text, "7|ok\n")
	}
}

func TestCSVSetAndLoadPath(t *testing.T) {
	dir := t.TempDir()
	if completeCSVSet(dir) {
		t.Fatal("empty directory reported as complete")
	}
	for _, table := range tpccTables {
		if err := os.WriteFile(filepath.Join(dir, table+".csv"), []byte("header\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	if !completeCSVSet(dir) {
		t.Fatal("complete CSV set not detected")
	}
	t.Chdir(dir)
	path, err := loadPath("data", "db", "warehouse")
	if err != nil {
		t.Fatal(err)
	}
	if path != "../data/warehouse.csv" {
		t.Fatalf("load path = %q", path)
	}
}
