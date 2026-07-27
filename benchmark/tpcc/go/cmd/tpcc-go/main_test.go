package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

func TestTPCCTxnMixMatchesOfficialWeights(t *testing.T) {
	counts := make(map[string]int)
	for bucket := 0; bucket < 100; bucket++ {
		counts[txnTypeForBucket(bucket)]++
	}
	want := map[string]int{"new_order": 45, "payment": 43, "order_status": 4, "delivery": 4, "stock_level": 4}
	if fmt.Sprint(counts) != fmt.Sprint(want) {
		t.Fatalf("transaction mix = %#v, want %#v", counts, want)
	}
}

func TestOfficialTerminalHomePairsWorkers(t *testing.T) {
	p := profile{warehouses: 50, districtsPerWarehouse: 10, customersPerDistrict: 3000, itemCount: 100000}
	first := chooseContext(p, 0, "official-terminal-home", rand.New(rand.NewSource(1)))
	second := chooseContext(p, 1, "official-terminal-home", rand.New(rand.NewSource(1)))
	twentyFifth := chooseContext(p, 48, "official-terminal-home", rand.New(rand.NewSource(1)))
	if first.wID != 1 || second.wID != 1 || twentyFifth.wID != 25 {
		t.Fatalf("terminal homes = %d, %d, %d; want 1, 1, 25", first.wID, second.wID, twentyFifth.wID)
	}
}

func TestOfficialModeValidation(t *testing.T) {
	if err := validateBenchmarkMode("official-equivalent", 50, 10, 60, 1); err != nil {
		t.Fatal(err)
	}
	if err := validateBenchmarkMode("official-equivalent", 16, 10, 60, 1); err == nil {
		t.Fatal("non-official worker count unexpectedly accepted")
	}
	if err := validateBenchmarkMode("sqlite-reference", 16, 30, 360, 1); err != nil {
		t.Fatal(err)
	}
	if err := validateBenchmarkMode("local", 16, 30, 360, 1); err == nil {
		t.Fatal("legacy local benchmark mode unexpectedly accepted")
	}
}

func TestOfficialTPCCTargetIndexesAreDeclared(t *testing.T) {
	want := []struct {
		rmdbStatement   string
		sqliteStatement string
	}{
		{
			rmdbStatement:   "create index customer_last on customer(c_w_id, c_d_id, c_last, c_first, c_id);",
			sqliteStatement: "create index idx_customer_last on customer(c_w_id, c_d_id, c_last, c_first, c_id);",
		},
		{
			rmdbStatement:   "create index orders_customer on orders(o_w_id, o_d_id, o_c_id, o_id);",
			sqliteStatement: "create index idx_orders_customer on orders(o_w_id, o_d_id, o_c_id, o_id);",
		},
	}
	for _, schemaName := range []string{"rmdb_indexes.sql", "sqlite_indexes.sql"} {
		data, err := os.ReadFile(tpccSchemaPath(schemaName))
		if err != nil {
			t.Fatal(err)
		}
		text := strings.ToLower(string(data))
		for _, index := range want {
			statement := index.sqliteStatement
			if schemaName == "rmdb_indexes.sql" {
				statement = index.rmdbStatement
			}
			if !strings.Contains(text, statement) {
				t.Errorf("%s does not declare %q", schemaName, statement)
			}
		}
		if strings.Contains(text, "distinct") || strings.Contains(text, "exec_batch") || strings.Contains(text, "prepare_set") {
			t.Errorf("%s contains out-of-scope protocol or DISTINCT changes", schemaName)
		}
	}
}

func TestOfficialTPCCTargetIndexesExecuteAndSupportQueries(t *testing.T) {
	backend, err := newSQLiteBackend(filepath.Join(t.TempDir(), "tpcc.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.close()
	if err := backend.execFile(tpccSchemaPath("sqlite_schema.sql")); err != nil {
		t.Fatal(err)
	}
	if err := backend.execFile(tpccSchemaPath("sqlite_indexes.sql")); err != nil {
		t.Fatal(err)
	}

	checks := []struct {
		name    string
		columns []string
		query   string
	}{
		{
			name:    "idx_customer_last",
			columns: []string{"c_w_id", "c_d_id", "c_last", "c_first", "c_id"},
			query:   "select c_id from customer where c_w_id = 1 and c_d_id = 1 and c_last = 'BARBARBAR' order by c_first, c_id;",
		},
		{
			name:    "idx_orders_customer",
			columns: []string{"o_w_id", "o_d_id", "o_c_id", "o_id"},
			query:   "select o_id from orders where o_w_id = 1 and o_d_id = 1 and o_c_id = 1 order by o_id desc limit 1;",
		},
	}
	for _, check := range checks {
		info, err := backend.exec("pragma index_info(" + check.name + ");")
		if err != nil {
			t.Fatalf("PRAGMA index_info(%s): %v", check.name, err)
		}
		if strings.TrimSpace(info) == "" {
			t.Fatalf("index %s was not created", check.name)
		}
		var gotColumns []string
		for _, line := range strings.Split(strings.TrimSpace(info), "\n") {
			fields := strings.Split(line, "|")
			if len(fields) != 3 {
				t.Fatalf("PRAGMA index_info(%s) returned malformed row %q", check.name, line)
			}
			gotColumns = append(gotColumns, fields[2])
		}
		if strings.Join(gotColumns, ",") != strings.Join(check.columns, ",") {
			t.Fatalf("index %s columns = %v, want %v", check.name, gotColumns, check.columns)
		}
		plan, err := backend.exec("explain query plan " + check.query)
		if err != nil {
			t.Fatalf("EXPLAIN for %s: %v", check.name, err)
		}
		if !strings.Contains(plan, check.name) {
			t.Fatalf("query plan does not use %s: %q", check.name, plan)
		}
	}
}

func tpccSchemaPath(name string) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "schema", name)
}

func TestWaitForReadyExecutesShowTables(t *testing.T) {
	received := make(chan []byte, 1)
	address := runTestServer(t, []byte("Total record(s): 0\n\x00"), received)
	if err := waitForReady(address, time.Second); err != nil {
		t.Fatal(err)
	}
	if got := string(<-received); got != "show tables;\x00" {
		t.Fatalf("readiness request = %q", got)
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
	if result.Committed["new_order"] != 2 || result.NewOrderPerMin != 2 {
		t.Fatalf("committed = %#v, NewOrder/min = %v", result.Committed, result.NewOrderPerMin)
	}
}

func TestWorkerSeedIsStableAndSeparatesStreams(t *testing.T) {
	const seed int64 = 8675309
	first := workerSeed(seed, 1, 0)
	if first != 321745938649985385 {
		t.Fatalf("workerSeed mapping changed: got %d", first)
	}
	if got := workerSeed(seed, 1, 0); got != first {
		t.Fatalf("workerSeed is not stable: first=%d second=%d", first, got)
	}
	for name, got := range map[string]int64{
		"seed":   workerSeed(seed+1, 1, 0),
		"round":  workerSeed(seed, 2, 0),
		"worker": workerSeed(seed, 1, 1),
	} {
		if got == first {
			t.Fatalf("%s did not change derived seed %d", name, first)
		}
	}
}

func TestMedianUsesStandardOddAndEvenDefinitions(t *testing.T) {
	if got := median([]float64{9, 1, 5}); got != 5 {
		t.Fatalf("odd median = %v, want 5", got)
	}
	if got := median([]float64{110, 100}); got != 105 {
		t.Fatalf("even median = %v, want 105", got)
	}
}

func TestResultMetricsExcludeWarmupButKeepWarmupCounts(t *testing.T) {
	result := newResult(60)
	result.record("warmup", "new_order", "commit", 1000, "")
	result.record("warmup", "payment", "server-abort", 1000, "warmup abort")
	result.record("measure", "new_order", "commit", 10, "")
	result.record("measure", "payment", "server-abort", 20, "measure abort")
	result.finalize()

	if got := result.Counts["warmup"]["new_order"]["commit"]; got != 1 {
		t.Fatalf("warmup commit count = %d, want 1", got)
	}
	if got := result.Counts["warmup"]["payment"]["server-abort"]; got != 1 {
		t.Fatalf("warmup abort count = %d, want 1", got)
	}
	if got := result.AbortRate; got != 0.5 {
		t.Fatalf("abort rate = %v, want measurement-only 0.5", got)
	}
	if got := result.LatencyMS["new_order"]; got.P50 != 10 || got.Max != 10 {
		t.Fatalf("measurement latency = %#v, want only 10ms", got)
	}
	if _, ok := result.LatencyMS["payment"]; ok {
		t.Fatalf("abort latency unexpectedly summarized: %#v", result.LatencyMS["payment"])
	}
}

func writeResultDocument(t *testing.T, path string, doc document) {
	t.Helper()
	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
}

func TestMergeResultFilesRejectsDifferentSeed(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "round-1.json")
	secondPath := filepath.Join(dir, "round-2.json")
	outputPath := filepath.Join(dir, "merged.json")
	baseConfig := config{Backend: "rmdb", Isolation: "read-committed", Warehouses: 1, Workers: 4, Warmup: 10, Measure: 60, Rounds: 1, Seed: 11, WarehousePolicy: "terminal-home"}
	writeResultDocument(t, firstPath, document{Config: baseConfig, Rounds: []*result{{TPMC: 100}}})
	secondConfig := baseConfig
	secondConfig.Seed = 12
	writeResultDocument(t, secondPath, document{Config: secondConfig, Rounds: []*result{{TPMC: 110}}})

	err := mergeResultFiles(outputPath, firstPath+","+secondPath)
	if err == nil || !strings.Contains(err.Error(), "materially different") {
		t.Fatalf("merge error = %v, want config mismatch", err)
	}
}

func TestMergeResultFilesAllowsRoundAndProgressDifferences(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "round-1.json")
	secondPath := filepath.Join(dir, "round-2.json")
	outputPath := filepath.Join(dir, "merged.json")
	baseConfig := config{Backend: "rmdb", Isolation: "read-committed", Warehouses: 1, Workers: 4, Warmup: 10, Measure: 60, Rounds: 1, ProgressInterval: 1, Seed: 11, WarehousePolicy: "terminal-home"}
	writeResultDocument(t, firstPath, document{Config: baseConfig, Rounds: []*result{{TPMC: 100}}})
	secondConfig := baseConfig
	secondConfig.Rounds = 99
	secondConfig.ProgressInterval = 10
	writeResultDocument(t, secondPath, document{Config: secondConfig, Rounds: []*result{{TPMC: 110}}})

	if err := mergeResultFiles(outputPath, firstPath+","+secondPath); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	var merged document
	if err := json.Unmarshal(data, &merged); err != nil {
		t.Fatal(err)
	}
	if merged.Config.Rounds != 2 || len(merged.Rounds) != 2 || merged.MedianTPMC != 105 {
		t.Fatalf("merged document = %#v", merged)
	}
}

type beginErrorBackend struct {
	err error
}

func (b *beginErrorBackend) exec(string) (string, error) { return "", nil }
func (b *beginErrorBackend) begin() error                { return b.err }
func (b *beginErrorBackend) commit() error               { return nil }
func (b *beginErrorBackend) rollback()                   {}
func (b *beginErrorBackend) close()                      {}

func shortRound(factory backendFactory) (*result, error) {
	end := time.Now().Add(5 * time.Millisecond)
	return runRound(1, 1, 1, profile{warehouses: 1, districtsPerWarehouse: 1, customersPerDistrict: 1, itemCount: 1}, "terminal-home", time.Now(), end, 1, 0, false, &liveStats{}, factory)
}

func TestRunRoundRejectsInitialConnectFailure(t *testing.T) {
	want := fmt.Errorf("connect failed")
	result, err := shortRound(func() (txnBackend, error) { return nil, want })
	if result != nil || err == nil || !strings.Contains(err.Error(), want.Error()) {
		t.Fatalf("runRound() = (%#v, %v), want nil result and connect error", result, err)
	}
}

func TestRunRoundRejectsTransactionBackendError(t *testing.T) {
	want := fmt.Errorf("backend failed")
	result, err := shortRound(func() (txnBackend, error) { return &beginErrorBackend{err: want}, nil })
	if result != nil || err == nil || !strings.Contains(err.Error(), want.Error()) {
		t.Fatalf("runRound() = (%#v, %v), want nil result and backend error", result, err)
	}
}

func TestRunRoundAllowsExpectedAbort(t *testing.T) {
	result, err := shortRound(func() (txnBackend, error) { return &beginErrorBackend{err: errAbort}, nil })
	if err != nil || result == nil {
		t.Fatalf("runRound() = (%#v, %v), want successful abort-only round", result, err)
	}
	if result.hasBackendError() {
		t.Fatal("expected abort was classified as backend error")
	}
}

func TestMergeResultFilesRejectsBackendErrorRound(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "failed-round.json")
	outputPath := filepath.Join(dir, "merged.json")
	failed := newResult(60)
	failed.record("measure", "payment", "backend-error", 1, "connection reset")
	writeResultDocument(t, inputPath, document{Rounds: []*result{failed}})

	err := mergeResultFiles(outputPath, inputPath)
	if err == nil || !strings.Contains(err.Error(), "backend-error") {
		t.Fatalf("merge error = %v, want backend-error rejection", err)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("merge output exists after rejection: %v", err)
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

func TestDatasetManifestValidation(t *testing.T) {
	dir := t.TempDir()
	for _, table := range tpccTables {
		if err := os.WriteFile(filepath.Join(dir, table+".csv"), []byte("header\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeDatasetManifest(dir, 3, 17); err != nil {
		t.Fatal(err)
	}
	if err := validateDataset(dir, 3, 17); err != nil {
		t.Fatalf("matching manifest rejected: %v", err)
	}
	if err := validateDataset(dir, 4, 17); err == nil || !strings.Contains(err.Error(), "warehouses mismatch") {
		t.Fatalf("warehouse mismatch error = %v", err)
	}
	if err := validateDataset(dir, 3, 18); err == nil || !strings.Contains(err.Error(), "seed mismatch") {
		t.Fatalf("seed mismatch error = %v", err)
	}
}

func TestDatasetValidationRequiresManifest(t *testing.T) {
	dir := t.TempDir()
	for _, table := range tpccTables {
		if err := os.WriteFile(filepath.Join(dir, table+".csv"), []byte("header\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	if err := validateDataset(dir, 1, 1); err == nil || !strings.Contains(err.Error(), datasetManifestName) {
		t.Fatalf("missing manifest error = %v", err)
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
