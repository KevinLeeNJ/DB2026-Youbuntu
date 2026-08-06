package main

import (
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
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
		handshake := make([]byte, 8)
		if _, err := io.ReadFull(conn, handshake); err != nil {
			return
		}
		_, _ = conn.Write(handshake)
		header := make([]byte, 8)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(header[:4])
		payload := make([]byte, length)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return
		}
		received <- payload
		_, _ = conn.Write(response)
	}()
	return listener.Addr().String()
}

func testWireFrame(tag byte, payload []byte) []byte {
	frame := make([]byte, 8, 8+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	frame[4] = tag
	return append(frame, payload...)
}

func TestClientExecUsesWireProtocol(t *testing.T) {
	received := make(chan []byte, 1)
	meta := []byte{0, 1, 0, 5, 'v', 'a', 'l', 'u', 'e', 1}
	row := []byte{1, 0, 0, 0, 42}
	response := testWireFrame(0x01, meta)
	response = append(response, testWireFrame(0x02, row)...)
	response = append(response, testWireFrame(0x11, []byte{0, 0, 0, 0, 0, 0, 0, 1})...)
	address := runTestServer(t, response, received)
	client, err := newClient(address, time.Second, "read-committed")
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	text, err := client.exec("select 1;")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "42") {
		t.Fatalf("response = %q, want row 42", text)
	}
	if got := string(<-received); got != "select 1;" {
		t.Fatalf("request = %q", got)
	}
}

func TestClientExecClassifiesAbort(t *testing.T) {
	received := make(chan []byte, 1)
	response := testWireFrame(0x12, []byte("abort"))
	address := runTestServer(t, response, received)
	client, err := newClient(address, time.Second, "read-committed")
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	if _, err := client.exec("commit;"); err != errAbort {
		t.Fatalf("error = %v, want errAbort", err)
	}
}

func TestClientRejectsUnknownWireResponseTag(t *testing.T) {
	received := make(chan []byte, 1)
	address := runTestServer(t, testWireFrame(0x7f, nil), received)
	client, err := newClient(address, time.Second, "read-committed")
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	if _, err := client.exec("select 1;"); err == nil || !strings.Contains(err.Error(), "invalid RMDB wire frame") {
		t.Fatalf("error = %v, want invalid frame error", err)
	}
}

func TestClientDoesNotSendRollbackAfterServerAutoAbort(t *testing.T) {
	received := make(chan []byte, 1)
	address := runTestServer(t, testWireFrame(wireTagError, []byte("statement failed")), received)
	client, err := newClient(address, time.Second, "read-committed")
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	if _, err := client.exec("update t set value = 1;"); err == nil {
		t.Fatal("exec unexpectedly succeeded")
	}
	if !client.autoAborted {
		t.Fatal("client did not record server-side AUTO_ABORT")
	}
	client.rollback()
	if client.autoAborted {
		t.Fatal("rollback did not clear AUTO_ABORT state")
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

func TestOfficialWheelCoversAllSlotsInFiveWaves(t *testing.T) {
	p := profile{warehouses: 50, districtsPerWarehouse: 10, customersPerDistrict: 3000, itemCount: 100000}
	plan, err := newOfficialRoutingPlan(1, p, 1)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[int]int{}
	for txnNo := uint64(0); txnNo < 5; txnNo++ {
		for clientID := 0; clientID < officialWorkers; clientID++ {
			counts[officialSlotIndex(clientID, txnNo)]++
		}
	}
	if len(counts) != officialSlotCount {
		t.Fatalf("five waves covered %d slots, want %d", len(counts), officialSlotCount)
	}
	for slot, count := range counts {
		if count != 1 {
			t.Fatalf("slot %d appeared %d times", slot, count)
		}
	}
	if len(plan.hotWarehouses) != officialHotWarehouseCount {
		t.Fatalf("hot warehouse count = %d", len(plan.hotWarehouses))
	}
}

func TestOfficialModeValidation(t *testing.T) {
	official := func(workers, warmup, measure, rounds int, think time.Duration, allow bool) error {
		return validateBenchmarkMode("official-equivalent", workers, warmup, measure, rounds, think, allow)
	}
	if err := official(officialWorkers, officialWarmupSeconds, officialMeasureSeconds, officialWindows, 0, false); err != nil {
		t.Fatal(err)
	}
	rejected := map[string]error{
		"workers": official(16, officialWarmupSeconds, officialMeasureSeconds, officialWindows, 0, false),
		"warmup":  official(officialWorkers, 10, officialMeasureSeconds, officialWindows, 0, false),
		"measure": official(officialWorkers, officialWarmupSeconds, 60, officialWindows, 0, false),
		"rounds":  official(officialWorkers, officialWarmupSeconds, officialMeasureSeconds, 1, 0, false),
		"think":   official(officialWorkers, officialWarmupSeconds, officialMeasureSeconds, officialWindows, time.Millisecond, false),
	}
	for name, err := range rejected {
		if err == nil {
			t.Fatalf("non-official %s unexpectedly accepted", name)
		}
	}
	// The escape hatch must be explicit, and it must still reject nonsense.
	if err := official(16, 10, 60, 1, time.Millisecond, true); err != nil {
		t.Fatal(err)
	}
	if err := official(16, 10, 0, 1, 0, true); err == nil {
		t.Fatal("zero measurement window accepted under --allow-nonofficial-timing")
	}
	if err := validateBenchmarkMode("sqlite-reference", 16, 30, 360, 1, 0, false); err != nil {
		t.Fatal(err)
	}
	if err := validateBenchmarkMode("local", 16, 30, 360, 1, 0, false); err == nil {
		t.Fatal("legacy local benchmark mode unexpectedly accepted")
	}
}

func TestOfficialWheelHasFinalV2Distribution(t *testing.T) {
	p := profile{warehouses: 50, districtsPerWarehouse: 10, customersPerDistrict: 3000, itemCount: 100000}
	plan, err := newOfficialRoutingPlan(11, p, 2)
	if err != nil {
		t.Fatal(err)
	}
	for phase, wheel := range plan.slots {
		counts := map[int]int{}
		for _, wID := range wheel {
			counts[wID]++
		}
		if len(counts) != officialWarehouses {
			t.Fatalf("phase %d covers %d warehouses", phase, len(counts))
		}
		extraCold := 0
		for wID, count := range counts {
			if _, hot := plan.hotWarehouse[wID]; hot {
				if count != officialHotWarehouseSlots {
					t.Fatalf("phase %d hot warehouse %d has %d slots", phase, wID, count)
				}
			} else if count == 2 {
				extraCold++
			} else if count != 1 {
				t.Fatalf("phase %d cold warehouse %d has %d slots", phase, wID, count)
			}
		}
		if extraCold != officialExtraColdSlots {
			t.Fatalf("phase %d has %d extra cold warehouses", phase, extraCold)
		}
	}
}

func TestStatementResultKindSeparatesQueriesFromCommands(t *testing.T) {
	cases := map[string]resultKind{
		"select count(*) from stock;": resultKindQuery,
		"  SELECT 1;":                 resultKindQuery,
		"insert into t values (1);":   resultKindCommand,
		"update t set a = 1;":         resultKindCommand,
		"delete from t where a = 1;":  resultKindCommand,
		"create index t(a);":          resultKindCommand,
		"load ./t.csv into t;":        resultKindCommand,
		"commit;":                     resultKindCommand,
		// final.md:33 lets `show tables;` answer with either terminator.
		"show tables;": resultKindEither,
	}
	for sql, want := range cases {
		if got := statementResultKind(sql); got != want {
			t.Errorf("statementResultKind(%q) = %d, want %d", sql, got, want)
		}
	}
}

func TestClientExecRejectsInterchangedResultTerminators(t *testing.T) {
	// final.md:645: a query may only succeed with RESULT_END and a non-query only
	// with COMMAND_OK; treating COMMAND_OK as an empty result set would hide a
	// protocol contract failure.
	received := make(chan []byte, 1)
	address := runTestServer(t, testWireFrame(wireTagCommandOK, nil), received)
	queryClient, err := newClient(address, time.Second, "read-committed")
	if err != nil {
		t.Fatal(err)
	}
	defer queryClient.close()
	if _, err := queryClient.exec("select count(*) from stock;"); err == nil ||
		!strings.Contains(err.Error(), "COMMAND_OK") {
		t.Fatalf("query answered with COMMAND_OK: error = %v", err)
	}
	<-received

	meta := []byte{0, 1, 0, 5, 'v', 'a', 'l', 'u', 'e', 1}
	response := testWireFrame(wireTagMeta, meta)
	response = append(response, testWireFrame(wireTagResultEnd, []byte{0, 0, 0, 0, 0, 0, 0, 0})...)
	commandAddress := runTestServer(t, response, make(chan []byte, 1))
	commandClient, err := newClient(commandAddress, time.Second, "read-committed")
	if err != nil {
		t.Fatal(err)
	}
	defer commandClient.close()
	if _, err := commandClient.exec("update t set a = 1;"); err == nil ||
		!strings.Contains(err.Error(), "META") {
		t.Fatalf("non-query answered with META: error = %v", err)
	}
}

func TestOfficialWorkerReportAttributesByCompletionTime(t *testing.T) {
	// final.md:214 counts the transactions that committed inside the window, so a
	// transaction that starts in one window and commits in the next belongs to
	// the later window.
	report := &officialWorkerReport{warmup: newResult(0), windows: []*result{newResult(150), newResult(150)}}
	warmupEnd := time.Now()
	measure := 150 * time.Second
	if phase, local, window := report.attribute(warmupEnd.Add(-time.Second), warmupEnd, measure); phase != "warmup" || local != report.warmup || window != -1 {
		t.Fatalf("warmup attribution = (%q, %p, %d)", phase, local, window)
	}
	if phase, local, window := report.attribute(warmupEnd.Add(measure-time.Millisecond), warmupEnd, measure); phase != "measure" || local != report.windows[0] || window != 0 {
		t.Fatalf("first window attribution = (%q, %p, %d)", phase, local, window)
	}
	if phase, local, window := report.attribute(warmupEnd.Add(measure), warmupEnd, measure); phase != "measure" || local != report.windows[1] || window != 1 {
		t.Fatalf("second window attribution = (%q, %p, %d)", phase, local, window)
	}
	if _, local, _ := report.attribute(warmupEnd.Add(2*measure), warmupEnd, measure); local != nil {
		t.Fatal("a completion after the final window was attributed to a window")
	}
}

func TestConflictRetryDefaultAllowsOneRetry(t *testing.T) {
	now := time.Unix(100, 0)
	attempts := 0
	err, outcome := runTxnWithConflictRetries(now.Add(time.Second), defaultMaxConflictRetries,
		make(chan struct{}), func() time.Time { return now }, func() error {
			attempts++
			if attempts == 1 {
				return errAbort
			}
			return nil
		})
	if err != nil || outcome != conflictRetryCompleted || attempts != 2 {
		t.Fatalf("retry result = (%v, %v), attempts = %d, want success after one retry", err, outcome, attempts)
	}
}

func TestConflictRetryFiniteBudgetAllowsNRetries(t *testing.T) {
	now := time.Unix(100, 0)
	for _, test := range []struct {
		name        string
		succeedLast bool
		wantErr     bool
	}{
		{name: "final retry succeeds", succeedLast: true},
		{name: "final retry aborts", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			attempts := 0
			err, outcome := runTxnWithConflictRetries(now.Add(time.Second), 2,
				make(chan struct{}), func() time.Time { return now }, func() error {
					attempts++
					if test.succeedLast && attempts == 3 {
						return nil
					}
					return errAbort
				})
			wantOutcome := conflictRetryCompleted
			if test.wantErr {
				wantOutcome = conflictRetryExhausted
			}
			if outcome != wantOutcome || attempts != 3 || errors.Is(err, errAbort) != test.wantErr {
				t.Fatalf("retry result = (%v, %v), attempts = %d", err, outcome, attempts)
			}
		})
	}
}

func TestConflictRetryStopsAtPhaseDeadline(t *testing.T) {
	start := time.Unix(100, 0)
	deadline := start.Add(time.Second)
	times := []time.Time{start, deadline}
	clockCalls := 0
	attempts := 0
	err, outcome := runTxnWithConflictRetries(deadline, unlimitedConflictRetries,
		make(chan struct{}), func() time.Time {
			now := times[clockCalls]
			clockCalls++
			return now
		}, func() error {
			attempts++
			return errAbort
		})
	if err != nil || outcome != conflictRetryDeadline || attempts != 1 {
		t.Fatalf("retry result = (%v, %v), attempts = %d, want phase deadline after one abort",
			err, outcome, attempts)
	}
}

func TestOfficialTPCCSchemaAndIndexesMatchFinalV3(t *testing.T) {
	const wantSchema = `create table warehouse (w_id int, w_name char(10), w_street_1 char(20), w_street_2 char(20), w_city char(20), w_state char(2), w_zip char(9), w_tax float, w_ytd float);
create table district (d_id int, d_w_id int, d_name char(10), d_street_1 char(20), d_street_2 char(20), d_city char(20), d_state char(2), d_zip char(9), d_tax float, d_ytd float, d_next_o_id int);
create table customer (c_id int, c_d_id int, c_w_id int, c_first char(16), c_middle char(2), c_last char(16), c_street_1 char(20), c_street_2 char(20), c_city char(20), c_state char(2), c_zip char(9), c_phone char(16), c_since char(30), c_credit char(2), c_credit_lim int, c_discount float, c_balance float, c_ytd_payment float, c_payment_cnt int, c_delivery_cnt int, c_data char(50));
create table history (h_c_id int, h_c_d_id int, h_c_w_id int, h_d_id int, h_w_id int, h_date char(30), h_amount float, h_data char(24));
create table new_orders (no_o_id int, no_d_id int, no_w_id int);
create table orders (o_id int, o_d_id int, o_w_id int, o_c_id int, o_entry_d char(30), o_carrier_id int, o_ol_cnt int, o_all_local int);
create table order_line (ol_o_id int, ol_d_id int, ol_w_id int, ol_number int, ol_i_id int, ol_supply_w_id int, ol_delivery_d char(30), ol_quantity int, ol_amount float, ol_dist_info char(24));
create table item (i_id int, i_im_id int, i_name char(24), i_price float, i_data char(50));
create table stock (s_i_id int, s_w_id int, s_quantity int, s_dist_01 char(24), s_dist_02 char(24), s_dist_03 char(24), s_dist_04 char(24), s_dist_05 char(24), s_dist_06 char(24), s_dist_07 char(24), s_dist_08 char(24), s_dist_09 char(24), s_dist_10 char(24), s_ytd float, s_order_cnt int, s_remote_cnt int, s_data char(50));`
	const wantIndexes = `create index warehouse(w_id);
create index district(d_w_id, d_id);
create index customer(c_w_id, c_d_id, c_id);
create index customer(c_w_id, c_d_id, c_last, c_id);
create index new_orders(no_w_id, no_d_id, no_o_id);
create index orders(o_w_id, o_d_id, o_id);
create index orders(o_w_id, o_d_id, o_c_id, o_id);
create index order_line(ol_w_id, ol_d_id, ol_o_id, ol_number);
create index item(i_id);
create index stock(s_w_id, s_i_id);`
	for file, want := range map[string]string{
		"rmdb_schema.sql":  wantSchema,
		"rmdb_indexes.sql": wantIndexes,
	} {
		data, err := os.ReadFile(tpccSchemaPath(file))
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.TrimSpace(string(data)); got != want {
			t.Errorf("%s differs from the finalv3 contract\n--- got ---\n%s\n--- want ---\n%s", file, got, want)
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
	customerInfo, err := backend.exec("pragma table_info(customer);")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToUpper(customerInfo), "|C_CREDIT_LIM|INTEGER|") {
		t.Fatalf("SQLite c_credit_lim does not use INTEGER affinity:\n%s", customerInfo)
	}

	checks := []struct {
		name    string
		columns []string
		query   string
	}{
		{
			name:    "idx_customer_last",
			columns: []string{"c_w_id", "c_d_id", "c_last", "c_id"},
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
	address := runTestServer(t, testWireFrame(0x10, nil), received)
	if err := waitForReady(address, time.Second); err != nil {
		t.Fatal(err)
	}
	if got := string(<-received); got != "show tables;" {
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

func TestResultFinalizeReportsLogicalAttemptOutcomes(t *testing.T) {
	result := newResult(60)
	result.record("measure", "new_order", "commit", 1, "")
	result.record("measure", "new_order", "invalid-item-rollback", 1, errInvalidItem.Error())
	result.record("measure", "new_order", "abandoned", 1, "deadline")
	result.record("measure", "payment", "commit", 1, "")
	result.finalize()

	if result.Attempted["new_order"] != 3 || result.Committed["new_order"] != 1 ||
		result.ExpectedRollback["new_order"] != 1 || result.Abandoned["new_order"] != 1 {
		t.Fatalf("NewOrder report = attempted:%d committed:%d expected:%d abandoned:%d",
			result.Attempted["new_order"], result.Committed["new_order"],
			result.ExpectedRollback["new_order"], result.Abandoned["new_order"])
	}
	if got, want := result.Completion["new_order"], 2.0/3.0; math.Abs(got-want) > 1e-9 {
		t.Fatalf("NewOrder completion = %v, want %v", got, want)
	}
	if result.NewOrderPerMin != 1 {
		t.Fatalf("NewOrder/min = %v, want committed-only numerator 1", result.NewOrderPerMin)
	}
	if got, want := result.AbortRate, 0.5; math.Abs(got-want) > 1e-9 {
		t.Fatalf("abort rate = %v, want expected rollback plus abandonment / all attempts = %v", got, want)
	}
}

func TestResultValidationDerivesAbortRateFromAllAbortedOutcomes(t *testing.T) {
	result := newResult(60)
	result.record("measure", "new_order", "commit", 1, "")
	result.record("measure", "new_order", "server-abort", 1, "conflict")
	result.record("measure", "new_order", "invalid-item-rollback", 1, errInvalidItem.Error())
	result.record("measure", "new_order", "abandoned", 1, "deadline")
	result.finalize()

	if result.AbortRate != 0.75 {
		t.Fatalf("abort rate = %v, want all three aborted outcomes / 4 attempted", result.AbortRate)
	}
	if err := validateResultWindow(result, 60, "sqlite-reference", 1); err != nil {
		t.Fatal(err)
	}
	result.AbortRate = 0.5
	if err := validateResultWindow(result, 60, "sqlite-reference", 1); err == nil ||
		!strings.Contains(err.Error(), "abort rate mismatch") {
		t.Fatalf("validation error = %v, want abort rate mismatch", err)
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

func TestLiveStatsIncludesExpectedRollbackAndAbandonmentInAbortRate(t *testing.T) {
	stats := &liveStats{}
	stats.record("measure", "new_order", "commit")
	stats.record("measure", "new_order", "invalid-item-rollback")
	stats.record("measure", "new_order", "abandoned")
	stats.record("measure", "payment", "backend-error")
	stats.record("measure", "payment", "server-abort")

	index := phaseIndex("measure")
	if got := stats.attempted[index].Load(); got != 5 {
		t.Fatalf("attempted = %d, want 5", got)
	}
	if got := stats.commits[index].Load(); got != 1 {
		t.Fatalf("commits = %d, want 1", got)
	}
	if got := stats.serverAborts[index].Load(); got != 1 {
		t.Fatalf("server aborts = %d, want 1", got)
	}
	if got := stats.expectedRollbacks[index].Load(); got != 1 {
		t.Fatalf("expected rollbacks = %d, want 1", got)
	}
	if got := stats.abandoned[index].Load(); got != 1 {
		t.Fatalf("abandoned = %d, want 1", got)
	}
	if got := stats.newOrderAttempted[index].Load(); got != 3 {
		t.Fatalf("NewOrder attempted = %d, want 3", got)
	}
	if got := stats.newOrderCommits[index].Load(); got != 1 {
		t.Fatalf("NewOrder commits = %d, want 1", got)
	}
	if got := liveAbortRatePercent(stats.attempted[index].Load(), stats.expectedRollbacks[index].Load(),
		stats.abandoned[index].Load(), stats.serverAborts[index].Load()); got != 60 {
		t.Fatalf("live abort rate = %v%%, want 60%%", got)
	}
}

func TestLiveTPMCUsesCommittedNewOrdersAndElapsedSeconds(t *testing.T) {
	if got := liveTPMC(30, 15); got != 30 {
		t.Fatalf("live tpmC = %v, want 30", got)
	}
	if got := liveTPMC(0, 15); got != 0 {
		t.Fatalf("zero-elapsed live tpmC = %v, want 0", got)
	}
}

func TestPaymentEdgeWriterDoesNotRetainCommittedEvidence(t *testing.T) {
	resultPath := filepath.Join(t.TempDir(), "result.json")
	sink, err := newPaymentEdgeWriter(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	total := newTxnLedgerWithSink(sink)
	attempt := newTxnLedger()
	attempt.addPaymentEdge(paymentFloatEdge{Kind: "warehouse", Warehouse: 1, BeforeBits: 1, AmountBits: 2, AfterBits: 3})
	if err := total.merge(attempt); err != nil {
		t.Fatal(err)
	}
	if len(total.paymentEdges) != 0 {
		t.Fatalf("streaming ledger retained %d payment edges", len(total.paymentEdges))
	}
	if err := sink.closeAndPublish(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(paymentEdgePath(resultPath))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Split(strings.TrimSpace(string(data)), "\n"); len(lines) != 1 {
		t.Fatalf("sidecar has %d lines, want 1", len(lines))
	}
}

func TestPublishResultDocumentOmitsLargeInlinePaymentEvidence(t *testing.T) {
	resultPath := filepath.Join(t.TempDir(), "result.json")
	window := completeResult(1)
	doc := document{
		Config:     config{Rounds: 1, Measure: 1},
		MedianTPMC: window.TPMC,
		Rounds:     []*result{window},
		PaymentEdges: []paymentFloatEdge{{Kind: "warehouse", Warehouse: 1,
			BeforeBits: 1, AmountBits: 2, AfterBits: 3}},
	}
	if err := publishResultDocument(resultPath, doc); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "payment_float_edges") {
		t.Fatal("result JSON still embeds payment FLOAT32 evidence")
	}
	if strings.Contains(string(data), "payment_chain_terminals") {
		t.Fatal("public result JSON contains validation evidence")
	}
	if _, err := os.Stat(paymentEdgePath(resultPath)); err != nil {
		t.Fatalf("payment evidence sidecar missing: %v", err)
	}
}

func TestPaymentChainAccumulatorFoldsOutOfOrderEdges(t *testing.T) {
	start := float32(300000)
	middle := start + 10
	end := middle + 20
	a1 := float32(10)
	a2 := float32(20)
	accumulator := newPaymentChainAccumulator()
	edges := []paymentFloatEdge{
		{Kind: "warehouse", Warehouse: 1, BeforeBits: math.Float32bits(middle), AmountBits: math.Float32bits(a2), AfterBits: math.Float32bits(end)},
		{Kind: "warehouse", Warehouse: 1, BeforeBits: math.Float32bits(start), AmountBits: math.Float32bits(a1), AfterBits: math.Float32bits(middle)},
	}
	if err := accumulator.write(edges); err != nil {
		t.Fatal(err)
	}
	terminals, count, err := accumulator.finalize(2)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 || terminals["warehouse:1"] != math.Float32bits(end) {
		t.Fatalf("online chain summary = count %d terminals %#v", count, terminals)
	}
}

func TestPaymentChainAccumulatorRejectsForkWithoutRetainingRawEdges(t *testing.T) {
	start := float32(300000)
	accumulator := newPaymentChainAccumulator()
	edges := []paymentFloatEdge{
		{Kind: "warehouse", Warehouse: 1, BeforeBits: math.Float32bits(start), AmountBits: math.Float32bits(float32(10)), AfterBits: math.Float32bits(start + 10)},
		{Kind: "warehouse", Warehouse: 1, BeforeBits: math.Float32bits(start), AmountBits: math.Float32bits(float32(20)), AfterBits: math.Float32bits(start + 20)},
	}
	if err := accumulator.write(edges[:1]); err != nil {
		t.Fatal(err)
	}
	if err := accumulator.write(edges[1:]); err == nil || !strings.Contains(err.Error(), "fork") {
		t.Fatalf("fork was not rejected: %v", err)
	}
}

func TestCompactResultPublishesChainSummaryWithoutSidecar(t *testing.T) {
	resultPath := filepath.Join(t.TempDir(), "result.json")
	window := completeResult(1)
	doc := document{
		Config: config{Rounds: 1, Measure: 1}, Rounds: []*result{window}, MedianTPMC: window.TPMC,
		Ledger: map[string]float64{ledgerPaymentCommits: 1}, PaymentEdgeCount: 2,
		PaymentChainTerminals: map[string]uint32{
			"warehouse:1":  math.Float32bits(300010),
			"district:1:1": math.Float32bits(30010),
		},
		PaymentTerminalBits: map[string]uint32{
			"warehouse:1":  math.Float32bits(300010),
			"district:1:1": math.Float32bits(30010),
		},
	}
	if err := publishResultDocument(resultPath, doc); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(paymentEdgePath(resultPath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("compact result unexpectedly created a sidecar: %v", err)
	}
	data, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "payment_chain_terminals") {
		t.Fatal("public result contains Payment chain validation state")
	}
	state, err := os.ReadFile(validationStatePath(resultPath))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(state), "payment_chain_terminals") {
		t.Fatal("validation state omitted chain terminals")
	}
	if strings.Contains(string(state), "payment_terminal_bits") {
		t.Fatal("validation state retained duplicate Payment terminal map")
	}
	if !strings.Contains(string(state), `"online_payment_validated":true`) {
		t.Fatal("validation state omitted online Payment validation marker")
	}
	loaded, err := loadResultDocument(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.PaymentEdgeCount != doc.PaymentEdgeCount ||
		loaded.PaymentChainTerminals["warehouse:1"] != doc.PaymentChainTerminals["warehouse:1"] {
		t.Fatalf("loaded compact validation state = %#v", loaded.PaymentChainTerminals)
	}
}

func TestPublicResultContainsOnlyConfigAndPerformance(t *testing.T) {
	resultPath := filepath.Join(t.TempDir(), "result.json")
	window := completeResult(1)
	doc := document{
		Config: config{
			Mode: "diagnostic", Backend: "rmdb", Warehouses: 50, Workers: 32, Measure: 1, Rounds: 1,
			BaselineCustomerTotal: 1500000,
		},
		Baselines: map[string]float64{"customer.rows": 1500000},
		Ledger: map[string]float64{
			ledgerPaymentCommits: 0,
		},
		MedianTPMC: window.TPMC,
		Rounds:     []*result{window},
	}
	if err := publishResultDocument(resultPath, doc); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	var output map[string]json.RawMessage
	if err := json.Unmarshal(data, &output); err != nil {
		t.Fatal(err)
	}
	if len(output) != 3 || output["config"] == nil || output["median_tpmc"] == nil || output["rounds"] == nil {
		t.Fatalf("public result fields = %#v", output)
	}
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(output["config"], &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg["baseline_customer_total"] != nil {
		t.Fatal("public config contains validation baseline")
	}
	var rounds []map[string]json.RawMessage
	if err := json.Unmarshal(output["rounds"], &rounds); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"counts", "errors", "coverage", "NewOrder/min"} {
		if rounds[0][field] != nil {
			t.Fatalf("public round contains diagnostic field %q", field)
		}
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

func writeLegacyResultDocument(t *testing.T, path string, doc document) {
	t.Helper()
	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	var configFields map[string]json.RawMessage
	if err := json.Unmarshal(fields["config"], &configFields); err != nil {
		t.Fatal(err)
	}
	delete(configFields, "max_conflict_retries")
	fields["config"], err = json.Marshal(configFields)
	if err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
}

func completeResult(measure int) *result {
	result := newResult(measure)
	for _, txnType := range []string{"new_order", "payment", "order_status", "delivery", "stock_level"} {
		result.record("measure", txnType, "commit", 1, "")
	}
	result.finalize()
	return result
}

func TestConfigJSONIncludesMaxConflictRetries(t *testing.T) {
	encoded, err := json.Marshal(config{MaxConflictRetries: defaultMaxConflictRetries})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"max_conflict_retries":1`) {
		t.Fatalf("config JSON = %s, want max_conflict_retries=1", encoded)
	}
}

func TestConfigJSONPreservesUnlimitedRetryForLegacyResults(t *testing.T) {
	var legacy config
	if err := json.Unmarshal([]byte(`{"rounds":1}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.MaxConflictRetries != unlimitedConflictRetries {
		t.Fatalf("legacy max conflict retries = %d, want %d",
			legacy.MaxConflictRetries, unlimitedConflictRetries)
	}

	var explicitZero config
	if err := json.Unmarshal([]byte(`{"max_conflict_retries":0}`), &explicitZero); err != nil {
		t.Fatal(err)
	}
	if explicitZero.MaxConflictRetries != 0 {
		t.Fatalf("explicit max conflict retries = %d, want 0", explicitZero.MaxConflictRetries)
	}
}

func TestMergeResultFilesTreatsLegacyRetryModeAsUnlimited(t *testing.T) {
	dir := t.TempDir()
	legacyPath := filepath.Join(dir, "legacy.json")
	unlimitedPath := filepath.Join(dir, "unlimited.json")
	outputPath := filepath.Join(dir, "merged.json")
	baseConfig := config{Backend: "rmdb", Isolation: "snapshot-isolation", Warehouses: 1, Workers: 4,
		Warmup: 10, Measure: 60, Rounds: 1, Seed: 11, MaxConflictRetries: unlimitedConflictRetries,
		WarehousePolicy: "terminal-home"}
	doc := document{Config: baseConfig, Rounds: []*result{completeResult(60)}}
	writeLegacyResultDocument(t, legacyPath, doc)
	writeResultDocument(t, unlimitedPath, doc)

	if err := mergeResultFiles(outputPath, legacyPath+","+unlimitedPath); err != nil {
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
	if merged.Config.MaxConflictRetries != unlimitedConflictRetries {
		t.Fatalf("merged max conflict retries = %d, want %d",
			merged.Config.MaxConflictRetries, unlimitedConflictRetries)
	}
}

func TestMergeResultFilesRejectsLegacyAndExplicitZeroRetryModes(t *testing.T) {
	dir := t.TempDir()
	legacyPath := filepath.Join(dir, "legacy.json")
	zeroPath := filepath.Join(dir, "zero.json")
	outputPath := filepath.Join(dir, "merged.json")
	baseConfig := config{Backend: "rmdb", Isolation: "snapshot-isolation", Warehouses: 1, Workers: 4,
		Warmup: 10, Measure: 60, Rounds: 1, Seed: 11, MaxConflictRetries: unlimitedConflictRetries,
		WarehousePolicy: "terminal-home"}
	writeLegacyResultDocument(t, legacyPath,
		document{Config: baseConfig, Rounds: []*result{completeResult(60)}})
	baseConfig.MaxConflictRetries = 0
	writeResultDocument(t, zeroPath,
		document{Config: baseConfig, Rounds: []*result{completeResult(60)}})

	err := mergeResultFiles(outputPath, legacyPath+","+zeroPath)
	if err == nil || !strings.Contains(err.Error(), "materially different") {
		t.Fatalf("merge error = %v, want retry-mode config mismatch", err)
	}
}

func TestResultAndMergeRejectInvalidMaxConflictRetries(t *testing.T) {
	badConfig := config{Measure: 60, Rounds: 1, MaxConflictRetries: -2}
	doc := document{Config: badConfig, Rounds: []*result{completeResult(60)}}
	if err := validateResultDocument(doc); err == nil || !strings.Contains(err.Error(), "max_conflict_retries") {
		t.Fatalf("result validation error = %v, want invalid retry limit", err)
	}

	dir := t.TempDir()
	inputPath := filepath.Join(dir, "invalid.json")
	outputPath := filepath.Join(dir, "merged.json")
	writeResultDocument(t, inputPath, doc)
	if err := mergeResultFiles(outputPath, inputPath); err == nil ||
		!strings.Contains(err.Error(), "max_conflict_retries") {
		t.Fatalf("merge error = %v, want invalid retry limit", err)
	}
}

func TestMergeResultFilesRejectsDifferentSeed(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "round-1.json")
	secondPath := filepath.Join(dir, "round-2.json")
	outputPath := filepath.Join(dir, "merged.json")
	baseConfig := config{Backend: "rmdb", Isolation: "read-committed", Warehouses: 1, Workers: 4, Warmup: 10, Measure: 60, Rounds: 1, Seed: 11, WarehousePolicy: "terminal-home"}
	writeResultDocument(t, firstPath, document{Config: baseConfig, Rounds: []*result{completeResult(60)}})
	secondConfig := baseConfig
	secondConfig.Seed = 12
	writeResultDocument(t, secondPath, document{Config: secondConfig, Rounds: []*result{completeResult(60)}})

	err := mergeResultFiles(outputPath, firstPath+","+secondPath)
	if err == nil || !strings.Contains(err.Error(), "materially different") {
		t.Fatalf("merge error = %v, want config mismatch", err)
	}
}

func TestMergeResultFilesRejectsIncompleteRound(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "round.json")
	outputPath := filepath.Join(dir, "merged.json")
	writeResultDocument(t, inputPath, document{Config: config{Rounds: 1, Measure: 60}, Rounds: []*result{{TPMC: 100}}})
	if err := mergeResultFiles(outputPath, inputPath); err == nil {
		t.Fatal("incomplete round was accepted")
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("merged output exists after rejection: %v", err)
	}
}

func TestMergeResultFilesAllowsRoundAndProgressDifferences(t *testing.T) {

	dir := t.TempDir()
	firstPath := filepath.Join(dir, "round-1.json")
	secondPath := filepath.Join(dir, "round-2.json")
	outputPath := filepath.Join(dir, "merged.json")
	baseConfig := config{Backend: "rmdb", Isolation: "read-committed", Warehouses: 1, Workers: 4, Warmup: 10, Measure: 60, Rounds: 1, ProgressInterval: 1, Seed: 11, WarehousePolicy: "terminal-home"}
	writeResultDocument(t, firstPath, document{Config: baseConfig, Rounds: []*result{completeResult(60)}})
	secondConfig := baseConfig
	secondConfig.Rounds = 99
	secondConfig.ProgressInterval = 10
	writeResultDocument(t, secondPath, document{Config: secondConfig, Rounds: []*result{completeResult(60)}})

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
	if merged.Config.Rounds != 2 || len(merged.Rounds) != 2 || merged.MedianTPMC != 1 {
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
	return runRound(1, 1, 1,
		profile{warehouses: 1, districtsPerWarehouse: 1, customersPerDistrict: 1, itemCount: 1},
		"terminal-home", time.Now(), end, 1, 0, false, defaultMaxConflictRetries, &liveStats{}, factory,
		newTxnLedger())
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

type lifecycleBackend struct {
	closeCount int
	abortOnce  bool
}

func (b *lifecycleBackend) exec(string) (string, error) { return "2\n", nil }
func (b *lifecycleBackend) begin() error {
	if b.abortOnce {
		b.abortOnce = false
		return errAbort
	}
	return nil
}
func (b *lifecycleBackend) commit() error { return nil }
func (b *lifecycleBackend) rollback()     {}
func (b *lifecycleBackend) close()        { b.closeCount++ }

type alwaysAbortBackend struct {
	attempts   int
	closeCount int
	onAttempt  func()
}

func (b *alwaysAbortBackend) exec(string) (string, error) { return "", nil }
func (b *alwaysAbortBackend) begin() error {
	b.attempts++
	if b.onAttempt != nil {
		b.onAttempt()
	}
	return errAbort
}
func (b *alwaysAbortBackend) commit() error { return nil }
func (b *alwaysAbortBackend) rollback()     {}
func (b *alwaysAbortBackend) close()        { b.closeCount++ }

func resultTransactionCount(result *result, phase string) int {
	total := 0
	for _, outcomes := range result.Counts[phase] {
		for _, count := range outcomes {
			total += count
		}
	}
	return total
}

func TestRunWorkerZeroConflictRetriesRecordsOneAbandonment(t *testing.T) {
	stop := make(chan struct{})
	backend := &alwaysAbortBackend{onAttempt: func() { close(stop) }}
	output := make(chan workerReport, 1)
	now := time.Now().Add(-time.Second)
	measureEnd := time.Now().Add(time.Second)
	runWorker(0, 1, 1,
		profile{warehouses: 1, districtsPerWarehouse: 1, customersPerDistrict: 1, itemCount: 1},
		"terminal-home", now, measureEnd, 1, 0, false, 0, &liveStats{}, stop, output,
		func() (txnBackend, error) { return backend, nil })
	report := <-output
	if report.err != nil {
		t.Fatal(report.err)
	}
	serverAborts, abandoned := 0, 0
	for _, outcomes := range report.result.Counts["measure"] {
		serverAborts += outcomes["server-abort"]
		abandoned += outcomes["abandoned"]
	}
	if backend.attempts != 1 || serverAborts != 0 || abandoned != 1 {
		t.Fatalf("attempts/server-aborts/abandoned = %d/%d/%d, want 1/0/1",
			backend.attempts, serverAborts, abandoned)
	}
}

func TestOfficialWorkerZeroConflictRetriesRecordsOneAbandonment(t *testing.T) {
	stop := make(chan struct{})
	backend := &alwaysAbortBackend{onAttempt: func() { close(stop) }}
	stats := []*liveStats{{}, {}}
	output := make(chan officialWorkerReport, 1)
	warmupEnd := time.Now().Add(-100 * time.Millisecond)
	p := profile{warehouses: 50, districtsPerWarehouse: 1, customersPerDistrict: 1, itemCount: 25}
	plan, err := newOfficialRoutingPlan(1, p, 2)
	if err != nil {
		t.Fatal(err)
	}
	runOfficialWorker(0, 1, 1, p, plan, warmupEnd, time.Second, 1, 0, 0, stats, stop, output, backend)
	report := <-output
	if report.err != nil {
		t.Fatal(report.err)
	}
	serverAborts, abandoned := 0, 0
	for _, outcomes := range report.windows[0].Counts["measure"] {
		serverAborts += outcomes["server-abort"]
		abandoned += outcomes["abandoned"]
	}
	if backend.attempts != 1 || serverAborts != 0 || abandoned != 1 {
		t.Fatalf("attempts/server-aborts/abandoned = %d/%d/%d, want 1/0/1",
			backend.attempts, serverAborts, abandoned)
	}
}

func TestOfficialWorkerRetainsBackendAcrossAllPhases(t *testing.T) {
	backend := &lifecycleBackend{abortOnce: true}
	stats := []*liveStats{{}, {}, {}}
	output := make(chan officialWorkerReport, 1)
	warmupEnd := time.Now().Add(20 * time.Millisecond)
	p := profile{warehouses: 50, districtsPerWarehouse: 1, customersPerDistrict: 1, itemCount: 25}
	plan, err := newOfficialRoutingPlan(1, p, 3)
	if err != nil {
		t.Fatal(err)
	}
	go runOfficialWorker(0, 1, 1,
		p, plan, warmupEnd, 20*time.Millisecond, 2, 0, defaultMaxConflictRetries, stats,
		make(chan struct{}), output, backend)
	report := <-output
	if report.err != nil {
		t.Fatal(report.err)
	}
	if backend.closeCount != 1 {
		t.Fatalf("backend close count = %d, want 1", backend.closeCount)
	}
	if got := resultTransactionCount(report.warmup, "warmup"); got == 0 {
		t.Fatal("warmup received no transactions")
	}
	for window, result := range report.windows {
		if got := resultTransactionCount(result, "measure"); got == 0 {
			t.Fatalf("measurement window %d received no transactions", window+1)
		}
	}
}

func TestOfficialWorkerCountsRetriedTransactionOnceWhenDeadlineAbandonsIt(t *testing.T) {
	backend := &alwaysAbortBackend{}
	stats := []*liveStats{{}, {}}
	output := make(chan officialWorkerReport, 1)
	warmupEnd := time.Now()
	p := profile{warehouses: 50, districtsPerWarehouse: 1, customersPerDistrict: 1, itemCount: 25}
	plan, err := newOfficialRoutingPlan(1, p, 2)
	if err != nil {
		t.Fatal(err)
	}
	go runOfficialWorker(0, 1, 1,
		p, plan, warmupEnd, 20*time.Millisecond, 1, 0, defaultMaxConflictRetries, stats,
		make(chan struct{}), output, backend)
	report := <-output
	if report.err != nil {
		t.Fatal(report.err)
	}
	window := report.windows[0]
	window.finalize()
	if backend.attempts < 2 {
		t.Fatalf("backend attempts = %d, want retries before deadline", backend.attempts)
	}
	attempted, abandoned := 0, 0
	for txnType, count := range window.Attempted {
		attempted += count
		abandoned += window.Abandoned[txnType]
	}
	if attempted != 1 || abandoned != 1 || window.Coverage.Completed != 0 {
		t.Fatalf("report = attempted:%d abandoned:%d coverage:%d, want 1/1/0",
			attempted, abandoned, window.Coverage.Completed)
	}
}

func TestOfficialWindowsRejectsReconnectEachTxnBeforeConnecting(t *testing.T) {
	factoryCalls := 0
	_, err := runOfficialWindows(1, 1, 1,
		profile{warehouses: 25, districtsPerWarehouse: 1, customersPerDistrict: 1, itemCount: 1},
		0, 1, 0, 0, defaultMaxConflictRetries, true, 0, func() (txnBackend, error) {
			factoryCalls++
			return &lifecycleBackend{}, nil
		}, newTxnLedger(), "")
	if err == nil || !strings.Contains(err.Error(), "does not allow reconnect-each-txn") {
		t.Fatalf("runOfficialWindows() error = %v, want reconnect rejection", err)
	}
	if factoryCalls != 0 {
		t.Fatalf("factory calls = %d, want 0", factoryCalls)
	}
}

func TestMergeResultFilesRejectsBackendErrorRound(t *testing.T) {
	dir := t.TempDir()
	inputPath := filepath.Join(dir, "failed-round.json")
	outputPath := filepath.Join(dir, "merged.json")
	failed := newResult(60)
	failed.record("measure", "payment", "backend-error", 1, "connection reset")
	writeResultDocument(t, inputPath, document{Config: config{Mode: "official-equivalent", Rounds: 1, Measure: 60}, Rounds: []*result{failed}})

	err := mergeResultFiles(outputPath, inputPath)
	if err == nil || !strings.Contains(err.Error(), "backend error") {
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

func TestStockLevelCountQueryUsesServerDistinct(t *testing.T) {
	query := stockLevelCountQuery(2, 3, 100, 17, false)
	if !strings.Contains(strings.ToLower(query), "count(distinct ol_i_id)") {
		t.Fatalf("query = %q, want SQL COUNT(DISTINCT)", query)
	}
	if strings.Contains(strings.ToLower(query), "select ol_i_id") {
		t.Fatalf("query = %q, unexpectedly selects rows for client-side deduplication", query)
	}
	parenthesized := stockLevelCountQuery(2, 3, 100, 17, true)
	if !strings.Contains(strings.ToLower(parenthesized), "count(distinct (ol_i_id))") {
		t.Fatalf("parenthesized query = %q, want COUNT(DISTINCT (col))", parenthesized)
	}
}

type preparedRollbackProbe struct {
	*fakeRankingBatcher
	statements []string
	leaveDirty bool
	dirty      bool
}

func newPreparedRollbackProbe(t *testing.T, leaveDirty bool) *preparedRollbackProbe {
	t.Helper()
	return &preparedRollbackProbe{fakeRankingBatcher: newFakeRankingBatcher(t), leaveDirty: leaveDirty}
}

func (p *preparedRollbackProbe) batchOperation(sql string) (batchOperation, error) {
	p.statements = append(p.statements, sql)
	return p.fakeRankingBatcher.batchOperation(sql)
}

func (p *preparedRollbackProbe) execBatch(operations []batchOperation) (batchResult, error) {
	result, err := p.fakeRankingBatcher.execBatch(operations)
	if err == nil {
		for _, operation := range operations {
			if operation.sql == "abort;" {
				p.dirty = p.leaveDirty
			}
		}
	}
	return result, err
}

func (p *preparedRollbackProbe) exec(sql string) (string, error) {
	switch {
	case strings.HasPrefix(sql, "select d_next_o_id"):
		if p.dirty {
			return "3002", nil
		}
		return "3001", nil
	case strings.HasPrefix(sql, "select count(*)"):
		if p.dirty {
			return "1", nil
		}
		return "0", nil
	case strings.HasPrefix(sql, "select s_quantity, s_ytd"):
		return "15|0.0|0|0", nil
	default:
		return "", fmt.Errorf("unexpected prepared rollback probe SQL: %s", sql)
	}
}

func (p *preparedRollbackProbe) begin() error  { return nil }
func (p *preparedRollbackProbe) commit() error { return nil }
func (p *preparedRollbackProbe) rollback()     {}
func (p *preparedRollbackProbe) close()        {}

func TestVerifyPreparedInvalidItemRollbackUsesPreparedWritesAndDetectsResidue(t *testing.T) {
	ctx := rankingTestContext()
	probe := newPreparedRollbackProbe(t, false)
	if err := verifyPreparedInvalidItemRollback(probe, ctx.profile); err != nil {
		t.Fatal(err)
	}
	validWrites, abortIndex := 0, -1
	for i, statement := range probe.statements {
		if strings.HasPrefix(statement, "insert into order_line") {
			validWrites++
		}
		if statement == "abort;" {
			abortIndex = i
		}
	}
	if validWrites == 0 || abortIndex < 0 {
		t.Fatalf("prepared probe emitted %d valid order_line writes, abort index %d", validWrites, abortIndex)
	}

	dirty := newPreparedRollbackProbe(t, true)
	err := verifyPreparedInvalidItemRollback(dirty, ctx.profile)
	if err == nil || !strings.Contains(err.Error(), "changed snapshot") {
		t.Fatalf("dirty rollback error = %v, want changed snapshot", err)
	}
}

func TestVerifyBenchmarkFeaturesUsesFormalDistinctAndRollback(t *testing.T) {
	backend, err := newSQLiteBackend(filepath.Join(t.TempDir(), "tpcc.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.close()
	if err := backend.execFile(tpccSchemaPath("sqlite_schema.sql")); err != nil {
		t.Fatal(err)
	}
	statements := []string{
		"insert into warehouse values (1, 'w', 's1', 's2', 'c', 'ST', '000000001', 0.1, 100.0);",
		"insert into district values (1, 1, 'd', 's1', 's2', 'c', 'ST', '000000001', 0.1, 100.0, 10);",
		"insert into order_line values (1, 1, 1, 1, 7, 1, '2026-01-01 00:00:00', 1, 1.0, 'dist');",
		"insert into order_line values (2, 1, 1, 1, 7, 1, '2026-01-01 00:00:00', 1, 1.0, 'dist');",
		"insert into stock values (7, 1, 5, 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 'd', 0.0, 0, 0, 'data');",
	}
	for _, statement := range statements {
		if _, err := backend.exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := verifyBenchmarkFeatures(backend, profile{warehouses: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestResultDocumentRejectsIncompleteMeasurement(t *testing.T) {
	config := config{Rounds: 1, Measure: 10}
	doc := document{Config: config, Rounds: []*result{newResult(10)}}
	if err := validateResultDocument(doc); err == nil || !strings.Contains(err.Error(), "no committed") {
		t.Fatalf("validation error = %v, want missing committed transaction", err)
	}
}

func TestResultDocumentRejectsWrongRoundShape(t *testing.T) {
	doc := document{Config: config{Rounds: 2, Measure: 10}, Rounds: []*result{newResult(10)}}
	if err := validateResultDocument(doc); err == nil || !strings.Contains(err.Error(), "rounds") {
		t.Fatalf("validation error = %v, want round count error", err)
	}
}

// scriptedExecutor answers each query with a canned scalar and records the SQL
// it was asked to run.
type scriptedExecutor struct {
	answers    map[string]int64
	statements []string
}

func (e *scriptedExecutor) exec(sql string) (string, error) {
	e.statements = append(e.statements, sql)
	value, ok := e.answers[sql]
	if !ok {
		return "", fmt.Errorf("unexpected SQL %q", sql)
	}
	return fmt.Sprintf("+---+\n| n |\n+---+\n| %d |\n+---+\nTotal record(s): 1\n", value), nil
}

func integrityManifest() datasetManifest {
	return datasetManifest{Warehouses: 1, Files: map[string]fileRecord{
		"order_line": {Rows: 300000, Size: 1},
		"new_orders": {Rows: districtsPerWarehouse * initialNewOrdersPerDistrict, Size: 1},
	}, Aggregates: map[string]float64{
		aggOrdersOlCntSum:         300000,
		aggOrdersCarrierZeroRows:  districtsPerWarehouse * initialNewOrdersPerDistrict,
		aggOrderLineDeliveryNulls: float64(expectedUndeliveredOrderLines(1)),
	}}
}

func passingIntegrityAnswers(manifest datasetManifest) map[string]int64 {
	return map[string]int64{
		"select sum(o_ol_cnt) from orders;":                         manifest.Files["order_line"].Rows,
		"select count(*) from stock where s_quantity < 10;":         0,
		"select count(*) from stock where s_quantity > 100;":        0,
		"select count(*) from orders where o_ol_cnt < 5;":           0,
		"select count(*) from orders where o_ol_cnt > 15;":          0,
		"select count(*) from orders where o_carrier_id = 0;":       manifest.Files["new_orders"].Rows,
		"select count(o_id) from orders where o_carrier_id = 0;":    int64(manifest.Aggregates[aggOrdersCarrierZeroRows]),
		"select count(*) from order_line where ol_delivery_d = '';": int64(manifest.Aggregates[aggOrderLineDeliveryNulls]),
		"select count(*) from stock where s_ytd <> 0.0;":            0,
		"select count(*) from stock where s_order_cnt <> 0;":        0,
		"select count(*) from stock where s_remote_cnt <> 0;":       0,
	}
}

func TestVerifyLoadIntegrityChecksThePublishedSemantics(t *testing.T) {
	// final.md:285-292 lists the publicly defined post-load invariants; each must
	// be issued as its own SQL statement and compared against the exact generated
	// numbers.
	manifest := integrityManifest()
	answers := passingIntegrityAnswers(manifest)
	executor := &scriptedExecutor{answers: answers}
	if err := verifyLoadIntegrity(executor, manifest); err != nil {
		t.Fatal(err)
	}
	if len(executor.statements) != len(answers) {
		t.Fatalf("integrity validation issued %d statements, want %d", len(executor.statements), len(answers))
	}
	for _, sql := range executor.statements {
		if _, ok := answers[sql]; !ok {
			t.Errorf("unexpected integrity statement %q", sql)
		}
	}
}

func TestVerifyLoadIntegrityRejectsEveryViolation(t *testing.T) {
	manifest := integrityManifest()
	violations := map[string]int64{
		"select sum(o_ol_cnt) from orders;":                         manifest.Files["order_line"].Rows - 1,
		"select count(*) from stock where s_quantity < 10;":         1,
		"select count(*) from stock where s_quantity > 100;":        1,
		"select count(*) from orders where o_ol_cnt < 5;":           1,
		"select count(*) from orders where o_ol_cnt > 15;":          1,
		"select count(*) from orders where o_carrier_id = 0;":       manifest.Files["new_orders"].Rows + 1,
		"select count(o_id) from orders where o_carrier_id = 0;":    int64(manifest.Aggregates[aggOrdersCarrierZeroRows]) - 1,
		"select count(*) from order_line where ol_delivery_d = '';": 0,
		"select count(*) from stock where s_ytd <> 0.0;":            1,
		"select count(*) from stock where s_order_cnt <> 0;":        1,
		"select count(*) from stock where s_remote_cnt <> 0;":       1,
	}
	for sql, wrong := range violations {
		answers := passingIntegrityAnswers(manifest)
		answers[sql] = wrong
		err := verifyLoadIntegrity(&scriptedExecutor{answers: answers}, manifest)
		if err == nil || !strings.Contains(err.Error(), "LOAD integrity mismatch") {
			t.Errorf("violating %q produced error %v", sql, err)
		}
	}
}

func TestExpectedUndeliveredOrderLinesMatchesGeneratedCSV(t *testing.T) {
	dir := t.TempDir()
	if err := generateData(1, dir, 7, true); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(filepath.Join(dir, "order_line.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	deliveryColumn, amountColumn := -1, -1
	for index, name := range header {
		if name == "ol_delivery_d" {
			deliveryColumn = index
		}
		if name == "ol_amount" {
			amountColumn = index
		}
	}
	if deliveryColumn < 0 || amountColumn < 0 {
		t.Fatal("order_line.csv is missing delivery or amount column")
	}
	empty := int64(0)
	for {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if row[deliveryColumn] == "" {
			empty++
			value, err := strconv.ParseFloat(row[amountColumn], 32)
			if err != nil || value < float64(float32(0.01)) || value > float64(float32(9999.99)) {
				t.Fatalf("invalid initial undelivered FLOAT32 amount %q", row[amountColumn])
			}
			if got := float32SQL(float32(value)); got != row[amountColumn] {
				t.Fatalf("amount %q does not round-trip through one binary32 conversion (canonical %q)",
					row[amountColumn], got)
			}
		}
	}
	if want := expectedUndeliveredOrderLines(1, 7); empty != want {
		t.Fatalf("generated %d order_line rows without a delivery time, expected %d", empty, want)
	}
}

func TestDatasetManifestValidation(t *testing.T) {
	dir := t.TempDir()
	if err := writeDatasetManifest(dir, 3, 17); err == nil {
		t.Fatal("empty dataset accepted")
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
