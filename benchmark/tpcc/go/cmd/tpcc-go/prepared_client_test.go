package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"strings"
	"testing"
	"time"
)

func appendU16(value []byte, number uint16) []byte {
	var bytes [2]byte
	binary.BigEndian.PutUint16(bytes[:], number)
	return append(value, bytes[:]...)
}

func appendU32(value []byte, number uint32) []byte {
	var bytes [4]byte
	binary.BigEndian.PutUint32(bytes[:], number)
	return append(value, bytes[:]...)
}

func TestParameterizeSQLPreservesTypedValues(t *testing.T) {
	template, args, err := parameterizeSQL("insert into t values (7, 1.50, 'a''b');")
	if err != nil {
		t.Fatal(err)
	}
	if template != "insert into t values ($1, $2, $3);" {
		t.Fatalf("template = %q", template)
	}
	if len(args) != 3 || args[0].typ != wireTypeInt32 || args[1].typ != wireTypeFloat32 || args[2].typ != wireTypeChar {
		t.Fatalf("argument types = %#v", args)
	}
	if args[0].int32 != 7 || args[1].floatBits != math.Float32bits(float32(1.5)) || args[2].text != "a'b" {
		t.Fatalf("argument values = %#v", args)
	}
}

func TestParameterizeSQLDoesNotRewriteDigitsInIdentifiers(t *testing.T) {
	template, args, err := parameterizeSQL("select s_dist_01, ol_dist_01 from stock where s_i_id = 12;")
	if err != nil {
		t.Fatal(err)
	}
	if template != "select s_dist_01, ol_dist_01 from stock where s_i_id = $1;" {
		t.Fatalf("template = %q", template)
	}
	if len(args) != 1 || args[0].typ != wireTypeInt32 || args[0].int32 != 12 {
		t.Fatalf("arguments = %#v", args)
	}
}

func TestRankingTemplatesDeclareSchemasAndFitProtocolBounds(t *testing.T) {
	pending, err := pendingRankingStatements()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) < 30 {
		t.Fatalf("prepared ranking template set has only %d statements", len(pending))
	}
	for _, statement := range pending {
		if len(statement.template) > maxWirePayload || len(statement.args) > 256 {
			t.Fatalf("template exceeds protocol bounds: %q", statement.template)
		}
		if statement.query != (len(statement.wantColumns) > 0) {
			t.Fatalf("template %q declares %d expected columns", statement.template, len(statement.wantColumns))
		}
		for index, sqlType := range statement.wantColumns {
			if sqlType != wireTypeInt32 && sqlType != wireTypeFloat32 && sqlType != wireTypeChar {
				t.Fatalf("template %q column %d declares unknown SQL type 0x%02x", statement.template, index+1, sqlType)
			}
		}
	}
}

func TestPaymentCreditLimitPreparedSchemaIsInt32(t *testing.T) {
	matches := 0
	for _, template := range rankingTemplates() {
		if !strings.Contains(template.sql, "c_credit_lim") {
			continue
		}
		matches++
		if len(template.columns) != 14 || template.columns[10] != wireTypeInt32 {
			t.Fatalf("Payment customer schema = %#v, want c_credit_lim INT32 at column 11", template.columns)
		}
	}
	if matches != 1 {
		t.Fatalf("found %d ranking templates projecting c_credit_lim, want 1", matches)
	}
}

// prepareOKBody encodes a PREPARE_OK response whose per-statement column types
// are supplied by the caller, so the decoder can be driven with both correct and
// deliberately wrong schemas.
func prepareOKBody(t *testing.T, pending []pendingStatement, columns [][]byte, legacy bool) []byte {
	t.Helper()
	body := appendU16(nil, uint16(len(pending)))
	for index, statement := range pending {
		body = appendU16(body, statement.id)
		if legacy {
			body = appendU16(body, uint16(len(statement.args)))
			for _, arg := range statement.args {
				body = append(body, arg.typ)
			}
		}
		body = appendU16(body, uint16(len(columns[index])))
		for position, sqlType := range columns[index] {
			name := fmt.Sprintf("c%d", position+1)
			body = appendU16(body, uint16(len(name)))
			body = append(body, name...)
			body = append(body, sqlType)
		}
	}
	return body
}

func prepareOKFixture() []pendingStatement {
	return []pendingStatement{
		{id: 1, query: false, template: "commit;"},
		{id: 2, query: true, template: "select a, b from t where c = $1;",
			args: []preparedArgument{{typ: wireTypeInt32}}, wantColumns: []byte{wireTypeInt32, wireTypeFloat32}},
	}
}

func TestDecodePrepareOKVerifiesEveryColumnType(t *testing.T) {
	pending := prepareOKFixture()
	decoded, err := decodePrepareOK(prepareOKBody(t, pending, [][]byte{nil, {wireTypeInt32, wireTypeFloat32}}, false), pending)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded[1].columns) != 0 || len(decoded[2].columns) != 2 ||
		decoded[2].columns[0].sqlType != wireTypeInt32 || decoded[2].columns[1].sqlType != wireTypeFloat32 {
		t.Fatalf("decoded schema = %#v", decoded)
	}

	// final.md:680 rejects a placeholder column, a missing projection column, a
	// numeric column disguised as CHAR, and a command that reports columns.
	rejected := map[string][][]byte{
		"single placeholder column": {nil, {wireTypeInt32}},
		"numeric column as CHAR":    {nil, {wireTypeInt32, wireTypeChar}},
		"swapped column order":      {nil, {wireTypeFloat32, wireTypeInt32}},
		"extra column":              {nil, {wireTypeInt32, wireTypeFloat32, wireTypeInt32}},
		"command reports a column":  {{wireTypeInt32}, {wireTypeInt32, wireTypeFloat32}},
	}
	for name, columns := range rejected {
		if _, err := decodePrepareOK(prepareOKBody(t, pending, columns, false), pending); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestDecodePrepareOKRejectsLegacyLayoutUnlessAllowed(t *testing.T) {
	pending := prepareOKFixture()
	body := prepareOKBody(t, pending, [][]byte{nil, {wireTypeInt32, wireTypeFloat32}}, true)
	if _, err := decodePrepareOK(body, pending); err == nil || !strings.Contains(err.Error(), "legacy layout") {
		t.Fatalf("legacy PREPARE_OK error = %v, want an explicit legacy layout rejection", err)
	}
	allowLegacyPrepare = true
	defer func() { allowLegacyPrepare = false }()
	decoded, err := decodePrepareOK(body, pending)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded[2].columns) != 2 {
		t.Fatalf("decoded legacy schema = %#v", decoded[2])
	}
}

func TestDecodeBatchCellSupportsNullAndFloat32(t *testing.T) {
	reader := &batchReader{body: []byte{0, 1, 0x3f, 0x80, 0, 0}}
	null, err := decodeBatchCell(reader, wireTypeInt32)
	if err != nil || null != "NULL" {
		t.Fatalf("NULL = %q, err = %v", null, err)
	}
	value, err := decodeBatchCell(reader, wireTypeFloat32)
	if err != nil || !strings.HasPrefix(value, "1") {
		t.Fatalf("FLOAT32 = %q, err = %v", value, err)
	}
}

func TestEncodeBatchOperationsPreservesOrderAndTypedArguments(t *testing.T) {
	operations := []batchOperation{
		{
			statement: rankingStatement{id: 7, parameterTypes: []byte{wireTypeInt32, wireTypeChar}},
			args: []preparedArgument{
				{typ: wireTypeInt32, present: true, int32: -2},
				{typ: wireTypeChar, present: true, text: "xy"},
			},
		},
		{
			statement: rankingStatement{id: 9, query: true, parameterTypes: []byte{wireTypeFloat32}},
			args:      []preparedArgument{{typ: wireTypeFloat32}},
		},
	}
	payload, err := encodeBatchOperations(operations)
	if err != nil {
		t.Fatal(err)
	}
	want := appendU16(nil, 2)
	want = appendU16(want, 7)
	want = append(want, 1)
	want = appendU32(want, ^uint32(1))
	want = append(want, 1)
	want = appendU32(want, 2)
	want = append(want, "xy"...)
	want = appendU16(want, 9)
	want = append(want, 0)
	if string(payload) != string(want) {
		t.Fatalf("payload = %x, want %x", payload, want)
	}
}

func TestDecodeBatchResultReturnsOrderedQueryResults(t *testing.T) {
	operations := []batchOperation{
		{statement: rankingStatement{id: 1}},
		{statement: rankingStatement{id: 2, query: true, columns: []wireColumn{{name: "n", sqlType: wireTypeInt32}}}},
		{statement: rankingStatement{id: 3, query: true, columns: []wireColumn{{name: "s", sqlType: wireTypeChar}}}},
	}
	body := appendU16(nil, 3)
	body = append(body, 0)
	body = appendU16(body, 0xffff)
	body = appendU32(body, 0)
	body = appendU16(body, 2)
	body = appendU16(body, 1)
	body = appendU32(body, 1)
	body = append(body, 1)
	body = appendU32(body, 42)
	body = appendU16(body, 2)
	body = appendU32(body, 1)
	body = append(body, 1)
	body = appendU32(body, 2)
	body = append(body, "ok"...)

	result, err := decodeBatchResult(body, operations)
	if err != nil {
		t.Fatal(err)
	}
	if result.executedOperations != 3 || result.status != 0 || result.failedOperation != 0xffff || result.diagnostic != "" {
		t.Fatalf("batch result header = %#v", result)
	}
	if len(result.results) != 2 || result.results[0].operationIndex != 1 || result.results[0].rows[0][0] != "42" || result.results[1].operationIndex != 2 || result.results[1].rows[0][0] != "ok" {
		t.Fatalf("batch operation results = %#v", result.results)
	}
}

func TestDecodeBatchResultPreservesFailureProgress(t *testing.T) {
	operations := []batchOperation{{statement: rankingStatement{id: 1}}, {statement: rankingStatement{id: 2}}, {statement: rankingStatement{id: 3}}}
	body := appendU16(nil, 2)
	body = append(body, 2)
	body = appendU16(body, 2)
	body = appendU32(body, 4)
	body = append(body, "boom"...)
	body = appendU16(body, 0)

	result, err := decodeBatchResult(body, operations)
	if err != nil {
		t.Fatal(err)
	}
	if result.executedOperations != 2 || result.status != 2 || result.failedOperation != 2 || result.diagnostic != "boom" || len(result.results) != 0 {
		t.Fatalf("batch failure = %#v", result)
	}
}

// fakeRankingBatcher mirrors the connection-level prepared dictionary: it only
// accepts statements whose parameterized template appears in rankingTemplates(),
// so a ranking transaction that emits an unprepared statement fails here exactly
// as it would against the server.
type fakeRankingBatcher struct {
	statements   map[string]rankingStatement
	batches      int
	emptyResults bool
	warehouseYTD float32
	districtYTD  float32
	balance      float32
	customerYTD  float32
	stockYTD     map[string]float32
}

func testRankingStatementDictionary(t *testing.T) map[string]rankingStatement {
	t.Helper()
	pending, err := pendingRankingStatements()
	if err != nil {
		t.Fatal(err)
	}
	statements := make(map[string]rankingStatement, len(pending))
	for _, statement := range pending {
		columns := make([]wireColumn, 0, len(statement.wantColumns))
		for index, sqlType := range statement.wantColumns {
			columns = append(columns, wireColumn{name: fmt.Sprintf("c%d", index+1), sqlType: sqlType})
		}
		statements[statement.template] = rankingStatement{id: statement.id, query: statement.query,
			parameterTypes: argumentTypes(statement.args), columns: columns}
	}
	return statements
}

func newFakeRankingBatcher(t *testing.T) *fakeRankingBatcher {
	t.Helper()
	return &fakeRankingBatcher{
		statements: testRankingStatementDictionary(t), warehouseYTD: 1, districtYTD: 1, balance: 1, customerYTD: 1,
		stockYTD: make(map[string]float32),
	}
}

func TestPreparedInvalidItemSnapshotQueriesUsePreparedTemplates(t *testing.T) {
	ranking := &rankingClient{statements: testRankingStatementDictionary(t)}
	ctx := txnContext{wID: 17, dID: 3}
	input := rankingNewOrderInput{itemIDs: []int{7, 8}, supplyWIDs: []int{17, 18}}
	for _, query := range preparedInvalidItemSnapshotQueries(ctx, input, 3001) {
		if _, err := ranking.batchOperation(query); err != nil {
			t.Fatalf("registered invalid-item snapshot query %q was rejected: %v", query, err)
		}
	}
	if _, err := ranking.batchOperation("select d_next_o_id from district where d_w_id = 17 and d_id = 3;"); err == nil ||
		!strings.Contains(err.Error(), "was not prepared") {
		t.Fatalf("unknown district lookup error = %v, want unprepared-template rejection", err)
	}
}

func (f *fakeRankingBatcher) batchOperation(sql string) (batchOperation, error) {
	template, args, err := parameterizeSQL(sql)
	if err != nil {
		return batchOperation{}, err
	}
	statement, ok := f.statements[template]
	if !ok {
		return batchOperation{}, fmt.Errorf("ranking SQL template was not prepared: %q", template)
	}
	if len(args) != len(statement.parameterTypes) {
		return batchOperation{}, fmt.Errorf("ranking SQL parameter count mismatch for %q", template)
	}
	for index := range args {
		if args[index].typ != statement.parameterTypes[index] {
			return batchOperation{}, fmt.Errorf("ranking SQL parameter %d type mismatch for %q", index+1, template)
		}
	}
	return batchOperation{statement: statement, args: args, sql: sql}, nil
}

func (f *fakeRankingBatcher) execBatch(operations []batchOperation) (batchResult, error) {
	f.batches++
	result := batchResult{executedOperations: uint16(len(operations)), failedOperation: 0xffff}
	for index, operation := range operations {
		switch {
		case strings.HasPrefix(operation.sql, "update warehouse set w_ytd"):
			f.warehouseYTD += fakeFloatArgument(operation.args[0])
		case strings.HasPrefix(operation.sql, "update district set d_ytd"):
			f.districtYTD += fakeFloatArgument(operation.args[0])
		case strings.HasPrefix(operation.sql, "update customer set c_balance = c_balance -"):
			f.balance -= fakeFloatArgument(operation.args[0])
			f.customerYTD += fakeFloatArgument(operation.args[1])
		case strings.HasPrefix(operation.sql, "update customer set c_balance = c_balance +"):
			f.balance += fakeFloatArgument(operation.args[0])
		case strings.HasPrefix(operation.sql, "update stock set s_ytd = s_ytd +"):
			key := fakeStockKey(operation.args[3], operation.args[4])
			f.stockYTD[key] = f.fakeStockYTD(key) + fakeFloatArgument(operation.args[0])
		}
		if !operation.statement.query {
			continue
		}
		rows := make([][]string, 0, 1)
		if !f.emptyResults {
			var row []string
			switch {
			case strings.HasPrefix(operation.sql, "select w_ytd from warehouse"):
				row = []string{float32SQL(f.warehouseYTD)}
			case strings.HasPrefix(operation.sql, "select d_ytd from district"):
				row = []string{float32SQL(f.districtYTD)}
			case strings.HasPrefix(operation.sql, "select c_balance, c_ytd_payment from customer"):
				row = []string{float32SQL(f.balance), float32SQL(f.customerYTD)}
			case strings.HasPrefix(operation.sql, "select c_balance from customer where c_id"):
				row = []string{float32SQL(f.balance)}
			case strings.HasPrefix(operation.sql, "select s_ytd from stock"):
				key := fakeStockKey(operation.args[0], operation.args[1])
				row = []string{float32SQL(f.fakeStockYTD(key))}
			default:
				row = make([]string, 0, len(operation.statement.columns))
				for _, column := range operation.statement.columns {
					if column.sqlType == wireTypeChar {
						row = append(row, "x")
					} else {
						row = append(row, "1")
					}
				}
			}
			rows = append(rows, row)
		}
		result.results = append(result.results, batchOperationResult{operationIndex: uint16(index), rows: rows})
	}
	return result, nil
}

func fakeFloatArgument(argument preparedArgument) float32 {
	if argument.typ == wireTypeFloat32 {
		return math.Float32frombits(argument.floatBits)
	}
	return float32(argument.int32)
}

func fakeStockKey(item, warehouse preparedArgument) string {
	return fmt.Sprintf("%d/%d", item.int32, warehouse.int32)
}

func (f *fakeRankingBatcher) fakeStockYTD(key string) float32 {
	if value, ok := f.stockYTD[key]; ok {
		return value
	}
	return 1
}

func rankingTestContext() txnContext {
	return txnContext{wID: 1, dID: 1, official: true, profile: profile{
		warehouses: officialTerminalHomes, districtsPerWarehouse: districtsPerWarehouse,
		customersPerDistrict: 3000, itemCount: 100000}}
}

func TestRankingTransactionsUseOfficialBatchBoundaries(t *testing.T) {
	// final.md:741-749 fixes the dependent-stage round trips at 2/2/3/3/2. Every
	// statement they emit must also be part of the prepared dictionary, otherwise
	// the batch fails and the whole measurement window is void.
	cases := []struct {
		name    string
		batches int
		run     func(rankingBatcher, txnContext, *rand.Rand) error
	}{
		{"new_order", 2, rankingNewOrder},
		{"payment", 2, rankingPayment},
		{"order_status", 3, rankingOrderStatus},
		{"delivery", 3, rankingDelivery},
		{"stock_level", 2, rankingStockLevel},
	}
	for _, test := range cases {
		for seed := int64(1); seed <= 25; seed++ {
			batcher := newFakeRankingBatcher(t)
			if err := test.run(batcher, rankingTestContext(), rand.New(rand.NewSource(seed))); err != nil &&
				!errors.Is(err, errInvalidItem) {
				t.Fatalf("%s seed %d: %v", test.name, seed, err)
			}
			if batcher.batches != test.batches {
				t.Fatalf("%s seed %d used %d batches, want %d", test.name, seed, batcher.batches, test.batches)
			}
		}
	}
}

func TestRankingDeliveryKeepsPreparedStatementsWhenEveryQueueIsEmpty(t *testing.T) {
	// Regression guard for the unprepared `select 0;` placeholder: when all ten
	// districts have an empty new_orders queue the second stage must still send a
	// prepared statement and Delivery must keep its three batch boundaries.
	batcher := newFakeRankingBatcher(t)
	batcher.emptyResults = true
	if err := rankingDelivery(batcher, rankingTestContext(), rand.New(rand.NewSource(1))); err != nil {
		t.Fatal(err)
	}
	if batcher.batches != 3 {
		t.Fatalf("delivery used %d batches on an empty warehouse, want 3", batcher.batches)
	}
}

func TestExecBatchAutoAbortDoesNotSendRollback(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	c := &rankingClient{client: &client{conn: clientConn, timeout: time.Second}}
	operation := batchOperation{statement: rankingStatement{id: 1}}
	serverDone := make(chan error, 1)
	go func() {
		header := make([]byte, 8)
		if _, err := io.ReadFull(serverConn, header); err != nil {
			serverDone <- err
			return
		}
		payload := make([]byte, binary.BigEndian.Uint32(header[:4]))
		if _, err := io.ReadFull(serverConn, payload); err != nil {
			serverDone <- err
			return
		}
		body := appendU16(nil, 0)
		body = append(body, 1)
		body = appendU16(body, 0)
		body = appendU32(body, 5)
		body = append(body, "abort"...)
		body = appendU16(body, 0)
		response := make([]byte, 8)
		binary.BigEndian.PutUint32(response[:4], uint32(len(body)))
		response[4] = wireTagBatchResult
		if _, err := serverConn.Write(append(response, body...)); err != nil {
			serverDone <- err
			return
		}
		if err := serverConn.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
			serverDone <- err
			return
		}
		one := make([]byte, 1)
		_, err := serverConn.Read(one)
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			serverDone <- nil
			return
		}
		serverDone <- errors.New("rollback wrote an additional request")
	}()

	result, err := c.execBatch([]batchOperation{operation})
	if !errors.Is(err, errAbort) || result.status != 1 || !c.autoAborted {
		t.Fatalf("result = %#v, err = %v, autoAborted = %v", result, err, c.autoAborted)
	}
	c.rollback()
	if c.autoAborted {
		t.Fatal("rollback did not clear AUTO_ABORT state")
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestClosedRankingClientReturnsErrorAndRollbackDoesNotPanic(t *testing.T) {
	c := &rankingClient{
		client: &client{timeout: time.Second},
		statements: map[string]rankingStatement{
			"rollback;": {id: 1},
		},
	}
	if _, err := c.exec("rollback;"); err == nil || !strings.Contains(err.Error(), "connection is closed") {
		t.Fatalf("closed client exec error = %v", err)
	}
	c.rollback()
}
