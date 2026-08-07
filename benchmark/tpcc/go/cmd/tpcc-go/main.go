package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	districtsPerWarehouse = 10
	initialOrdersPerDist  = 3000

	// Official ranking load shape (finalv2.md:69-74): exactly one 30 second
	// warmup, three continuous 150 second measurement windows and 32 clients.
	officialWarmupSeconds  = 30
	officialMeasureSeconds = 150
	officialWindows        = 3
	officialWorkers        = 32
	// Retained for the legacy diagnostic warehouse policy only. Official ranking
	// uses the deterministic wheel in routing.go.
	officialTerminalHomes = 25
	// officialWarehouses is the size of the official data set (final.md:47).
	// final.md:226 scores a run with the wrong data scale as zero, so an
	// official-equivalent run must refuse anything else rather than publish a
	// number produced against a smaller database. officialMinWarehouses is only
	// the floor for explicitly non-official smoke runs, which still need enough
	// warehouses for the 25 distinct terminal homes.
	officialWarehouses    = 50
	officialMinWarehouses = 1

	wireTagMeta             = 0x01
	wireTagRow              = 0x02
	wireTagCommandOK        = 0x10
	wireTagResultEnd        = 0x11
	wireTagTransactionAbort = 0x12
	wireTagError            = 0x13
	wireTagExecStream       = 0x20
	wireTypeInt32           = 0x01
	wireTypeFloat32         = 0x02
	wireTypeChar            = 0x03
	maxWirePayload          = 1 << 20
)

var (
	errAbort         = errors.New("rmdb transaction aborted")
	errInvalidItem   = errors.New("invalid item")
	integerRE        = regexp.MustCompile(`-?\d+`)
	floatRE          = regexp.MustCompile(`-?\d+(?:\.\d+)?`)
	oracleSeq        atomic.Uint64
	oracleAckMu      sync.Mutex
	oracleAckFile    string
	oracleIssuedFile string
	oracleIDPrefix   int
)

func writeAll(conn net.Conn, data []byte) error {
	for len(data) > 0 {
		written, err := conn.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return errors.New("RMDB wire write returned zero")
		}
		data = data[written:]
	}
	return nil
}

type client struct {
	address           string
	timeout           time.Duration
	conn              net.Conn
	autoAborted       bool
	txnType           string
	oracleID          int
	oraclePayloadHash int
}

func newClient(address string, timeout time.Duration, isolation string) (*client, error) {
	c := &client{address: address, timeout: timeout}
	if err := c.connect(); err != nil {
		return nil, err
	}
	if isolation == "snapshot-isolation" {
		if _, err := c.exec("set transaction isolation level snapshot isolation;"); err != nil {
			c.close()
			return nil, err
		}
	} else if isolation != "read-committed" {
		c.close()
		return nil, fmt.Errorf("unsupported isolation level: %s", isolation)
	}
	return c, nil
}

func (c *client) connect() error {
	conn, err := net.DialTimeout("tcp", c.address, c.timeout)
	if err != nil {
		return err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			conn.Close()
			return err
		}
	}
	c.conn = conn
	handshake := []byte{'R', 'M', 'D', 'B', 0, 3, 0, 0}
	if err := writeAll(conn, handshake); err != nil {
		conn.Close()
		return err
	}
	response := make([]byte, len(handshake))
	if _, err := io.ReadFull(conn, response); err != nil {
		conn.Close()
		return err
	}
	if string(response) != string(handshake) {
		conn.Close()
		return errors.New("invalid RMDB wire protocol handshake")
	}
	return nil
}

func (c *client) reconnect(isolation string) error {
	c.close()
	return c.connectWithIsolation(isolation)
}

func (c *client) connectWithIsolation(isolation string) error {
	if err := c.connect(); err != nil {
		return err
	}
	if isolation == "snapshot-isolation" {
		_, err := c.exec("set transaction isolation level snapshot isolation;")
		return err
	}
	return nil
}

type resultKind int

const (
	// resultKindCommand statements may only succeed with an empty COMMAND_OK.
	resultKindCommand resultKind = iota
	// resultKindQuery statements may only succeed with META -> ROW* -> RESULT_END.
	resultKindQuery
	// resultKindEither covers `show tables;`, which final.md:33 explicitly allows
	// to answer with COMMAND_OK or with META ... RESULT_END.
	resultKindEither
)

// statementResultKind reports which EXEC_STREAM terminator a statement is
// allowed to use. final.md:645 makes the two terminators non-interchangeable:
// answering a SELECT with COMMAND_OK (or a DML with META) is a protocol
// contract failure, not an empty result.
func statementResultKind(sql string) resultKind {
	trimmed := strings.ToLower(strings.TrimSpace(sql))
	switch {
	case strings.HasPrefix(trimmed, "select"):
		return resultKindQuery
	case strings.HasPrefix(trimmed, "show"), strings.HasPrefix(trimmed, "desc"):
		return resultKindEither
	default:
		return resultKindCommand
	}
}

func (c *client) exec(sql string) (string, error) {
	if c.conn == nil {
		return "", errors.New("rmdb connection is closed")
	}
	kind := statementResultKind(sql)
	deadline := time.Now().Add(c.timeout)
	if err := c.conn.SetDeadline(deadline); err != nil {
		return "", err
	}
	payload := []byte(sql)
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	header[4] = wireTagExecStream
	if err := writeAll(c.conn, append(header, payload...)); err != nil {
		c.close()
		return "", err
	}
	tag, body, err := c.readFrame()
	if err != nil {
		c.close()
		return "", err
	}
	if tag == wireTagCommandOK {
		if len(body) != 0 {
			c.close()
			return "", errors.New("COMMAND_OK contains a payload")
		}
		if kind == resultKindQuery {
			c.close()
			return "", fmt.Errorf("query answered with COMMAND_OK instead of META ... RESULT_END: %s", sql)
		}
		return "", nil
	}
	if tag == wireTagTransactionAbort {
		c.autoAborted = true
		return "", errAbort
	}
	if tag == wireTagError {
		c.autoAborted = true
		return "", errors.New(string(body))
	}
	if tag != wireTagMeta {
		return "", errors.New("unexpected RMDB wire response")
	}
	if kind == resultKindCommand {
		c.close()
		return "", fmt.Errorf("non-query answered with META instead of COMMAND_OK: %s", sql)
	}
	columns, err := decodeMeta(body)
	if err != nil {
		return "", err
	}
	rows := make([][]string, 0)
	for {
		tag, body, err = c.readFrame()
		if err != nil {
			return "", err
		}
		if tag == wireTagRow {
			row, rowErr := decodeRow(body, columns)
			if rowErr != nil {
				return "", rowErr
			}
			rows = append(rows, row)
			continue
		}
		if tag != wireTagResultEnd || len(body) != 8 || binary.BigEndian.Uint64(body) != uint64(len(rows)) {
			return "", errors.New("invalid EXEC_STREAM result sequence")
		}
		return formatWireRows(columns, rows), nil
	}
}

type wireColumn struct {
	name    string
	sqlType byte
}

func (c *client) readFrame() (byte, []byte, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(header[:4])
	if length > maxWirePayload || header[5] != 0 || header[6] != 0 || header[7] != 0 || !knownWireTag(header[4]) {
		return 0, nil, errors.New("invalid RMDB wire frame")
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(c.conn, body); err != nil {
		return 0, nil, err
	}
	return header[4], body, nil
}

func knownWireTag(tag byte) bool {
	switch tag {
	case wireTagMeta, wireTagRow, wireTagCommandOK, wireTagResultEnd, wireTagTransactionAbort, wireTagError:
		return true
	case wireTagPrepareOK, wireTagBatchResult:
		return true
	default:
		return false
	}
}

func decodeMeta(body []byte) ([]wireColumn, error) {
	if len(body) < 2 {
		return nil, errors.New("truncated META")
	}
	count := int(binary.BigEndian.Uint16(body[:2]))
	if count == 0 {
		return nil, errors.New("empty META")
	}
	offset := 2
	columns := make([]wireColumn, 0, count)
	for i := 0; i < count; i++ {
		if offset+2 > len(body) {
			return nil, errors.New("truncated column definition")
		}
		nameLen := int(binary.BigEndian.Uint16(body[offset : offset+2]))
		offset += 2
		if nameLen == 0 || offset+nameLen+1 > len(body) {
			return nil, errors.New("invalid column definition")
		}
		columns = append(columns, wireColumn{name: string(body[offset : offset+nameLen]), sqlType: body[offset+nameLen]})
		offset += nameLen + 1
	}
	if offset != len(body) {
		return nil, errors.New("META contains trailing bytes")
	}
	return columns, nil
}

func decodeRow(body []byte, columns []wireColumn) ([]string, error) {
	offset := 0
	row := make([]string, 0, len(columns))
	for _, column := range columns {
		if offset >= len(body) {
			return nil, errors.New("truncated ROW")
		}
		present := body[offset]
		offset++
		if present == 0 {
			row = append(row, "NULL")
			continue
		}
		if present != 1 {
			return nil, errors.New("invalid ROW present flag")
		}
		switch column.sqlType {
		case wireTypeInt32:
			if offset+4 > len(body) {
				return nil, errors.New("truncated INT32")
			}
			row = append(row, strconv.FormatInt(int64(int32(binary.BigEndian.Uint32(body[offset:offset+4]))), 10))
			offset += 4
		case wireTypeFloat32:
			if offset+4 > len(body) {
				return nil, errors.New("truncated FLOAT32")
			}
			row = append(row, strconv.FormatFloat(float64(math.Float32frombits(binary.BigEndian.Uint32(body[offset:offset+4]))), 'f', -1, 32))
			offset += 4
		case wireTypeChar:
			if offset+4 > len(body) {
				return nil, errors.New("truncated CHAR length")
			}
			length := int(binary.BigEndian.Uint32(body[offset : offset+4]))
			offset += 4
			if offset+length > len(body) {
				return nil, errors.New("truncated CHAR")
			}
			row = append(row, string(body[offset:offset+length]))
			offset += length
		default:
			return nil, errors.New("unknown wire type")
		}
	}
	if offset != len(body) {
		return nil, errors.New("ROW contains trailing bytes")
	}
	return row, nil
}

func formatWireRows(columns []wireColumn, rows [][]string) string {
	widths := make([]int, len(columns))
	for i, column := range columns {
		widths[i] = len(column.name)
	}
	for _, row := range rows {
		for i, value := range row {
			if len(value) > widths[i] {
				widths[i] = len(value)
			}
		}
	}
	separator := func() string {
		var builder strings.Builder
		for _, width := range widths {
			builder.WriteByte('+')
			builder.WriteString(strings.Repeat("-", width+2))
		}
		builder.WriteString("+\n")
		return builder.String()
	}
	var builder strings.Builder
	builder.WriteString(separator())
	writeRow := func(row []string) {
		builder.WriteByte('|')
		for i, value := range row {
			builder.WriteByte(' ')
			builder.WriteString(value)
			builder.WriteString(strings.Repeat(" ", widths[i]-len(value)+1))
			builder.WriteByte('|')
		}
		builder.WriteByte('\n')
	}
	headers := make([]string, len(columns))
	for i, column := range columns {
		headers[i] = column.name
	}
	writeRow(headers)
	builder.WriteString(separator())
	for _, row := range rows {
		writeRow(row)
	}
	builder.WriteString(separator())
	fmt.Fprintf(&builder, "Total record(s): %d\n", len(rows))
	return builder.String()
}

func (c *client) close() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func (c *client) begin() error {
	c.autoAborted = false
	_, err := c.exec("begin;")
	return err
}

func (c *client) commit() error {
	oracleID := 0
	if oracleAckFile != "" {
		if c.oracleID != 0 {
			oracleID = c.oracleID
		} else {
			seq := oracleSeq.Add(1)
			if seq >= 1_000_000 {
				return errors.New("crash oracle transaction sequence exhausted")
			}
			oracleID = oracleIDPrefix*1_000_000 + int(seq)
		}
		payloadHash := c.oraclePayloadHash
		if payloadHash == 0 {
			for _, ch := range c.txnType {
				payloadHash = (payloadHash*131 + int(ch)) & 0x7fffffff
			}
		}
		if _, err := c.exec(fmt.Sprintf("insert into crash_txn_log values (%d, '%s', %d);", oracleID, c.txnType, payloadHash)); err != nil {
			return err
		}
	}
	if _, err := c.exec("commit;"); err != nil {
		return err
	}
	if oracleID != 0 {
		oracleAckMu.Lock()
		defer oracleAckMu.Unlock()
		file, err := os.OpenFile(oracleAckFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		if _, err = fmt.Fprintf(file, "%d\n", oracleID); err == nil {
			err = file.Sync()
		}
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func (c *client) rollback() {
	if c.autoAborted {
		c.autoAborted = false
		return
	}
	_, _ = c.exec("rollback;")
}

type profile struct {
	warehouses            int
	districtsPerWarehouse int
	customersPerDistrict  int
	itemCount             int
}

type txnContext struct {
	wID        int
	dID        int
	official   bool
	hotItemIDs []int
	// ledger receives the row and amount effects of this attempt. The worker
	// resets it before every attempt and folds it into its running total only
	// once the transaction's outcome is known, so a retried or aborted attempt
	// never contributes. May be nil for callers that do not reconcile.
	ledger *txnLedger
	profile
}

// txnLedger accumulates the effects committed transactions had on the database.
// The post-crash consistency validation reconciles the recovered database
// against baseline + ledger (final.md:322), and it must be able to do so from
// the published result JSON alone, without any in-memory state surviving the
// crash.
type txnLedger struct {
	values          map[string]float64
	paymentEdges    []paymentFloatEdge
	paymentEdgeSink paymentEdgeSink
}

type paymentFloatEdge struct {
	Kind       string `json:"kind"`
	Warehouse  int    `json:"warehouse_id"`
	District   int    `json:"district_id,omitempty"`
	BeforeBits uint32 `json:"before_bits"`
	AmountBits uint32 `json:"amount_bits"`
	AfterBits  uint32 `json:"after_bits"`
}

func newTxnLedger() *txnLedger {
	return newTxnLedgerWithSink(nil)
}

type paymentEdgeSink interface {
	write([]paymentFloatEdge) error
}

func newTxnLedgerWithSink(sink paymentEdgeSink) *txnLedger {
	values := make(map[string]float64, len(ledgerKeys))
	for _, key := range ledgerKeys {
		values[key] = 0
	}
	return &txnLedger{values: values, paymentEdgeSink: sink}
}

type paymentChainNode struct {
	selfLoops  int
	advance    uint32
	hasAdvance bool
}

type paymentChain struct {
	initial uint32
	current uint32
	pending map[uint32]*paymentChainNode
}

// paymentChainAccumulator validates each committed Payment edge as it arrives.
// Edges that are already connected to the known chain head are discarded; only
// genuinely out-of-order fragments remain pending. This keeps the common case
// bounded by the amount of cross-worker reordering instead of all committed
// transactions.
type paymentChainAccumulator struct {
	mu     sync.Mutex
	chains map[string]*paymentChain
	edges  int
	err    error
}

func newPaymentChainAccumulator() *paymentChainAccumulator {
	return &paymentChainAccumulator{chains: make(map[string]*paymentChain)}
}

func canonicalPaymentBits(bits uint32) uint32 {
	if bits<<1 == 0 {
		return 0
	}
	return bits
}

func (a *paymentChainAccumulator) write(edges []paymentFloatEdge) error {
	if a == nil || len(edges) == 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	for _, edge := range edges {
		if err := a.addLocked(edge); err != nil {
			a.err = err
			return err
		}
	}
	return nil
}

func (a *paymentChainAccumulator) addLocked(edge paymentFloatEdge) error {
	key, start, err := paymentEdgeKey(edge)
	if err != nil {
		return err
	}
	amount := math.Float32frombits(edge.AmountBits)
	before := math.Float32frombits(edge.BeforeBits)
	if amount <= 0 || math.IsNaN(float64(amount)) || math.IsInf(float64(amount), 0) {
		return fmt.Errorf("Payment FLOAT32 edge %s has invalid amount 0x%08x", key, edge.AmountBits)
	}
	want := math.Float32bits(before + amount)
	if !equalFloat32Bits(edge.AfterBits, want) {
		return fmt.Errorf("Payment FLOAT32 edge %s is not 0 ULP: before=0x%08x amount=0x%08x after=0x%08x want=0x%08x",
			key, edge.BeforeBits, edge.AmountBits, edge.AfterBits, want)
	}
	chain := a.chains[key]
	if chain == nil {
		chain = &paymentChain{initial: start, current: start, pending: make(map[uint32]*paymentChainNode)}
		a.chains[key] = chain
	}
	nodeKey := canonicalPaymentBits(edge.BeforeBits)
	currentValue := math.Float32frombits(chain.current)
	if before < currentValue {
		if !equalFloat32Bits(edge.BeforeBits, edge.AfterBits) {
			return fmt.Errorf("Payment FLOAT32 chain %s forks before current value at 0x%08x", key, edge.BeforeBits)
		}
		// A self-loop may have completed after a later advancing edge even
		// though it belongs to the already traversed chain prefix.
		a.edges++
		return nil
	}
	node := chain.pending[nodeKey]
	if node == nil {
		node = &paymentChainNode{}
		chain.pending[nodeKey] = node
	}
	if equalFloat32Bits(edge.BeforeBits, edge.AfterBits) {
		node.selfLoops++
	} else {
		if node.hasAdvance {
			return fmt.Errorf("Payment FLOAT32 chain %s forks at 0x%08x", key, edge.BeforeBits)
		}
		node.advance = edge.AfterBits
		node.hasAdvance = true
	}
	a.edges++
	a.drainLocked(chain)
	return nil
}

func (a *paymentChainAccumulator) drainLocked(chain *paymentChain) {
	for {
		nodeKey := canonicalPaymentBits(chain.current)
		node := chain.pending[nodeKey]
		if node == nil {
			return
		}
		delete(chain.pending, nodeKey)
		if !node.hasAdvance {
			return
		}
		chain.current = node.advance
	}
}

func (a *paymentChainAccumulator) finalize(expectedEdges int) (map[string]uint32, int, error) {
	if a == nil {
		return nil, 0, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return nil, a.edges, a.err
	}
	terminals := make(map[string]uint32, len(a.chains))
	for key, chain := range a.chains {
		a.drainLocked(chain)
		if len(chain.pending) != 0 {
			return nil, a.edges, fmt.Errorf("Payment FLOAT32 chain %s has an unlinked edge fragment", key)
		}
		terminals[key] = chain.current
	}
	if a.edges != expectedEdges {
		return nil, a.edges, fmt.Errorf("Payment FLOAT32 evidence has %d edges, want two for each of %d commit(s)",
			a.edges, expectedEdges/2)
	}
	return terminals, a.edges, nil
}

func (l *txnLedger) add(key string, delta float64) {
	if l == nil {
		return
	}
	l.values[key] += delta
}

func (l *txnLedger) reset() {
	if l == nil {
		return
	}
	for key := range l.values {
		l.values[key] = 0
	}
	l.paymentEdges = l.paymentEdges[:0]
}

func (l *txnLedger) merge(other *txnLedger) error {
	if l == nil || other == nil {
		return nil
	}
	for key, value := range other.values {
		l.values[key] += value
	}
	if l.paymentEdgeSink != nil {
		return l.paymentEdgeSink.write(other.paymentEdges)
	}
	l.paymentEdges = append(l.paymentEdges, other.paymentEdges...)
	return nil
}

func (l *txnLedger) addPaymentEdge(edge paymentFloatEdge) {
	if l != nil {
		l.paymentEdges = append(l.paymentEdges, edge)
	}
}

func (l *txnLedger) paymentEdgeSnapshot() []paymentFloatEdge {
	if l == nil {
		return nil
	}
	return append([]paymentFloatEdge(nil), l.paymentEdges...)
}

// paymentEdgeWriter is retained only for reading/re-publishing legacy result
// files that carried raw Payment evidence. New benchmark runs use the online
// paymentChainAccumulator instead.
type paymentEdgeWriter struct {
	mu        sync.Mutex
	file      *os.File
	encoder   *json.Encoder
	tmpPath   string
	finalPath string
	closed    bool
}

func paymentEdgePath(resultPath string) string {
	return resultPath + ".payment-edges.ndjson"
}

func validationStatePath(resultPath string) string {
	dir, base := filepath.Split(resultPath)
	return filepath.Join(dir, "."+base+".validation-state.json")
}

func newPaymentEdgeWriter(resultPath string) (*paymentEdgeWriter, error) {
	finalPath := paymentEdgePath(resultPath)
	tmp, err := os.CreateTemp(filepath.Dir(finalPath), ".tpcc-payment-edges-*.tmp")
	if err != nil {
		return nil, err
	}
	return &paymentEdgeWriter{
		file:      tmp,
		encoder:   json.NewEncoder(tmp),
		tmpPath:   tmp.Name(),
		finalPath: finalPath,
	}, nil
}

func (w *paymentEdgeWriter) write(edges []paymentFloatEdge) error {
	if w == nil || len(edges) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("payment edge writer is closed")
	}
	for _, edge := range edges {
		if err := w.encoder.Encode(edge); err != nil {
			return err
		}
	}
	return nil
}

func (w *paymentEdgeWriter) closeAndPublish() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		_ = os.Remove(w.tmpPath)
		w.closed = true
		return err
	}
	if err := w.file.Close(); err != nil {
		_ = os.Remove(w.tmpPath)
		w.closed = true
		return err
	}
	if err := os.Rename(w.tmpPath, w.finalPath); err != nil {
		_ = os.Remove(w.tmpPath)
		w.closed = true
		return err
	}
	w.closed = true
	return nil
}

func (w *paymentEdgeWriter) abort() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	_ = w.file.Close()
	_ = os.Remove(w.tmpPath)
	w.closed = true
}

func (l *txnLedger) snapshot() map[string]float64 {
	if l == nil {
		return nil
	}
	copied := make(map[string]float64, len(l.values))
	for key, value := range l.values {
		copied[key] = value
	}
	return copied
}

type result struct {
	MeasureSeconds   int                                  `json:"measure_seconds"`
	TPMC             float64                              `json:"tpmc"`
	NewOrderPerMin   float64                              `json:"NewOrder/min"`
	TxnTPM           map[string]float64                   `json:"txn_tpm"`
	Attempted        map[string]int                       `json:"attempted"`
	Committed        map[string]int                       `json:"committed"`
	ExpectedRollback map[string]int                       `json:"expected_rollback"`
	Abandoned        map[string]int                       `json:"abandoned"`
	Completion       map[string]float64                   `json:"completion"`
	AbortRate        float64                              `json:"abort_rate"`
	Counts           map[string]map[string]map[string]int `json:"counts"`
	LatencyMS        map[string]latencySummary            `json:"latency_ms"`
	Errors           map[string]map[string]map[string]int `json:"errors"`
	Coverage         coverageSummary                      `json:"coverage"`
	latencies        map[string][]float64
	covered          map[int]struct{}
}

type coverageSummary struct {
	Completed              int   `json:"completed"`
	Warehouses             []int `json:"warehouses"`
	WarehouseCount         int   `json:"warehouse_count"`
	RequiredWarehouseCount int   `json:"required_warehouse_count"`
	HotWarehouses          []int `json:"hot_warehouses"`
	HotWarehouseCount      int   `json:"hot_warehouse_count"`
	RequireAllHot          bool  `json:"require_all_hot"`
	DeliveryProcessed      int   `json:"delivery_processed_orders"`
}

type latencySummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

type liveStats struct {
	attempted         [2]atomic.Uint64
	commits           [2]atomic.Uint64
	expectedRollbacks [2]atomic.Uint64
	abandoned         [2]atomic.Uint64
	serverAborts      [2]atomic.Uint64
	newOrderAttempted [2]atomic.Uint64
	newOrderCommits   [2]atomic.Uint64
}

func phaseIndex(phase string) int {
	if phase == "measure" {
		return 1
	}
	return 0
}

func (s *liveStats) record(phase, txnType, outcome string) {
	index := phaseIndex(phase)
	s.attempted[index].Add(1)
	if txnType == "new_order" {
		s.newOrderAttempted[index].Add(1)
	}
	if outcome == "commit" {
		s.commits[index].Add(1)
		if txnType == "new_order" {
			s.newOrderCommits[index].Add(1)
		}
		return
	}
	if outcome == "server-abort" {
		s.serverAborts[index].Add(1)
	}
	if outcome == "invalid-item-rollback" {
		s.expectedRollbacks[index].Add(1)
	}
	if outcome == "abandoned" {
		s.abandoned[index].Add(1)
	}
}

func liveAbortRatePercent(attempted, expectedRollbacks, abandoned, serverAborts uint64) float64 {
	if attempted == 0 {
		return 0
	}
	return float64(expectedRollbacks+abandoned+serverAborts) / float64(attempted) * 100
}

func liveTPMC(elapsed int, newOrderCommits uint64) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(newOrderCommits) / (float64(elapsed) / 60.0)
}

func printProgress(round, rounds int, phase string, elapsed, total int, stats *liveStats) {
	index := phaseIndex(phase)
	attempted := stats.attempted[index].Load()
	commits := stats.commits[index].Load()
	expectedRollbacks := stats.expectedRollbacks[index].Load()
	abandoned := stats.abandoned[index].Load()
	serverAborts := stats.serverAborts[index].Load()
	newOrderAttempted := stats.newOrderAttempted[index].Load()
	newOrderCommits := stats.newOrderCommits[index].Load()
	abortRate := liveAbortRatePercent(attempted, expectedRollbacks, abandoned, serverAborts)
	tpmc := liveTPMC(elapsed, newOrderCommits)
	fmt.Printf("[round %d/%d %s %d/%ds] attempted=%d commits=%d expected_rollback=%d abandoned=%d server_aborts=%d new_order_attempted=%d new_order_commit=%d tpmC=%.2f abort_rate=%.2f%%\n",
		round, rounds, phase, elapsed, total, attempted, commits, expectedRollbacks, abandoned, serverAborts,
		newOrderAttempted, newOrderCommits, tpmc, abortRate)
}

func monitorProgress(round, rounds, warmupSeconds, measureSeconds, interval int, warmupEnd, measureEnd time.Time, stats *liveStats, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-ticker.C:
			if now.Before(warmupEnd) {
				printProgress(round, rounds, "warmup", int(now.Sub(warmupEnd.Add(-time.Duration(warmupSeconds)*time.Second)).Seconds()), warmupSeconds, stats)
			} else if now.Before(measureEnd) {
				printProgress(round, rounds, "measure", int(now.Sub(warmupEnd).Seconds()), measureSeconds, stats)
			} else {
				return
			}
		}
	}
}

func newResult(measureSeconds int) *result {
	return &result{
		MeasureSeconds:   measureSeconds,
		TxnTPM:           make(map[string]float64),
		Attempted:        make(map[string]int),
		Committed:        make(map[string]int),
		ExpectedRollback: make(map[string]int),
		Abandoned:        make(map[string]int),
		Completion:       make(map[string]float64),
		Counts:           make(map[string]map[string]map[string]int),
		LatencyMS:        make(map[string]latencySummary),
		Errors:           make(map[string]map[string]map[string]int),
		latencies:        make(map[string][]float64),
		covered:          make(map[int]struct{}),
	}
}

func (r *result) recordCompletion(wID int, deliveryProcessed int) {
	r.Coverage.Completed++
	r.Coverage.DeliveryProcessed += deliveryProcessed
	r.covered[wID] = struct{}{}
}

func (r *result) record(phase, txnType, outcome string, latency float64, detail string) {
	if r.Counts[phase] == nil {
		r.Counts[phase] = make(map[string]map[string]int)
	}
	if r.Counts[phase][txnType] == nil {
		r.Counts[phase][txnType] = make(map[string]int)
	}
	r.Counts[phase][txnType][outcome]++
	if phase == "measure" && outcome == "commit" {
		r.latencies[txnType] = append(r.latencies[txnType], latency)
		return
	}
	if outcome == "commit" {
		return
	}
	if detail == "" {
		return
	}
	if r.Errors[phase] == nil {
		r.Errors[phase] = make(map[string]map[string]int)
	}
	if r.Errors[phase][txnType] == nil {
		r.Errors[phase][txnType] = make(map[string]int)
	}
	r.Errors[phase][txnType][detail]++
}

func (r *result) merge(other *result) {
	for phase, txns := range other.Counts {
		for txnType, outcomes := range txns {
			if r.Counts[phase] == nil {
				r.Counts[phase] = make(map[string]map[string]int)
			}
			if r.Counts[phase][txnType] == nil {
				r.Counts[phase][txnType] = make(map[string]int)
			}
			for outcome, count := range outcomes {
				r.Counts[phase][txnType][outcome] += count
			}
		}
	}
	for txnType, values := range other.latencies {
		r.latencies[txnType] = append(r.latencies[txnType], values...)
	}
	for phase, txns := range other.Errors {
		for txnType, details := range txns {
			if r.Errors[phase] == nil {
				r.Errors[phase] = make(map[string]map[string]int)
			}
			if r.Errors[phase][txnType] == nil {
				r.Errors[phase][txnType] = make(map[string]int)
			}
			for detail, count := range details {
				r.Errors[phase][txnType][detail] += count
			}
		}
	}
	r.Coverage.Completed += other.Coverage.Completed
	r.Coverage.DeliveryProcessed += other.Coverage.DeliveryProcessed
	for wID := range other.covered {
		r.covered[wID] = struct{}{}
	}
}

func (r *result) finalize() {
	attemptedTotal, abortTotal := 0, 0
	newOrderCommitted := r.Counts["measure"]["new_order"]["commit"]
	if r.Attempted == nil {
		r.Attempted = make(map[string]int)
	}
	if r.Committed == nil {
		r.Committed = make(map[string]int)
	}
	if r.ExpectedRollback == nil {
		r.ExpectedRollback = make(map[string]int)
	}
	if r.Abandoned == nil {
		r.Abandoned = make(map[string]int)
	}
	if r.Completion == nil {
		r.Completion = make(map[string]float64)
	}
	for txnType, outcomes := range r.Counts["measure"] {
		attempted := 0
		for _, count := range outcomes {
			attempted += count
		}
		expectedRollback := outcomes["invalid-item-rollback"]
		r.Attempted[txnType] = attempted
		r.Committed[txnType] = outcomes["commit"]
		r.ExpectedRollback[txnType] = expectedRollback
		r.Abandoned[txnType] = outcomes["abandoned"]
		if attempted > 0 {
			r.Completion[txnType] = float64(outcomes["commit"]+expectedRollback) / float64(attempted)
		}
		attemptedTotal += attempted
		abortTotal += expectedRollback + outcomes["abandoned"] + outcomes["server-abort"]
	}
	if r.MeasureSeconds > 0 {
		r.TPMC = float64(newOrderCommitted) / (float64(r.MeasureSeconds) / 60.0)
		r.NewOrderPerMin = r.TPMC
		for txnType, outcomes := range r.Counts["measure"] {
			r.TxnTPM[txnType] = float64(outcomes["commit"]) / (float64(r.MeasureSeconds) / 60.0)
		}
	}
	if attemptedTotal > 0 {
		// The benchmark table defines aborts as expected business rollbacks plus
		// abandoned attempts. Keep server-abort for backward-compatible result
		// parsing; it is also a failed logical attempt if encountered.
		r.AbortRate = float64(abortTotal) / float64(attemptedTotal)
	}
	allLatencyCount := 0
	for _, values := range r.latencies {
		allLatencyCount += len(values)
	}
	// latencies contains committed measurement attempts only. Build the global
	// view at finalization so recording an individual transaction stays unchanged.
	allLatencies := make([]float64, 0, allLatencyCount)
	for txnType, values := range r.latencies {
		if len(values) == 0 {
			continue
		}
		allLatencies = append(allLatencies, values...)
		sort.Float64s(values)
		r.LatencyMS[txnType] = latencySummary{
			P50: percentile(values, 50), P95: percentile(values, 95), P99: percentile(values, 99), Max: values[len(values)-1],
		}
	}
	if len(allLatencies) > 0 {
		sort.Float64s(allLatencies)
		r.LatencyMS["global"] = latencySummary{
			P50: percentile(allLatencies, 50), P95: percentile(allLatencies, 95), P99: percentile(allLatencies, 99),
			Max: allLatencies[len(allLatencies)-1],
		}
	}
	r.Coverage.Warehouses = r.Coverage.Warehouses[:0]
	for wID := range r.covered {
		r.Coverage.Warehouses = append(r.Coverage.Warehouses, wID)
	}
	sort.Ints(r.Coverage.Warehouses)
	r.Coverage.WarehouseCount = len(r.Coverage.Warehouses)
}

func (r *result) hasBackendError() bool {
	for _, txns := range r.Counts {
		for _, outcomes := range txns {
			if outcomes["backend-error"] > 0 {
				return true
			}
		}
	}
	return false
}

func (r *result) reportTotals() (attempted, committed, expectedRollback, abandoned int, completion float64) {
	for txnType, count := range r.Attempted {
		attempted += count
		committed += r.Committed[txnType]
		expectedRollback += r.ExpectedRollback[txnType]
		abandoned += r.Abandoned[txnType]
	}
	if attempted > 0 {
		completion = float64(committed+expectedRollback) / float64(attempted)
	}
	return
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return values[middle]
	}
	return (values[middle-1] + values[middle]) / 2
}

func percentile(values []float64, pct int) float64 {
	rank := int(math.Ceil(float64(pct) / 100.0 * float64(len(values))))
	if rank < 1 {
		rank = 1
	}
	return values[rank-1]
}

func parseRows(text string) [][]string {
	rows := make([][]string, 0)
	hasTotal := false
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "+") || strings.HasPrefix(line, "-") {
			continue
		}
		if strings.HasPrefix(line, "Total record") {
			hasTotal = true
			continue
		}
		if strings.Contains(line, "|") {
			parts := strings.Split(strings.Trim(line, "|"), "|")
			for i := range parts {
				parts[i] = strings.TrimSpace(parts[i])
			}
			rows = append(rows, parts)
		} else if strings.Contains(line, ",") {
			parts := strings.Split(line, ",")
			for i := range parts {
				parts[i] = strings.TrimSpace(parts[i])
			}
			rows = append(rows, parts)
		} else {
			rows = append(rows, []string{line})
		}
	}
	if hasTotal && len(rows) > 0 {
		return rows[1:]
	}
	return rows
}

func scalarText(text, fallback string) string {
	rows := parseRows(text)
	if len(rows) == 0 || len(rows[len(rows)-1]) == 0 {
		return fallback
	}
	return rows[len(rows)-1][0]
}

func scalarInt(text string, fallback int) int {
	match := integerRE.FindString(scalarText(text, ""))
	if match == "" {
		return fallback
	}
	value, err := strconv.Atoi(match)
	if err != nil {
		return fallback
	}
	return value
}

func scalarFloat(text string, fallback float64) float64 {
	match := floatRE.FindString(scalarText(text, ""))
	if match == "" {
		return fallback
	}
	value, err := strconv.ParseFloat(match, 64)
	if err != nil {
		return fallback
	}
	return value
}

func scalarIntStrict(text string) (int, error) {
	value := strings.TrimSpace(scalarText(text, ""))
	if value == "" {
		return 0, errors.New("query returned no scalar value")
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid integer scalar %q: %w", value, err)
	}
	return parsed, nil
}

func scalarFloatStrict(text string) (float64, error) {
	value := strings.TrimSpace(scalarText(text, ""))
	if value == "" {
		return 0, errors.New("query returned no scalar value")
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid float scalar %q: %w", value, err)
	}
	return parsed, nil
}

func nowText() string { return time.Now().Format("2006-01-02 15:04:05") }

func surname(number int) string {
	sy := []string{"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"}
	return sy[number/100] + sy[(number/10)%10] + sy[number%10]
}

// float32SQL is the shortest decimal spelling that round-trips to the same
// binary32 value. A decimal point is kept for integral values so PREPARE_SET
// declares the marker as FLOAT32 rather than INT32.
func float32SQL(value float32) string {
	text := strconv.FormatFloat(float64(value), 'g', -1, 32)
	if !strings.ContainsAny(text, ".eE") {
		text += ".0"
	}
	return text
}

func newOrder(c txnBackend, ctx txnContext, rng *rand.Rand) error {
	cID, olCount := rng.Intn(ctx.customersPerDistrict)+1, rng.Intn(11)+5
	if err := c.begin(); err != nil {
		return err
	}
	rollback := true
	defer func() {
		if rollback {
			c.rollback()
		}
	}()
	if _, err := c.exec(fmt.Sprintf("select c_discount, c_last, c_credit, w_tax from customer, warehouse where w_id = %d and c_w_id = w_id and c_d_id = %d and c_id = %d;", ctx.wID, ctx.dID, cID)); err != nil {
		return err
	}
	if _, err := c.exec(fmt.Sprintf("update district set d_next_o_id = d_next_o_id + 1 where d_id = %d and d_w_id = %d;", ctx.dID, ctx.wID)); err != nil {
		return err
	}
	nextText, err := c.exec(fmt.Sprintf("select d_next_o_id, d_tax from district where d_id = %d and d_w_id = %d;", ctx.dID, ctx.wID))
	if err != nil {
		return err
	}
	dNext := scalarInt(nextText, -1) - 1
	if dNext < 1 {
		return errors.New("district next order id not found")
	}
	if _, err := c.exec(fmt.Sprintf("insert into orders values (%d, %d, %d, %d, '%s', 0, %d, 1);", dNext, ctx.dID, ctx.wID, cID, nowText(), olCount)); err != nil {
		return err
	}
	if _, err := c.exec(fmt.Sprintf("insert into new_orders values (%d, %d, %d);", dNext, ctx.dID, ctx.wID)); err != nil {
		return err
	}
	invalid, allLocal := rng.Intn(100) == 0, true
	for number := 1; number <= olCount; number++ {
		itemID := rng.Intn(ctx.itemCount) + 1
		if invalid && number == olCount {
			itemID = ctx.itemCount + 1
		}
		priceText, err := c.exec(fmt.Sprintf("select i_price, i_name, i_data from item where i_id = %d;", itemID))
		if err != nil {
			return err
		}
		if scalarText(priceText, "") == "" {
			return errInvalidItem
		}
		qty, supplyWID := rng.Intn(10)+1, ctx.wID
		if ctx.warehouses > 1 && rng.Intn(100) == 0 {
			for supplyWID == ctx.wID {
				supplyWID = rng.Intn(ctx.warehouses) + 1
			}
			allLocal = false
		}
		remote := 0
		if supplyWID != ctx.wID {
			remote = 1
		}
		if _, err := c.exec(fmt.Sprintf("update stock set s_ytd = s_ytd + %d, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + %d where s_i_id = %d and s_w_id = %d;", qty, remote, itemID, supplyWID)); err != nil {
			return err
		}
		stockText, err := c.exec(fmt.Sprintf("select s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 from stock where s_i_id = %d and s_w_id = %d;", itemID, supplyWID))
		if err != nil {
			return err
		}
		stockQty := scalarInt(stockText, 10)
		delta := -qty
		if stockQty-qty < 10 {
			delta = 91 - qty
		}
		op := "+"
		if delta < 0 {
			op, delta = "-", -delta
		}
		if _, err := c.exec(fmt.Sprintf("update stock set s_quantity = s_quantity %s %d where s_i_id = %d and s_w_id = %d;", op, delta, itemID, supplyWID)); err != nil {
			return err
		}
		amount := math.Round(scalarFloat(priceText, 1.0)*float64(qty)*100) / 100
		if _, err := c.exec(fmt.Sprintf("insert into order_line values (%d, %d, %d, %d, %d, %d, '', %d, %.2f, 'dist');", dNext, ctx.dID, ctx.wID, number, itemID, supplyWID, qty, amount)); err != nil {
			return err
		}
	}
	if !allLocal {
		if _, err := c.exec(fmt.Sprintf("update orders set o_all_local = 0 where o_id = %d and o_d_id = %d and o_w_id = %d;", dNext, ctx.dID, ctx.wID)); err != nil {
			return err
		}
	}
	err = c.commit()
	rollback = err != nil
	return err
}

func payment(c txnBackend, ctx txnContext, rng *rand.Rand) error {
	cID := rng.Intn(ctx.customersPerDistrict) + 1
	cWID, cDID := ctx.wID, ctx.dID
	if ctx.official && ctx.warehouses > 1 && rng.Intn(100) < 15 {
		cWID = rng.Intn(ctx.warehouses-1) + 1
		if cWID >= ctx.wID {
			cWID++
		}
		cDID = rng.Intn(ctx.districtsPerWarehouse) + 1
	}
	amount := math.Round((rng.Float64()*4999+1)*100) / 100
	if err := c.begin(); err != nil {
		return err
	}
	rollback := true
	defer func() {
		if rollback {
			c.rollback()
		}
	}()
	if ctx.official && rng.Intn(100) < 60 {
		last := surname(rng.Intn(1000))
		customerText, err := c.exec(fmt.Sprintf("select c_id, c_first from customer where c_w_id = %d and c_d_id = %d and c_last = '%s' order by c_first, c_id;", cWID, cDID, last))
		if err != nil {
			return err
		}
		rows := parseRows(customerText)
		if len(rows) > 0 {
			cID = scalarInt(rows[(len(rows)-1)/2][0], cID)
		}
	}
	queries := []string{
		fmt.Sprintf("update warehouse set w_ytd = w_ytd + %.2f where w_id = %d;", amount, ctx.wID),
		fmt.Sprintf("select w_street_1, w_street_2, w_city, w_state, w_zip, w_name from warehouse where w_id = %d;", ctx.wID),
		fmt.Sprintf("update district set d_ytd = d_ytd + %.2f where d_w_id = %d and d_id = %d;", amount, ctx.wID, ctx.dID),
		fmt.Sprintf("select d_street_1, d_street_2, d_city, d_state, d_zip, d_name from district where d_w_id = %d and d_id = %d;", ctx.wID, ctx.dID),
		fmt.Sprintf("update customer set c_balance = c_balance - %.2f, c_ytd_payment = c_ytd_payment + %.2f, c_payment_cnt = c_payment_cnt + 1 where c_w_id = %d and c_d_id = %d and c_id = %d;", amount, amount, cWID, cDID, cID),
		fmt.Sprintf("select c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since from customer where c_w_id = %d and c_d_id = %d and c_id = %d;", cWID, cDID, cID),
		fmt.Sprintf("insert into history values (%d, %d, %d, %d, %d, '%s', %.2f, 'payment');", cID, cDID, cWID, ctx.dID, ctx.wID, nowText(), amount),
	}
	for _, query := range queries {
		if _, err := c.exec(query); err != nil {
			return err
		}
	}
	err := c.commit()
	rollback = err != nil
	return err
}

func orderStatus(c txnBackend, ctx txnContext, rng *rand.Rand) error {
	if err := c.begin(); err != nil {
		return err
	}
	rollback := true
	defer func() {
		if rollback {
			c.rollback()
		}
	}()
	cID := rng.Intn(ctx.customersPerDistrict) + 1
	if rng.Intn(100) < 60 {
		last := surname(rng.Intn(1000))
		customerText, err := c.exec(fmt.Sprintf("select c_id, c_balance, c_first, c_middle, c_last from customer where c_w_id = %d and c_d_id = %d and c_last = '%s' order by c_first, c_id;", ctx.wID, ctx.dID, last))
		if err != nil {
			return err
		}
		rows := parseRows(customerText)
		if len(rows) > 0 {
			cID = scalarInt(rows[(len(rows)-1)/2][0], cID)
		}
	} else if _, err := c.exec(fmt.Sprintf("select c_balance, c_first, c_middle, c_last from customer where c_w_id = %d and c_d_id = %d and c_id = %d;", ctx.wID, ctx.dID, cID)); err != nil {
		return err
	}
	oID := rng.Intn(initialOrdersPerDist) + 1
	if ctx.official {
		orderText, err := c.exec(fmt.Sprintf("select o_id, o_entry_d, o_carrier_id from orders where o_w_id = %d and o_d_id = %d and o_c_id = %d order by o_id desc limit 1;", ctx.wID, ctx.dID, cID))
		if err != nil {
			return err
		}
		oID = scalarInt(orderText, 0)
	} else if _, err := c.exec(fmt.Sprintf("select o_id, o_entry_d, o_carrier_id from orders where o_w_id = %d and o_d_id = %d and o_c_id = %d and o_id = %d;", ctx.wID, ctx.dID, cID, oID)); err != nil {
		return err
	}
	if oID > 0 {
		if _, err := c.exec(fmt.Sprintf("select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from order_line where ol_w_id = %d and ol_d_id = %d and ol_o_id = %d;", ctx.wID, ctx.dID, oID)); err != nil {
			return err
		}
	}
	err := c.commit()
	rollback = err != nil
	return err
}

func delivery(c txnBackend, ctx txnContext, rng *rand.Rand) error {
	if err := c.begin(); err != nil {
		return err
	}
	rollback := true
	defer func() {
		if rollback {
			c.rollback()
		}
	}()
	carrierID := rng.Intn(10) + 1
	for dID := 1; dID <= ctx.districtsPerWarehouse; dID++ {
		oID := 0
		for {
			oIDText, err := c.exec(fmt.Sprintf("select min(no_o_id) from new_orders where no_w_id = %d and no_d_id = %d;", ctx.wID, dID))
			if err != nil {
				return err
			}
			candidate := scalarInt(oIDText, 0)
			if candidate == 0 {
				break
			}
			// Claim the candidate while holding its row lock. A concurrent
			// delivery either waits for this update or observes that the
			// candidate disappeared and retries the minimum lookup.
			if _, err := c.exec(fmt.Sprintf("update new_orders set no_o_id = no_o_id where no_w_id = %d and no_d_id = %d and no_o_id = %d;", ctx.wID, dID, candidate)); err != nil {
				return err
			}
			claimedText, err := c.exec(fmt.Sprintf("select min(no_o_id) from new_orders where no_w_id = %d and no_d_id = %d and no_o_id = %d;", ctx.wID, dID, candidate))
			if err != nil {
				return err
			}
			if scalarInt(claimedText, 0) == candidate {
				oID = candidate
				break
			}
		}
		if oID == 0 {
			continue
		}
		if _, err := c.exec(fmt.Sprintf("delete from new_orders where no_w_id = %d and no_d_id = %d and no_o_id = %d;", ctx.wID, dID, oID)); err != nil {
			return err
		}
		customerText, err := c.exec(fmt.Sprintf("select o_c_id from orders where o_id = %d and o_d_id = %d and o_w_id = %d;", oID, dID, ctx.wID))
		if err != nil {
			return err
		}
		customerID := scalarInt(customerText, 0)
		queries := []string{
			fmt.Sprintf("update orders set o_carrier_id = %d where o_id = %d and o_d_id = %d and o_w_id = %d;", carrierID, oID, dID, ctx.wID),
			fmt.Sprintf("update order_line set ol_delivery_d = '%s' where ol_o_id = %d and ol_d_id = %d and ol_w_id = %d;", nowText(), oID, dID, ctx.wID),
		}
		for _, query := range queries {
			if _, err := c.exec(query); err != nil {
				return err
			}
		}
		amountText, err := c.exec(fmt.Sprintf("select sum(ol_amount) from order_line where ol_o_id = %d and ol_d_id = %d and ol_w_id = %d;", oID, dID, ctx.wID))
		if err != nil {
			return err
		}
		if customerID > 0 {
			if _, err := c.exec(fmt.Sprintf("update customer set c_balance = c_balance + %.2f, c_delivery_cnt = c_delivery_cnt + 1 where c_id = %d and c_d_id = %d and c_w_id = %d;", scalarFloat(amountText, 0), customerID, dID, ctx.wID)); err != nil {
				return err
			}
		}
	}
	err := c.commit()
	rollback = err != nil
	return err
}

func stockLevel(c txnBackend, ctx txnContext, rng *rand.Rand) error {
	if err := c.begin(); err != nil {
		return err
	}
	rollback := true
	defer func() {
		if rollback {
			c.rollback()
		}
	}()
	threshold := rng.Intn(11) + 10
	dNextText, err := c.exec(fmt.Sprintf("select d_next_o_id from district where d_id = %d and d_w_id = %d;", ctx.dID, ctx.wID))
	if err != nil {
		return err
	}
	dNext, err := scalarIntStrict(dNextText)
	if err != nil {
		return fmt.Errorf("stock level district boundary: %w", err)
	}
	countText, err := c.exec(stockLevelCountQuery(ctx.wID, ctx.dID, dNext, threshold, false))
	if err != nil {
		return err
	}
	count, err := scalarIntStrict(countText)
	if err != nil {
		return fmt.Errorf("stock level COUNT(DISTINCT) result: %w", err)
	}
	if count < 0 {
		return fmt.Errorf("stock level COUNT(DISTINCT) returned negative count %d", count)
	}
	err = c.commit()
	rollback = err != nil
	return err
}

func stockLevelCountQuery(wID, dID, dNext, threshold int, parenthesized bool) string {
	column := "ol_i_id"
	if parenthesized {
		column = "(ol_i_id)"
	}
	return fmt.Sprintf("select count(distinct %s) from order_line, stock where ol_w_id = %d and ol_d_id = %d and ol_o_id >= %d and ol_o_id < %d and s_w_id = %d and s_i_id = ol_i_id and s_quantity < %d;", column, wID, dID, max(1, dNext-20), dNext, wID, threshold)
}

type rankingBatcher interface {
	batchOperation(string) (batchOperation, error)
	execBatch([]batchOperation) (batchResult, error)
}

func rankingBatch(c rankingBatcher, sqls ...string) (batchResult, error) {
	operations := make([]batchOperation, len(sqls))
	for i, sql := range sqls {
		operation, err := c.batchOperation(sql)
		if err != nil {
			return batchResult{}, err
		}
		operations[i] = operation
	}
	return c.execBatch(operations)
}

func rankingResult(result batchResult, index int) [][]string {
	for _, query := range result.results {
		if int(query.operationIndex) == index {
			return query.rows
		}
	}
	return nil
}

func rankingScalar(result batchResult, index int, fallback string) string {
	rows := rankingResult(result, index)
	if len(rows) == 0 || len(rows[0]) == 0 {
		return fallback
	}
	return rows[0][0]
}

func rankingFloat32(result batchResult, index int, label string) (float32, error) {
	raw := rankingScalar(result, index, "")
	value, err := strconv.ParseFloat(raw, 32)
	if err != nil {
		return 0, fmt.Errorf("%s returned invalid FLOAT32 %q", label, raw)
	}
	parsed := float32(value)
	if math.IsNaN(float64(parsed)) || math.IsInf(float64(parsed), 0) {
		return 0, fmt.Errorf("%s returned non-finite FLOAT32 %q", label, raw)
	}
	return parsed, nil
}

func verifyRankingFloat32Update(result batchResult, beforeOp, afterOp int, delta float32, subtract bool,
	label string) error {
	before, err := rankingFloat32(result, beforeOp, label+" before")
	if err != nil {
		return err
	}
	after, err := rankingFloat32(result, afterOp, label+" after")
	if err != nil {
		return err
	}
	want := before + delta
	if subtract {
		want = before - delta
	}
	gotBits, wantBits := math.Float32bits(after), math.Float32bits(want)
	if !equalFloat32Bits(gotBits, wantBits) {
		return fmt.Errorf("%s update evidence mismatch: before=0x%08x delta=0x%08x after=0x%08x want=0x%08x (0 ULP required)",
			label, math.Float32bits(before), math.Float32bits(delta), gotBits, wantBits)
	}
	return nil
}

type stockKey struct {
	wID, iID int
}

func projectedStockQuantityDeltas(itemIDs, supplyWIDs, quantities, initialQuantities []int) []int {
	deltas := make([]int, len(itemIDs))
	projectedQuantities := make(map[stockKey]int, len(itemIDs))
	for i, itemID := range itemIDs {
		key := stockKey{wID: supplyWIDs[i], iID: itemID}
		current, ok := projectedQuantities[key]
		if !ok {
			current = initialQuantities[i]
		}
		delta := -quantities[i]
		if current-quantities[i] < 10 {
			delta = 91 - quantities[i]
		}
		deltas[i] = delta
		projectedQuantities[key] = current + delta
	}
	return deltas
}

type rankingNewOrderInput struct {
	cID        int
	itemIDs    []int
	quantities []int
	supplyWIDs []int
	invalid    bool
	allLocal   bool
}

func makeRankingNewOrderInput(ctx txnContext, rng *rand.Rand, forceInvalid bool) rankingNewOrderInput {
	cID, lineCount := rng.Intn(ctx.customersPerDistrict)+1, rng.Intn(11)+5
	itemIDs := make([]int, lineCount)
	quantities := make([]int, lineCount)
	supplyWIDs := make([]int, lineCount)
	invalid := rng.Intn(100) == 0 || forceInvalid
	allLocal := true
	for i := range itemIDs {
		if len(ctx.hotItemIDs) > 0 && rng.Intn(100) < 25 {
			itemIDs[i] = ctx.hotItemIDs[rng.Intn(len(ctx.hotItemIDs))]
		} else {
			for {
				itemIDs[i] = rng.Intn(ctx.itemCount) + 1
				if !containsInt(ctx.hotItemIDs, itemIDs[i]) {
					break
				}
			}
		}
		if invalid && i == lineCount-1 {
			itemIDs[i] = ctx.itemCount + 1
		}
		quantities[i] = rng.Intn(10) + 1
		supplyWIDs[i] = ctx.wID
		if ctx.warehouses > 1 && rng.Intn(100) < 8 {
			for supplyWIDs[i] == ctx.wID {
				supplyWIDs[i] = rng.Intn(ctx.warehouses) + 1
			}
			allLocal = false
		}
	}
	return rankingNewOrderInput{
		cID: cID, itemIDs: itemIDs, quantities: quantities, supplyWIDs: supplyWIDs,
		invalid: invalid, allLocal: allLocal,
	}
}

func rankingNewOrder(c rankingBatcher, ctx txnContext, rng *rand.Rand) error {
	return runRankingNewOrder(c, ctx, makeRankingNewOrderInput(ctx, rng, false))
}

func runRankingNewOrder(c rankingBatcher, ctx txnContext, input rankingNewOrderInput) error {
	cID := input.cID
	itemIDs, quantities, supplyWIDs := input.itemIDs, input.quantities, input.supplyWIDs
	invalid, allLocal := input.invalid, input.allLocal
	lineCount := len(itemIDs)

	stage1 := []string{
		"begin;",
		fmt.Sprintf("select c_discount, c_last, c_credit, w_tax from customer, warehouse where w_id = %d and c_w_id = w_id and c_d_id = %d and c_id = %d;", ctx.wID, ctx.dID, cID),
		fmt.Sprintf("update district set d_next_o_id = d_next_o_id + 1 where d_id = %d and d_w_id = %d;", ctx.dID, ctx.wID),
		fmt.Sprintf("select d_next_o_id, d_tax from district where d_id = %d and d_w_id = %d;", ctx.dID, ctx.wID),
	}
	for _, itemID := range itemIDs {
		stage1 = append(stage1, fmt.Sprintf("select i_price, i_name, i_data from item where i_id = %d;", itemID))
	}
	lockKeys := make([]stockKey, len(itemIDs))
	for i := range itemIDs {
		lockKeys[i] = stockKey{wID: supplyWIDs[i], iID: itemIDs[i]}
	}
	sort.Slice(lockKeys, func(i, j int) bool {
		if lockKeys[i].wID != lockKeys[j].wID {
			return lockKeys[i].wID < lockKeys[j].wID
		}
		return lockKeys[i].iID < lockKeys[j].iID
	})
	uniqueLocks := lockKeys[:0]
	for _, key := range lockKeys {
		if len(uniqueLocks) == 0 || uniqueLocks[len(uniqueLocks)-1] != key {
			uniqueLocks = append(uniqueLocks, key)
		}
	}
	for _, key := range uniqueLocks {
		stage1 = append(stage1, fmt.Sprintf(
			"update stock set s_ytd = s_ytd where s_w_id = %d and s_i_id = %d;", key.wID, key.iID))
	}
	for i, itemID := range itemIDs {
		stage1 = append(stage1, fmt.Sprintf("select s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 from stock where s_i_id = %d and s_w_id = %d;", itemID, supplyWIDs[i]))
	}
	for i, itemID := range itemIDs {
		stage1 = append(stage1,
			fmt.Sprintf("select s_ytd from stock where s_i_id = %d and s_w_id = %d;", itemID, supplyWIDs[i]))
	}
	result, err := rankingBatch(c, stage1...)
	if err != nil {
		return err
	}
	dNext, err := strconv.Atoi(rankingScalar(result, 3, "0"))
	if err != nil || dNext < 1 {
		return errors.New("district next order id not found")
	}
	dNext--

	prices := make([]float32, lineCount)
	stockQtys := make([]int, lineCount)
	stockYTDBefore := make([]float32, lineCount)
	for i := 0; i < lineCount; i++ {
		itemRows := rankingResult(result, 4+i)
		if len(itemRows) == 0 {
			invalid = true
			continue
		}
		price, _ := strconv.ParseFloat(itemRows[0][0], 32)
		prices[i] = float32(price)
		stockRows := rankingResult(result, 4+lineCount+len(uniqueLocks)+i)
		if len(stockRows) == 0 {
			stockQtys[i] = 10
		} else {
			stockQtys[i], _ = strconv.Atoi(stockRows[0][0])
		}
		beforeOp := 4 + lineCount + len(uniqueLocks) + lineCount + i
		stockYTDBefore[i], err = rankingFloat32(result, beforeOp, "NewOrder stock.s_ytd before")
		if err != nil {
			return err
		}
	}

	stage2 := []string{
		fmt.Sprintf("insert into orders values (%d, %d, %d, %d, '%s', 0, %d, 1);", dNext, ctx.dID, ctx.wID, cID, nowText(), lineCount),
		fmt.Sprintf("insert into new_orders values (%d, %d, %d);", dNext, ctx.dID, ctx.wID),
	}
	// Stage 1 reads each key's starting quantity. Project repeated keys through
	// the original order-line sequence so every later line chooses its branch
	// from the quantity produced by the preceding line for that key.
	stockQuantityDeltas := projectedStockQuantityDeltas(itemIDs, supplyWIDs, quantities, stockQtys)
	stockAfterOps := make([]int, lineCount)
	for i, itemID := range itemIDs {
		if invalid && i == lineCount-1 {
			continue
		}
		remote := 0
		if supplyWIDs[i] != ctx.wID {
			remote = 1
		}
		stage2 = append(stage2,
			fmt.Sprintf("update stock set s_ytd = s_ytd + %d, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + %d where s_i_id = %d and s_w_id = %d;", quantities[i], remote, itemID, supplyWIDs[i]),
		)
		stockAfterOps[i] = len(stage2)
		stage2 = append(stage2,
			fmt.Sprintf("select s_ytd from stock where s_i_id = %d and s_w_id = %d;", itemID, supplyWIDs[i]))
		delta := stockQuantityDeltas[i]
		op := "+"
		if delta < 0 {
			op, delta = "-", -delta
		}
		amount := float32(float64(prices[i]) * float64(quantities[i]))
		stage2 = append(stage2,
			fmt.Sprintf("update stock set s_quantity = s_quantity %s %d where s_i_id = %d and s_w_id = %d;", op, delta, itemID, supplyWIDs[i]),
			fmt.Sprintf("insert into order_line values (%d, %d, %d, %d, %d, %d, '', %d, %s, 'dist');", dNext, ctx.dID, ctx.wID, i+1, itemID, supplyWIDs[i], quantities[i], float32SQL(amount)),
		)
	}
	if !allLocal {
		stage2 = append(stage2, fmt.Sprintf("update orders set o_all_local = 0 where o_id = %d and o_d_id = %d and o_w_id = %d;", dNext, ctx.dID, ctx.wID))
	}
	if invalid {
		stage2 = append(stage2, "abort;")
	} else {
		stage2 = append(stage2, "commit;")
	}

	result, err = rankingBatch(c, stage2...)
	if err != nil {
		return err
	}
	if invalid {
		// The order number increment lives in the same transaction that just
		// aborted, so a correct engine undoes it; the count is recorded anyway
		// because the official ledger formula for d_next_o_id is "committed +
		// expected rollback" and the diagnostic needs both numbers.
		ctx.ledger.add(ledgerNewOrderRollbacks, 1)
		return errInvalidItem
	}
	currentYTD := make(map[stockKey]float32, lineCount)
	for i, itemID := range itemIDs {
		key := stockKey{wID: supplyWIDs[i], iID: itemID}
		before, ok := currentYTD[key]
		if !ok {
			before = stockYTDBefore[i]
		}
		after, err := rankingFloat32(result, stockAfterOps[i], "NewOrder stock.s_ytd after")
		if err != nil {
			return err
		}
		want := before + float32(quantities[i])
		if !equalFloat32Bits(math.Float32bits(after), math.Float32bits(want)) {
			return fmt.Errorf("NewOrder stock.s_ytd update evidence mismatch w=%d i=%d: before=0x%08x quantity=%d after=0x%08x want=0x%08x (0 ULP required)",
				key.wID, key.iID, math.Float32bits(before), quantities[i], math.Float32bits(after), math.Float32bits(want))
		}
		currentYTD[key] = after
	}
	ctx.ledger.add(ledgerNewOrderCommits, 1)
	ctx.ledger.add(ledgerNewOrderLines, float64(lineCount))
	for i := range itemIDs {
		ctx.ledger.add(ledgerNewOrderQuantity, float64(quantities[i]))
		ctx.ledger.add(ledgerNewOrderStockDelta, float64(stockQuantityDeltas[i]))
		ctx.ledger.add(ledgerNewOrderAmount, float64(float32(float64(prices[i])*float64(quantities[i]))))
		if supplyWIDs[i] != ctx.wID {
			ctx.ledger.add(ledgerNewOrderRemote, 1)
		}
	}
	return nil
}

func containsInt(values []int, target int) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func rankingPayment(c rankingBatcher, ctx txnContext, rng *rand.Rand) error {
	cID := rng.Intn(ctx.customersPerDistrict) + 1
	cWID, cDID := ctx.wID, ctx.dID
	if ctx.official && ctx.warehouses > 1 && rng.Intn(100) < 30 {
		cWID = rng.Intn(ctx.warehouses-1) + 1
		if cWID >= ctx.wID {
			cWID++
		}
		cDID = rng.Intn(ctx.districtsPerWarehouse) + 1
	}
	amountCents := rng.Intn(499900) + 100
	amount := float32(float64(amountCents) / 100)
	amountSQL := float32SQL(amount)

	stage1 := []string{"begin;"}
	search := false
	if ctx.official && rng.Intn(100) < 60 {
		search = true
		stage1 = append(stage1, fmt.Sprintf("select c_id, c_first from customer where c_w_id = %d and c_d_id = %d and c_last = '%s' order by c_first, c_id;", cWID, cDID, surname(rng.Intn(1000))))
	}
	wBeforeOp := len(stage1)
	stage1 = append(stage1,
		fmt.Sprintf("select w_ytd from warehouse where w_id = %d;", ctx.wID),
		fmt.Sprintf("update warehouse set w_ytd = w_ytd + %s where w_id = %d;", amountSQL, ctx.wID),
		fmt.Sprintf("select w_ytd from warehouse where w_id = %d;", ctx.wID),
		fmt.Sprintf("select w_street_1, w_street_2, w_city, w_state, w_zip, w_name from warehouse where w_id = %d;", ctx.wID),
	)
	dBeforeOp := len(stage1)
	stage1 = append(stage1,
		fmt.Sprintf("select d_ytd from district where d_w_id = %d and d_id = %d;", ctx.wID, ctx.dID),
		fmt.Sprintf("update district set d_ytd = d_ytd + %s where d_w_id = %d and d_id = %d;", amountSQL, ctx.wID, ctx.dID),
		fmt.Sprintf("select d_ytd from district where d_w_id = %d and d_id = %d;", ctx.wID, ctx.dID),
		fmt.Sprintf("select d_street_1, d_street_2, d_city, d_state, d_zip, d_name from district where d_w_id = %d and d_id = %d;", ctx.wID, ctx.dID),
	)
	result, err := rankingBatch(c, stage1...)
	if err != nil {
		return err
	}
	if err := verifyRankingFloat32Update(result, wBeforeOp, wBeforeOp+2, amount, false, "Payment w_ytd"); err != nil {
		return err
	}
	if err := verifyRankingFloat32Update(result, dBeforeOp, dBeforeOp+2, amount, false, "Payment d_ytd"); err != nil {
		return err
	}
	wBefore, _ := rankingFloat32(result, wBeforeOp, "Payment w_ytd before")
	wAfter, _ := rankingFloat32(result, wBeforeOp+2, "Payment w_ytd after")
	dBefore, _ := rankingFloat32(result, dBeforeOp, "Payment d_ytd before")
	dAfter, _ := rankingFloat32(result, dBeforeOp+2, "Payment d_ytd after")
	if search {
		rows := rankingResult(result, 1)
		if len(rows) > 0 {
			cID, _ = strconv.Atoi(rows[(len(rows)-1)/2][0])
		}
	}
	stage2 := []string{
		fmt.Sprintf("select c_balance, c_ytd_payment from customer where c_w_id = %d and c_d_id = %d and c_id = %d;", cWID, cDID, cID),
		fmt.Sprintf("update customer set c_balance = c_balance - %s, c_ytd_payment = c_ytd_payment + %s, c_payment_cnt = c_payment_cnt + 1 where c_w_id = %d and c_d_id = %d and c_id = %d;", amountSQL, amountSQL, cWID, cDID, cID),
		fmt.Sprintf("select c_balance, c_ytd_payment from customer where c_w_id = %d and c_d_id = %d and c_id = %d;", cWID, cDID, cID),
		fmt.Sprintf("select c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since from customer where c_w_id = %d and c_d_id = %d and c_id = %d;", cWID, cDID, cID),
		fmt.Sprintf("insert into history values (%d, %d, %d, %d, %d, '%s', %s, 'payment');", cID, cDID, cWID, ctx.dID, ctx.wID, nowText(), amountSQL),
		"commit;",
	}
	result, err = rankingBatch(c, stage2...)
	if err != nil {
		return err
	}
	if err := verifyRankingFloat32Update(result, 0, 2, amount, true, "Payment c_balance"); err != nil {
		return err
	}
	beforeRows, afterRows := rankingResult(result, 0), rankingResult(result, 2)
	if len(beforeRows) != 1 || len(beforeRows[0]) != 2 || len(afterRows) != 1 || len(afterRows[0]) != 2 {
		return errors.New("Payment c_ytd_payment evidence did not return one two-column row before and after")
	}
	ytdEvidence := batchResult{results: []batchOperationResult{
		{operationIndex: 0, rows: [][]string{{beforeRows[0][1]}}},
		{operationIndex: 2, rows: [][]string{{afterRows[0][1]}}},
	}}
	if err := verifyRankingFloat32Update(ytdEvidence, 0, 2, amount, false, "Payment c_ytd_payment"); err != nil {
		return err
	}
	ctx.ledger.add(ledgerPaymentCommits, 1)
	ctx.ledger.add(ledgerPaymentAmount, float64(amount))
	ctx.ledger.addPaymentEdge(paymentFloatEdge{
		Kind: "warehouse", Warehouse: ctx.wID, BeforeBits: math.Float32bits(wBefore),
		AmountBits: math.Float32bits(amount), AfterBits: math.Float32bits(wAfter),
	})
	ctx.ledger.addPaymentEdge(paymentFloatEdge{
		Kind: "district", Warehouse: ctx.wID, District: ctx.dID, BeforeBits: math.Float32bits(dBefore),
		AmountBits: math.Float32bits(amount), AfterBits: math.Float32bits(dAfter),
	})
	// w_ytd and d_ytd of the terminal home take one binary32 accumulation step per
	// committed Payment. The post-crash tolerance is derived from these counts.
	ctx.ledger.add(ledgerPaymentWarehousePrefix+strconv.Itoa(ctx.wID), 1)
	return nil
}

func rankingOrderStatus(c rankingBatcher, ctx txnContext, rng *rand.Rand) error {
	cID := rng.Intn(ctx.customersPerDistrict) + 1
	stage1 := []string{"begin;"}
	search := rng.Intn(100) < 60
	if search {
		stage1 = append(stage1, fmt.Sprintf("select c_id, c_balance, c_first, c_middle, c_last from customer where c_w_id = %d and c_d_id = %d and c_last = '%s' order by c_first, c_id;", ctx.wID, ctx.dID, surname(rng.Intn(1000))))
	} else {
		stage1 = append(stage1, fmt.Sprintf("select c_balance, c_first, c_middle, c_last from customer where c_w_id = %d and c_d_id = %d and c_id = %d;", ctx.wID, ctx.dID, cID))
	}
	result, err := rankingBatch(c, stage1...)
	if err != nil {
		return err
	}
	if search {
		rows := rankingResult(result, 1)
		if len(rows) > 0 {
			cID, _ = strconv.Atoi(rows[(len(rows)-1)/2][0])
		}
	}
	stage2 := []string{fmt.Sprintf("select o_id, o_entry_d, o_carrier_id from orders where o_w_id = %d and o_d_id = %d and o_c_id = %d order by o_id desc limit 1;", ctx.wID, ctx.dID, cID)}
	result, err = rankingBatch(c, stage2...)
	if err != nil {
		return err
	}
	oID, _ := strconv.Atoi(rankingScalar(result, 0, "0"))
	stage3 := []string{}
	if oID > 0 {
		stage3 = append(stage3, fmt.Sprintf("select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from order_line where ol_w_id = %d and ol_d_id = %d and ol_o_id = %d;", ctx.wID, ctx.dID, oID))
	}
	stage3 = append(stage3, "commit;")
	_, err = rankingBatch(c, stage3...)
	return err
}

type deliveryPlan struct {
	dID        int
	oID        int
	confirmOp  int
	customerOp int
	amountOp   int
}

func rankingDelivery(c rankingBatcher, ctx txnContext, rng *rand.Rand) error {
	carrierID := rng.Intn(10) + 1
	stage1 := []string{"begin;"}
	for dID := 1; dID <= ctx.districtsPerWarehouse; dID++ {
		stage1 = append(stage1, fmt.Sprintf("select min(no_o_id) from new_orders where no_w_id = %d and no_d_id = %d;", ctx.wID, dID))
	}
	result, err := rankingBatch(c, stage1...)
	if err != nil {
		return err
	}
	plans := make([]deliveryPlan, 0, ctx.districtsPerWarehouse)
	stage2 := make([]string, 0, ctx.districtsPerWarehouse*5)
	// Operation indices are zero-based inside the batch. Unlike stage 1, stage 2
	// has no leading `begin;`, so the first statement of the first plan is at
	// index 0. Starting the cursor at 1 shifted every read by one: the "claim
	// confirmed" test read o_c_id (never empty, so every claim looked confirmed
	// even when the row had already been delivered by a concurrent Delivery), the
	// customer id read SUM(ol_amount) (which fails to parse as an integer, so the
	// customer was almost never credited) and the amount read a non-query
	// operation and fell back to zero.
	opIndex := 0
	for dID := 1; dID <= ctx.districtsPerWarehouse; dID++ {
		oID, _ := strconv.Atoi(rankingScalar(result, dID, "0"))
		if oID == 0 {
			continue
		}
		plans = append(plans, deliveryPlan{dID: dID, oID: oID, confirmOp: opIndex + 1, customerOp: opIndex + 2, amountOp: opIndex + 3})
		stage2 = append(stage2,
			fmt.Sprintf("update new_orders set no_o_id = no_o_id where no_w_id = %d and no_d_id = %d and no_o_id = %d;", ctx.wID, dID, oID),
			fmt.Sprintf("select min(no_o_id) from new_orders where no_w_id = %d and no_d_id = %d and no_o_id = %d;", ctx.wID, dID, oID),
			fmt.Sprintf("select o_c_id from orders where o_id = %d and o_d_id = %d and o_w_id = %d;", oID, dID, ctx.wID),
			fmt.Sprintf("select sum(ol_amount) from order_line where ol_o_id = %d and ol_d_id = %d and ol_w_id = %d;", oID, dID, ctx.wID),
		)
		opIndex += 4
	}
	if len(stage2) == 0 {
		// Every district of this warehouse had an empty new_orders queue. Re-read
		// one prepared MIN instead of an unprepared placeholder such as
		// `select 0;`: an unprepared template fails the whole batch and
		// invalidates the measurement window, while this keeps Delivery on its
		// official three batch boundaries (final.md:746).
		stage2 = append(stage2, fmt.Sprintf("select min(no_o_id) from new_orders where no_w_id = %d and no_d_id = 1;", ctx.wID))
	}
	result, err = rankingBatch(c, stage2...)
	if err != nil {
		return err
	}
	stage3 := make([]string, 0, len(plans)*6+1)
	type deliveryBalanceEvidence struct {
		beforeOp int
		afterOp  int
		amount   float32
		dID      int
		cID      int
	}
	evidence := make([]deliveryBalanceEvidence, 0, len(plans))
	deliveredOrders, deliveredCustomers, deliveredAmount := 0, 0, 0.0
	for _, plan := range plans {
		// The claim confirmation is `SELECT MIN(no_o_id) ... WHERE no_o_id = X`.
		// When another Delivery already took the row, that is not an empty
		// result: SQL returns exactly one row holding NULL, which the wire
		// protocol sends as present=0 (final.md:763) and decodeRow renders as
		// the literal string "NULL". Testing for "" therefore never fired and
		// every lost claim was treated as won — the driver then deleted a queue
		// entry it had not claimed and credited the customer a second time.
		// Compare against the order id we asked for, the way the non-ranking
		// delivery() path already does.
		if rankingScalar(result, plan.confirmOp, "") != strconv.Itoa(plan.oID) {
			continue
		}
		customerID, _ := strconv.Atoi(rankingScalar(result, plan.customerOp, "0"))
		amount64, _ := strconv.ParseFloat(rankingScalar(result, plan.amountOp, "0"), 32)
		amount := float32(amount64)
		stage3 = append(stage3,
			fmt.Sprintf("delete from new_orders where no_w_id = %d and no_d_id = %d and no_o_id = %d;", ctx.wID, plan.dID, plan.oID),
			fmt.Sprintf("update orders set o_carrier_id = %d where o_id = %d and o_d_id = %d and o_w_id = %d;", carrierID, plan.oID, plan.dID, ctx.wID),
			fmt.Sprintf("update order_line set ol_delivery_d = '%s' where ol_o_id = %d and ol_d_id = %d and ol_w_id = %d;", nowText(), plan.oID, plan.dID, ctx.wID),
		)
		deliveredOrders++
		if customerID > 0 {
			// Bind the exact binary32 SUM returned by the server. Re-formatting
			// through cents would change valid order totals.
			beforeOp := len(stage3)
			stage3 = append(stage3,
				fmt.Sprintf("select c_balance from customer where c_id = %d and c_d_id = %d and c_w_id = %d;", customerID, plan.dID, ctx.wID),
				fmt.Sprintf("update customer set c_balance = c_balance + %s, c_delivery_cnt = c_delivery_cnt + 1 where c_id = %d and c_d_id = %d and c_w_id = %d;", float32SQL(amount), customerID, plan.dID, ctx.wID),
				fmt.Sprintf("select c_balance from customer where c_id = %d and c_d_id = %d and c_w_id = %d;", customerID, plan.dID, ctx.wID),
			)
			evidence = append(evidence, deliveryBalanceEvidence{
				beforeOp: beforeOp, afterOp: beforeOp + 2, amount: amount, dID: plan.dID, cID: customerID,
			})
			deliveredCustomers++
			deliveredAmount += float64(amount)
		}
	}
	stage3 = append(stage3, "commit;")
	result, err = rankingBatch(c, stage3...)
	if err != nil {
		return err
	}
	for _, update := range evidence {
		label := fmt.Sprintf("Delivery c_balance w=%d d=%d c=%d", ctx.wID, update.dID, update.cID)
		if err := verifyRankingFloat32Update(result, update.beforeOp, update.afterOp, update.amount, false, label); err != nil {
			return err
		}
	}
	ctx.ledger.add(ledgerDeliveryOrders, float64(deliveredOrders))
	ctx.ledger.add(ledgerDeliveryCustomers, float64(deliveredCustomers))
	ctx.ledger.add(ledgerDeliveryAmount, deliveredAmount)
	return nil
}

func rankingStockLevel(c rankingBatcher, ctx txnContext, rng *rand.Rand) error {
	threshold := rng.Intn(11) + 10
	result, err := rankingBatch(c, "begin;", rankingDistrictNextOrderSQL(ctx.wID, ctx.dID))
	if err != nil {
		return err
	}
	dNext, err := strconv.Atoi(rankingScalar(result, 1, "0"))
	if err != nil {
		return err
	}
	_, err = rankingBatch(c, stockLevelCountQuery(ctx.wID, ctx.dID, dNext, threshold, false), "commit;")
	return err
}

func verifyBenchmarkFeatures(c txnBackend, p profile) error {
	if p.warehouses < 1 {
		return errors.New("feature check requires at least one warehouse")
	}
	const wID, dID, threshold = 1, 1, 20
	dNextText, err := c.exec("select d_next_o_id from district where d_w_id = 1 and d_id = 1;")
	if err != nil {
		return fmt.Errorf("feature check district lookup: %w", err)
	}
	dNext, err := scalarIntStrict(dNextText)
	if err != nil || dNext < 1 {
		if err != nil {
			return fmt.Errorf("feature check district boundary: %w", err)
		}
		return fmt.Errorf("feature check district boundary is invalid: %d", dNext)
	}
	counts := make([]int, 2)
	for i, parenthesized := range []bool{false, true} {
		text, queryErr := c.exec(stockLevelCountQuery(wID, dID, dNext, threshold, parenthesized))
		if queryErr != nil {
			return fmt.Errorf("feature check COUNT(DISTINCT) syntax %d: %w", i+1, queryErr)
		}
		counts[i], err = scalarIntStrict(text)
		if err != nil {
			return fmt.Errorf("feature check COUNT(DISTINCT) syntax %d result: %w", i+1, err)
		}
		if counts[i] < 0 {
			return fmt.Errorf("feature check COUNT(DISTINCT) syntax %d returned negative count %d", i+1, counts[i])
		}
	}
	if counts[0] != counts[1] {
		return fmt.Errorf("COUNT(DISTINCT col)=%d differs from COUNT(DISTINCT (col))=%d", counts[0], counts[1])
	}

	beforeText, err := c.exec("select w_ytd from warehouse where w_id = 1;")
	if err != nil {
		return fmt.Errorf("feature check transaction read: %w", err)
	}
	before, err := scalarFloatStrict(beforeText)
	if err != nil {
		return fmt.Errorf("feature check transaction value: %w", err)
	}
	if err := c.begin(); err != nil {
		return fmt.Errorf("feature check BEGIN: %w", err)
	}
	rollbackNeeded := true
	defer func() {
		if rollbackNeeded {
			c.rollback()
		}
	}()
	if _, err := c.exec("update warehouse set w_ytd = w_ytd + 1.0 where w_id = 1;"); err != nil {
		return fmt.Errorf("feature check transactional UPDATE: %w", err)
	}
	if _, err := c.exec("rollback;"); err != nil {
		return fmt.Errorf("feature check ROLLBACK: %w", err)
	}
	rollbackNeeded = false
	afterText, err := c.exec("select w_ytd from warehouse where w_id = 1;")
	if err != nil {
		return fmt.Errorf("feature check post-rollback read: %w", err)
	}
	after, err := scalarFloatStrict(afterText)
	if err != nil {
		return fmt.Errorf("feature check post-rollback value: %w", err)
	}
	if math.Abs(after-before) > 1e-5 {
		return fmt.Errorf("transaction rollback changed warehouse w_ytd from %v to %v", before, after)
	}
	fmt.Printf("[feature-check] COUNT(DISTINCT)=%d; explicit transaction rollback preserved w_ytd=%.6g\n", counts[0], after)
	return nil
}

func preparedInvalidItemSnapshotQueries(ctx txnContext, input rankingNewOrderInput, orderID int) []string {
	queries := []string{
		rankingDistrictNextOrderSQL(ctx.wID, ctx.dID),
		rankingOrderCountSQL(ctx.wID, ctx.dID, orderID),
		rankingNewOrderCountSQL(ctx.wID, ctx.dID, orderID),
		rankingOrderLineCountSQL(ctx.wID, ctx.dID, orderID),
	}
	keys := make([]stockKey, 0, len(input.itemIDs))
	for i, itemID := range input.itemIDs {
		if input.invalid && i == len(input.itemIDs)-1 {
			continue
		}
		keys = append(keys, stockKey{wID: input.supplyWIDs[i], iID: itemID})
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].wID != keys[j].wID {
			return keys[i].wID < keys[j].wID
		}
		return keys[i].iID < keys[j].iID
	})
	for i, key := range keys {
		if i > 0 && keys[i-1] == key {
			continue
		}
		queries = append(queries, rankingStockSnapshotSQL(key.wID, key.iID))
	}
	return queries
}

func captureQueryResults(c txnBackend, queries []string) ([]string, error) {
	results := make([]string, len(queries))
	for i, query := range queries {
		text, err := c.exec(query)
		if err != nil {
			return nil, fmt.Errorf("snapshot query %d: %w", i+1, err)
		}
		results[i] = strings.TrimSpace(text)
	}
	return results, nil
}

func verifyPreparedInvalidItemRollback(c txnBackend, p profile) error {
	batcher, ok := c.(rankingBatcher)
	if !ok {
		return errors.New("prepared invalid-item check requires a prepared ranking client")
	}
	ctx := txnContext{wID: 1, dID: 1, official: true, profile: p}
	input := makeRankingNewOrderInput(ctx, rand.New(rand.NewSource(1)), true)
	if len(input.itemIDs) < 2 || !input.invalid || input.itemIDs[len(input.itemIDs)-1] != p.itemCount+1 {
		return errors.New("prepared invalid-item check did not generate valid writes followed by an invalid item")
	}
	nextText, err := c.exec(rankingDistrictNextOrderSQL(1, 1))
	if err != nil {
		return fmt.Errorf("prepared invalid-item check district lookup: %w", err)
	}
	orderID, err := scalarIntStrict(nextText)
	if err != nil || orderID < 1 {
		if err != nil {
			return fmt.Errorf("prepared invalid-item check district boundary: %w", err)
		}
		return fmt.Errorf("prepared invalid-item check district boundary is invalid: %d", orderID)
	}
	queries := preparedInvalidItemSnapshotQueries(ctx, input, orderID)
	before, err := captureQueryResults(c, queries)
	if err != nil {
		return fmt.Errorf("prepared invalid-item check before rollback: %w", err)
	}
	err = runRankingNewOrder(batcher, ctx, input)
	if !errors.Is(err, errInvalidItem) {
		return fmt.Errorf("prepared invalid-item check returned %v, want business rollback", err)
	}
	after, err := captureQueryResults(c, queries)
	if err != nil {
		return fmt.Errorf("prepared invalid-item check after rollback: %w", err)
	}
	for i := range queries {
		if before[i] != after[i] {
			return fmt.Errorf("prepared invalid-item rollback changed snapshot query %d", i+1)
		}
	}
	fmt.Printf("[feature-check] prepared invalid-item NewOrder rolled back %d prior valid line(s)\n", len(input.itemIDs)-1)
	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func txnTypeForBucket(bucket int) string {
	bucket %= 100
	if bucket < 45 {
		return "new_order"
	}
	if bucket < 88 {
		return "payment"
	}
	if bucket < 92 {
		return "order_status"
	}
	if bucket < 96 {
		return "delivery"
	}
	return "stock_level"
}

func chooseTxn(rng *rand.Rand) string {
	return txnTypeForBucket(rng.Intn(100))
}

// officialTerminalHome maps a client index onto its terminal home warehouse:
// two clients per home over officialTerminalHomes homes (final.md:47).
func officialTerminalHome(workerID int) int { return (workerID/2)%officialTerminalHomes + 1 }

// officialTerminalHomeCount reports how many distinct terminal homes a client
// count actually uses under the official pairing policy.
func officialTerminalHomeCount(workers int) int {
	homes := make(map[int]struct{}, workers)
	for workerID := 0; workerID < workers; workerID++ {
		homes[officialTerminalHome(workerID)] = struct{}{}
	}
	return len(homes)
}

func chooseContext(p profile, workerID int, policy string, rng *rand.Rand) txnContext {
	wID := workerID%p.warehouses + 1
	if policy == "random-per-txn" {
		wID = rng.Intn(p.warehouses) + 1
	} else if policy == "official-terminal-home" {
		// The caller validates that this mode has enough warehouses.
		wID = officialTerminalHome(workerID)
	}
	return txnContext{wID: wID, dID: rng.Intn(p.districtsPerWarehouse) + 1, official: policy == "official-terminal-home", profile: p}
}

func validateBenchmarkMode(mode string, workers, warmup, measure, rounds int, think time.Duration, allowNonOfficialTiming bool) error {
	switch mode {
	case "sqlite-reference", "rmdb-diagnostic":
		return nil
	case "official-equivalent":
		if allowNonOfficialTiming {
			if workers < 1 || warmup < 0 || measure < 1 || rounds < 1 || think < 0 {
				return errors.New("official-equivalent requires positive workers/measure/rounds and non-negative warmup/think")
			}
			return nil
		}
		// The ranking metric is only comparable to the official one when the load
		// shape is identical, so refuse to start instead of publishing a number
		// produced by a different shape. --allow-nonofficial-timing opts out.
		if workers != officialWorkers {
			return fmt.Errorf("official-equivalent requires workers=%d, got %d (use --allow-nonofficial-timing for smoke runs)",
				officialWorkers, workers)
		}
		if warmup != officialWarmupSeconds || measure != officialMeasureSeconds || rounds != officialWindows {
			return fmt.Errorf("official-equivalent requires warmup=%d, measure=%d, rounds=%d, got %d/%d/%d (use --allow-nonofficial-timing for smoke runs)",
				officialWarmupSeconds, officialMeasureSeconds, officialWindows, warmup, measure, rounds)
		}
		if think != 0 {
			return fmt.Errorf("official-equivalent is a saturated load and requires think=0, got %s (use --allow-nonofficial-timing for smoke runs)", think)
		}
		return nil
	default:
		return fmt.Errorf("unsupported benchmark mode: %s", mode)
	}
}

// validateOfficialWarehouses enforces the official data scale. The previous
// check only required 25 warehouses, which is looser than the evaluator: the
// official data set has 50 (final.md:47) and a wrong data scale scores zero
// (final.md:226).
func validateOfficialWarehouses(warehouses int, allowNonOfficialTiming bool) error {
	if allowNonOfficialTiming {
		if warehouses < officialMinWarehouses {
			return fmt.Errorf("official-equivalent requires at least %d warehouses even for a smoke run, got %d",
				officialMinWarehouses, warehouses)
		}
		if warehouses != officialWarehouses {
			fmt.Fprintf(os.Stderr, "[warning] official-equivalent against %d warehouses instead of the official %d; the official evaluator scores a wrong data scale as zero\n",
				warehouses, officialWarehouses)
		}
		return nil
	}
	if warehouses != officialWarehouses {
		return fmt.Errorf("official-equivalent requires exactly %d warehouses, got %d (use --allow-nonofficial-timing for smoke runs)",
			officialWarehouses, warehouses)
	}
	return nil
}

func runTxn(c txnBackend, txnType string, ctx txnContext, rng *rand.Rand) error {
	switch rmdbClient := c.(type) {
	case *client:
		rmdbClient.txnType = txnType
	case *rankingClient:
		rmdbClient.txnType = txnType
	}
	switch txnType {
	case "new_order":
		if ranking, ok := c.(*rankingClient); ok {
			return rankingNewOrder(ranking, ctx, rng)
		}
		return newOrder(c, ctx, rng)
	case "payment":
		if ranking, ok := c.(*rankingClient); ok {
			return rankingPayment(ranking, ctx, rng)
		}
		return payment(c, ctx, rng)
	case "order_status":
		if ranking, ok := c.(*rankingClient); ok {
			return rankingOrderStatus(ranking, ctx, rng)
		}
		return orderStatus(c, ctx, rng)
	case "delivery":
		if ranking, ok := c.(*rankingClient); ok {
			return rankingDelivery(ranking, ctx, rng)
		}
		return delivery(c, ctx, rng)
	default:
		if ranking, ok := c.(*rankingClient); ok {
			return rankingStockLevel(ranking, ctx, rng)
		}
		return stockLevel(c, ctx, rng)
	}
}

// runMixedSQL executes a deterministic SQL stream before entering an open
// stream. The ready file is published only after minOps complete, allowing a
// crash harness to guarantee that every cycle exercised ordinary SELECT and
// UPDATE traffic before killing the server.
func runMixedSQL(address string, timeout time.Duration, isolation string, minOps int, readyFile string,
	ackFile string, issuedFile string, oraclePrefix int, think time.Duration) error {
	if minOps < 100 {
		return fmt.Errorf("mixed-sql requires at least 100 operations")
	}
	if readyFile == "" || ackFile == "" || issuedFile == "" || oraclePrefix <= 0 || oraclePrefix > 2000 {
		return fmt.Errorf("mixed-sql requires ready, ACK, issued files, and oracle prefix")
	}
	oracleAckFile = ackFile
	oracleIssuedFile = issuedFile
	oracleIDPrefix = oraclePrefix
	oracleSeq.Store(0)
	c, err := newClient(address, timeout, isolation)
	if err != nil {
		return err
	}
	defer c.close()
	c.txnType = "mixed_sql"

	appendIssued := func(id int) error {
		file, err := os.OpenFile(issuedFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		_, writeErr := fmt.Fprintf(file, "%d\n", id)
		if writeErr == nil {
			writeErr = file.Sync()
		}
		closeErr := file.Close()
		if writeErr != nil {
			return writeErr
		}
		return closeErr
	}
	runOne := func(op int) error {
		seq := oracleSeq.Add(1)
		if seq >= 1_000_000 {
			return errors.New("crash oracle transaction sequence exhausted")
		}
		oracleID := oraclePrefix*1_000_000 + int(seq)
		if err := appendIssued(oracleID); err != nil {
			return err
		}
		c.oracleID = oracleID
		c.oraclePayloadHash = mixedPayloadHash(oracleID)
		defer func() {
			c.oracleID = 0
			c.oraclePayloadHash = 0
		}()
		if err := c.begin(); err != nil {
			return err
		}
		rollback := true
		defer func() {
			if rollback {
				c.rollback()
			}
		}()
		id := op%128 + 1
		if _, err := c.exec(fmt.Sprintf("select value from crash_sql_state where id = %d;", id)); err != nil {
			return err
		}
		if _, err := c.exec(fmt.Sprintf("update crash_sql_state set value = value + 1 where id = %d;", id)); err != nil {
			return err
		}
		if _, err := c.exec(fmt.Sprintf("insert into crash_atomic_a values (%d, 1);", oracleID)); err != nil {
			return err
		}
		if _, err := c.exec(fmt.Sprintf("insert into crash_atomic_b values (%d, -1);", oracleID)); err != nil {
			return err
		}
		if _, err := c.exec(fmt.Sprintf("insert into crash_atomic_unique values (%d, 1);", oracleID)); err != nil {
			return err
		}
		if op%10 == 0 {
			if _, err := c.exec("select count(*) from crash_sql_state;"); err != nil {
				return err
			}
		}
		if err := c.commit(); err != nil {
			return err
		}
		rollback = false
		return nil
	}

	for op := 0; op < minOps; op++ {
		if err := runOne(op); err != nil {
			return err
		}
		if think > 0 {
			time.Sleep(think)
		}
	}
	readyTmp := readyFile + ".tmp"
	if err := os.WriteFile(readyTmp, []byte("ready\n"), 0o644); err != nil {
		return err
	}
	if err := os.Rename(readyTmp, readyFile); err != nil {
		return err
	}
	for op := minOps; ; op++ {
		if err := runOne(op); err != nil {
			// A killed server closes the socket; that is the expected terminal
			// condition for the crash harness.
			if errors.Is(err, errAbort) || strings.Contains(err.Error(), "closed") ||
				strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "connection reset") {
				return nil
			}
			return err
		}
		if think > 0 {
			time.Sleep(think)
		}
	}
}

func workerSeed(seed int64, round, workerID int) int64 {
	// SplitMix64 gives each stable (seed, round, worker) tuple a well-separated
	// stream without depending on goroutine scheduling or wall-clock time.
	value := uint64(seed) + uint64(round)*0x9e3779b97f4a7c15 + uint64(workerID)*0xbf58476d1ce4e5b9
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return int64(value ^ (value >> 31))
}

type workerReport struct {
	result *result
	ledger *txnLedger
	err    error
}

type officialWorkerReport struct {
	warmup  *result
	windows []*result
	// ledger covers every transaction this worker committed, including the warmup
	// and the ones that finished after the last measurement window. Row counts in
	// the recovered database reflect those too, so the reconciliation must see
	// them even though no measurement bucket owns them.
	ledger *txnLedger
	err    error
}

type conflictRetryOutcome int

const (
	conflictRetryCompleted conflictRetryOutcome = iota
	conflictRetryDeadline
	conflictRetryExhausted
	conflictRetryStopped
)

const (
	defaultMaxConflictRetries = 1
	unlimitedConflictRetries  = -1
)

// runTxnWithConflictRetries keeps the logical transaction inputs in the caller:
// attempt must recreate only the per-attempt RNG from the same seed. Exhausting
// a finite retry budget and reaching the phase deadline are both abandoned
// logical attempts in the benchmark report.
func runTxnWithConflictRetries(phaseEnd time.Time, maxConflictRetries int, stop <-chan struct{},
	now func() time.Time, attempt func() error) (error, conflictRetryOutcome) {
	retries := 0
	for now().Before(phaseEnd) {
		select {
		case <-stop:
			return nil, conflictRetryStopped
		default:
		}
		err := attempt()
		if !errors.Is(err, errAbort) {
			return err, conflictRetryCompleted
		}
		if maxConflictRetries >= 0 && retries >= maxConflictRetries {
			return err, conflictRetryExhausted
		}
		retries++
	}
	return nil, conflictRetryDeadline
}

// attribute maps a transaction completion instant onto the bucket that owns it.
// The official rate counts the NewOrder transactions whose COMMIT succeeded
// inside the window (final.md:214), so attribution follows the completion time
// and not the time the transaction started. A transaction that finishes after
// the last window belongs to no bucket and returns a nil result.
func (r *officialWorkerReport) attribute(finish, warmupEnd time.Time, measure time.Duration) (string, *result, int) {
	if finish.Before(warmupEnd) {
		return "warmup", r.warmup, -1
	}
	window := int(finish.Sub(warmupEnd) / measure)
	if window >= len(r.windows) {
		return "", nil, window
	}
	return "measure", r.windows[window], window
}

func runWorker(workerID, round int, seed int64, p profile, policy string, warmupEnd, measureEnd time.Time,
	measureSeconds int, think time.Duration, reconnectEachTxn bool, maxConflictRetries int, stats *liveStats,
	stop <-chan struct{}, output chan<- workerReport, factory backendFactory, edgeSinks ...paymentEdgeSink) {
	runWorkerWithMaxProvenance(workerID, round, seed, p, policy, warmupEnd, measureEnd, measureSeconds, think,
		reconnectEachTxn, maxConflictRetries, stats, stop, output, factory, nil, edgeSinks...)
}

func runWorkerWithMaxProvenance(workerID, round int, seed int64, p profile, policy string, warmupEnd, measureEnd time.Time,
	measureSeconds int, think time.Duration, reconnectEachTxn bool, maxConflictRetries int, stats *liveStats,
	stop <-chan struct{}, output chan<- workerReport, factory backendFactory, maxProvenance *maxProvenanceObserver,
	edgeSinks ...paymentEdgeSink) {
	var edgeSink paymentEdgeSink
	if len(edgeSinks) > 0 {
		edgeSink = edgeSinks[0]
	}
	rng := rand.New(rand.NewSource(workerSeed(seed, round, workerID)))
	local := newResult(measureSeconds)
	total, attempt := newTxnLedgerWithSink(edgeSink), newTxnLedger()
	report := func(err error) {
		output <- workerReport{result: local, ledger: total, err: err}
	}
	c, err := factory()
	if err != nil {
		local.record("warmup", "connect", "backend-error", 0, err.Error())
		report(fmt.Errorf("worker %d initial connect: %w", workerID, err))
		return
	}
	defer func() { c.close() }()
	for {
		select {
		case <-stop:
			report(nil)
			return
		default:
		}
		now := time.Now()
		phase := ""
		if now.Before(warmupEnd) {
			phase = "warmup"
		} else if now.Before(measureEnd) {
			phase = "measure"
		} else {
			break
		}
		txnType := chooseTxn(rng)
		ctx := chooseContext(p, workerID, policy, rng)
		ctx.ledger = attempt
		attemptSeed := rng.Int63()
		phaseEnd := measureEnd
		if phase == "warmup" {
			phaseEnd = warmupEnd
		}
		latencyBackend, _ := c.(latencyTxnBackend)
		if latencyBackend != nil {
			latencyBackend.beginLatencyPhaseTxn()
		}
		start := time.Now()
		attempts := 0
		err, retryOutcome := runTxnWithConflictRetries(phaseEnd, maxConflictRetries, stop, time.Now, func() error {
			attempts++
			attempt.reset()
			return runTxn(c, txnType, ctx, rand.New(rand.NewSource(attemptSeed)))
		})
		if retryOutcome == conflictRetryStopped {
			if latencyBackend != nil {
				latencyBackend.finishLatencyPhaseTxn(phase, false, attempts > 1)
			}
			report(nil)
			return
		}
		if retryOutcome == conflictRetryDeadline || retryOutcome == conflictRetryExhausted {
			if latencyBackend != nil {
				latencyBackend.finishLatencyPhaseTxn(phase, false, attempts > 1)
			}
			latency := float64(time.Since(start).Microseconds()) / 1000.0
			detail := "conflict retry budget exhausted"
			if retryOutcome == conflictRetryDeadline {
				detail = "phase deadline reached during conflict retry"
			}
			local.record(phase, txnType, "abandoned", latency, detail)
			stats.record(phase, txnType, "abandoned")
			continue
		}
		finish := time.Now()
		latency := float64(finish.Sub(start).Microseconds()) / 1000.0
		if latencyBackend != nil {
			latencyBackend.finishLatencyPhaseTxn(phase, err == nil, attempts > 1)
		}
		if err == nil || errors.Is(err, errInvalidItem) {
			if mergeErr := total.merge(attempt); mergeErr != nil {
				c.rollback()
				local.record(phase, txnType, "backend-error", latency, mergeErr.Error())
				stats.record(phase, txnType, "backend-error")
				report(fmt.Errorf("worker %d %s payment evidence: %w", workerID, txnType, mergeErr))
				return
			}
		}

		if err == nil {
			if phase == "measure" && maxProvenance != nil {
				maxProvenance.recordMeasureCommit(finish, txnType, latency, attempts)
			}
			local.record(phase, txnType, "commit", latency, "")
			stats.record(phase, txnType, "commit")
		} else if errors.Is(err, errInvalidItem) {
			local.record(phase, txnType, "invalid-item-rollback", latency, err.Error())
			stats.record(phase, txnType, "invalid-item-rollback")
		} else if errors.Is(err, errAbort) {
			local.record(phase, txnType, "server-abort", latency, err.Error())
			stats.record(phase, txnType, "server-abort")
		} else {
			c.rollback()
			local.record(phase, txnType, "backend-error", latency, err.Error())
			stats.record(phase, txnType, "backend-error")
			report(fmt.Errorf("worker %d %s transaction: %w", workerID, txnType, err))
			return
		}
		if reconnectEachTxn {
			c.close()
			if c, err = factory(); err != nil {
				local.record(phase, txnType, "backend-error", 0, err.Error())
				stats.record(phase, txnType, "backend-error")
				report(fmt.Errorf("worker %d reconnect after %s: %w", workerID, txnType, err))
				return
			}
		}
		if think > 0 {
			select {
			case <-stop:
				report(nil)
				return
			case <-time.After(think):
			}
		}
	}
	report(nil)
}

func runRound(round, workers int, seed int64, p profile, policy string, warmupEnd, measureEnd time.Time,
	measureSeconds int, think time.Duration, reconnectEachTxn bool, maxConflictRetries int, stats *liveStats,
	factory backendFactory, ledger *txnLedger, edgeSinks ...paymentEdgeSink) (*result, error) {
	return runRoundWithMaxProvenance(round, workers, seed, p, policy, warmupEnd, measureEnd, measureSeconds, think,
		reconnectEachTxn, maxConflictRetries, stats, factory, ledger, nil, edgeSinks...)
}

func runRoundWithMaxProvenance(round, workers int, seed int64, p profile, policy string, warmupEnd, measureEnd time.Time,
	measureSeconds int, think time.Duration, reconnectEachTxn bool, maxConflictRetries int, stats *liveStats,
	factory backendFactory, ledger *txnLedger, maxProvenance *maxProvenanceObserver,
	edgeSinks ...paymentEdgeSink) (*result, error) {
	var edgeSink paymentEdgeSink
	if len(edgeSinks) > 0 {
		edgeSink = edgeSinks[0]
	}
	partials := make(chan workerReport, workers)
	stop := make(chan struct{})
	var stopOnce sync.Once
	var wg sync.WaitGroup
	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runWorkerWithMaxProvenance(id, round, seed, p, policy, warmupEnd, measureEnd, measureSeconds, think,
				reconnectEachTxn, maxConflictRetries, stats, stop, partials, factory, maxProvenance, edgeSink)
		}(workerID)
	}
	go func() {
		wg.Wait()
		close(partials)
	}()
	combined := newResult(measureSeconds)
	var roundErr error
	for partial := range partials {
		combined.merge(partial.result)
		ledger.merge(partial.ledger)
		if partial.err != nil && roundErr == nil {
			roundErr = partial.err
			stopOnce.Do(func() { close(stop) })
		}
	}
	if roundErr != nil || combined.hasBackendError() {
		if roundErr == nil {
			roundErr = errors.New("round contains a backend error")
		}
		return nil, roundErr
	}
	combined.finalize()
	return combined, nil
}

func runOfficialWorker(workerID, round int, seed int64, p profile, plan *officialRoutingPlan, warmupEnd time.Time,
	measure time.Duration, windows int, think time.Duration, maxConflictRetries int, stats []*liveStats, stop <-chan struct{},
	output chan<- officialWorkerReport, c txnBackend, edgeSinks ...paymentEdgeSink) {
	runOfficialWorkerWithMaxProvenance(workerID, round, seed, p, plan, warmupEnd, measure, windows, think,
		maxConflictRetries, stats, stop, output, c, nil, edgeSinks...)
}

func runOfficialWorkerWithMaxProvenance(workerID, round int, seed int64, p profile, plan *officialRoutingPlan,
	warmupEnd time.Time, measure time.Duration, windows int, think time.Duration, maxConflictRetries int,
	stats []*liveStats, stop <-chan struct{}, output chan<- officialWorkerReport, c txnBackend,
	maxProvenance *maxProvenanceObserver, edgeSinks ...paymentEdgeSink) {
	var edgeSink paymentEdgeSink
	if len(edgeSinks) > 0 {
		edgeSink = edgeSinks[0]
	}
	rng := rand.New(rand.NewSource(workerSeed(seed, round, workerID)))
	router := newOfficialRouter(plan, workerID)
	report := officialWorkerReport{warmup: newResult(0), windows: make([]*result, windows), ledger: newTxnLedgerWithSink(edgeSink)}
	attempt := newTxnLedger()
	for window := range report.windows {
		report.windows[window] = newResult(int(measure / time.Second))
	}
	defer func() {
		c.close()
		output <- report
	}()
	measureEnd := warmupEnd.Add(time.Duration(windows) * measure)
	for {
		select {
		case <-stop:
			return
		default:
		}
		now := time.Now()
		if !now.Before(measureEnd) {
			return
		}

		txnType := chooseTxn(rng)
		routingPhase := 0
		if !now.Before(warmupEnd) {
			routingPhase = int(now.Sub(warmupEnd)/measure) + 1
		}
		ctx := router.next(routingPhase)
		ctx.ledger = attempt
		attemptSeed := rng.Int63()
		phaseEnd := measureEnd
		if now.Before(warmupEnd) {
			phaseEnd = warmupEnd
		}
		latencyBackend, _ := c.(latencyTxnBackend)
		if latencyBackend != nil {
			latencyBackend.beginLatencyPhaseTxn()
		}
		start := time.Now()
		attempts := 0
		err, retryOutcome := runTxnWithConflictRetries(phaseEnd, maxConflictRetries, stop, time.Now, func() error {
			attempts++
			attempt.reset()
			return runTxn(c, txnType, ctx, rand.New(rand.NewSource(attemptSeed)))
		})
		if retryOutcome == conflictRetryStopped {
			if latencyBackend != nil {
				latencyBackend.finishLatencyPhaseTxn("warmup", false, attempts > 1)
			}
			return
		}
		if retryOutcome == conflictRetryDeadline || retryOutcome == conflictRetryExhausted {
			phase, local, window := report.attribute(start, warmupEnd, measure)
			if latencyBackend != nil {
				latencyBackend.finishLatencyPhaseTxn(phase, false, attempts > 1)
			}
			if local != nil {
				latency := float64(time.Since(start).Microseconds()) / 1000.0
				detail := "conflict retry budget exhausted"
				if retryOutcome == conflictRetryDeadline {
					detail = "phase deadline reached during conflict retry"
				}
				local.record(phase, txnType, "abandoned", latency, detail)
				stats[window+1].record(phase, txnType, "abandoned")
			}
			continue
		}
		finish := time.Now()
		latency := float64(finish.Sub(start).Microseconds()) / 1000.0
		// The ledger is folded in before attribution: the database was changed
		// regardless of which measurement bucket, if any, owns the completion.
		if err == nil || errors.Is(err, errInvalidItem) {
			if mergeErr := report.ledger.merge(attempt); mergeErr != nil {
				report.err = fmt.Errorf("worker %d %s payment evidence: %w", workerID, txnType, mergeErr)
				return
			}
		}
		phase, local, window := report.attribute(finish, warmupEnd, measure)
		if latencyBackend != nil {
			latencyBackend.finishLatencyPhaseTxn(phase, err == nil, attempts > 1)
		}
		if local == nil {
			// Completed after the final window: it belongs to no measurement
			// interval. A backend error still invalidates the run.
			if err != nil && !errors.Is(err, errInvalidItem) && !errors.Is(err, errAbort) {
				c.rollback()
				report.err = fmt.Errorf("worker %d %s transaction: %w", workerID, txnType, err)
			}
			return
		}
		phaseStats := stats[window+1]

		if err == nil {
			if phase == "measure" && maxProvenance != nil {
				maxProvenance.recordMeasureCommit(finish, txnType, latency, attempts)
			}
			local.record(phase, txnType, "commit", latency, "")
			local.recordCompletion(ctx.wID, int(attempt.values[ledgerDeliveryOrders]))
			phaseStats.record(phase, txnType, "commit")
		} else if errors.Is(err, errInvalidItem) {
			local.record(phase, txnType, "invalid-item-rollback", latency, err.Error())
			local.recordCompletion(ctx.wID, 0)
			phaseStats.record(phase, txnType, "invalid-item-rollback")
		} else if errors.Is(err, errAbort) {
			local.record(phase, txnType, "server-abort", latency, err.Error())
			phaseStats.record(phase, txnType, "server-abort")
		} else {
			c.rollback()
			local.record(phase, txnType, "backend-error", latency, err.Error())
			phaseStats.record(phase, txnType, "backend-error")
			report.err = fmt.Errorf("worker %d %s transaction: %w", workerID, txnType, err)
			return
		}
		if think > 0 {
			select {
			case <-stop:
				return
			case <-time.After(think):
			}
		}
	}
}

func monitorOfficialProgress(rounds, warmup, measure, interval int, start time.Time, stats []*liveStats, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()
	warmupEnd := start.Add(time.Duration(warmup) * time.Second)
	measureDuration := time.Duration(measure) * time.Second
	finalEnd := warmupEnd.Add(time.Duration(rounds) * measureDuration)
	for {
		select {
		case <-stop:
			return
		case now := <-ticker.C:
			if now.Before(warmupEnd) {
				printProgress(0, rounds, "warmup", int(now.Sub(start).Seconds()), warmup, stats[0])
			} else if now.Before(finalEnd) {
				round := int(now.Sub(warmupEnd)/measureDuration) + 1
				windowStart := warmupEnd.Add(time.Duration(round-1) * measureDuration)
				printProgress(round, rounds, "measure", int(now.Sub(windowStart).Seconds()), measure, stats[round])
			} else {
				return
			}
		}
	}
}

const walPhaseMarkerPayloadMax = 256

type walPhaseMarker struct {
	phase     string
	window    int
	planned   time.Time
	actual    time.Time
	lateness  time.Duration
}

type walPhaseMarkerSender func(walPhaseMarker) (walPhaseMarker, error)
type walPhaseWaiter func(time.Time, <-chan struct{}) bool

type walPhaseMarkerRunResult struct {
	sent     int
	canceled bool
	err      error
}

func walPhaseMarkerDeadlines(start time.Time, warmup, measure time.Duration, rounds int) []walPhaseMarker {
	warmupEnd := start.Add(warmup)
	markers := make([]walPhaseMarker, 0, rounds+1)
	markers = append(markers, walPhaseMarker{phase: "measure_start", window: 0, planned: warmupEnd})
	for window := 1; window <= rounds; window++ {
		markers = append(markers, walPhaseMarker{
			phase: "window_end", window: window, planned: warmupEnd.Add(time.Duration(window) * measure),
		})
	}
	return markers
}

func validateWalPhaseMarker(marker walPhaseMarker) error {
	if marker.phase == "measure_start" {
		if marker.window != 0 {
			return errors.New("measure_start requires window 0")
		}
	} else if marker.phase == "window_end" {
		if marker.window < 1 {
			return errors.New("window_end requires a positive window")
		}
	} else {
		return fmt.Errorf("unsupported phase %q", marker.phase)
	}
	if marker.planned.IsZero() || marker.actual.IsZero() {
		return errors.New("planned and actual send times are required")
	}
	if marker.lateness < 0 {
		return errors.New("monotonic lateness must be non-negative")
	}
	return nil
}

func encodeWalPhaseMarker(marker walPhaseMarker) ([]byte, error) {
	if err := validateWalPhaseMarker(marker); err != nil {
		return nil, err
	}
	payload := []byte(fmt.Sprintf("v1 phase=%s window=%d planned_unix_ns=%d actual_send_unix_ns=%d monotonic_lateness_ns=%d",
		marker.phase, marker.window, marker.planned.UnixNano(), marker.actual.UnixNano(), marker.lateness.Nanoseconds()))
	if len(payload) > walPhaseMarkerPayloadMax {
		return nil, fmt.Errorf("WAL phase marker payload is %d bytes, limit %d", len(payload), walPhaseMarkerPayloadMax)
	}
	return payload, nil
}

func sendUnixWalPhaseMarker(socketName string, marker walPhaseMarker) (walPhaseMarker, error) {
	address, err := net.ResolveUnixAddr("unixgram", socketName)
	if err != nil {
		return marker, err
	}
	conn, err := net.DialUnix("unixgram", nil, address)
	if err != nil {
		return marker, err
	}
	defer conn.Close()
	if err := conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		return marker, err
	}
	marker.actual = time.Now()
	marker.lateness = marker.actual.Sub(marker.planned)
	payload, err := encodeWalPhaseMarker(marker)
	if err != nil {
		return marker, err
	}
	written, err := conn.Write(payload)
	if err != nil {
		return marker, err
	}
	if written != len(payload) {
		return marker, io.ErrShortWrite
	}
	return marker, nil
}

func validateWalPhaseMarkerSocketName(socketName string) error {
	if socketName == "" {
		return errors.New("WAL phase marker socket name is empty")
	}
	if socketName[0] != '@' || len(socketName) == 1 {
		return errors.New("WAL phase marker socket name must be a non-empty Linux abstract Unix name beginning with @")
	}
	if strings.IndexByte(socketName, 0) >= 0 || len(socketName) > 108 {
		return errors.New("WAL phase marker socket name exceeds the Linux abstract sockaddr_un limit")
	}
	return nil
}

func validateWalPhaseMarkerFlagScope(command, backend, mode, socketName string) error {
	if socketName == "" {
		return nil
	}
	if command != "run" || backend != "rmdb" || mode != "official-equivalent" {
		return errors.New("--wal-phase-marker-socket is supported only by command=run, backend=rmdb, mode=official-equivalent")
	}
	return nil
}

func waitUntilWalPhaseDeadline(deadline time.Time, cancel <-chan struct{}) bool {
	delay := time.Until(deadline)
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-cancel:
		return false
	case <-timer.C:
		return true
	}
}

func runWalPhaseMarkers(start time.Time, warmup, measure time.Duration, rounds int, cancel <-chan struct{},
	wait walPhaseWaiter, now func() time.Time, send walPhaseMarkerSender) <-chan walPhaseMarkerRunResult {
	done := make(chan walPhaseMarkerRunResult, 1)
	go func() {
		defer close(done)
		result := walPhaseMarkerRunResult{}
		var sendErrors []error
		for _, marker := range walPhaseMarkerDeadlines(start, warmup, measure, rounds) {
			if !wait(marker.planned, cancel) {
				result.canceled = true
				result.err = errors.Join(sendErrors...)
				done <- result
				return
			}
			marker.actual = now()
			marker.lateness = marker.actual.Sub(marker.planned)
			marker, err := send(marker)
			result.sent++
			fmt.Printf("[wal-phase] phase=%s window=%d planned_unix_ns=%d actual_send_unix_ns=%d monotonic_lateness_ns=%d error=%v\n",
				marker.phase, marker.window, marker.planned.UnixNano(), marker.actual.UnixNano(), marker.lateness.Nanoseconds(), err)
			if err != nil {
				sendErrors = append(sendErrors, fmt.Errorf("%s window %d: %w", marker.phase, marker.window, err))
			}
		}
		result.err = errors.Join(sendErrors...)
		done <- result
	}()
	return done
}

func runOfficialWindows(rounds, workers int, seed int64, p profile, warmup, measure, progress int, think time.Duration,
	maxConflictRetries int, reconnectEachTxn bool, roundOffset int, factory backendFactory,
	ledger *txnLedger, walPhaseMarkerSocket string, edgeSinks ...paymentEdgeSink) ([]*result, error) {
	return runOfficialWindowsWithMaxProvenance(rounds, workers, seed, p, warmup, measure, progress, think,
		maxConflictRetries, reconnectEachTxn, roundOffset, factory, ledger, walPhaseMarkerSocket, nil, edgeSinks...)
}

func runOfficialWindowsWithMaxProvenance(rounds, workers int, seed int64, p profile, warmup, measure, progress int,
	think time.Duration, maxConflictRetries int, reconnectEachTxn bool, roundOffset int, factory backendFactory,
	ledger *txnLedger, walPhaseMarkerSocket string, maxProvenance *maxProvenanceObserver,
	edgeSinks ...paymentEdgeSink) ([]*result, error) {
	var edgeSink paymentEdgeSink
	if len(edgeSinks) > 0 {
		edgeSink = edgeSinks[0]
	}
	if reconnectEachTxn {
		return nil, errors.New("official-equivalent does not allow reconnect-each-txn")
	}
	backends := make([]txnBackend, 0, workers)
	plan, err := newOfficialRoutingPlan(seed, p, rounds+1)
	if err != nil {
		return nil, err
	}
	for workerID := 0; workerID < workers; workerID++ {
		backend, err := factory()
		if err != nil {
			for _, connected := range backends {
				connected.close()
			}
			return nil, fmt.Errorf("official worker %d initial connect: %w", workerID, err)
		}
		backends = append(backends, backend)
	}

	stats := make([]*liveStats, rounds+1)
	for i := range stats {
		stats[i] = &liveStats{}
	}
	start := time.Now()
	warmupEnd := start.Add(time.Duration(warmup) * time.Second)
	measureDuration := time.Duration(measure) * time.Second
	stop := make(chan struct{})
	phaseMarkerCancel := make(chan struct{})
	var phaseMarkerDone <-chan walPhaseMarkerRunResult
	if walPhaseMarkerSocket != "" {
		phaseMarkerDone = runWalPhaseMarkers(start, time.Duration(warmup)*time.Second, measureDuration, rounds,
			phaseMarkerCancel, waitUntilWalPhaseDeadline, time.Now,
			func(marker walPhaseMarker) (walPhaseMarker, error) {
				return sendUnixWalPhaseMarker(walPhaseMarkerSocket, marker)
			})
	}
	partials := make(chan officialWorkerReport, workers)
	monitorStop := make(chan struct{})
	monitorDone := make(chan struct{})
	printProgress(0, rounds, "warmup", 0, warmup, stats[0])
	go monitorOfficialProgress(rounds, warmup, measure, progress, start, stats, monitorStop, monitorDone)
	for workerID, backend := range backends {
		go runOfficialWorkerWithMaxProvenance(workerID, roundOffset+1, seed, p, plan, warmupEnd, measureDuration,
			rounds, think, maxConflictRetries, stats, stop, partials, backend, maxProvenance, edgeSink)
	}

	warmupResult := newResult(0)
	windows := make([]*result, rounds)
	for round := range windows {
		windows[round] = newResult(measure)
	}
	var runErr error
	for workerID := 0; workerID < workers; workerID++ {
		partial := <-partials
		warmupResult.merge(partial.warmup)
		ledger.merge(partial.ledger)
		for round := range windows {
			windows[round].merge(partial.windows[round])
		}
		if partial.err != nil && runErr == nil {
			runErr = partial.err
			close(stop)
			if phaseMarkerDone != nil {
				close(phaseMarkerCancel)
			}
		}
	}
	close(monitorStop)
	<-monitorDone
	var phaseMarkerResult walPhaseMarkerRunResult
	if phaseMarkerDone != nil {
		phaseMarkerResult = <-phaseMarkerDone
	}
	if runErr != nil || warmupResult.hasBackendError() {
		if runErr == nil {
			runErr = errors.New("warmup contains a backend error")
		}
		return nil, fmt.Errorf("official run invalid: %w", runErr)
	}
	for round, window := range windows {
		if window.hasBackendError() {
			return nil, fmt.Errorf("official measurement window %d contains a backend error", round+1)
		}
		window.finalize()
		for _, txnType := range []string{"new_order", "payment", "order_status", "delivery", "stock_level"} {
			if window.Committed[txnType] < 1 {
				return nil, fmt.Errorf("official measurement window %d has no committed %s transaction", round+1, txnType)
			}
		}
		if window.Coverage.DeliveryProcessed < 1 {
			return nil, fmt.Errorf("official measurement window %d has no Delivery that processed a queued order", round+1)
		}
		if err := applyCoverageGate(window, plan.hotWarehouses, false); err != nil {
			return nil, fmt.Errorf("official measurement window %d: %w", round+1, err)
		}
	}
	combinedCoverage := newResult(measure * rounds)
	for _, window := range windows {
		combinedCoverage.merge(window)
	}
	combinedCoverage.finalize()
	if err := applyCoverageGate(combinedCoverage, plan.hotWarehouses, true); err != nil {
		return nil, fmt.Errorf("official combined windows: %w", err)
	}
	for round, window := range windows {
		attempted, committed, expectedRollback, abandoned, completion := window.reportTotals()
		fmt.Printf("[official window %d/%d] tpmC=%.2f attempted=%d committed=%d expected_rollback=%d abandoned=%d completion=%.2f%% abort_rate=%.2f%%\n",
			round+1, rounds, window.NewOrderPerMin, attempted, committed, expectedRollback, abandoned,
			completion*100, window.AbortRate*100)
	}
	if phaseMarkerResult.err != nil {
		return windows, fmt.Errorf("WAL phase marker delivery failed after workload: %w", phaseMarkerResult.err)
	}
	return windows, nil
}

func inspectProfile(c txnBackend) (profile, error) {
	queries := []string{"select count(*) from warehouse;", "select max(d_id) from district;", "select max(c_id) from customer;", "select max(i_id) from item;"}
	values := make([]int, len(queries))
	for i, query := range queries {
		text, err := c.exec(query)
		if err != nil {
			return profile{}, err
		}
		values[i] = max(1, scalarInt(text, 1))
	}
	return profile{warehouses: values[0], districtsPerWarehouse: values[1], customersPerDistrict: values[2], itemCount: values[3]}, nil
}

type config struct {
	Mode                   string `json:"mode"`
	Backend                string `json:"backend"`
	Isolation              string `json:"isolation"`
	SQLitePath             string `json:"sqlite_path,omitempty"`
	SQLiteBegin            string `json:"sqlite_begin,omitempty"`
	Warehouses             int    `json:"warehouses"`
	Workers                int    `json:"workers"`
	Warmup                 int    `json:"warmup"`
	Measure                int    `json:"measure"`
	Rounds                 int    `json:"rounds"`
	ProgressInterval       int    `json:"progress_interval"`
	Seed                   int64  `json:"seed"`
	Think                  string `json:"think"`
	ReconnectEachTxn       bool   `json:"reconnect_each_txn"`
	MaxConflictRetries     int    `json:"max_conflict_retries"`
	WarehousePolicy        string `json:"warehouse_policy"`
	BaselineWarehouseTotal int    `json:"baseline_warehouse_total"`
	BaselineDistrictTotal  int    `json:"baseline_district_total"`
	BaselineCustomerTotal  int    `json:"baseline_customer_total"`
	BaselineItemTotal      int    `json:"baseline_item_total"`
	BaselineStockTotal     int    `json:"baseline_stock_total"`
	BaselineOrdersTotal    int    `json:"baseline_orders_total"`
}

func (value *config) UnmarshalJSON(data []byte) error {
	type configJSON config
	// Results written before the finite retry default had no field and used
	// unlimited retries, so preserve their historical interpretation.
	decoded := configJSON{MaxConflictRetries: unlimitedConflictRetries}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*value = config(decoded)
	return nil
}

type document struct {
	Config config `json:"config"`
	// Baselines is the aggregate snapshot of the freshly loaded database, taken
	// before the first warmup transaction. Ledger is the total effect of every
	// transaction this run committed, including the warmup and the transactions
	// that finished after the last measurement window. Together they let the
	// post-crash validation reconcile the recovered database from the published
	// result artifacts without any in-memory state surviving the crash (final.md:322).
	Baselines map[string]float64 `json:"baselines,omitempty"`
	Ledger    map[string]float64 `json:"ledger,omitempty"`
	// PaymentEdges is the legacy inline form of the committed per-key binary32
	// update graph. New runs validate this graph online and publish only its
	// compact terminal summary.
	PaymentEdges           []paymentFloatEdge `json:"payment_float_edges,omitempty"`
	PaymentEdgeCount       int                `json:"payment_edge_count,omitempty"`
	PaymentChainTerminals  map[string]uint32  `json:"payment_chain_terminals,omitempty"`
	PaymentTerminalBits    map[string]uint32  `json:"payment_terminal_bits,omitempty"`
	OnlinePaymentValidated bool               `json:"online_payment_validated,omitempty"`
	// OnlineFloatBits captures the seven pre-crash aggregate cells as raw
	// binary32 so recovery validation never passes through decimal tolerance.
	OnlineFloatBits map[string]uint32 `json:"online_float_bits,omitempty"`
	MedianTPMC      float64           `json:"median_tpmc"`
	Rounds          []*result         `json:"rounds"`
	// paymentEdgesPath is an internal lazy sidecar reference for legacy results.
	paymentEdgesPath string
}

type publicConfig struct {
	Mode               string `json:"mode"`
	Backend            string `json:"backend"`
	Isolation          string `json:"isolation"`
	SQLitePath         string `json:"sqlite_path,omitempty"`
	SQLiteBegin        string `json:"sqlite_begin,omitempty"`
	Warehouses         int    `json:"warehouses"`
	Workers            int    `json:"workers"`
	Warmup             int    `json:"warmup"`
	Measure            int    `json:"measure"`
	Rounds             int    `json:"rounds"`
	ProgressInterval   int    `json:"progress_interval"`
	Seed               int64  `json:"seed"`
	Think              string `json:"think"`
	ReconnectEachTxn   bool   `json:"reconnect_each_txn"`
	MaxConflictRetries int    `json:"max_conflict_retries"`
	WarehousePolicy    string `json:"warehouse_policy"`
}

type publicResult struct {
	MeasureSeconds   int                       `json:"measure_seconds"`
	TPMC             float64                   `json:"tpmc"`
	TxnTPM           map[string]float64        `json:"txn_tpm"`
	Attempted        map[string]int            `json:"attempted"`
	Committed        map[string]int            `json:"committed"`
	ExpectedRollback map[string]int            `json:"expected_rollback"`
	Abandoned        map[string]int            `json:"abandoned"`
	Completion       map[string]float64        `json:"completion"`
	AbortRate        float64                   `json:"abort_rate"`
	LatencyMS        map[string]latencySummary `json:"latency_ms"`
}

type publicDocument struct {
	Config     publicConfig    `json:"config"`
	MedianTPMC float64         `json:"median_tpmc"`
	Rounds     []*publicResult `json:"rounds"`
}

type validationState struct {
	Config                 config             `json:"config"`
	Baselines              map[string]float64 `json:"baselines,omitempty"`
	Ledger                 map[string]float64 `json:"ledger,omitempty"`
	PaymentEdgeCount       int                `json:"payment_edge_count,omitempty"`
	PaymentChainTerminals  map[string]uint32  `json:"payment_chain_terminals,omitempty"`
	OnlinePaymentValidated bool               `json:"online_payment_validated,omitempty"`
	OnlineFloatBits        map[string]uint32  `json:"online_float_bits,omitempty"`
	Rounds                 []*result          `json:"rounds"`
}

func makePublicDocument(doc document) publicDocument {
	cfg := doc.Config
	sqlitePath, sqliteBegin := "", ""
	if cfg.Backend == "sqlite" {
		sqlitePath, sqliteBegin = cfg.SQLitePath, cfg.SQLiteBegin
	}
	output := publicDocument{
		Config: publicConfig{
			Mode: cfg.Mode, Backend: cfg.Backend, Isolation: cfg.Isolation, SQLitePath: sqlitePath,
			SQLiteBegin: sqliteBegin, Warehouses: cfg.Warehouses, Workers: cfg.Workers, Warmup: cfg.Warmup,
			Measure: cfg.Measure, Rounds: cfg.Rounds, ProgressInterval: cfg.ProgressInterval, Seed: cfg.Seed,
			Think: cfg.Think, ReconnectEachTxn: cfg.ReconnectEachTxn, MaxConflictRetries: cfg.MaxConflictRetries,
			WarehousePolicy: cfg.WarehousePolicy,
		},
		MedianTPMC: doc.MedianTPMC,
		Rounds:     make([]*publicResult, 0, len(doc.Rounds)),
	}
	for _, window := range doc.Rounds {
		output.Rounds = append(output.Rounds, &publicResult{
			MeasureSeconds: window.MeasureSeconds, TPMC: window.TPMC, TxnTPM: window.TxnTPM,
			Attempted: window.Attempted, Committed: window.Committed, ExpectedRollback: window.ExpectedRollback,
			Abandoned: window.Abandoned, Completion: window.Completion, AbortRate: window.AbortRate,
			LatencyMS: window.LatencyMS,
		})
	}
	return output
}

func makeValidationState(doc document) validationState {
	return validationState{
		Config: doc.Config, Baselines: doc.Baselines, Ledger: doc.Ledger, PaymentEdgeCount: doc.PaymentEdgeCount,
		PaymentChainTerminals: doc.PaymentChainTerminals, OnlinePaymentValidated: doc.OnlinePaymentValidated,
		OnlineFloatBits: doc.OnlineFloatBits, Rounds: doc.Rounds,
	}
}

func validateMaxConflictRetries(value int) error {
	if value < unlimitedConflictRetries {
		return fmt.Errorf("max_conflict_retries must be -1 or greater, got %d", value)
	}
	return nil
}

func validateResultDocument(doc document) error {
	if err := validateMaxConflictRetries(doc.Config.MaxConflictRetries); err != nil {
		return fmt.Errorf("result config: %w", err)
	}
	if doc.Config.Rounds < 1 || len(doc.Rounds) != doc.Config.Rounds {
		return fmt.Errorf("result has %d rounds, want %d", len(doc.Rounds), doc.Config.Rounds)
	}
	if doc.Config.Measure < 1 {
		return errors.New("result has non-positive measurement duration")
	}
	for round, window := range doc.Rounds {
		if err := validateResultWindow(window, doc.Config.Measure, doc.Config.Mode, round+1); err != nil {
			return err
		}
	}
	if doc.Config.Mode == "official-equivalent" {
		combined := newResult(doc.Config.Measure * doc.Config.Rounds)
		hot := make(map[int]struct{}, officialHotWarehouseCount)
		for _, window := range doc.Rounds {
			combined.Coverage.Completed += window.Coverage.Completed
			for _, wID := range window.Coverage.Warehouses {
				combined.covered[wID] = struct{}{}
			}
			for _, wID := range window.Coverage.HotWarehouses {
				hot[wID] = struct{}{}
			}
		}
		combined.finalize()
		hotIDs := make([]int, 0, len(hot))
		for wID := range hot {
			hotIDs = append(hotIDs, wID)
		}
		if err := applyCoverageGate(combined, hotIDs, true); err != nil {
			return fmt.Errorf("combined coverage: %w", err)
		}
		if combined.Coverage.Completed >= 400 && len(hot) != officialHotWarehouseCount {
			return fmt.Errorf("combined coverage includes %d/%d hot warehouses", len(hot), officialHotWarehouseCount)
		}
	}
	if math.IsNaN(doc.MedianTPMC) || math.IsInf(doc.MedianTPMC, 0) || doc.MedianTPMC < 0 {
		return fmt.Errorf("result has invalid median tpmC %.6g", doc.MedianTPMC)
	}
	if doc.PaymentChainTerminals != nil || doc.PaymentEdgeCount != 0 {
		expectedEdges := int(math.Round(doc.Ledger[ledgerPaymentCommits])) * 2
		if doc.PaymentEdgeCount != expectedEdges {
			return fmt.Errorf("result has %d Payment FLOAT32 edges, want %d", doc.PaymentEdgeCount, expectedEdges)
		}
		if expectedEdges > 0 && len(doc.PaymentChainTerminals) == 0 {
			return errors.New("result has Payment edges but no chain terminals")
		}
	}
	return nil
}

func validateResultWindow(window *result, measure int, mode string, round int) error {
	if window == nil {
		return fmt.Errorf("result round %d is nil", round)
	}
	if window.MeasureSeconds != measure {
		return fmt.Errorf("result round %d measures %d seconds, want %d", round, window.MeasureSeconds, measure)
	}
	if window.hasBackendError() {
		return fmt.Errorf("result round %d contains a backend error", round)
	}
	if math.IsNaN(window.TPMC) || math.IsInf(window.TPMC, 0) || window.TPMC < 0 {
		return fmt.Errorf("result round %d has invalid tpmC %.6g", round, window.TPMC)
	}
	if window.MeasureSeconds <= 0 {
		return fmt.Errorf("result round %d has non-positive measurement duration", round)
	}
	measureSeconds := float64(window.MeasureSeconds)
	for txnType, committed := range window.Committed {
		if committed < 0 {
			return fmt.Errorf("result round %d has negative committed count for %s", round, txnType)
		}
		if window.Counts["measure"][txnType]["commit"] != committed {
			return fmt.Errorf("result round %d committed/count mismatch for %s", round, txnType)
		}
		expectedTPM := float64(committed) / measureSeconds * 60
		if math.Abs(window.TxnTPM[txnType]-expectedTPM) > 1e-9 {
			return fmt.Errorf("result round %d txn_tpm mismatch for %s", round, txnType)
		}
	}
	newOrders := window.Committed["new_order"]
	expectedTPMC := float64(newOrders) / measureSeconds * 60
	if math.Abs(window.TPMC-expectedTPMC) > 1e-9 || math.Abs(window.NewOrderPerMin-expectedTPMC) > 1e-9 {
		return fmt.Errorf("result round %d new_order throughput mismatch", round)
	}
	measureCounts := window.Counts["measure"]
	attemptedTotal, committedTotal, abortTotal := 0, 0, 0
	for txnType, outcomes := range measureCounts {
		attempted := 0
		for outcome, count := range outcomes {
			if count < 0 {
				return fmt.Errorf("result round %d has negative count for %s/%s", round, txnType, outcome)
			}
			attempted += count
		}
		attemptedTotal += attempted
		committedTotal += outcomes["commit"]
		expectedRollback := outcomes["invalid-item-rollback"]
		abortTotal += expectedRollback + outcomes["abandoned"] + outcomes["server-abort"]
		if window.Attempted[txnType] != attempted {
			return fmt.Errorf("result round %d attempted/count mismatch for %s", round, txnType)
		}
		if window.ExpectedRollback[txnType] != expectedRollback {
			return fmt.Errorf("result round %d expected rollback/count mismatch for %s", round, txnType)
		}
		if window.Abandoned[txnType] != outcomes["abandoned"] {
			return fmt.Errorf("result round %d abandoned/count mismatch for %s", round, txnType)
		}
		expectedCompletion := 0.0
		if attempted > 0 {
			expectedCompletion = float64(outcomes["commit"]+expectedRollback) / float64(attempted)
		}
		if math.Abs(window.Completion[txnType]-expectedCompletion) > 1e-9 {
			return fmt.Errorf("result round %d completion mismatch for %s", round, txnType)
		}
	}
	if attemptedTotal > 0 {
		expectedAbortRate := float64(abortTotal) / float64(attemptedTotal)
		if math.Abs(window.AbortRate-expectedAbortRate) > 1e-9 {
			return fmt.Errorf("result round %d abort rate mismatch", round)
		}
	}
	if newOrders < 1 {
		return fmt.Errorf("result round %d has no committed new_order transaction", round)
	}
	if mode == "official-equivalent" {
		for _, txnType := range []string{"payment", "order_status", "delivery", "stock_level"} {
			if window.Committed[txnType] < 1 {
				return fmt.Errorf("result round %d has no committed %s transaction", round, txnType)
			}
		}
		completed := committedTotal + measureCounts["new_order"]["invalid-item-rollback"]
		if window.Coverage.Completed != completed {
			return fmt.Errorf("result round %d coverage completed=%d, want %d from commits + business rollbacks",
				round, window.Coverage.Completed, completed)
		}
		if window.Coverage.DeliveryProcessed < 1 {
			return fmt.Errorf("result round %d has no Delivery that processed a queued order", round)
		}
		seen := make(map[int]struct{}, len(window.Coverage.Warehouses))
		for _, wID := range window.Coverage.Warehouses {
			seen[wID] = struct{}{}
		}
		if len(seen) != window.Coverage.WarehouseCount {
			return fmt.Errorf("result round %d warehouse coverage count mismatch", round)
		}
		required := int(math.Ceil(float64(45*completed) / 400))
		if required > 45 {
			required = 45
		}
		if window.Coverage.RequiredWarehouseCount != required || len(seen) < required {
			return fmt.Errorf("result round %d covers %d warehouses, want at least %d", round, len(seen), required)
		}
		if completed >= 400 && window.Coverage.HotWarehouseCount != officialHotWarehouseCount {
			return fmt.Errorf("result round %d covers %d/%d hot warehouses", round,
				window.Coverage.HotWarehouseCount, officialHotWarehouseCount)
		}
	}
	return nil
}

func publishResultDocument(path string, doc document) error {
	if doc.paymentEdgesPath == "" && len(doc.PaymentEdges) > 0 {
		edgeSink, err := newPaymentEdgeWriter(path)
		if err != nil {
			return err
		}
		if err := edgeSink.write(doc.PaymentEdges); err != nil {
			edgeSink.abort()
			return err
		}
		if err := edgeSink.closeAndPublish(); err != nil {
			return err
		}
		doc.paymentEdgesPath = paymentEdgePath(path)
	}
	if err := validateResultDocument(doc); err != nil {
		return err
	}
	if len(doc.PaymentTerminalBits) > 0 {
		if _, err := paymentTerminalsForConsistency(doc); err != nil {
			return err
		}
		doc.OnlinePaymentValidated = true
		doc.PaymentTerminalBits = nil
	}
	if err := writeJSONAtomic(validationStatePath(path), makeValidationState(doc), false); err != nil {
		return err
	}
	return writeJSONAtomic(path, makePublicDocument(doc), true)
}

func writeJSONAtomic(path string, value any, indent bool) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tpcc-result-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	encoder := json.NewEncoder(tmp)
	if indent {
		encoder.SetIndent("", "  ")
	}
	if err := encoder.Encode(value); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return nil
}

func verifyCrashOracle(address string, timeout time.Duration, isolation, ackPath string) error {
	data, err := os.ReadFile(ackPath)

	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	acked := make(map[int]struct{})
	for _, line := range strings.Fields(string(data)) {
		id, parseErr := strconv.Atoi(line)
		if parseErr != nil {
			return fmt.Errorf("invalid ACK transaction id %q: %w", line, parseErr)
		}
		acked[id] = struct{}{}
	}
	c, err := newClient(address, timeout, isolation)
	if err != nil {
		return err
	}
	defer c.close()
	present := make(map[int]struct{})
	ids := make([]int, 0, len(acked))
	for id := range acked {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	// Keep each response well below RMDB's fixed response buffer. Querying the
	// whole oracle table can silently truncate a large result and produce a
	// false "missing ACK" diagnosis.
	for begin := 0; begin < len(ids); {
		lower := ids[begin]
		upper := lower + 128
		end := begin
		for end < len(ids) && ids[end] < upper {
			end++
		}
		text, queryErr := c.exec(fmt.Sprintf("select txn_id from crash_txn_log where txn_id >= %d and txn_id < %d;", lower, upper))
		if queryErr != nil {
			return queryErr
		}
		for _, row := range parseRows(text) {
			if len(row) > 0 {
				present[scalarInt(row[0], -1)] = struct{}{}
			}
		}
		begin = end
	}
	for id := range acked {
		if _, ok := present[id]; !ok {
			return fmt.Errorf("ACKed transaction %d is missing after recovery", id)
		}
	}
	fmt.Printf("[oracle] verified %d ACKed transactions\n", len(acked))
	return nil
}

func mixedPayloadHash(id int) int {
	value := 17
	for _, ch := range fmt.Sprintf("mixed_sql:%d", id) {
		value = (value*131 + int(ch)) & 0x7fffffff
	}
	return value
}

func readIDFile(path string) (map[int]struct{}, error) {
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	ids := make(map[int]struct{})
	for _, line := range strings.Fields(string(data)) {
		id, parseErr := strconv.Atoi(strings.Split(line, ",")[0])
		if parseErr != nil {
			return nil, fmt.Errorf("invalid transaction id %q: %w", line, parseErr)
		}
		ids[id] = struct{}{}
	}
	return ids, nil
}

func verifyAtomicSums(sumTextA, sumTextB string) error {
	sumRowsA, sumRowsB := parseRows(sumTextA), parseRows(sumTextB)
	if len(sumRowsA) == 0 || len(sumRowsA[0]) == 0 || len(sumRowsB) == 0 || len(sumRowsB[0]) == 0 {
		return errors.New("atomic oracle invariant query returned no value")
	}
	// Parse the complete responses so a negative scalar such as -175 is not
	// mistaken for a separator when passed through parseRows a second time.
	sumA, sumB := scalarInt(sumTextA, 0), scalarInt(sumTextB, 0)
	// The two tables are populated with opposite values for every committed
	// transaction, so their combined sum must remain zero after recovery.
	if sumA+sumB != 0 {
		return fmt.Errorf("atomic oracle sum invariant violated: A=%d B=%d", sumA, sumB)
	}
	return nil
}

func verifyAtomicOracle(address string, timeout time.Duration, isolation, issuedPath, ackPath string) error {
	issued, err := readIDFile(issuedPath)
	if err != nil {
		return err
	}
	acked, err := readIDFile(ackPath)
	if err != nil {
		return err
	}
	if len(issued) == 0 {
		return errors.New("atomic oracle has no issued transactions")
	}
	c, err := newClient(address, timeout, isolation)
	if err != nil {
		return err
	}
	defer c.close()

	ids := make([]int, 0, len(issued))
	for id := range issued {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	marker := make(map[int]int)
	aPresent := make(map[int]struct{})
	bPresent := make(map[int]struct{})
	uniquePresent := make(map[int]struct{})
	for begin := 0; begin < len(ids); {
		lower := ids[begin]
		upper := lower + 128
		end := begin
		for end < len(ids) && ids[end] < upper {
			end++
		}
		queries := []string{
			fmt.Sprintf("select txn_id, payload_hash from crash_txn_log where txn_id >= %d and txn_id < %d;", lower, upper),
			fmt.Sprintf("select id from crash_atomic_a where id >= %d and id < %d;", lower, upper),
			fmt.Sprintf("select id from crash_atomic_b where id >= %d and id < %d;", lower, upper),
			fmt.Sprintf("select id from crash_atomic_unique where id >= %d and id < %d;", lower, upper),
		}
		texts := make([]string, len(queries))
		for i, query := range queries {
			texts[i], err = c.exec(query)
			if err != nil {
				return err
			}
		}
		for _, row := range parseRows(texts[0]) {
			if len(row) >= 2 {
				marker[scalarInt(row[0], -1)] = scalarInt(row[1], -1)
			}
		}
		for _, row := range parseRows(texts[1]) {
			if len(row) > 0 {
				aPresent[scalarInt(row[0], -1)] = struct{}{}
			}
		}
		for _, row := range parseRows(texts[2]) {
			if len(row) > 0 {
				bPresent[scalarInt(row[0], -1)] = struct{}{}
			}
		}
		for _, row := range parseRows(texts[3]) {
			if len(row) > 0 {
				uniquePresent[scalarInt(row[0], -1)] = struct{}{}
			}
		}
		begin = end
	}
	for id := range issued {
		_, hasMarker := marker[id]
		_, hasA := aPresent[id]
		_, hasB := bPresent[id]
		_, hasUnique := uniquePresent[id]
		if _, ok := acked[id]; ok && !hasMarker {
			return fmt.Errorf("ACKed transaction %d is missing its commit marker", id)
		}
		if hasMarker {
			if !hasA || !hasB || !hasUnique || marker[id] != mixedPayloadHash(id) {
				return fmt.Errorf("atomic transaction %d has partial or invalid effects", id)
			}
			if _, ok := acked[id]; ok {
				continue
			}
		} else if hasA || hasB || hasUnique {
			return fmt.Errorf("unmarked transaction %d has partial effects", id)
		}
	}
	sumTextA, err := c.exec("select sum(value) from crash_atomic_a;")
	if err != nil {
		return err
	}
	sumTextB, err := c.exec("select sum(value) from crash_atomic_b;")
	if err != nil {
		return err
	}
	return verifyAtomicSums(sumTextA, sumTextB)
}

func main() {
	latencyPhaseSampleDefault, latencyPhaseSampleEnvErr := latencyPhaseSampleEveryFromEnv(os.Getenv("RMDB_CLIENT_LATENCY_PHASE_SAMPLE_EVERY"))
	command := flag.String("command", "run", "run, feature-check, mixed-sql, data-ready, refresh-manifest, datagen, load, consistency, validate-result, compact-result, oracle-init, oracle-verify, atomic-verify, wait-port, wait-ready, or merge-results")
	mode := flag.String("mode", "official-equivalent", "official-equivalent for rmdb or sqlite-reference for SQLite")
	backend := flag.String("backend", "rmdb", "rmdb or sqlite")
	host := flag.String("host", "127.0.0.1", "RMDB host")
	port := flag.Int("port", 8765, "RMDB port")
	warehouses := flag.Int("warehouses", 1, "warehouses for data generation")
	workers := flag.Int("workers", officialWorkers, "concurrent workers")
	warmup := flag.Int("warmup", officialWarmupSeconds, "warmup seconds")
	measure := flag.Int("measure", officialMeasureSeconds, "measurement seconds")
	rounds := flag.Int("rounds", officialWindows, "benchmark rounds")
	roundOffset := flag.Int("round-offset", 0, "zero-based round offset used for deterministic workload streams")
	isolation := flag.String("isolation", "snapshot-isolation", "read-committed or snapshot-isolation")
	policy := flag.String("warehouse-policy", "terminal-home", "terminal-home or random-per-txn")
	timeout := flag.Duration("timeout", 30*time.Second, "RMDB connection timeout")
	jsonOut := flag.String("json-out", "benchmark/tpcc/result.json", "result JSON path")
	progress := flag.Int("progress-interval", 5, "seconds between live progress lines; 0 disables")
	think := flag.Duration("think", 0, "delay between transactions")
	reconnectEachTxn := flag.Bool("reconnect-each-txn", false, "reconnect after every transaction")
	maxConflictRetries := flag.Int("max-conflict-retries", defaultMaxConflictRetries,
		"maximum retries after the first TRANSACTION_ABORT; -1 retries until the phase deadline (default 1)")
	dataDir := flag.String("data-dir", "benchmark/tpcc/data", "TPC-C CSV directory")
	schemaDir := flag.String("schema-dir", "benchmark/tpcc/schema", "RMDB schema directory")
	rmdbDBDir := flag.String("rmdb-db-dir", "", "RMDB database directory, used to resolve load paths")
	sqlitePath := flag.String("sqlite-path", "benchmark/tpcc/tpcc.sqlite", "SQLite database path")
	sqliteBegin := flag.String("sqlite-begin", "immediate", "SQLite transaction begin mode: immediate or deferred")
	seed := flag.Int64("seed", 1, "data generation seed")
	overwriteData := flag.Bool("overwrite-data-dir", false, "overwrite existing CSV files")
	reuseData := flag.Bool("reuse-data-dir", false, "skip data generation when all CSV files exist")
	resultJSON := flag.String("result-json", "", "existing result JSON used by consistency")
	consistencyStage := flag.String("consistency-stage", "standalone", "consistency stage label")
	waitTimeout := flag.Duration("wait-timeout", 30*time.Second, "wait-port timeout")
	resultInputs := flag.String("result-inputs", "", "comma-separated per-round result JSON files for merge-results")
	oracleAck := flag.String("oracle-ack-file", "", "host-side fsynced ACK file for crash oracle")
	oracleIssued := flag.String("oracle-issued-file", "", "host-side fsynced issued transaction file")
	oraclePrefix := flag.Int("oracle-id-prefix", 0, "positive per-run crash oracle ID prefix")
	sqlOps := flag.Int("sql-ops", 100, "minimum mixed-sql operations before publishing readiness")
	sqlReadyFile := flag.String("sql-ready-file", "", "readiness file for mixed-sql crash workloads")
	allowNonOfficialTiming := flag.Bool("allow-nonofficial-timing", false,
		fmt.Sprintf("allow official-equivalent runs that deviate from workers=%d, warmup=%d, measure=%d, rounds=%d, think=0; results are not comparable to the official ranking",
			officialWorkers, officialWarmupSeconds, officialMeasureSeconds, officialWindows))
	allowLegacyPrepareFlag := flag.Bool("allow-legacy-prepare", false,
		"accept the legacy PREPARE_OK layout that echoes parameter types; the official evaluator does not")
	walPhaseMarkerSocket := flag.String("wal-phase-marker-socket", "",
		"Linux abstract Unix datagram address for diagnostic WAL phase markers during an official rmdb run")
	latencyPhaseObserve := flag.Bool("latency-phase-observe", latencyPhaseEnabledFromEnv(os.Getenv("RMDB_CLIENT_LATENCY_PHASES")),
		"sample client-side EXEC_BATCH encode/write/read/decode phases; also enabled by RMDB_CLIENT_LATENCY_PHASES=1")
	latencyPhaseSampleEvery := flag.Uint64("latency-phase-sample-every", latencyPhaseSampleDefault,
		"sample every N final successful logical transactions per connection when --latency-phase-observe is set")
	latencyPhaseOut := flag.String("latency-phase-out", "", "optional JSON sidecar for --latency-phase-observe; result JSON is unchanged")
	maxProvenance := flag.Bool("max-provenance", maxProvenanceEnabledFromEnv(os.Getenv("RMDB_CLIENT_MAX_PROVENANCE")),
		"record sparse measurement-success max-latency provenance; also enabled by RMDB_CLIENT_MAX_PROVENANCE=1")
	maxProvenanceOut := flag.String("max-provenance-out", "", "JSONL sidecar for --max-provenance; defaults to <json-out>.max.jsonl")
	flag.Parse()
	if latencyPhaseSampleEnvErr != nil {
		fmt.Fprintln(os.Stderr, latencyPhaseSampleEnvErr)
		os.Exit(2)
	}
	if *latencyPhaseSampleEvery == 0 {
		fmt.Fprintln(os.Stderr, "--latency-phase-sample-every must be positive")
		os.Exit(2)
	}
	allowLegacyPrepare = *allowLegacyPrepareFlag
	if *backend != "rmdb" && *backend != "sqlite" {
		fmt.Fprintln(os.Stderr, "--backend must be rmdb or sqlite")
		os.Exit(2)
	}
	if *walPhaseMarkerSocket != "" {
		if err := validateWalPhaseMarkerFlagScope(*command, *backend, *mode, *walPhaseMarkerSocket); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if err := validateWalPhaseMarkerSocketName(*walPhaseMarkerSocket); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
	if *command == "run" {
		if err := validateMaxConflictRetries(*maxConflictRetries); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if err := validateBenchmarkMode(*mode, *workers, *warmup, *measure, *rounds, *think, *allowNonOfficialTiming); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if *mode == "official-equivalent" && *reconnectEachTxn {
			fmt.Fprintln(os.Stderr, "official-equivalent does not allow --reconnect-each-txn")
			os.Exit(2)
		}
		if *mode == "official-equivalent" && *isolation != "snapshot-isolation" {
			fmt.Fprintln(os.Stderr, "official-equivalent requires --isolation snapshot-isolation")
			os.Exit(2)
		}
		if *mode == "official-equivalent" && *allowNonOfficialTiming {
			fmt.Fprintf(os.Stderr, "[warning] --allow-nonofficial-timing: workers=%d warmup=%d measure=%d rounds=%d think=%s deviate from the official %d/%d/%d/%d/0 shape; this result does not predict the official ranking\n",
				*workers, *warmup, *measure, *rounds, *think, officialWorkers, officialWarmupSeconds, officialMeasureSeconds, officialWindows)
		}
		if *backend == "rmdb" && *oracleAck != "" {
			// Every rmdb `run` uses the prepared ranking client, whose transactions
			// submit `commit;` inside their final EXEC_BATCH and never route
			// through a client-side commit hook, so nothing would ever be
			// appended to the ACK file. Fail instead of pretending the crash
			// oracle covers the TPC-C load.
			fmt.Fprintln(os.Stderr, "--oracle-ack-file is not supported by the ranking backend: ranking transactions commit inside EXEC_BATCH, so no ACK can be recorded; use --command mixed-sql for crash oracle coverage")
			os.Exit(2)
		}
	}
	if *mode == "official-equivalent" && *backend != "rmdb" {
		fmt.Fprintln(os.Stderr, "official-equivalent mode requires the rmdb backend")
		os.Exit(2)
	}
	if *mode == "sqlite-reference" && *backend != "sqlite" {
		fmt.Fprintln(os.Stderr, "sqlite-reference mode requires the sqlite backend")
		os.Exit(2)
	}
	if *sqliteBegin != "immediate" && *sqliteBegin != "deferred" {
		fmt.Fprintln(os.Stderr, "--sqlite-begin must be immediate or deferred")
		os.Exit(2)
	}
	if *command == "data-ready" {
		if err := validateDataset(*dataDir, *warehouses, *seed); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *command == "refresh-manifest" {
		if err := writeDatasetManifest(*dataDir, *warehouses, *seed); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("[tpcc] refreshed dataset manifest in %s\n", *dataDir)
		return
	}
	if *command == "datagen" {
		if *reuseData {
			if err := validateDataset(*dataDir, *warehouses, *seed); err == nil {
				fmt.Printf("[tpcc] datagen skipped, reusing CSV files in %s\n", *dataDir)
				return
			}
		}
		if err := generateData(*warehouses, *dataDir, *seed, *overwriteData); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *command == "wait-port" {
		if err := waitForPort(net.JoinHostPort(*host, strconv.Itoa(*port)), *waitTimeout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *command == "wait-ready" {
		if err := waitForReady(net.JoinHostPort(*host, strconv.Itoa(*port)), *waitTimeout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *command == "merge-results" {
		if err := mergeResultFiles(*jsonOut, *resultInputs); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *workers < 1 || *warmup < 0 || *measure < 1 || *rounds < 1 || *roundOffset < 0 {
		fmt.Fprintln(os.Stderr, "workers must be positive, warmup and round-offset non-negative, measure and rounds positive")
		os.Exit(2)
	}
	if *policy != "terminal-home" && *policy != "random-per-txn" && *policy != "official-terminal-home" {
		fmt.Fprintln(os.Stderr, "unsupported warehouse policy")
		os.Exit(2)
	}
	address := net.JoinHostPort(*host, strconv.Itoa(*port))
	if *command == "feature-check" {
		var featureBackend txnBackend
		var featureErr error
		if *backend == "sqlite" {
			featureBackend, featureErr = newSQLiteBackendWithBegin(*sqlitePath, *sqliteBegin)
		} else {
			featureBackend, featureErr = newClient(address, *timeout, *isolation)
		}
		if featureErr == nil {
			defer featureBackend.close()
		}
		if featureErr == nil {
			profile, profileErr := inspectProfile(featureBackend)
			if profileErr != nil {
				featureErr = profileErr
			} else {
				featureErr = verifyBenchmarkFeatures(featureBackend, profile)
			}
		}
		if featureErr != nil {
			fmt.Fprintln(os.Stderr, featureErr)
			os.Exit(1)
		}
		return
	}
	if *command == "oracle-init" {
		c, err := newClient(address, *timeout, *isolation)
		if err == nil {
			_, err = c.exec("create table crash_txn_log (txn_id int, txn_type char(16), payload_hash int);")
			if err == nil {
				_, err = c.exec("create index crash_txn_log(txn_id);")
			}
			if err == nil {
				_, err = c.exec("create table crash_sql_state (id int, value int);")
			}
			if err == nil {
				_, err = c.exec("create index crash_sql_state(id);")
			}
			if err == nil {
				_, err = c.exec("create table crash_atomic_a (id int, value int);")
			}
			if err == nil {
				_, err = c.exec("create index crash_atomic_a(id);")
			}
			if err == nil {
				_, err = c.exec("create table crash_atomic_b (id int, value int);")
			}
			if err == nil {
				_, err = c.exec("create index crash_atomic_b(id);")
			}
			if err == nil {
				_, err = c.exec("create table crash_atomic_unique (id int, value int);")
			}
			if err == nil {
				_, err = c.exec("create index crash_atomic_unique(id);")
			}
			if err == nil {
				if _, err = c.exec("begin;"); err == nil {
					for id := 1; id <= 128 && err == nil; id++ {
						_, err = c.exec(fmt.Sprintf("insert into crash_sql_state values (%d, 0);", id))
					}
					if err == nil {
						_, err = c.exec("commit;")
					} else {
						c.rollback()
					}
				}
			}
			c.close()
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *command == "mixed-sql" {
		if err := runMixedSQL(address, *timeout, *isolation, *sqlOps, *sqlReadyFile, *oracleAck, *oracleIssued, *oraclePrefix, *think); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *command == "oracle-verify" {
		if *oracleAck == "" {
			fmt.Fprintln(os.Stderr, "oracle-verify requires --oracle-ack-file")
			os.Exit(2)
		}
		if err := verifyCrashOracle(address, *timeout, *isolation, *oracleAck); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *command == "atomic-verify" {
		if *oracleIssued == "" || *oracleAck == "" {
			fmt.Fprintln(os.Stderr, "atomic-verify requires --oracle-issued-file and --oracle-ack-file")
			os.Exit(2)
		}
		issued, err := readIDFile(*oracleIssued)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := verifyAtomicOracle(address, *timeout, *isolation, *oracleIssued, *oracleAck); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("[atomic-oracle] verified %d issued transactions\n", len(issued))
		return
	}
	if *command == "load" {
		var err error
		if *backend == "sqlite" {
			err = importCSVToSQLite(*sqlitePath, *dataDir, *schemaDir)
		} else {
			err = loadData(address, *timeout, *isolation, *dataDir, *schemaDir, *rmdbDBDir)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *command == "consistency" {
		if *backend != "rmdb" {
			fmt.Fprintln(os.Stderr, "consistency command is currently supported for rmdb only")
			os.Exit(2)
		}
		if err := checkConsistency(address, *timeout, *isolation, *resultJSON, *consistencyStage,
			time.Duration(*progress)*time.Second); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *command == "validate-result" {
		doc, err := loadResultDocument(*resultJSON)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := validateResultDocument(doc); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *command == "compact-result" {
		doc, err := loadResultDocument(*resultJSON)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := publishResultDocument(*resultJSON, doc); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("[benchmark] compacted result=%s median_tpmC=%.2f\n", *resultJSON, doc.MedianTPMC)
		return
	}
	if *command != "run" {
		fmt.Fprintf(os.Stderr, "unsupported command: %s\n", *command)
		os.Exit(2)
	}
	fmt.Printf("[run] max_conflict_retries=%d (-1 retries until phase deadline; default 1)\n", *maxConflictRetries)
	if *oracleAck != "" {
		if *oraclePrefix <= 0 || *oraclePrefix > 2000 {
			fmt.Fprintln(os.Stderr, "oracle run requires --oracle-id-prefix in 1..2000")
			os.Exit(2)
		}
		oracleAckFile = *oracleAck
		oracleIDPrefix = *oraclePrefix
		oracleSeq.Store(0)
	}
	if err := os.Remove(*jsonOut); err != nil && !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *latencyPhaseObserve && *backend != "rmdb" {
		fmt.Fprintln(os.Stderr, "--latency-phase-observe requires --backend rmdb")
		os.Exit(2)
	}
	latencyObserver := newLatencyPhaseObserver(*latencyPhaseObserve, *latencyPhaseSampleEvery)
	maxProvenanceEnabled := *maxProvenance || *maxProvenanceOut != ""
	maxProvenancePath := *maxProvenanceOut
	if maxProvenanceEnabled && maxProvenancePath == "" {
		maxProvenancePath = *jsonOut + ".max.jsonl"
	}
	maxObserver, maxObserverErr := newMaxProvenanceObserver(maxProvenanceEnabled, maxProvenancePath, time.Now())
	if maxObserverErr != nil {
		fmt.Fprintln(os.Stderr, maxObserverErr)
		os.Exit(1)
	}
	if maxObserver != nil {
		fmt.Printf("[max-provenance] sidecar=%s\n", maxProvenancePath)
	}
	var factory backendFactory
	if *backend == "sqlite" {
		factory = func() (txnBackend, error) {
			return newSQLiteBackendWithBegin(*sqlitePath, *sqliteBegin)
		}
	} else {
		factory = func() (txnBackend, error) {
			ranking, err := newRankingClient(address, *timeout, *isolation)
			if err != nil {
				return nil, err
			}
			ranking.latency = latencyObserver.newSampler()
			return ranking, nil
		}
	}
	var probe txnBackend
	var err error
	if *backend == "sqlite" {
		probe, err = newSQLiteBackendWithBegin(*sqlitePath, *sqliteBegin)
	} else {
		probe, err = newClient(address, *timeout, *isolation)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	p, err := inspectProfile(probe)
	if err != nil {
		probe.close()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *mode == "official-equivalent" {
		if err := validateOfficialWarehouses(p.warehouses, *allowNonOfficialTiming); err != nil {
			probe.close()
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
	if err := verifyBenchmarkFeatures(probe, p); err != nil {
		probe.close()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *mode == "official-equivalent" {
		preparedProbe, preparedErr := factory()
		if preparedErr == nil {
			preparedErr = verifyPreparedInvalidItemRollback(preparedProbe, p)
			preparedProbe.close()
		}
		if preparedErr != nil {
			probe.close()
			fmt.Fprintln(os.Stderr, preparedErr)
			os.Exit(1)
		}
	}
	ordersText, err := probe.exec("select count(*) from orders;")
	if err != nil {
		probe.close()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	baseOrders := scalarInt(ordersText, 0)
	// The pre-workload aggregate snapshot is the reference the post-crash
	// reconciliation compares against, so it has to be taken here, while the
	// database is still quiescent, and published with the result.
	var baselines map[string]float64
	if *backend == "rmdb" {
		snapshotStart := time.Now()
		baselines, err = captureBaselines(probe)
		if err != nil {
			probe.close()
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("[baseline] aggregate snapshot took %s\n", time.Since(snapshotStart).Round(time.Millisecond))
		manifest, manifestErr := readDatasetManifest(*dataDir)
		if manifestErr != nil {
			probe.close()
			fmt.Fprintf(os.Stderr, "read exact FLOAT32 dataset baselines: %v\n", manifestErr)
			os.Exit(1)
		}
		baselines[baseHistoryAmount] = manifest.Aggregates[aggHistoryAmountSum]
		baselines[baseOrderLineAmount] = manifest.Aggregates[aggOrderLineAmountSum]
	}
	probe.close()
	if *mode == "official-equivalent" {
		*policy = "official-terminal-home"
	}
	ledger := newTxnLedger()
	edgeSink := newPaymentChainAccumulator()
	doc := document{Config: config{Mode: *mode, Backend: *backend, Isolation: *isolation, SQLitePath: *sqlitePath, SQLiteBegin: *sqliteBegin, Warehouses: p.warehouses, Workers: *workers, Warmup: *warmup, Measure: *measure, Rounds: *rounds, ProgressInterval: *progress, Seed: *seed, Think: think.String(), ReconnectEachTxn: *reconnectEachTxn, MaxConflictRetries: *maxConflictRetries, WarehousePolicy: *policy, BaselineWarehouseTotal: p.warehouses, BaselineDistrictTotal: p.warehouses * p.districtsPerWarehouse, BaselineCustomerTotal: p.warehouses * p.districtsPerWarehouse * p.customersPerDistrict, BaselineItemTotal: p.itemCount, BaselineStockTotal: p.warehouses * p.itemCount, BaselineOrdersTotal: baseOrders}, Baselines: baselines}
	var postRunDiagnosticErr error
	if *mode == "official-equivalent" {
		windows, runErr := runOfficialWindowsWithMaxProvenance(*rounds, *workers, *seed, p, *warmup, *measure, *progress,
			*think, *maxConflictRetries, *reconnectEachTxn, *roundOffset, factory, ledger, *walPhaseMarkerSocket,
			maxObserver, edgeSink)
		if runErr != nil {
			if windows == nil {
				fmt.Fprintln(os.Stderr, runErr)
				os.Exit(1)
			}
			postRunDiagnosticErr = runErr
		}
		doc.Rounds = windows
	} else {
		for round := 1; round <= *rounds; round++ {
			warmupEnd := time.Now().Add(time.Duration(*warmup) * time.Second)
			measureEnd := warmupEnd.Add(time.Duration(*measure) * time.Second)
			stats := &liveStats{}
			printProgress(round, *rounds, "warmup", 0, *warmup, stats)
			monitorStop := make(chan struct{})
			monitorDone := make(chan struct{})
			go monitorProgress(round, *rounds, *warmup, *measure, *progress, warmupEnd, measureEnd, stats, monitorStop, monitorDone)
			combined, roundErr := runRoundWithMaxProvenance(*roundOffset+round, *workers, *seed, p, *policy, warmupEnd,
				measureEnd, *measure, *think, *reconnectEachTxn, *maxConflictRetries, stats, factory, ledger, maxObserver, edgeSink)
			close(monitorStop)
			<-monitorDone
			if roundErr != nil {
				fmt.Fprintf(os.Stderr, "round %d invalid: %v\n", round, roundErr)
				os.Exit(1)
			}
			doc.Rounds = append(doc.Rounds, combined)
			fmt.Printf("[round %d/%d] tpmC=%.2f abort_rate=%.2f%%\n", round, *rounds, combined.TPMC, combined.AbortRate*100)

		}
	}
	values := make([]float64, len(doc.Rounds))
	for i, round := range doc.Rounds {
		values[i] = round.TPMC
	}
	doc.MedianTPMC = median(values)
	doc.Ledger = ledger.snapshot()
	expectedEdges := int(math.Round(ledger.values[ledgerPaymentCommits])) * 2
	terminals, edgeCount, err := edgeSink.finalize(expectedEdges)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	doc.PaymentEdgeCount = edgeCount
	doc.PaymentChainTerminals = terminals
	if err := publishResultDocument(*jsonOut, doc); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := latencyObserver.emit(*latencyPhaseOut); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := maxObserver.close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("[benchmark] result=%s median_tpmC=%.2f\n", *jsonOut, doc.MedianTPMC)
	if postRunDiagnosticErr != nil {
		fmt.Fprintln(os.Stderr, postRunDiagnosticErr)
		os.Exit(1)
	}
}
