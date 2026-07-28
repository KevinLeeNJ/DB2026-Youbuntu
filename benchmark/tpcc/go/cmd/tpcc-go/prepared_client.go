package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	wireTagPrepareOK   = 0x14
	wireTagBatchResult = 0x15
	wireTagPrepareSet  = 0x21
	wireTagExecBatch   = 0x22
)

type preparedArgument struct {
	typ       byte
	present   bool
	int32     int32
	floatBits uint32
	text      string
}

type rankingStatement struct {
	id             uint16
	query          bool
	parameterTypes []byte
	columns        []wireColumn
}

type batchOperation struct {
	statement rankingStatement
	args      []preparedArgument
	// sql is retained locally for deterministic test backends. The wire encoder
	// uses only statement and args.
	sql string
}

type batchOperationResult struct {
	operationIndex uint16
	rows           [][]string
}

type batchResult struct {
	executedOperations uint16
	status             byte
	failedOperation    uint16
	diagnostic         string
	results            []batchOperationResult
}

type pendingStatement struct {
	id          uint16
	query       bool
	template    string
	args        []preparedArgument
	wantColumns []byte
}

// allowLegacyPrepare opts back into the pre-final PREPARE_OK layout that echoed
// parameter types before the column count. The official evaluator only decodes
// the final layout (final.md:669-680), so the strict layout is the default and a
// legacy response is reported as a protocol contract failure.
var allowLegacyPrepare bool

type rankingClient struct {
	*client
	statements map[string]rankingStatement
}

func newRankingClient(address string, timeout time.Duration, isolation string) (*rankingClient, error) {
	base, err := newClient(address, timeout, isolation)
	if err != nil {
		return nil, err
	}
	ranking := &rankingClient{client: base, statements: make(map[string]rankingStatement)}
	if err := ranking.prepare(); err != nil {
		ranking.close()
		return nil, err
	}
	return ranking, nil
}

// pendingRankingStatements turns the ranking template table into the statement
// dictionary sent by PREPARE_SET, keeping the expected query schema of every
// template so PREPARE_OK can be verified column by column.
func pendingRankingStatements() ([]pendingStatement, error) {
	templates := rankingTemplates()
	pending := make([]pendingStatement, 0, len(templates))
	seen := make(map[string]int, len(templates))
	for _, sample := range templates {
		query := statementResultKind(sample.sql) == resultKindQuery
		if query != (len(sample.columns) > 0) {
			return nil, fmt.Errorf("ranking template %q declares %d expected columns", sample.sql, len(sample.columns))
		}
		template, args, err := parameterizeSQL(sample.sql)
		if err != nil {
			return nil, fmt.Errorf("parameterize ranking SQL %q: %w", sample.sql, err)
		}
		if index, ok := seen[template]; ok {
			if string(pending[index].wantColumns) != string(sample.columns) {
				return nil, fmt.Errorf("ranking template %q duplicates %q with a different schema", sample.sql, template)
			}
			continue
		}
		if len(pending) >= 256 {
			return nil, errors.New("ranking prepared statement set exceeds protocol limit")
		}
		seen[template] = len(pending)
		pending = append(pending, pendingStatement{
			id:          uint16(len(pending) + 1),
			query:       query,
			template:    template,
			args:        args,
			wantColumns: sample.columns,
		})
	}
	return pending, nil
}

func (c *rankingClient) prepare() error {
	pending, err := pendingRankingStatements()
	if err != nil {
		return err
	}

	payload := make([]byte, 2)
	binary.BigEndian.PutUint16(payload, uint16(len(pending)))
	for _, statement := range pending {
		if statement.id == 0 || len(statement.args) > math.MaxUint16 || len(statement.template) > maxWirePayload {
			return errors.New("invalid ranking prepared statement")
		}
		var field [5]byte
		binary.BigEndian.PutUint16(field[:2], statement.id)
		if statement.query {
			field[2] = 1
		}
		binary.BigEndian.PutUint16(field[3:5], uint16(len(statement.args)))
		payload = append(payload, field[:]...)
		for _, arg := range statement.args {
			payload = append(payload, arg.typ)
		}
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(statement.template)))
		payload = append(payload, length[:]...)
		payload = append(payload, statement.template...)
	}
	if err := c.writeRequest(wireTagPrepareSet, 0, payload); err != nil {
		return err
	}
	tag, body, err := c.readFrame()
	if err != nil {
		return err
	}
	if tag == wireTagError {
		return fmt.Errorf("PREPARE_SET failed: %s", body)
	}
	if tag != wireTagPrepareOK {
		return fmt.Errorf("PREPARE_SET returned unexpected tag 0x%02x", tag)
	}
	statements, err := decodePrepareOK(body, pending)
	if err != nil {
		return err
	}
	for _, statement := range pending {
		decoded, ok := statements[statement.id]
		if !ok {
			return fmt.Errorf("PREPARE_OK omitted statement %d", statement.id)
		}
		decoded.query = statement.query
		c.statements[statement.template] = decoded
	}
	return nil
}

func (c *rankingClient) writeRequest(tag, flags byte, payload []byte) error {
	if len(payload) > maxWirePayload {
		return errors.New("RMDB wire request payload exceeds 1 MiB")
	}
	if err := c.conn.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return err
	}
	frame := make([]byte, 8, 8+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	frame[4], frame[5] = tag, flags
	frame = append(frame, payload...)
	return writeAll(c.conn, frame)
}

func parameterizeSQL(sql string) (string, []preparedArgument, error) {
	var builder strings.Builder
	args := make([]preparedArgument, 0, 8)
	for i := 0; i < len(sql); {
		if sql[i] == '\'' {
			value, next, err := readSQLString(sql, i)
			if err != nil {
				return "", nil, err
			}
			args = append(args, preparedArgument{typ: wireTypeChar, present: true, text: value})
			fmt.Fprintf(&builder, "$%d", len(args))
			i = next
			continue
		}
		if isNumericStart(sql, i) {
			literal, next := readNumericLiteral(sql, i)
			arg, err := numericArgument(literal)
			if err != nil {
				return "", nil, err
			}
			args = append(args, arg)
			fmt.Fprintf(&builder, "$%d", len(args))
			i = next
			continue
		}
		builder.WriteByte(sql[i])
		i++
	}
	return builder.String(), args, nil
}

func readSQLString(sql string, start int) (string, int, error) {
	var value strings.Builder
	for i := start + 1; i < len(sql); i++ {
		if sql[i] != '\'' {
			value.WriteByte(sql[i])
			continue
		}
		if i+1 < len(sql) && sql[i+1] == '\'' {
			value.WriteByte('\'')
			i++
			continue
		}
		return value.String(), i + 1, nil
	}
	return "", 0, errors.New("unterminated SQL string literal")
}

func isNumericStart(sql string, index int) bool {
	if index > 0 && isSQLIdentifierByte(sql[index-1]) {
		return false
	}
	if sql[index] < '0' || sql[index] > '9' {
		if sql[index] != '-' || index+1 >= len(sql) || sql[index+1] < '0' || sql[index+1] > '9' {
			return false
		}
	}
	_, end := readNumericLiteral(sql, index)
	return end == len(sql) || !isSQLIdentifierByte(sql[end])
}

func isSQLIdentifierByte(value byte) bool {
	return value == '_' || value == '$' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}

func readNumericLiteral(sql string, start int) (string, int) {
	i := start
	if sql[i] == '-' {
		i++
	}
	for i < len(sql) && sql[i] >= '0' && sql[i] <= '9' {
		i++
	}
	if i < len(sql) && sql[i] == '.' {
		i++
		for i < len(sql) && sql[i] >= '0' && sql[i] <= '9' {
			i++
		}
	}
	if i < len(sql) && (sql[i] == 'e' || sql[i] == 'E') {
		i++
		if i < len(sql) && (sql[i] == '+' || sql[i] == '-') {
			i++
		}
		for i < len(sql) && sql[i] >= '0' && sql[i] <= '9' {
			i++
		}
	}
	return sql[start:i], i
}

func numericArgument(literal string) (preparedArgument, error) {
	if strings.ContainsAny(literal, ".eE") {
		value, err := strconv.ParseFloat(literal, 32)
		if err != nil {
			return preparedArgument{}, fmt.Errorf("invalid FLOAT32 literal %q: %w", literal, err)
		}
		return preparedArgument{typ: wireTypeFloat32, present: true, floatBits: math.Float32bits(float32(value))}, nil
	}
	value, err := strconv.ParseInt(literal, 10, 32)
	if err != nil {
		return preparedArgument{}, fmt.Errorf("invalid INT32 literal %q: %w", literal, err)
	}
	return preparedArgument{typ: wireTypeInt32, present: true, int32: int32(value)}, nil
}

// rankingPrepareItem is the wire-independent shape used by the decoder.
type rankingPrepareItem struct {
	id          uint16
	query       bool
	template    string
	args        []preparedArgument
	wantColumns []byte
}

func decodePrepareCandidate(body []byte, pending []rankingPrepareItem, legacy bool) (map[uint16]rankingStatement, error) {
	if len(body) < 2 {
		return nil, errors.New("truncated PREPARE_OK")
	}
	count := int(binary.BigEndian.Uint16(body[:2]))
	if count != len(pending) {
		return nil, fmt.Errorf("PREPARE_OK statement count %d, want %d", count, len(pending))
	}
	offset := 2
	decoded := make(map[uint16]rankingStatement, count)
	for i := 0; i < count; i++ {
		if offset+4 > len(body) {
			return nil, errors.New("truncated PREPARE_OK statement")
		}
		id := binary.BigEndian.Uint16(body[offset : offset+2])
		offset += 2
		if id != pending[i].id {
			return nil, fmt.Errorf("PREPARE_OK statement id %d, want %d", id, pending[i].id)
		}
		if legacy {
			parameterCount := int(binary.BigEndian.Uint16(body[offset : offset+2]))
			offset += 2
			if parameterCount != len(pending[i].args) || offset+parameterCount > len(body) {
				return nil, fmt.Errorf("PREPARE_OK parameter count for statement %d is invalid", id)
			}
			for _, arg := range pending[i].args {
				if body[offset] != arg.typ {
					return nil, fmt.Errorf("PREPARE_OK parameter type mismatch for statement %d", id)
				}
				offset++
			}
		}
		if offset+2 > len(body) {
			return nil, errors.New("truncated PREPARE_OK column count")
		}
		columnCount := int(binary.BigEndian.Uint16(body[offset : offset+2]))
		offset += 2
		if !pending[i].query && columnCount != 0 {
			return nil, fmt.Errorf("command statement %d returned columns", id)
		}
		// final.md:137,680: the column count, order and SQL type of every query
		// must match its real projection schema. A placeholder column, a missing
		// projection column or a numeric column reported as CHAR is a protocol
		// contract failure, so compare the whole declared schema position by
		// position instead of only accepting "some column of a known type".
		if columnCount != len(pending[i].wantColumns) {
			return nil, fmt.Errorf("PREPARE_OK statement %d reported %d columns, want %d for %q",
				id, columnCount, len(pending[i].wantColumns), pending[i].template)
		}
		columns := make([]wireColumn, 0, columnCount)
		for j := 0; j < columnCount; j++ {
			column, next, err := decodeColumn(body, offset)
			if err != nil {
				return nil, err
			}
			if column.sqlType != pending[i].wantColumns[j] {
				return nil, fmt.Errorf("PREPARE_OK statement %d column %d has SQL type 0x%02x, want 0x%02x for %q",
					id, j+1, column.sqlType, pending[i].wantColumns[j], pending[i].template)
			}
			columns = append(columns, column)
			offset = next
		}
		decoded[id] = rankingStatement{id: id, query: pending[i].query, parameterTypes: argumentTypes(pending[i].args), columns: columns}
	}
	if offset != len(body) {
		return nil, errors.New("PREPARE_OK contains trailing bytes")
	}
	return decoded, nil
}

func decodePrepareOK(body []byte, pending []pendingStatement) (map[uint16]rankingStatement, error) {
	items := make([]rankingPrepareItem, len(pending))
	for i, item := range pending {
		items[i] = rankingPrepareItem{id: item.id, query: item.query, template: item.template,
			args: item.args, wantColumns: item.wantColumns}
	}
	if allowLegacyPrepare {
		return decodePrepareCandidate(body, items, true)
	}
	decoded, err := decodePrepareCandidate(body, items, false)
	if err == nil {
		return decoded, nil
	}
	// Never fall back silently: a server that still speaks the legacy layout
	// would make every local run pass while the official evaluator reports a
	// protocol contract failure.
	if _, legacyErr := decodePrepareCandidate(body, items, true); legacyErr == nil {
		return nil, fmt.Errorf("PREPARE_OK uses the legacy layout that echoes parameter types; "+
			"the final protocol sends statement id, column count and column definitions only "+
			"(rerun with --allow-legacy-prepare to accept it locally): %w", err)
	}
	return nil, err
}

func argumentTypes(args []preparedArgument) []byte {
	types := make([]byte, len(args))
	for i, arg := range args {
		types[i] = arg.typ
	}
	return types
}

func decodeColumn(body []byte, offset int) (wireColumn, int, error) {
	if offset+2 > len(body) {
		return wireColumn{}, 0, errors.New("truncated prepared column name")
	}
	nameLength := int(binary.BigEndian.Uint16(body[offset : offset+2]))
	offset += 2
	if nameLength == 0 || offset+nameLength+1 > len(body) {
		return wireColumn{}, 0, errors.New("invalid prepared column definition")
	}
	column := wireColumn{name: string(body[offset : offset+nameLength]), sqlType: body[offset+nameLength]}
	if column.sqlType != wireTypeInt32 && column.sqlType != wireTypeFloat32 && column.sqlType != wireTypeChar {
		return wireColumn{}, 0, errors.New("unknown prepared column type")
	}
	return column, offset + nameLength + 1, nil
}

func (c *rankingClient) exec(sql string) (string, error) {
	operation, err := c.batchOperation(sql)
	if err != nil {
		return "", err
	}
	result, err := c.execBatch([]batchOperation{operation})
	if err != nil {
		return "", err
	}
	if !operation.statement.query {
		return "", nil
	}
	return formatWireRows(operation.statement.columns, result.results[0].rows), nil
}

func (c *rankingClient) batchOperation(sql string) (batchOperation, error) {
	template, args, err := parameterizeSQL(sql)
	if err != nil {
		return batchOperation{}, err
	}
	statement, ok := c.statements[template]
	if !ok {
		return batchOperation{}, fmt.Errorf("ranking SQL template was not prepared: %q", template)
	}
	if len(args) != len(statement.parameterTypes) {
		return batchOperation{}, fmt.Errorf("ranking SQL parameter count mismatch for %q", template)
	}
	for i := range args {
		if args[i].typ != statement.parameterTypes[i] {
			return batchOperation{}, fmt.Errorf("ranking SQL parameter %d type mismatch for %q", i+1, template)
		}
	}
	return batchOperation{statement: statement, args: args, sql: sql}, nil
}

func (c *rankingClient) begin() error {
	c.autoAborted = false
	_, err := c.exec("begin;")
	return err
}

// commit exists to satisfy txnBackend. The ranking transactions never call it:
// they carry `commit;` as the last operation of their final EXEC_BATCH so the
// official batch round-trip counts stay at 2/2/3/3/2 (final.md:741-749). It
// therefore also carries no crash-oracle bookkeeping — main rejects
// --oracle-ack-file for the ranking backend instead of silently recording
// nothing.
func (c *rankingClient) commit() error {
	_, err := c.exec("commit;")
	return err
}

func (c *rankingClient) rollback() {
	if c.autoAborted {
		c.autoAborted = false
		return
	}
	_, _ = c.exec("rollback;")
}

type batchReader struct {
	body   []byte
	offset int
}

func (r *batchReader) take(count int) ([]byte, error) {
	if count < 0 || r.offset > len(r.body)-count {
		return nil, errors.New("truncated BATCH_RESULT")
	}
	value := r.body[r.offset : r.offset+count]
	r.offset += count
	return value, nil
}

func (r *batchReader) u8() (byte, error) {
	value, err := r.take(1)
	if err != nil {
		return 0, err
	}
	return value[0], nil
}

func (r *batchReader) u16() (uint16, error) {
	value, err := r.take(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(value), nil
}

func (r *batchReader) u32() (uint32, error) {
	value, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(value), nil
}

func (r *batchReader) remaining() int { return len(r.body) - r.offset }

func encodeBatchOperations(operations []batchOperation) ([]byte, error) {
	if len(operations) == 0 || len(operations) > 256 {
		return nil, errors.New("EXEC_BATCH operation count must be between 1 and 256")
	}
	payload := make([]byte, 2)
	binary.BigEndian.PutUint16(payload, uint16(len(operations)))
	for _, operation := range operations {
		statement, args := operation.statement, operation.args
		if statement.id == 0 {
			return nil, errors.New("EXEC_BATCH statement id must be nonzero")
		}
		if len(args) != len(statement.parameterTypes) {
			return nil, errors.New("EXEC_BATCH argument count does not match prepared statement")
		}
		var id [2]byte
		binary.BigEndian.PutUint16(id[:], statement.id)
		payload = append(payload, id[:]...)
		for i, arg := range args {
			if arg.typ != statement.parameterTypes[i] {
				return nil, errors.New("typed EXEC_BATCH argument does not match prepared type")
			}
			if arg.present {
				payload = append(payload, 1)
			} else {
				payload = append(payload, 0)
			}
			if !arg.present {
				continue
			}
			switch arg.typ {
			case wireTypeInt32:
				var value [4]byte
				binary.BigEndian.PutUint32(value[:], uint32(arg.int32))
				payload = append(payload, value[:]...)
			case wireTypeFloat32:
				var value [4]byte
				binary.BigEndian.PutUint32(value[:], arg.floatBits)
				payload = append(payload, value[:]...)
			case wireTypeChar:
				if len(arg.text) > math.MaxUint32 {
					return nil, errors.New("typed CHAR parameter exceeds protocol limit")
				}
				var length [4]byte
				binary.BigEndian.PutUint32(length[:], uint32(len(arg.text)))
				payload = append(payload, length[:]...)
				payload = append(payload, arg.text...)
			default:
				return nil, errors.New("unknown typed EXEC_BATCH argument")
			}
		}
	}
	if len(payload) > maxWirePayload {
		return nil, errors.New("EXEC_BATCH payload exceeds 1 MiB")
	}
	return payload, nil
}

func (c *rankingClient) execBatch(operations []batchOperation) (batchResult, error) {
	payload, err := encodeBatchOperations(operations)
	if err != nil {
		return batchResult{}, err
	}
	if err := c.writeRequest(wireTagExecBatch, 1, payload); err != nil {
		c.close()
		return batchResult{}, err
	}
	tag, body, err := c.readFrame()
	if err != nil {
		c.close()
		return batchResult{}, err
	}
	if tag == wireTagError {
		c.autoAborted = true
		return batchResult{}, errors.New(string(body))
	}
	if tag != wireTagBatchResult {
		return batchResult{}, fmt.Errorf("EXEC_BATCH returned unexpected tag 0x%02x", tag)
	}
	result, err := decodeBatchResult(body, operations)
	if err != nil {
		return batchResult{}, err
	}
	if result.status != 0 {
		c.autoAborted = true
		if result.status == 1 {
			return result, errAbort
		}
		return result, errors.New(result.diagnostic)
	}
	return result, nil
}

func decodeBatchResult(body []byte, operations []batchOperation) (batchResult, error) {
	reader := batchReader{body: body}
	executed, err := reader.u16()
	if err != nil {
		return batchResult{}, err
	}
	status, err := reader.u8()
	if err != nil {
		return batchResult{}, err
	}
	failed, err := reader.u16()
	if err != nil {
		return batchResult{}, err
	}
	diagnosticLength, err := reader.u32()
	if err != nil {
		return batchResult{}, err
	}
	diagnostic, err := reader.take(int(diagnosticLength))
	if err != nil || diagnosticLength > 64<<10 {
		return batchResult{}, errors.New("invalid BATCH_RESULT diagnostic")
	}
	resultCount, err := reader.u16()
	if err != nil {
		return batchResult{}, err
	}
	result := batchResult{
		executedOperations: executed,
		status:             status,
		failedOperation:    failed,
		diagnostic:         string(diagnostic),
		results:            make([]batchOperationResult, 0, resultCount),
	}
	if status != 0 {
		if (status != 1 && status != 2) || executed >= uint16(len(operations)) || failed != executed || resultCount != 0 || reader.remaining() != 0 {
			return batchResult{}, errors.New("invalid failed BATCH_RESULT")
		}
		return result, nil
	}
	if executed != uint16(len(operations)) || failed != 0xffff || len(diagnostic) != 0 {
		return batchResult{}, errors.New("invalid successful BATCH_RESULT counters")
	}
	lastIndex := -1
	for i := 0; i < int(resultCount); i++ {
		operationIndex, err := reader.u16()
		if err != nil {
			return batchResult{}, err
		}
		if int(operationIndex) <= lastIndex || int(operationIndex) >= len(operations) || !operations[operationIndex].statement.query {
			return batchResult{}, errors.New("invalid BATCH_RESULT operation index")
		}
		lastIndex = int(operationIndex)
		rowCount, err := reader.u32()
		if err != nil {
			return batchResult{}, err
		}
		rows := make([][]string, 0, rowCount)
		for row := uint32(0); row < rowCount; row++ {
			columns := operations[operationIndex].statement.columns
			values := make([]string, 0, len(columns))
			for _, column := range columns {
				value, valueErr := decodeBatchCell(&reader, column.sqlType)
				if valueErr != nil {
					return batchResult{}, valueErr
				}
				values = append(values, value)
			}
			rows = append(rows, values)
		}
		result.results = append(result.results, batchOperationResult{operationIndex: operationIndex, rows: rows})
	}
	queryCount := 0
	for _, operation := range operations {
		if operation.statement.query {
			queryCount++
		}
	}
	if int(resultCount) != queryCount {
		return batchResult{}, errors.New("successful BATCH_RESULT does not contain one result per query")
	}
	if reader.remaining() != 0 {
		return batchResult{}, errors.New("BATCH_RESULT contains trailing bytes")
	}
	return result, nil
}

func decodeBatchCell(reader *batchReader, typ byte) (string, error) {
	present, err := reader.u8()
	if err != nil {
		return "", err
	}
	if present == 0 {
		return "NULL", nil
	}
	if present != 1 {
		return "", errors.New("invalid BATCH_RESULT present flag")
	}
	switch typ {
	case wireTypeInt32:
		value, err := reader.u32()
		return strconv.FormatInt(int64(int32(value)), 10), err
	case wireTypeFloat32:
		value, err := reader.u32()
		return strconv.FormatFloat(float64(math.Float32frombits(value)), 'f', -1, 32), err
	case wireTypeChar:
		length, err := reader.u32()
		if err != nil {
			return "", err
		}
		value, err := reader.take(int(length))
		return string(value), err
	default:
		return "", errors.New("unknown BATCH_RESULT column type")
	}
}

// rankingTemplate pairs a ranking SQL sample with the exact PREPARE_OK query
// schema its projection must report. columns is nil for commands, whose
// column_count must be 0.
type rankingTemplate struct {
	sql     string
	columns []byte
}

// rankingTemplates lists every statement the ranking transactions execute plus
// the projection schema each query must report. The expected types follow the
// benchmark DDL in benchmark/tpcc/schema/rmdb_schema.sql: COUNT yields INT32,
// MIN/SUM keep their input column type, and CHAR(n) columns (including the
// timestamp columns) are reported as CHAR.
func rankingTemplates() []rankingTemplate {
	const (
		i = wireTypeInt32
		f = wireTypeFloat32
		c = wireTypeChar
	)
	return []rankingTemplate{
		{sql: "begin;"},
		{sql: "commit;"},
		{sql: "rollback;"},
		{sql: "abort;"},
		{sql: "select c_discount, c_last, c_credit, w_tax from customer, warehouse where w_id = 1 and c_w_id = w_id and c_d_id = 1 and c_id = 1;",
			columns: []byte{f, c, c, f}},
		{sql: "update district set d_next_o_id = d_next_o_id + 1 where d_id = 1 and d_w_id = 1;"},
		{sql: "select d_next_o_id, d_tax from district where d_id = 1 and d_w_id = 1;",
			columns: []byte{i, f}},
		{sql: "insert into orders values (1, 1, 1, 1, '2026-01-01 00:00:00', 0, 5, 1);"},
		{sql: "insert into new_orders values (1, 1, 1);"},
		{sql: "select i_price, i_name, i_data from item where i_id = 1;",
			columns: []byte{f, c, c}},
		{sql: "update stock set s_ytd = s_ytd + 1, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + 0 where s_i_id = 1 and s_w_id = 1;"},
		{sql: "update stock set s_ytd = s_ytd where s_w_id = 1 and s_i_id = 1;"},
		{sql: "select s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 from stock where s_i_id = 1 and s_w_id = 1;",
			columns: []byte{i, c, c, c, c, c, c, c, c, c, c, c}},
		{sql: "select s_ytd from stock where s_i_id = 1 and s_w_id = 1;",
			columns: []byte{f}},
		{sql: "update stock set s_quantity = s_quantity + 1 where s_i_id = 1 and s_w_id = 1;"},
		{sql: "update stock set s_quantity = s_quantity - 1 where s_i_id = 1 and s_w_id = 1;"},
		{sql: "insert into order_line values (1, 1, 1, 1, 1, 1, '', 1, 1.00, 'dist');"},
		{sql: "update orders set o_all_local = 0 where o_id = 1 and o_d_id = 1 and o_w_id = 1;"},
		{sql: "select c_id, c_first from customer where c_w_id = 1 and c_d_id = 1 and c_last = 'BARBARBAR' order by c_first, c_id;",
			columns: []byte{i, c}},
		{sql: "update warehouse set w_ytd = w_ytd + 1.00 where w_id = 1;"},
		{sql: "select w_ytd from warehouse where w_id = 1;",
			columns: []byte{f}},
		{sql: "select w_street_1, w_street_2, w_city, w_state, w_zip, w_name from warehouse where w_id = 1;",
			columns: []byte{c, c, c, c, c, c}},
		{sql: "update district set d_ytd = d_ytd + 1.00 where d_w_id = 1 and d_id = 1;"},
		{sql: "select d_ytd from district where d_w_id = 1 and d_id = 1;",
			columns: []byte{f}},
		{sql: "select d_street_1, d_street_2, d_city, d_state, d_zip, d_name from district where d_w_id = 1 and d_id = 1;",
			columns: []byte{c, c, c, c, c, c}},
		{sql: "update customer set c_balance = c_balance - 1.00, c_ytd_payment = c_ytd_payment + 1.00, c_payment_cnt = c_payment_cnt + 1 where c_w_id = 1 and c_d_id = 1 and c_id = 1;"},
		{sql: "select c_balance, c_ytd_payment from customer where c_w_id = 1 and c_d_id = 1 and c_id = 1;",
			columns: []byte{f, f}},
		{sql: "select c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since from customer where c_w_id = 1 and c_d_id = 1 and c_id = 1;",
			columns: []byte{c, c, c, c, c, c, c, c, c, c, i, f, f, c}},
		{sql: "insert into history values (1, 1, 1, 1, 1, '2026-01-01 00:00:00', 1.00, 'payment');"},
		{sql: "select c_id, c_balance, c_first, c_middle, c_last from customer where c_w_id = 1 and c_d_id = 1 and c_last = 'BARBARBAR' order by c_first, c_id;",
			columns: []byte{i, f, c, c, c}},
		{sql: "select c_balance, c_first, c_middle, c_last from customer where c_w_id = 1 and c_d_id = 1 and c_id = 1;",
			columns: []byte{f, c, c, c}},
		{sql: "select o_id, o_entry_d, o_carrier_id from orders where o_w_id = 1 and o_d_id = 1 and o_c_id = 1 order by o_id desc limit 1;",
			columns: []byte{i, c, i}},
		{sql: "select o_id, o_entry_d, o_carrier_id from orders where o_w_id = 1 and o_d_id = 1 and o_c_id = 1 and o_id = 1;",
			columns: []byte{i, c, i}},
		{sql: "select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from order_line where ol_w_id = 1 and ol_d_id = 1 and ol_o_id = 1;",
			columns: []byte{i, i, i, f, c}},
		{sql: "select min(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1;",
			columns: []byte{i}},
		{sql: "update new_orders set no_o_id = no_o_id where no_w_id = 1 and no_d_id = 1 and no_o_id = 1;"},
		{sql: "select min(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1 and no_o_id = 1;",
			columns: []byte{i}},
		{sql: "delete from new_orders where no_w_id = 1 and no_d_id = 1 and no_o_id = 1;"},
		{sql: "select o_c_id from orders where o_id = 1 and o_d_id = 1 and o_w_id = 1;",
			columns: []byte{i}},
		{sql: "update orders set o_carrier_id = 1 where o_id = 1 and o_d_id = 1 and o_w_id = 1;"},
		{sql: "update order_line set ol_delivery_d = '2026-01-01 00:00:00' where ol_o_id = 1 and ol_d_id = 1 and ol_w_id = 1;"},
		{sql: "select sum(ol_amount) from order_line where ol_o_id = 1 and ol_d_id = 1 and ol_w_id = 1;",
			columns: []byte{f}},
		{sql: "update customer set c_balance = c_balance + 1.00, c_delivery_cnt = c_delivery_cnt + 1 where c_id = 1 and c_d_id = 1 and c_w_id = 1;"},
		{sql: "select c_balance from customer where c_id = 1 and c_d_id = 1 and c_w_id = 1;",
			columns: []byte{f}},
		{sql: "select d_next_o_id from district where d_id = 1 and d_w_id = 1;",
			columns: []byte{i}},
		{sql: "select count(distinct ol_i_id) from order_line, stock where ol_w_id = 1 and ol_d_id = 1 and ol_o_id >= 1 and ol_o_id < 2 and s_w_id = 1 and s_i_id = ol_i_id and s_quantity < 10;",
			columns: []byte{i}},
	}
}
