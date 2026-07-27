package main

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
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

func TestRankingTemplateSamplesAreBoundedAndUnique(t *testing.T) {
	seen := make(map[string]struct{})
	for _, sample := range rankingTemplateSamples() {
		template, args, err := parameterizeSQL(sample)
		if err != nil {
			t.Fatal(err)
		}
		if len(template) > maxWirePayload || len(args) > 256 {
			t.Fatalf("template exceeds protocol bounds: %q", sample)
		}
		if _, duplicate := seen[template]; duplicate {
			continue
		}
		seen[template] = struct{}{}
	}
	if len(seen) < 30 {
		t.Fatalf("prepared ranking template set has only %d statements", len(seen))
	}
}

func TestDecodePrepareOKSupportsFinalAndCurrentServerSchemas(t *testing.T) {
	pending := []pendingStatement{
		{id: 1, query: false},
		{id: 2, query: true, args: []preparedArgument{{typ: wireTypeInt32}}},
	}
	columnName := "value"
	makeBody := func(legacy bool) []byte {
		body := appendU16(nil, 2)
		body = appendU16(body, 1)
		if legacy {
			body = appendU16(body, 0)
		}
		body = appendU16(body, 0)
		body = appendU16(body, 2)
		if legacy {
			body = appendU16(body, 1)
			body = append(body, wireTypeInt32)
		}
		body = appendU16(body, 1)
		body = appendU16(body, uint16(len(columnName)))
		body = append(body, columnName...)
		body = append(body, wireTypeInt32)
		return body
	}
	for _, legacy := range []bool{false, true} {
		decoded, err := decodePrepareOK(makeBody(legacy), pending)
		if err != nil {
			t.Fatalf("legacy=%v: %v", legacy, err)
		}
		if decoded[2].columns[0].name != columnName || decoded[2].columns[0].sqlType != wireTypeInt32 {
			t.Fatalf("legacy=%v: decoded schema = %#v", legacy, decoded[2])
		}
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
