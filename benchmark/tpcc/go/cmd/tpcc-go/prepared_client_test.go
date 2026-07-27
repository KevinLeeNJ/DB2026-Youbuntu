package main

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"
)

func appendU16(value []byte, number uint16) []byte {
	var bytes [2]byte
	binary.BigEndian.PutUint16(bytes[:], number)
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
