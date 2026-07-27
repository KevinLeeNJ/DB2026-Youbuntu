package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const sampleTestWarehouses = 3

// sampleTestManifest generates a small but multi-warehouse dataset and returns
// its manifest, which now carries the aggregates and row samples the post-load
// relation and content validation compare against.
func sampleTestManifest(t *testing.T) datasetManifest {
	t.Helper()
	dir := t.TempDir()
	if err := generateData(sampleTestWarehouses, dir, 11, true); err != nil {
		t.Fatal(err)
	}
	manifest, err := readDatasetManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func TestDatasetManifestRecordsAggregatesMatchingTheCSV(t *testing.T) {
	dir := t.TempDir()
	if err := generateData(sampleTestWarehouses, dir, 13, true); err != nil {
		t.Fatal(err)
	}
	manifest, err := readDatasetManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Recount straight from the CSV files with an independent reader.
	olCntSum := csvColumnSum(t, filepath.Join(dir, "orders.csv"), "o_ol_cnt")
	carrierZero := csvColumnMatches(t, filepath.Join(dir, "orders.csv"), "o_carrier_id", func(v string) bool { return v == "0" })
	deliveryNulls := csvColumnMatches(t, filepath.Join(dir, "order_line.csv"), "ol_delivery_d", func(v string) bool { return v == "" })
	for _, check := range []struct {
		key  string
		want float64
	}{
		{aggOrdersOlCntSum, olCntSum},
		{aggOrdersCarrierZeroRows, carrierZero},
		{aggOrderLineDeliveryNulls, deliveryNulls},
	} {
		if got := manifest.Aggregates[check.key]; got != check.want {
			t.Errorf("manifest aggregate %s = %v, want %v", check.key, got, check.want)
		}
	}
	// The generator formula and the recorded aggregate must agree today; once
	// PLAN.md item 1.10 makes the generator random only the manifest survives.
	if want := float64(expectedUndeliveredOrderLines(sampleTestWarehouses)); deliveryNulls != want {
		t.Errorf("CSV has %v rows without a delivery time, generator formula says %v", deliveryNulls, want)
	}
}

func TestDatasetManifestSamplesSpreadOverPartitions(t *testing.T) {
	manifest := sampleTestManifest(t)
	for _, spec := range datasetSampleSpecs {
		samples := manifest.Samples[spec.table]
		// A table smaller than the reservoir contributes every row it has.
		wantSamples := datasetSampleCount
		if rows := int(manifest.Files[spec.table].Rows); rows < wantSamples {
			wantSamples = rows
		}
		if len(samples) != wantSamples {
			t.Errorf("%s has %d samples, want %d", spec.table, len(samples), wantSamples)
		}
		if len(spec.partitionColumns) == 0 {
			continue
		}
		partitions := map[string]struct{}{}
		for _, sample := range samples {
			partitions[samplePartitionKey(sample, spec.partitionColumns)] = struct{}{}
		}
		want := datasetSampleMinPartitions
		if available := availablePartitions(spec.partitionScale, manifest.Warehouses); available < want {
			want = available
		}
		if len(partitions) < want {
			t.Errorf("%s samples cover %d partitions, want at least %d", spec.table, len(partitions), want)
		}
	}
}

func TestReadDatasetManifestRejectsAManifestWithoutDerivedFacts(t *testing.T) {
	dir := t.TempDir()
	if err := generateData(1, dir, 5, true); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, datasetManifestName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, drop := range []string{`"aggregates"`, `"samples"`} {
		stripped := strings.Replace(string(raw), drop, `"unused_`+strings.Trim(drop, `"`)+`"`, 1)
		if err := os.WriteFile(path, []byte(stripped), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := readDatasetManifest(dir); err == nil || !strings.Contains(err.Error(), "refresh-manifest") {
			t.Errorf("manifest without %s was accepted: %v", drop, err)
		}
	}
}

// sampleSQLExecutor answers the relation and content sampling queries from the
// manifest itself, so a correct database is modelled exactly.
type sampleSQLExecutor struct {
	manifest datasetManifest
	// overrides replaces the answer of one query, which is how a violation is
	// injected.
	overrides  map[string]string
	statements []string
}

func (e *sampleSQLExecutor) exec(sql string) (string, error) {
	e.statements = append(e.statements, sql)
	if answer, ok := e.overrides[sql]; ok {
		return answer, nil
	}
	if answer, ok := e.answerRelation(sql); ok {
		return answer, nil
	}
	if answer, ok := e.answerLineCount(sql); ok {
		return answer, nil
	}
	if answer, ok := e.answerContent(sql); ok {
		return answer, nil
	}
	return "", fmt.Errorf("unexpected SQL %q", sql)
}

func scalarAnswer(value string) string {
	return fmt.Sprintf("+---+\n| n |\n+---+\n| %s |\n+---+\nTotal record(s): 1\n", value)
}

func rowAnswer(values []string) string {
	header := make([]string, len(values))
	for i := range values {
		header[i] = fmt.Sprintf("c%d", i+1)
	}
	return fmt.Sprintf("| %s |\n| %s |\nTotal record(s): 1\n", strings.Join(header, " | "), strings.Join(values, " | "))
}

func (e *sampleSQLExecutor) answerRelation(sql string) (string, bool) {
	for _, spec := range relationSampleSpecs {
		for _, sample := range e.manifest.Samples[spec.child] {
			args, err := sampleArguments(sample, spec.columns)
			if err != nil {
				continue
			}
			if fmt.Sprintf(spec.template, args...) == sql {
				return scalarAnswer("1"), true
			}
		}
	}
	return "", false
}

func (e *sampleSQLExecutor) answerLineCount(sql string) (string, bool) {
	for _, sample := range e.manifest.Samples["orders"] {
		args, err := sampleArguments(sample, []string{"o_w_id", "o_d_id", "o_id"})
		if err != nil {
			continue
		}
		want := fmt.Sprintf("select count(*) from order_line where ol_w_id = %v and ol_d_id = %v and ol_o_id = %v;",
			args[0], args[1], args[2])
		if want == sql {
			return scalarAnswer(sample["o_ol_cnt"]), true
		}
	}
	return "", false
}

func (e *sampleSQLExecutor) answerContent(sql string) (string, bool) {
	for _, table := range contentSampleTables {
		spec, ok := datasetSampleSpecFor(table)
		if !ok {
			continue
		}
		for _, sample := range e.manifest.Samples[table] {
			predicate, err := sampleKeyPredicate(sample, spec.keys)
			if err != nil {
				continue
			}
			want := fmt.Sprintf("select %s from %s where %s;", strings.Join(spec.values, ", "), table, predicate)
			if want != sql {
				continue
			}
			values := make([]string, len(spec.values))
			for i, column := range spec.values {
				if sample[column] == "" {
					values[i] = "NULL"
				} else {
					values[i] = sample[column]
				}
			}
			return rowAnswer(values), true
		}
	}
	return "", false
}

func TestVerifyLoadSamplingAcceptsACorrectlyLoadedDatabase(t *testing.T) {
	manifest := sampleTestManifest(t)
	executor := &sampleSQLExecutor{manifest: manifest}
	if err := verifyLoadRelationSamples(executor, manifest); err != nil {
		t.Fatal(err)
	}
	want := 0
	for _, spec := range relationSampleSpecs {
		want += len(manifest.Samples[spec.child])
	}
	if got := len(executor.statements); got != want {
		t.Errorf("relation sampling issued %d queries, want %d", got, want)
	}
	executor.statements = nil
	if err := verifyLoadContentSamples(executor, manifest); err != nil {
		t.Fatal(err)
	}
	// One keyed row read per sampled row per content table, plus the order line
	// count of every sampled order.
	want = len(manifest.Samples["orders"])
	for _, table := range contentSampleTables {
		want += len(manifest.Samples[table])
	}
	if got := len(executor.statements); got != want {
		t.Errorf("content sampling issued %d queries, want %d", got, want)
	}
}

func TestVerifyLoadRelationSamplesRejectsAMissingParentForEveryRelation(t *testing.T) {
	manifest := sampleTestManifest(t)
	for _, spec := range relationSampleSpecs {
		sample := manifest.Samples[spec.child][0]
		args, err := sampleArguments(sample, spec.columns)
		if err != nil {
			t.Fatal(err)
		}
		sql := fmt.Sprintf(spec.template, args...)
		executor := &sampleSQLExecutor{manifest: manifest, overrides: map[string]string{sql: scalarAnswer("0")}}
		err = verifyLoadRelationSamples(executor, manifest)
		if err == nil || !strings.Contains(err.Error(), spec.name) {
			t.Errorf("a dangling %s reference reported %v, want the relation name", spec.name, err)
		}
	}
}

func TestVerifyLoadContentSamplesRejectsAWrongValueForEveryTable(t *testing.T) {
	manifest := sampleTestManifest(t)
	for _, table := range contentSampleTables {
		spec, ok := datasetSampleSpecFor(table)
		if !ok {
			t.Fatalf("%s has no sample specification", table)
		}
		sample := manifest.Samples[table][0]
		predicate, err := sampleKeyPredicate(sample, spec.keys)
		if err != nil {
			t.Fatal(err)
		}
		sql := fmt.Sprintf("select %s from %s where %s;", strings.Join(spec.values, ", "), table, predicate)
		values := make([]string, len(spec.values))
		for i, column := range spec.values {
			values[i] = corruptValue(sample[column])
		}
		executor := &sampleSQLExecutor{manifest: manifest, overrides: map[string]string{sql: rowAnswer(values)}}
		err = verifyLoadContentSamples(executor, manifest)
		if err == nil || !strings.Contains(err.Error(), "LOAD content mismatch") || !strings.Contains(err.Error(), table) {
			t.Errorf("a wrong %s value reported %v, want a content mismatch naming the table", table, err)
		}
	}
}

func TestVerifyLoadContentSamplesRejectsAWrongOrderLineCount(t *testing.T) {
	manifest := sampleTestManifest(t)
	sample := manifest.Samples["orders"][0]
	args, err := sampleArguments(sample, []string{"o_w_id", "o_d_id", "o_id"})
	if err != nil {
		t.Fatal(err)
	}
	sql := fmt.Sprintf("select count(*) from order_line where ol_w_id = %v and ol_d_id = %v and ol_o_id = %v;",
		args[0], args[1], args[2])
	lines, err := strconv.Atoi(sample["o_ol_cnt"])
	if err != nil {
		t.Fatal(err)
	}
	executor := &sampleSQLExecutor{manifest: manifest,
		overrides: map[string]string{sql: scalarAnswer(strconv.Itoa(lines - 1))}}
	if err := verifyLoadContentSamples(executor, manifest); err == nil ||
		!strings.Contains(err.Error(), "orders line count") {
		t.Errorf("a wrong order line count reported %v, want an order line count mismatch", err)
	}
}

func TestVerifyLoadContentSamplesRejectsANonNullDeliveryTime(t *testing.T) {
	// order_line.ol_delivery_d is the only sampled column the generator can leave
	// empty, so it is the one that proves the loader turned an empty CSV field into
	// SQL NULL rather than into an empty string.
	manifest := sampleTestManifest(t)
	spec, _ := datasetSampleSpecFor("order_line")
	injected := false
	for _, sample := range manifest.Samples["order_line"] {
		if sample["ol_delivery_d"] != "" {
			continue
		}
		predicate, err := sampleKeyPredicate(sample, spec.keys)
		if err != nil {
			t.Fatal(err)
		}
		sql := fmt.Sprintf("select %s from order_line where %s;", strings.Join(spec.values, ", "), predicate)
		values := make([]string, len(spec.values))
		for i, column := range spec.values {
			if column == "ol_delivery_d" {
				values[i] = fixedTimestamp
			} else {
				values[i] = sample[column]
			}
		}
		executor := &sampleSQLExecutor{manifest: manifest, overrides: map[string]string{sql: rowAnswer(values)}}
		if err := verifyLoadContentSamples(executor, manifest); err == nil ||
			!strings.Contains(err.Error(), "want NULL") {
			t.Errorf("a non-NULL delivery time reported %v, want a NULL mismatch", err)
		}
		injected = true
		break
	}
	if !injected {
		t.Skip("this sample carries no order_line row without a delivery time")
	}
}

func TestCompareSampledValueHandlesEveryColumnShape(t *testing.T) {
	cases := []struct {
		column, want, got string
		ok                bool
	}{
		{"c_credit", "BC", "BC", true},
		{"c_credit", "BC", "GC", false},
		{"w_ytd", "300000.0", "300000", true},
		{"d_tax", "0.1234", "0.12340001", true},
		{"d_tax", "0.1234", "0.1244", false},
		{"ol_delivery_d", "", "NULL", true},
		{"ol_delivery_d", "", "", false},
		{"ol_delivery_d", fixedTimestamp, "NULL", false},
		{"c_last", "BARBARBAR", "BARBARBAR ", true},
	}
	for _, test := range cases {
		err := compareSampledValue(test.column, test.want, test.got)
		if (err == nil) != test.ok {
			t.Errorf("compareSampledValue(%q, %q, %q) = %v, want ok=%v", test.column, test.want, test.got, err, test.ok)
		}
	}
}

func corruptValue(value string) string {
	if value == "" {
		return fixedTimestamp
	}
	if number, err := strconv.ParseFloat(value, 64); err == nil {
		return strconv.FormatFloat(number+1, 'f', -1, 64)
	}
	return value + "Z"
}

func csvColumn(t *testing.T, path, column string) (int, [][]string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	index := -1
	for i, name := range header {
		if name == column {
			index = i
		}
	}
	if index < 0 {
		t.Fatalf("%s has no column %s", path, column)
	}
	rows := make([][]string, 0)
	for {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, append([]string{}, row...))
	}
	return index, rows
}

func csvColumnSum(t *testing.T, path, column string) float64 {
	t.Helper()
	index, rows := csvColumn(t, path, column)
	total := 0.0
	for _, row := range rows {
		value, err := strconv.ParseFloat(row[index], 64)
		if err != nil {
			t.Fatal(err)
		}
		total += value
	}
	return total
}

func csvColumnMatches(t *testing.T, path, column string, match func(string) bool) float64 {
	t.Helper()
	index, rows := csvColumn(t, path, column)
	count := 0.0
	for _, row := range rows {
		if match(row[index]) {
			count++
		}
	}
	return count
}
