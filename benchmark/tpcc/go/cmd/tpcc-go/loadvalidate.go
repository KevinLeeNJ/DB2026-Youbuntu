package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Post-load index-key relation sampling and cross-partition content sampling
// (final.md:258-259, 294-296). The official evaluator does not run a full outer
// join over the large tables; it picks scattered index keys and checks that the
// referenced parent row exists, then compares the real generated values of
// sampled rows. Both sets of expected values come from the dataset manifest, so
// they stay correct when PLAN.md item 1.10 makes the generator truly random.

var float32Columns = map[string]struct{}{
	"w_tax": {}, "w_ytd": {},
	"d_tax": {}, "d_ytd": {},
	"c_discount": {}, "c_balance": {}, "c_ytd_payment": {},
	"h_amount": {}, "ol_amount": {}, "i_price": {}, "s_ytd": {},
}

// contentSampleTables are the tables whose sampled rows are compared value by
// value. final.md:295 names item prices, stock quantities, warehouse and
// district initial values, customer state, and order state plus line counts.
// `history` is deliberately excluded: the official index set (final.md:253)
// declares no index for it, so a keyed lookup would be a sequential scan of a
// 1.5 M row table per sample and would eat the 900 s load budget. `new_orders`
// carries no non-key column to compare.
var contentSampleTables = []string{"warehouse", "district", "customer", "item", "stock", "orders", "order_line"}

// relationSampleSpec turns a sampled child row into an existence probe against
// the parent table through the parent's index key.
type relationSampleSpec struct {
	name     string
	child    string
	columns  []string
	template string
}

var relationSampleSpecs = []relationSampleSpec{
	{"district -> warehouse", "district", []string{"d_w_id"},
		"select count(*) from warehouse where w_id = %s;"},
	{"customer -> district", "customer", []string{"c_w_id", "c_d_id"},
		"select count(*) from district where d_w_id = %s and d_id = %s;"},
	{"orders -> customer", "orders", []string{"o_w_id", "o_d_id", "o_c_id"},
		"select count(*) from customer where c_w_id = %s and c_d_id = %s and c_id = %s;"},
	{"new_orders -> orders", "new_orders", []string{"no_w_id", "no_d_id", "no_o_id"},
		"select count(*) from orders where o_w_id = %s and o_d_id = %s and o_id = %s;"},
	{"order_line -> orders", "order_line", []string{"ol_w_id", "ol_d_id", "ol_o_id"},
		"select count(*) from orders where o_w_id = %s and o_d_id = %s and o_id = %s;"},
	{"order_line -> item", "order_line", []string{"ol_i_id"},
		"select count(*) from item where i_id = %s;"},
	{"order_line -> stock", "order_line", []string{"ol_supply_w_id", "ol_i_id"},
		"select count(*) from stock where s_w_id = %s and s_i_id = %s;"},
	{"history -> customer", "history", []string{"h_c_w_id", "h_c_d_id", "h_c_id"},
		"select count(*) from customer where c_w_id = %s and c_d_id = %s and c_id = %s;"},
}

func sampleArguments(sample map[string]string, columns []string) ([]any, error) {
	args := make([]any, len(columns))
	for i, column := range columns {
		value, ok := sample[column]
		if !ok {
			return nil, fmt.Errorf("sampled row is missing column %s", column)
		}
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			return nil, fmt.Errorf("sampled key column %s holds a non-integer value %q", column, value)
		}
		args[i] = value
	}
	return args, nil
}

// verifyLoadRelationSamples checks that every sampled child row resolves to
// exactly one parent row through the parent's index key.
func verifyLoadRelationSamples(c sqlExecutor, manifest datasetManifest) error {
	failures := make([]string, 0)
	checks := 0
	for _, spec := range relationSampleSpecs {
		samples := manifest.Samples[spec.child]
		if len(samples) == 0 {
			failures = append(failures, fmt.Sprintf("%s: the dataset manifest has no %s samples", spec.name, spec.child))
			continue
		}
		for _, sample := range samples {
			args, err := sampleArguments(sample, spec.columns)
			if err != nil {
				failures = append(failures, fmt.Sprintf("%s: %v", spec.name, err))
				continue
			}
			sql := fmt.Sprintf(spec.template, args...)
			checks++
			text, err := c.exec(sql)
			if err != nil {
				failures = append(failures, fmt.Sprintf("%s: %v", spec.name, err))
				continue
			}
			got, err := scalarIntStrict(text)
			if err != nil {
				failures = append(failures, fmt.Sprintf("%s: %v", spec.name, err))
				continue
			}
			if got != 1 {
				failures = append(failures, fmt.Sprintf("%s: %s referenced by %v resolved to %d parent row(s), want 1",
					spec.name, spec.child, sampleKeyText(sample, spec.columns), got))
			}
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("LOAD integrity mismatch: index-key relation sampling failed %d of %d check(s)\n%s",
			len(failures), checks, strings.Join(failures, "\n"))
	}
	fmt.Printf("[load] index-key relation sampling passed %d checks across %d relations\n", checks, len(relationSampleSpecs))
	return nil
}

func sampleKeyText(sample map[string]string, columns []string) string {
	parts := make([]string, len(columns))
	for i, column := range columns {
		parts[i] = fmt.Sprintf("%s=%s", column, sample[column])
	}
	return strings.Join(parts, " ")
}

// verifyLoadContentSamples compares the loaded rows against the real generated
// values recorded in the manifest, spread over many warehouses and districts.
func verifyLoadContentSamples(c sqlExecutor, manifest datasetManifest) error {
	failures := make([]string, 0)
	checks := 0
	for _, table := range contentSampleTables {
		spec, ok := datasetSampleSpecFor(table)
		if !ok || len(spec.values) == 0 {
			failures = append(failures, fmt.Sprintf("%s: no content sample specification", table))
			continue
		}
		samples := manifest.Samples[table]
		if len(samples) == 0 {
			failures = append(failures, fmt.Sprintf("%s: the dataset manifest has no samples", table))
			continue
		}
		for _, sample := range samples {
			predicate, err := sampleKeyPredicate(sample, spec.keys)
			if err != nil {
				failures = append(failures, fmt.Sprintf("%s: %v", table, err))
				continue
			}
			sql := fmt.Sprintf("select %s from %s where %s;", strings.Join(spec.values, ", "), table, predicate)
			text, err := c.exec(sql)
			if err != nil {
				failures = append(failures, fmt.Sprintf("%s [%s]: %v", table, predicate, err))
				continue
			}
			rows := parseRows(text)
			if len(rows) != 1 || len(rows[0]) != len(spec.values) {
				failures = append(failures, fmt.Sprintf("%s [%s]: returned %d row(s) instead of one row of %d column(s)",
					table, predicate, len(rows), len(spec.values)))
				continue
			}
			for i, column := range spec.values {
				checks++
				if err := compareSampledValue(column, sample[column], rows[0][i]); err != nil {
					failures = append(failures, fmt.Sprintf("%s [%s]: %v", table, predicate, err))
				}
			}
		}
	}
	// final.md:295 also requires the sampled order's line count to match, which
	// is a content fact rather than a row-count aggregate.
	for _, sample := range manifest.Samples["orders"] {
		args, err := sampleArguments(sample, []string{"o_w_id", "o_d_id", "o_id"})
		if err != nil {
			failures = append(failures, fmt.Sprintf("orders: %v", err))
			continue
		}
		predicate := fmt.Sprintf("o_w_id = %v and o_d_id = %v and o_id = %v", args[0], args[1], args[2])
		linePredicate := fmt.Sprintf("ol_w_id = %v and ol_d_id = %v and ol_o_id = %v", args[0], args[1], args[2])
		checks++
		text, err := c.exec(fmt.Sprintf("select count(*) from order_line where %s;", linePredicate))
		if err != nil {
			failures = append(failures, fmt.Sprintf("orders line count [%s]: %v", predicate, err))
			continue
		}
		got, err := scalarIntStrict(text)
		if err != nil {
			failures = append(failures, fmt.Sprintf("orders line count [%s]: %v", predicate, err))
			continue
		}
		want, err := strconv.Atoi(sample["o_ol_cnt"])
		if err != nil {
			failures = append(failures, fmt.Sprintf("orders line count [%s]: manifest o_ol_cnt %q is not an integer", predicate, sample["o_ol_cnt"]))
			continue
		}
		if got != want {
			failures = append(failures, fmt.Sprintf("orders line count [%s]: got %d order_line row(s), want o_ol_cnt=%d", predicate, got, want))
		}
	}
	for _, check := range manifest.OrderLineChecks {
		predicate := fmt.Sprintf("ol_w_id = %d and ol_d_id = %d and ol_o_id = %d",
			check.WarehouseID, check.DistrictID, check.OrderID)
		text, err := c.exec(fmt.Sprintf(
			"select ol_number, ol_amount from order_line where %s order by ol_number;", predicate))
		if err != nil {
			failures = append(failures, fmt.Sprintf("fixed order_line [%s]: %v", predicate, err))
			continue
		}
		rows := parseRows(text)
		if len(rows) != len(check.AmountBits) {
			failures = append(failures, fmt.Sprintf("fixed order_line [%s]: got %d rows, want %d",
				predicate, len(rows), len(check.AmountBits)))
			continue
		}
		bins := exactFloat32Bins{}
		for index, row := range rows {
			checks++
			if len(row) != 2 || row[0] != strconv.Itoa(index+1) {
				failures = append(failures, fmt.Sprintf("fixed order_line [%s]: invalid line %d", predicate, index+1))
				continue
			}
			value, err := strconv.ParseFloat(row[1], 32)
			if err != nil {
				failures = append(failures, fmt.Sprintf("fixed order_line [%s]: invalid FLOAT32 %q", predicate, row[1]))
				continue
			}
			gotBits := math.Float32bits(float32(value))
			if !equalFloat32Bits(gotBits, check.AmountBits[index]) {
				failures = append(failures, fmt.Sprintf(
					"fixed order_line [%s] line %d: got 0x%08x, want 0x%08x, tolerance 0 ULP",
					predicate, index+1, gotBits, check.AmountBits[index]))
			}
			if err := bins.add(math.Float32frombits(check.AmountBits[index])); err != nil {
				failures = append(failures, fmt.Sprintf("fixed order_line [%s]: %v", predicate, err))
			}
		}
		checks++
		sumText, err := c.exec(fmt.Sprintf("select sum(ol_amount) from order_line where %s;", predicate))
		if err != nil {
			failures = append(failures, fmt.Sprintf("fixed order_line SUM [%s]: %v", predicate, err))
			continue
		}
		sumValue, err := scalarFloatStrict(sumText)
		if err != nil {
			failures = append(failures, fmt.Sprintf("fixed order_line SUM [%s]: %v", predicate, err))
			continue
		}
		gotBits := math.Float32bits(float32(sumValue))
		wantBits := math.Float32bits(float32(bins.float64()))
		if !equalFloat32Bits(gotBits, wantBits) {
			failures = append(failures, fmt.Sprintf(
				"fixed order_line SUM [%s]: got 0x%08x, want 0x%08x, tolerance 0 ULP",
				predicate, gotBits, wantBits))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("LOAD content mismatch: cross-partition content sampling failed %d of %d comparison(s)\n%s",
			len(failures), checks, strings.Join(failures, "\n"))
	}
	fmt.Printf("[load] cross-partition content sampling passed %d value comparisons across %d tables\n", checks, len(contentSampleTables))
	return nil
}

func sampleKeyPredicate(sample map[string]string, keys []string) (string, error) {
	args, err := sampleArguments(sample, keys)
	if err != nil {
		return "", err
	}
	parts := make([]string, len(keys))
	for i, key := range keys {
		parts[i] = fmt.Sprintf("%s = %v", key, args[i])
	}
	return strings.Join(parts, " and "), nil
}

// compareSampledValue compares one generated CSV value against what the database
// returned. An empty CSV field in the finalv3 TPC-C data is a non-NULL empty
// CHAR value.
func compareSampledValue(column, want, got string) error {
	got = strings.TrimSpace(got)
	if want == "" {
		if got != "" {
			return fmt.Errorf("%s: got %q, want empty CHAR", column, got)
		}
		return nil
	}
	if got == "NULL" {
		return fmt.Errorf("%s: got NULL, want %q", column, want)
	}
	if _, isFloat32 := float32Columns[column]; isFloat32 {
		wantNumber, wantErr := strconv.ParseFloat(want, 32)
		gotNumber, gotErr := strconv.ParseFloat(got, 32)
		if wantErr != nil || gotErr != nil {
			return fmt.Errorf("%s: invalid FLOAT32 got=%q want=%q", column, got, want)
		}
		if math.IsNaN(wantNumber) || math.IsInf(wantNumber, 0) ||
			math.IsNaN(gotNumber) || math.IsInf(gotNumber, 0) {
			return fmt.Errorf("%s: non-finite FLOAT32 got=%q want=%q", column, got, want)
		}
		wantBits, gotBits := math.Float32bits(float32(wantNumber)), math.Float32bits(float32(gotNumber))
		if !equalFloat32Bits(wantBits, gotBits) {
			return fmt.Errorf("%s: got %s (0x%08x), want %s (0x%08x), tolerance 0 ULP",
				column, got, gotBits, want, wantBits)
		}
		return nil
	}
	if got != strings.TrimSpace(want) {
		return fmt.Errorf("%s: got %q, want %q", column, got, want)
	}
	return nil
}
