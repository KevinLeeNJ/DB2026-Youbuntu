package main

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
)

// Dataset facts recorded in the manifest by a single pass over the generated
// CSV set.
//
// IMPORTANT (dependency on PLAN.md item 1.10): every expected value used by the
// post-load integrity, relation and content validation must come from here, not
// from a generator formula. The generator is deterministic today
// (`initialOrderLineCount` is `5+(oID+dID+wID)%11`, the ORIGINAL marker is
// `iID%10==0`), so a formula would work — but item 1.10 replaces those formulas
// with true randomness precisely because the fixed shape hides bugs. Once that
// lands the generator can no longer be inverted, and only manifest-recorded
// facts survive. `expectedUndeliveredOrderLines` is therefore kept only as a
// cross-check of the recorded aggregate inside the unit tests.
const (
	// datasetSampleCount is how many rows per table the manifest keeps for the
	// index-key relation sampling and the content sampling of final.md:258-259.
	// The rows are drawn uniformly over the whole file, which is ordered by
	// warehouse and district, so the sample is spread over many partitions.
	datasetSampleCount = 24
	// datasetSampleMinPartitions is the minimum number of distinct partitions a
	// warehouse- or district-partitioned sample must touch. final.md:259 requires
	// the content sampling to be "分布在多个仓库与分区"; refusing to publish a
	// manifest that fails this keeps that property auditable instead of implicit.
	datasetSampleMinPartitions = 4
)

type aggregateKind string

const (
	aggregateSum   aggregateKind = "sum"
	aggregateZeros aggregateKind = "zeros"
	aggregateNulls aggregateKind = "nulls"
)

// Manifest aggregate keys. These are the exact values of the CSV set generated
// by this run, which is what final.md:277,285-292 compares the loaded database
// against.
const (
	aggOrdersOlCntSum         = "orders.o_ol_cnt.sum"
	aggOrdersCarrierZeroRows  = "orders.o_carrier_id.zeros"
	aggOrderLineDeliveryNulls = "order_line.ol_delivery_d.nulls"
)

type manifestAggregateSpec struct {
	key    string
	table  string
	column string
	kind   aggregateKind
}

var datasetAggregateSpecs = []manifestAggregateSpec{
	{aggOrdersOlCntSum, "orders", "o_ol_cnt", aggregateSum},
	{aggOrdersCarrierZeroRows, "orders", "o_carrier_id", aggregateZeros},
	{aggOrderLineDeliveryNulls, "order_line", "ol_delivery_d", aggregateNulls},
}

// partitionScale says how many partitions a table has, which bounds how many
// distinct partitions a sample can possibly cover.
type partitionScale int

const (
	partitionNone partitionScale = iota
	partitionByWarehouse
	partitionByDistrict
)

func availablePartitions(scale partitionScale, warehouses int) int {
	switch scale {
	case partitionByWarehouse:
		return warehouses
	case partitionByDistrict:
		return warehouses * districtsPerWarehouse
	default:
		return 0
	}
}

// datasetSampleSpec describes which columns of a table are captured for every
// sampled row. `keys` identifies the row (all key columns are integers so they
// can be inlined into a WHERE clause without quoting); `values` are the columns
// the content validation compares.
type datasetSampleSpec struct {
	table  string
	keys   []string
	values []string
	// partitionColumns are the columns whose distinct combinations must cover at
	// least datasetSampleMinPartitions values. Empty for unpartitioned tables.
	partitionColumns []string
	partitionScale   partitionScale
}

var datasetSampleSpecs = []datasetSampleSpec{
	{table: "warehouse", keys: []string{"w_id"}, values: []string{"w_name", "w_tax", "w_ytd"},
		partitionColumns: []string{"w_id"}, partitionScale: partitionByWarehouse},
	{table: "district", keys: []string{"d_w_id", "d_id"}, values: []string{"d_name", "d_tax", "d_ytd", "d_next_o_id"},
		partitionColumns: []string{"d_w_id", "d_id"}, partitionScale: partitionByDistrict},
	{table: "customer", keys: []string{"c_w_id", "c_d_id", "c_id"},
		values:           []string{"c_last", "c_credit", "c_discount", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt"},
		partitionColumns: []string{"c_w_id", "c_d_id"}, partitionScale: partitionByDistrict},
	{table: "item", keys: []string{"i_id"}, values: []string{"i_im_id", "i_name", "i_price"}},
	{table: "stock", keys: []string{"s_w_id", "s_i_id"},
		values:           []string{"s_quantity", "s_ytd", "s_order_cnt", "s_remote_cnt"},
		partitionColumns: []string{"s_w_id"}, partitionScale: partitionByWarehouse},
	{table: "orders", keys: []string{"o_w_id", "o_d_id", "o_id"},
		values:           []string{"o_c_id", "o_carrier_id", "o_ol_cnt", "o_all_local"},
		partitionColumns: []string{"o_w_id", "o_d_id"}, partitionScale: partitionByDistrict},
	{table: "new_orders", keys: []string{"no_w_id", "no_d_id", "no_o_id"},
		partitionColumns: []string{"no_w_id", "no_d_id"}, partitionScale: partitionByDistrict},
	{table: "order_line", keys: []string{"ol_w_id", "ol_d_id", "ol_o_id", "ol_number"},
		values:           []string{"ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount"},
		partitionColumns: []string{"ol_w_id", "ol_d_id"}, partitionScale: partitionByDistrict},
	{table: "history", keys: []string{"h_c_w_id", "h_c_d_id", "h_c_id"}, values: []string{"h_amount"},
		partitionColumns: []string{"h_c_w_id", "h_c_d_id"}, partitionScale: partitionByDistrict},
}

func datasetSampleSpecFor(table string) (datasetSampleSpec, bool) {
	for _, spec := range datasetSampleSpecs {
		if spec.table == table {
			return spec, true
		}
	}
	return datasetSampleSpec{}, false
}

// csvScan holds everything a single streaming pass over one CSV file produces.
type csvScan struct {
	record     fileRecord
	aggregates map[string]float64
	samples    []map[string]string
}

// sampleStreamSeed derives a per-table reservoir stream from the dataset seed so
// the manifest is reproducible for a given (seed, table) pair.
func sampleStreamSeed(seed int64, table string) int64 {
	value := uint64(seed) * 0x9e3779b97f4a7c15
	for i := 0; i < len(table); i++ {
		value = (value ^ uint64(table[i])) * 0x100000001b3
	}
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return int64(value ^ (value >> 31))
}

func columnIndex(header []string, name string) (int, error) {
	for i, column := range header {
		if column == name {
			return i, nil
		}
	}
	return 0, fmt.Errorf("column %q is missing from the CSV header", name)
}

// scanCSVFile streams one CSV file and derives its row count, the aggregates
// declared for it, and a uniform reservoir sample of its rows. Passing an empty
// table name skips the derived facts and only measures size, rows and header.
func scanCSVFile(path, table string, seed int64) (csvScan, error) {
	scan := csvScan{aggregates: map[string]float64{}}
	aggregates := make([]manifestAggregateSpec, 0, len(datasetAggregateSpecs))
	if table != "" {
		for _, spec := range datasetAggregateSpecs {
			if spec.table == table {
				aggregates = append(aggregates, spec)
			}
		}
	}
	spec, wantSamples := datasetSampleSpec{}, false
	if table != "" {
		spec, wantSamples = datasetSampleSpecFor(table)
	}

	var (
		aggregateColumns []int
		sampleColumns    []string
		sampleIndexes    []int
		reservoir        []map[string]string
		rng              *rand.Rand
		seen             int64
	)
	onHeader := func(header []string) error {
		aggregateColumns = make([]int, len(aggregates))
		for i, agg := range aggregates {
			index, err := columnIndex(header, agg.column)
			if err != nil {
				return err
			}
			aggregateColumns[i] = index
		}
		if wantSamples {
			sampleColumns = append(append([]string{}, spec.keys...), spec.values...)
			sampleIndexes = make([]int, len(sampleColumns))
			for i, name := range sampleColumns {
				index, err := columnIndex(header, name)
				if err != nil {
					return err
				}
				sampleIndexes[i] = index
			}
			reservoir = make([]map[string]string, 0, datasetSampleCount)
			rng = rand.New(rand.NewSource(sampleStreamSeed(seed, table)))
		}
		return nil
	}
	onRow := func(row []string) error {
		for i, agg := range aggregates {
			raw := row[aggregateColumns[i]]
			switch agg.kind {
			case aggregateSum:
				value, err := strconv.ParseFloat(raw, 64)
				if err != nil {
					return fmt.Errorf("column %s holds a non-numeric value %q: %w", agg.column, raw, err)
				}
				scan.aggregates[agg.key] += value
			case aggregateZeros:
				value, err := strconv.ParseFloat(raw, 64)
				if err != nil {
					return fmt.Errorf("column %s holds a non-numeric value %q: %w", agg.column, raw, err)
				}
				if value == 0 {
					scan.aggregates[agg.key]++
				}
			case aggregateNulls:
				if raw == "" {
					scan.aggregates[agg.key]++
				}
			}
		}
		if wantSamples {
			// Algorithm R: a uniform sample of an unknown-length stream, so the
			// manifest never depends on a row-count formula.
			if len(reservoir) < datasetSampleCount {
				reservoir = append(reservoir, sampleRow(row, sampleColumns, sampleIndexes))
			} else if slot := rng.Int63n(seen + 1); slot < int64(datasetSampleCount) {
				reservoir[slot] = sampleRow(row, sampleColumns, sampleIndexes)
			}
		}
		seen++
		return nil
	}
	record, err := streamCSVFile(path, onHeader, onRow)
	if err != nil {
		return csvScan{}, err
	}
	scan.record = record
	for _, agg := range aggregates {
		if _, ok := scan.aggregates[agg.key]; !ok {
			scan.aggregates[agg.key] = 0
		}
	}
	if wantSamples {
		sortSamples(reservoir, spec.keys)
		scan.samples = reservoir
	}
	return scan, nil
}

func sampleRow(row, columns []string, indexes []int) map[string]string {
	sample := make(map[string]string, len(columns))
	for i, name := range columns {
		sample[name] = row[indexes[i]]
	}
	return sample
}

func sortSamples(samples []map[string]string, keys []string) {
	sort.SliceStable(samples, func(a, b int) bool {
		for _, key := range keys {
			left, _ := strconv.ParseFloat(samples[a][key], 64)
			right, _ := strconv.ParseFloat(samples[b][key], 64)
			if left != right {
				return left < right
			}
		}
		return false
	})
}

func samplePartitionKey(sample map[string]string, columns []string) string {
	parts := make([]string, len(columns))
	for i, column := range columns {
		parts[i] = sample[column]
	}
	return strings.Join(parts, "/")
}

// validateSampleCoverage enforces that the sample really is spread over several
// warehouses or districts (final.md:259), capped by how many partitions this
// dataset size actually has.
func validateSampleCoverage(table string, spec datasetSampleSpec, samples []map[string]string, warehouses int) error {
	if len(samples) == 0 {
		return fmt.Errorf("dataset sampling produced no rows for %s", table)
	}
	if len(spec.partitionColumns) == 0 {
		return nil
	}
	partitions := make(map[string]struct{}, len(samples))
	for _, sample := range samples {
		partitions[samplePartitionKey(sample, spec.partitionColumns)] = struct{}{}
	}
	want := datasetSampleMinPartitions
	if len(samples) < want {
		want = len(samples)
	}
	if available := availablePartitions(spec.partitionScale, warehouses); available > 0 && available < want {
		want = available
	}
	if len(partitions) < want {
		return fmt.Errorf("dataset sampling of %s covers %d partitions of %v, want at least %d",
			table, len(partitions), spec.partitionColumns, want)
	}
	return nil
}
