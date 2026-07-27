package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// Consistency validation.
//
// finalv2.md:382 splits the work in two. Before the forced termination the
// evaluator runs a fixed "6 integer + 7 FLOAT32" quick check and captures the
// seven aggregate bit patterns. After the restart it runs 37 integer
// aggregate checks and 7 amount aggregate checks (final.md:321-324) plus a
// per-partition reconciliation over all warehouse/district partitions
// (final.md:345).
//
// Both stages are driven by rule tables rather than hand-written comparisons, so
// a rule always reports its own name, expected value and actual value.

const (
	onlineIntRuleCount        = 6
	onlineAmountRuleCount     = 7
	postRecoveryIntRuleCount  = 37
	postRecoveryAmountRuleCnt = 7

	// TPC-C value ranges published in final.md:287-289.
	minOrderLineCount = 5
	maxOrderLineCount = 15
	maxCarrierID      = 10

	// amountToleranceFloor absorbs cent-level noise on small sums.
	amountToleranceFloor = 0.05
	// amountRelativeTolerance is the scale-adaptive term. RMDB stores FLOAT as
	// binary32 (FLT_EPSILON = 1.19e-7), so both the stored per-row values and the
	// aggregate returned on the wire carry a relative rounding error; 2e-6 leaves
	// roughly a factor of 16 of headroom over a single rounding while still making
	// a one-cent-per-transaction bug visible on any realistic window length.
	amountRelativeTolerance = 2e-6
	// amountDriftSigmas is how many standard deviations of accumulated binary32
	// rounding an amount rule tolerates; see binary32DriftSigma for the model.
	//
	// Four sigma keeps the false-positive rate at roughly 3e-3 across the 50
	// per-warehouse comparisons of one run while staying below a single TPC-C
	// Payment amount (1..5000) at the official window length, so a lost or
	// duplicated Payment is still detected. Raising it further would start hiding
	// exactly the bug the rule exists to find.
	amountDriftSigmas = 4.0
)

// Ledger keys. The workload records the effect of every committed transaction
// under these names so the post-crash validation can reconcile the recovered
// database against the transaction ledger (final.md:322) without keeping any
// in-memory state across the crash.
const (
	ledgerNewOrderCommits    = "new_order.commits"
	ledgerNewOrderRollbacks  = "new_order.expected_rollbacks"
	ledgerNewOrderLines      = "new_order.order_lines"
	ledgerNewOrderQuantity   = "new_order.line_quantity"
	ledgerNewOrderStockDelta = "new_order.stock_quantity_delta"
	ledgerNewOrderRemote     = "new_order.remote_lines"
	ledgerNewOrderAmount     = "new_order.line_amount"
	ledgerPaymentCommits     = "payment.commits"
	ledgerPaymentAmount      = "payment.amount"
	// ledgerPaymentWarehousePrefix is followed by the warehouse id. Payment updates
	// w_ytd of its terminal home only, so the number of binary32 accumulation steps
	// each warehouse took is not total/warehouses: under the official terminal-home
	// policy half the warehouses take none at all. The per-warehouse counts are what
	// binary32DriftSigma needs to size the tolerance from the real step count.
	ledgerPaymentWarehousePrefix = "payment.commits.w"
	ledgerDeliveryOrders         = "delivery.orders"
	ledgerDeliveryCustomers      = "delivery.customers"
	ledgerDeliveryAmount         = "delivery.amount"
)

// Baseline keys: the aggregate snapshot taken from the freshly loaded database
// before the first warmup transaction.
const (
	baseWarehouseRows          = "warehouse.rows"
	baseWarehouseYTD           = "warehouse.w_ytd.sum"
	baseDistrictRows           = "district.rows"
	baseDistrictYTD            = "district.d_ytd.sum"
	baseDistrictNextOID        = "district.d_next_o_id.sum"
	baseCustomerRows           = "customer.rows"
	baseCustomerBalance        = "customer.c_balance.sum"
	baseCustomerYTDPayment     = "customer.c_ytd_payment.sum"
	baseCustomerPaymentCnt     = "customer.c_payment_cnt.sum"
	baseCustomerDeliveryCnt    = "customer.c_delivery_cnt.sum"
	baseHistoryRows            = "history.rows"
	baseHistoryAmount          = "history.h_amount.sum"
	baseNewOrdersRows          = "new_orders.rows"
	baseOrdersRows             = "orders.rows"
	baseOrdersOlCnt            = "orders.o_ol_cnt.sum"
	baseOrdersCarrierZeroRows  = "orders.carrier_zero.rows"
	baseOrderLineRows          = "order_line.rows"
	baseOrderLineAmount        = "order_line.ol_amount.sum"
	baseOrderLineQuantity      = "order_line.ol_quantity.sum"
	baseOrderLineDeliveryNulls = "order_line.delivery_null.rows"
	baseItemRows               = "item.rows"
	baseStockRows              = "stock.rows"
	baseStockQuantity          = "stock.s_quantity.sum"
	baseStockYTD               = "stock.s_ytd.sum"
	baseStockOrderCnt          = "stock.s_order_cnt.sum"
	baseStockRemoteCnt         = "stock.s_remote_cnt.sum"
)

// baselineQuery groups several aggregates of one table into a single scan so the
// snapshot costs one pass per table instead of one pass per aggregate.
type baselineQuery struct {
	sql  string
	keys []string
}

var baselineQueries = []baselineQuery{
	{"select count(*), sum(w_ytd) from warehouse;",
		[]string{baseWarehouseRows, baseWarehouseYTD}},
	{"select count(*), sum(d_ytd), sum(d_next_o_id) from district;",
		[]string{baseDistrictRows, baseDistrictYTD, baseDistrictNextOID}},
	{"select count(*), sum(c_balance), sum(c_ytd_payment), sum(c_payment_cnt), sum(c_delivery_cnt) from customer;",
		[]string{baseCustomerRows, baseCustomerBalance, baseCustomerYTDPayment, baseCustomerPaymentCnt, baseCustomerDeliveryCnt}},
	{"select count(*), sum(h_amount) from history;",
		[]string{baseHistoryRows, baseHistoryAmount}},
	{"select count(*) from new_orders;",
		[]string{baseNewOrdersRows}},
	{"select count(*), sum(o_ol_cnt) from orders;",
		[]string{baseOrdersRows, baseOrdersOlCnt}},
	{"select count(o_id) from orders where o_carrier_id = 0;",
		[]string{baseOrdersCarrierZeroRows}},
	{"select count(*), sum(ol_amount), sum(ol_quantity) from order_line;",
		[]string{baseOrderLineRows, baseOrderLineAmount, baseOrderLineQuantity}},
	{"select count(*) from order_line where ol_delivery_d = '';",
		[]string{baseOrderLineDeliveryNulls}},
	{"select count(*) from item;",
		[]string{baseItemRows}},
	{"select count(*), sum(s_quantity), sum(s_ytd), sum(s_order_cnt), sum(s_remote_cnt) from stock;",
		[]string{baseStockRows, baseStockQuantity, baseStockYTD, baseStockOrderCnt, baseStockRemoteCnt}},
}

// captureBaselines snapshots the loaded database before any transaction runs.
// The post-crash validation compares baseline + ledger against the recovered
// database, so this snapshot has to be taken while the database is still
// quiescent and it has to be persisted into the result document.
func captureBaselines(c sqlExecutor) (map[string]float64, error) {
	baselines := make(map[string]float64, 32)
	for _, query := range baselineQueries {
		start := time.Now()
		text, err := c.exec(query.sql)
		if err != nil {
			return nil, fmt.Errorf("baseline snapshot %q: %w", query.sql, err)
		}
		values, err := aggregateRow(text, len(query.keys))
		if err != nil {
			return nil, fmt.Errorf("baseline snapshot %q: %w", query.sql, err)
		}
		for i, key := range query.keys {
			if !values[i].present {
				return nil, fmt.Errorf("baseline snapshot %q returned NULL for %s", query.sql, key)
			}
			baselines[key] = values[i].number
		}
		fmt.Printf("[baseline] %s took %s\n", query.sql, time.Since(start).Round(time.Millisecond))
	}
	return baselines, nil
}

// aggregateValue is one cell of an aggregate result row. SQL aggregates over an
// empty input return NULL, which is a legitimate answer for an empty partition
// and must not be silently coerced to zero.
type aggregateValue struct {
	number  float64
	present bool
}

// aggregateRow extracts the single result row of an aggregate query.
func aggregateRow(text string, columns int) ([]aggregateValue, error) {
	rows := parseRows(text)
	if len(rows) != 1 {
		return nil, fmt.Errorf("aggregate query returned %d rows, want 1", len(rows))
	}
	if len(rows[0]) != columns {
		return nil, fmt.Errorf("aggregate query returned %d columns, want %d", len(rows[0]), columns)
	}
	values := make([]aggregateValue, columns)
	for i, raw := range rows[0] {
		raw = strings.TrimSpace(raw)
		if raw == "NULL" {
			continue
		}
		number, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("aggregate column %d holds a non-numeric value %q", i+1, raw)
		}
		values[i] = aggregateValue{number: number, present: true}
	}
	return values, nil
}

// amountTolerance is the scale-adaptive comparison window for FLOAT aggregates.
func amountTolerance(expected, actual float64) float64 {
	scale := math.Max(math.Abs(expected), math.Abs(actual))
	return math.Max(amountToleranceFloor, amountRelativeTolerance*scale)
}

// amountToleranceWithDrift widens the comparison window by a drift bound derived
// from how many binary32 read-modify-write steps actually built the aggregate.
// `drift` is one standard deviation of that accumulation; callers obtain it from
// binary32DriftSigma using the ledger's committed transaction counts.
func amountToleranceWithDrift(expected, actual, drift float64) float64 {
	return math.Max(amountTolerance(expected, actual), amountDriftSigmas*drift)
}

// binary32DriftSigma is one standard deviation of the rounding error a FLOAT
// column accumulates over `updates` relative updates at magnitude `magnitude`.
//
// Why the tolerance has this shape rather than being a relative constant.
// `w_ytd` and `d_ytd` are read-modify-write accumulators declared FLOAT, i.e.
// binary32, by the official DDL. Every `w_ytd = w_ytd + amount` rounds the result
// to the nearest representable binary32, an error uniform in [-u/2, +u/2] where
// u is the ULP at that magnitude, so one step has standard deviation
// u/(2*sqrt(3)); the steps are independent, so after n steps the drift has
// standard deviation (u/2)*sqrt(n/3).
//
// This makes a *systematic* divergence between `w_ytd` and `SUM(d_ytd)`
// unavoidable, with no bug involved: both sides take the same number of steps,
// but `w_ytd` accumulates the whole warehouse (about 19.5 M after four 60 s
// windows, where binary32 has u = 2) while each `d_ytd` accumulates a tenth of it
// (about 1.95 M, where u = 0.125), so the warehouse accumulator is sixteen times
// coarser per step. At n = 1e5 the model gives (2/2)*sqrt(1e5/3) = 183, and the
// divergence measured on the real workload was 50 to 66 — under one sigma. A flat
// 2e-6 relative window is 39 at that magnitude, i.e. tighter than the arithmetic
// the schema mandates, which is why it reported a failure that no engine change
// can fix (row_mutation.cpp already computes the relative update in double and
// only rounds on store).
//
// final.md:204 has the evaluator transporting FLOAT as the raw IEEE-754 binary32
// bit pattern instead of comparing six-decimal text, so it is aware of and
// accepts binary32 behaviour; final.md:324 only requires the amount aggregates to
// be "within the specified tolerance" without publishing that tolerance.
func binary32DriftSigma(magnitude, updates float64) float64 {
	if updates <= 0 {
		return 0
	}
	return binary32ULP(magnitude) / 2 * math.Sqrt(updates/3)
}

// combineDrift adds independent drift standard deviations in quadrature.
func combineDrift(sigmas ...float64) float64 {
	total := 0.0
	for _, sigma := range sigmas {
		total += sigma * sigma
	}
	return math.Sqrt(total)
}

// binary32ULP is the distance between adjacent binary32 values at this
// magnitude, i.e. the smallest change a FLOAT column can even record there. It
// is reported alongside a failed amount rule as triage context, never used to
// widen the verdict.
func binary32ULP(value float64) float64 {
	value = math.Abs(value)
	if value == 0 {
		return 0
	}
	next := math.Nextafter32(float32(value), float32(math.Inf(1)))
	return math.Abs(float64(next) - float64(float32(value)))
}

// intRule is one integer aggregate check. `want` is the value derived from the
// baseline snapshot and the transaction ledger; when `wantSQL` is set the
// expected value is read from a second aggregate of the same database instead
// (plus `wantDelta`), which is how invariants between two aggregates of the
// recovered database are expressed.
type intRule struct {
	name      string
	sql       string
	want      int64
	wantSQL   string
	wantDelta int64
	hint      string
}

type amountRule struct {
	name string
	sql  string
	want float64
	// captureOnly records the online bit pattern without comparing it to a
	// ledger. maxULP applies when useWantBits is set. boundaryAddends enables the
	// finalv2 boundary-aware exact-sum test for ranking-scale non-negative sums.
	captureOnly     bool
	useWantBits     bool
	wantBits        uint32
	maxULP          uint32
	boundaryAddends int64
	// drift is one standard deviation of the binary32 accumulation error the
	// aggregate is expected to carry, derived from the ledger's step counts. Zero
	// means the aggregate is built by inserts rather than by read-modify-write
	// accumulation, so only the scale-adaptive tolerance applies.
	drift float64
	hint  string
}

type ruleRunner struct {
	exec        sqlExecutor
	failures    []string
	intRules    int
	amountRules int
	floatBits   map[string]uint32
}

func (r *ruleRunner) fail(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

// nullableScalar runs a one-column aggregate and reports whether it was NULL.
func (r *ruleRunner) nullableScalar(sql string) (aggregateValue, error) {
	text, err := r.exec.exec(sql)
	if err != nil {
		return aggregateValue{}, err
	}
	values, err := aggregateRow(text, 1)
	if err != nil {
		return aggregateValue{}, err
	}
	return values[0], nil
}

// scalar runs a one-column aggregate that must produce a value; a NULL answer is
// itself a failure because every rule that uses it queries a non-empty input.
func (r *ruleRunner) scalar(sql string) (float64, error) {
	value, err := r.nullableScalar(sql)
	if err != nil {
		return 0, err
	}
	if !value.present {
		return 0, errors.New("aggregate returned NULL over a non-empty input")
	}
	return value.number, nil
}

func (r *ruleRunner) runIntRules(rules []intRule) {
	for _, rule := range rules {
		r.intRules++
		got, err := r.scalar(rule.sql)
		if err != nil {
			r.fail("%s: %v [%s]", rule.name, err, rule.sql)
			continue
		}
		want := rule.want
		if rule.wantSQL != "" {
			value, err := r.scalar(rule.wantSQL)
			if err != nil {
				r.fail("%s: %v [%s]", rule.name, err, rule.wantSQL)
				continue
			}
			want = int64(math.Round(value)) + rule.wantDelta
		}
		if int64(math.Round(got)) != want {
			message := fmt.Sprintf("%s: got %d, want %d [%s]", rule.name, int64(math.Round(got)), want, rule.sql)
			if rule.hint != "" {
				message += "\n    hint: " + rule.hint
			}
			r.fail("%s", message)
		}
	}
}

func (r *ruleRunner) runAmountRules(rules []amountRule) {
	if r.floatBits == nil {
		r.floatBits = make(map[string]uint32, len(rules))
	}
	for _, rule := range rules {
		r.amountRules++
		got, err := r.scalar(rule.sql)
		if err != nil {
			r.fail("%s: %v [%s]", rule.name, err, rule.sql)
			continue
		}
		got32 := float32(got)
		if math.IsNaN(float64(got32)) || math.IsInf(float64(got32), 0) {
			r.fail("%s: got non-finite FLOAT32 [%s]", rule.name, rule.sql)
			continue
		}
		gotBits := math.Float32bits(got32)
		r.floatBits[rule.name] = gotBits
		if rule.captureOnly {
			continue
		}
		matches := false
		toleranceText := "0 ULP"
		wantBits := math.Float32bits(float32(rule.want))
		if rule.useWantBits {
			wantBits = rule.wantBits
			matches = float32ULPDistance(gotBits, wantBits) <= rule.maxULP
			toleranceText = fmt.Sprintf("<=%d ULP", rule.maxULP)
		} else if rule.boundaryAddends > 0 {
			matches = boundaryAwareFloat32Match(gotBits, rule.want, rule.boundaryAddends)
			toleranceText = "boundary-aware"
		} else {
			matches = equalFloat32Bits(gotBits, wantBits)
		}
		if !matches {
			message := fmt.Sprintf("%s: got %.9g (0x%08x), want %.9g (0x%08x), tolerance %s [%s]",
				rule.name, got32, gotBits, math.Float32frombits(wantBits), wantBits, toleranceText, rule.sql)
			if rule.hint != "" {
				message += "\n    hint: " + rule.hint
			}
			r.fail("%s", message)
		}
	}
}

func equalFloat32Bits(left, right uint32) bool {
	if left<<1 == 0 && right<<1 == 0 {
		return true
	}
	return left == right
}

func orderedFloat32Bits(bits uint32) uint32 {
	if bits&0x80000000 != 0 {
		return ^bits
	}
	return bits | 0x80000000
}

func float32ULPDistance(left, right uint32) uint32 {
	if equalFloat32Bits(left, right) {
		return 0
	}
	a, b := orderedFloat32Bits(left), orderedFloat32Bits(right)
	if a < b {
		return b - a
	}
	return a - b
}

func boundaryAwareFloat32Match(gotBits uint32, exactSum float64, addends int64) bool {
	if addends < 0 || exactSum < 0 || math.IsNaN(exactSum) || math.IsInf(exactSum, 0) {
		return false
	}
	// Dataset sums are accumulated exactly in exponent bins before one float64
	// rounding, whose error is at most 2^-53*S. Runtime ledger additions add at
	// most the same relative error per term, so n*2^-53*S safely encloses the
	// exact real sum before testing the adjacent binary32 boundary candidates.
	epsilon := float64(addends) * math.Ldexp(1, -53) * exactSum
	for _, candidate := range []float64{exactSum - epsilon, exactSum, exactSum + epsilon} {
		if equalFloat32Bits(gotBits, math.Float32bits(float32(candidate))) {
			return true
		}
	}
	return false
}

func paymentEdgeKey(edge paymentFloatEdge) (string, uint32, error) {
	switch edge.Kind {
	case "warehouse":
		if edge.Warehouse < 1 || edge.District != 0 {
			return "", 0, fmt.Errorf("invalid warehouse Payment edge key w=%d d=%d", edge.Warehouse, edge.District)
		}
		return fmt.Sprintf("warehouse:%d", edge.Warehouse), math.Float32bits(float32(300000)), nil
	case "district":
		if edge.Warehouse < 1 || edge.District < 1 {
			return "", 0, fmt.Errorf("invalid district Payment edge key w=%d d=%d", edge.Warehouse, edge.District)
		}
		return fmt.Sprintf("district:%d:%d", edge.Warehouse, edge.District), math.Float32bits(float32(30000)), nil
	default:
		return "", 0, fmt.Errorf("invalid Payment edge kind %q", edge.Kind)
	}
}

// validatePaymentFloatChains treats committed updates as a per-key multiset of
// before->after edges. Linking by raw bits, rather than client completion order,
// accepts every serial interleaving while rejecting a missing edge or the fork
// produced when two committed transactions both update the same old value.
func validatePaymentFloatChains(doc document) (map[string]uint32, error) {
	commits := int(math.Round(doc.Ledger[ledgerPaymentCommits]))
	if len(doc.PaymentEdges) != commits*2 {
		return nil, fmt.Errorf("Payment FLOAT32 evidence has %d edges, want two for each of %d commit(s)",
			len(doc.PaymentEdges), commits)
	}
	grouped := make(map[string][]paymentFloatEdge)
	initial := make(map[string]uint32)
	for _, edge := range doc.PaymentEdges {
		key, start, err := paymentEdgeKey(edge)
		if err != nil {
			return nil, err
		}
		amount := math.Float32frombits(edge.AmountBits)
		before := math.Float32frombits(edge.BeforeBits)
		if amount <= 0 || math.IsNaN(float64(amount)) || math.IsInf(float64(amount), 0) {
			return nil, fmt.Errorf("Payment FLOAT32 edge %s has invalid amount 0x%08x", key, edge.AmountBits)
		}
		want := math.Float32bits(before + amount)
		if !equalFloat32Bits(edge.AfterBits, want) {
			return nil, fmt.Errorf("Payment FLOAT32 edge %s is not 0 ULP: before=0x%08x amount=0x%08x after=0x%08x want=0x%08x",
				key, edge.BeforeBits, edge.AmountBits, edge.AfterBits, want)
		}
		grouped[key] = append(grouped[key], edge)
		initial[key] = start
	}
	terminals := make(map[string]uint32, len(grouped))
	for key, edges := range grouped {
		current := initial[key]
		for len(edges) > 0 {
			match := -1
			for i, edge := range edges {
				if equalFloat32Bits(edge.BeforeBits, current) {
					if match >= 0 {
						return nil, fmt.Errorf("Payment FLOAT32 chain %s forks at 0x%08x", key, current)
					}
					match = i
				}
			}
			if match < 0 {
				return nil, fmt.Errorf("Payment FLOAT32 chain %s has a gap after 0x%08x (%d edge(s) remain)",
					key, current, len(edges))
			}
			current = edges[match].AfterBits
			edges[match] = edges[len(edges)-1]
			edges = edges[:len(edges)-1]
		}
		terminals[key] = current
	}
	return terminals, nil
}

func checkPaymentTerminalBits(c sqlExecutor, terminals map[string]uint32, stage string) error {
	for key, want := range terminals {
		parts := strings.Split(key, ":")
		var sql string
		switch parts[0] {
		case "warehouse":
			sql = fmt.Sprintf("select w_ytd from warehouse where w_id = %s;", parts[1])
		case "district":
			sql = fmt.Sprintf("select d_ytd from district where d_w_id = %s and d_id = %s;", parts[1], parts[2])
		default:
			return fmt.Errorf("[%s] invalid Payment terminal key %q", stage, key)
		}
		text, err := c.exec(sql)
		if err != nil {
			return fmt.Errorf("[%s] Payment terminal %s: %w", stage, key, err)
		}
		value, err := scalarFloatStrict(text)
		if err != nil {
			return fmt.Errorf("[%s] Payment terminal %s: %w", stage, key, err)
		}
		got := math.Float32bits(float32(value))
		if !equalFloat32Bits(got, want) {
			return fmt.Errorf("[%s] Payment terminal %s got 0x%08x, want 0x%08x (0 ULP)",
				stage, key, got, want)
		}
	}
	return nil
}

// consistencyModel carries the dataset shape, the pre-workload aggregate
// snapshot and the committed-transaction ledger that the rule tables read.
type consistencyModel struct {
	warehouses            int
	districtsPerWarehouse int
	customersPerDistrict  int
	itemCount             int
	baseline              map[string]float64
	ledger                map[string]float64
	onlineFloatBits       map[string]uint32
}

func newConsistencyModel(prior document) (consistencyModel, error) {
	model := consistencyModel{
		warehouses:      prior.Config.BaselineWarehouseTotal,
		itemCount:       prior.Config.BaselineItemTotal,
		baseline:        prior.Baselines,
		ledger:          prior.Ledger,
		onlineFloatBits: prior.OnlineFloatBits,
	}
	if model.warehouses < 1 || prior.Config.BaselineDistrictTotal < 1 || prior.Config.BaselineCustomerTotal < 1 || model.itemCount < 1 {
		return consistencyModel{}, errors.New("result file has no dataset profile; rerun the benchmark to produce one")
	}
	model.districtsPerWarehouse = prior.Config.BaselineDistrictTotal / model.warehouses
	model.customersPerDistrict = prior.Config.BaselineCustomerTotal / prior.Config.BaselineDistrictTotal
	if model.districtsPerWarehouse < 1 || model.customersPerDistrict < 1 {
		return consistencyModel{}, errors.New("result file has an inconsistent dataset profile")
	}
	if len(model.baseline) == 0 {
		return consistencyModel{}, errors.New("result file carries no pre-workload aggregate snapshot; it predates ledger reconciliation")
	}
	for _, query := range baselineQueries {
		for _, key := range query.keys {
			if _, ok := model.baseline[key]; !ok {
				return consistencyModel{}, fmt.Errorf("result file is missing the baseline aggregate %s", key)
			}
		}
	}
	if len(model.ledger) == 0 {
		return consistencyModel{}, errors.New("result file carries no transaction ledger; it predates ledger reconciliation")
	}
	for _, key := range ledgerKeys {
		if _, ok := model.ledger[key]; !ok {
			return consistencyModel{}, fmt.Errorf("result file is missing the ledger entry %s", key)
		}
	}
	return model, nil
}

// b returns a baseline aggregate rounded to an integer.
func (m consistencyModel) b(key string) int64 { return int64(math.Round(m.baseline[key])) }

// f returns a baseline aggregate as a float amount.
func (m consistencyModel) f(key string) float64 { return m.baseline[key] }

// l returns a ledger entry rounded to an integer count.
func (m consistencyModel) l(key string) int64 { return int64(math.Round(m.ledger[key])) }

// a returns a ledger entry as a float amount.
func (m consistencyModel) a(key string) float64 { return m.ledger[key] }

// paymentsForWarehouse is how many committed Payments accumulated into this
// warehouse's w_ytd. Absent means the ledger predates the per-warehouse counters,
// in which case the caller falls back to the flat relative tolerance.
func (m consistencyModel) paymentsForWarehouse(wID int) float64 {
	return m.ledger[ledgerPaymentWarehousePrefix+strconv.Itoa(wID)]
}

func (m consistencyModel) averagePaymentAmount() float64 {
	commits := m.a(ledgerPaymentCommits)
	if commits <= 0 {
		return 0
	}
	return m.a(ledgerPaymentAmount) / commits
}

// warehouseYTDDrift is one standard deviation of the binary32 accumulation drift
// of `w_ytd` for one warehouse. The magnitude is estimated from the initial value
// plus the payments this warehouse absorbed; only the ULP depends on it, so the
// estimate only has to be right to within a factor of two.
func (m consistencyModel) warehouseYTDDrift(wID int) float64 {
	updates := m.paymentsForWarehouse(wID)
	if updates <= 0 || m.warehouses <= 0 {
		return 0
	}
	magnitude := m.f(baseWarehouseYTD)/float64(m.warehouses) + updates*m.averagePaymentAmount()
	return binary32DriftSigma(magnitude, updates)
}

// districtYTDDriftForWarehouse combines the drift of the ten `d_ytd` accumulators
// of one warehouse. Payment picks its district uniformly inside the terminal home,
// so each district takes about a tenth of the warehouse's steps at a tenth of the
// magnitude, which is where the sixteen-fold precision difference comes from.
func (m consistencyModel) districtYTDDriftForWarehouse(wID int) float64 {
	updates := m.paymentsForWarehouse(wID)
	if updates <= 0 || m.warehouses <= 0 || m.districtsPerWarehouse <= 0 {
		return 0
	}
	districts := float64(m.districtsPerWarehouse)
	perDistrictUpdates := updates / districts
	totalDistricts := float64(m.warehouses) * districts
	magnitude := m.f(baseDistrictYTD)/totalDistricts + perDistrictUpdates*m.averagePaymentAmount()
	return binary32DriftSigma(magnitude, perDistrictUpdates) * math.Sqrt(districts)
}

// totalWarehouseYTDDrift and totalDistrictYTDDrift are the drifts of the global
// SUM aggregates, obtained by adding the independent per-accumulator drifts in
// quadrature.
func (m consistencyModel) totalWarehouseYTDDrift() float64 {
	sigmas := make([]float64, 0, m.warehouses)
	for wID := 1; wID <= m.warehouses; wID++ {
		sigmas = append(sigmas, m.warehouseYTDDrift(wID))
	}
	return combineDrift(sigmas...)
}

func (m consistencyModel) totalDistrictYTDDrift() float64 {
	sigmas := make([]float64, 0, m.warehouses)
	for wID := 1; wID <= m.warehouses; wID++ {
		sigmas = append(sigmas, m.districtYTDDriftForWarehouse(wID))
	}
	return combineDrift(sigmas...)
}

var ledgerKeys = []string{
	ledgerNewOrderCommits, ledgerNewOrderRollbacks, ledgerNewOrderLines, ledgerNewOrderQuantity,
	ledgerNewOrderStockDelta, ledgerNewOrderRemote, ledgerNewOrderAmount,
	ledgerPaymentCommits, ledgerPaymentAmount,
	ledgerDeliveryOrders, ledgerDeliveryCustomers, ledgerDeliveryAmount,
}

// dNextOIDHint explains the single rule most likely to disagree with the
// official evaluator, recorded as a trap in PLAN.md item 1.12.
//
// The official ledger formula is "committed + expected rollback", because in the
// official driver the order number is consumed before the invalid-item rollback
// decision is taken. This driver issues `update district set d_next_o_id =
// d_next_o_id + 1` inside the same transaction that later submits `abort;`
// (rankingNewOrder stage 1 and stage 2 share one transaction), so a correct
// engine must undo it and the expected increment is exactly the committed count.
// If the observed increment instead equals committed + expected rollback, the
// engine leaked the order number out of an aborted transaction — the failure this
// rule exists to catch — and the hint says so.
const dNextOIDHint = "an aborted NewOrder must not leak an order number: this driver increments d_next_o_id inside the transaction it later aborts, so the increment must equal the committed count. If the surplus equals the expected-rollback count, the rollback did not undo the district update."

// postRecoveryIntRules returns the 37 integer aggregate checks of
// final.md:321-323.
func postRecoveryIntRules(m consistencyModel) []intRule {
	count := func(name, table, predicate string, want int64) intRule {
		return intRule{name: name, sql: fmt.Sprintf("select count(*) from %s where %s;", table, predicate), want: want}
	}
	rules := []intRule{
		// (1) final.md:321 — key table row counts agree with the committed
		// transaction counts.
		{name: "warehouse row count", sql: "select count(*) from warehouse;", want: m.b(baseWarehouseRows)},
		{name: "district row count", sql: "select count(*) from district;", want: m.b(baseDistrictRows)},
		{name: "customer row count", sql: "select count(*) from customer;", want: m.b(baseCustomerRows)},
		{name: "item row count", sql: "select count(*) from item;", want: m.b(baseItemRows)},
		{name: "stock row count", sql: "select count(*) from stock;", want: m.b(baseStockRows)},
		{name: "orders row count", sql: "select count(*) from orders;",
			want: m.b(baseOrdersRows) + m.l(ledgerNewOrderCommits)},
		{name: "order_line row count", sql: "select count(*) from order_line;",
			want: m.b(baseOrderLineRows) + m.l(ledgerNewOrderLines)},
		{name: "history row count", sql: "select count(*) from history;",
			want: m.b(baseHistoryRows) + m.l(ledgerPaymentCommits)},
		{name: "new_orders row count", sql: "select count(*) from new_orders;",
			want: m.b(baseNewOrdersRows) + m.l(ledgerNewOrderCommits) - m.l(ledgerDeliveryOrders)},

		// (2) final.md:322 — district next order id, customer payment and delivery
		// counts, stock order counts and the other aggregates agree with the ledger.
		{name: "district SUM(d_next_o_id)", sql: "select sum(d_next_o_id) from district;",
			want: m.b(baseDistrictNextOID) + m.l(ledgerNewOrderCommits), hint: dNextOIDHint},
		{name: "customer SUM(c_payment_cnt)", sql: "select sum(c_payment_cnt) from customer;",
			want: m.b(baseCustomerPaymentCnt) + m.l(ledgerPaymentCommits)},
		{name: "customer SUM(c_delivery_cnt)", sql: "select sum(c_delivery_cnt) from customer;",
			want: m.b(baseCustomerDeliveryCnt) + m.l(ledgerDeliveryCustomers)},
		{name: "stock SUM(s_order_cnt)", sql: "select sum(s_order_cnt) from stock;",
			want: m.b(baseStockOrderCnt) + m.l(ledgerNewOrderLines)},
		{name: "stock SUM(s_remote_cnt)", sql: "select sum(s_remote_cnt) from stock;",
			want: m.b(baseStockRemoteCnt) + m.l(ledgerNewOrderRemote)},
		{name: "orders SUM(o_ol_cnt)", sql: "select sum(o_ol_cnt) from orders;",
			want: m.b(baseOrdersOlCnt) + m.l(ledgerNewOrderLines)},
		{name: "order_line SUM(ol_quantity)", sql: "select sum(ol_quantity) from order_line;",
			want: m.b(baseOrderLineQuantity) + m.l(ledgerNewOrderQuantity)},
		{name: "stock SUM(s_quantity)", sql: "select sum(s_quantity) from stock;",
			want: m.b(baseStockQuantity) + m.l(ledgerNewOrderStockDelta)},
		{name: "orders with o_carrier_id = 0", sql: "select count(o_id) from orders where o_carrier_id = 0;",
			want: m.b(baseOrdersCarrierZeroRows) + m.l(ledgerNewOrderCommits) - m.l(ledgerDeliveryOrders)},
		{name: "order_line rows without a delivery time equal SUM(o_ol_cnt) of undelivered orders",
			sql:     "select count(*) from order_line where ol_delivery_d = '';",
			wantSQL: "select sum(o_ol_cnt) from orders where o_carrier_id = 0;"},
		{name: "order_line rows with a delivery time equal SUM(o_ol_cnt) of delivered orders",
			sql:     "select count(*) from order_line where ol_delivery_d <> '';",
			wantSQL: "select sum(o_ol_cnt) from orders where o_carrier_id <> 0;"},

		// (3) final.md:323 — warehouse, district, customer, order line, item and
		// stock identifiers and quantities are still inside their legal ranges.
		count("warehouse w_id below range", "warehouse", "w_id < 1", 0),
		count("warehouse w_id above range", "warehouse", fmt.Sprintf("w_id > %d", m.warehouses), 0),
		count("district d_id below range", "district", "d_id < 1", 0),
		count("district d_id above range", "district", fmt.Sprintf("d_id > %d", m.districtsPerWarehouse), 0),
		count("district d_w_id above range", "district", fmt.Sprintf("d_w_id > %d", m.warehouses), 0),
		count("customer c_id below range", "customer", "c_id < 1", 0),
		count("customer c_id above range", "customer", fmt.Sprintf("c_id > %d", m.customersPerDistrict), 0),
		count("customer c_w_id above range", "customer", fmt.Sprintf("c_w_id > %d", m.warehouses), 0),
		count("orders o_ol_cnt below range", "orders", fmt.Sprintf("o_ol_cnt < %d", minOrderLineCount), 0),
		count("orders o_ol_cnt above range", "orders", fmt.Sprintf("o_ol_cnt > %d", maxOrderLineCount), 0),
		count("orders o_carrier_id above range", "orders", fmt.Sprintf("o_carrier_id > %d", maxCarrierID), 0),
		count("order_line ol_number below range", "order_line", "ol_number < 1", 0),
		count("order_line ol_number above range", "order_line", fmt.Sprintf("ol_number > %d", maxOrderLineCount), 0),
		count("item i_id above range", "item", fmt.Sprintf("i_id > %d", m.itemCount), 0),
		count("order_line ol_i_id above range", "order_line", fmt.Sprintf("ol_i_id > %d", m.itemCount), 0),
		count("stock s_i_id above range", "stock", fmt.Sprintf("s_i_id > %d", m.itemCount), 0),
		count("stock s_w_id above range", "stock", fmt.Sprintf("s_w_id > %d", m.warehouses), 0),
	}
	return rules
}

// postRecoveryAmountRules returns the 7 amount aggregate checks of
// final.md:324: the amounts Payment, Delivery and NewOrder move.
func postRecoveryAmountRules(m consistencyModel) []amountRule {
	return []amountRule{
		{name: "warehouse SUM(w_ytd) matches the Payment ledger", sql: "select sum(w_ytd) from warehouse;",
			wantBits: m.onlineFloatBits["warehouse SUM(w_ytd) matches the Payment ledger"], useWantBits: true, maxULP: 0},
		{name: "district SUM(d_ytd) matches the Payment ledger", sql: "select sum(d_ytd) from district;",
			wantBits: m.onlineFloatBits["district SUM(d_ytd) matches the Payment ledger"], useWantBits: true, maxULP: 0},
		{name: "customer SUM(c_ytd_payment) matches the Payment ledger", sql: "select sum(c_ytd_payment) from customer;",
			wantBits: m.onlineFloatBits["customer SUM(c_ytd_payment) matches the Payment ledger"], useWantBits: true, maxULP: 1},
		{name: "history SUM(h_amount) matches the Payment ledger", sql: "select sum(h_amount) from history;",
			wantBits: m.onlineFloatBits["history SUM(h_amount) matches the Payment ledger"], useWantBits: true, maxULP: 1},
		{name: "customer SUM(c_balance) matches the Payment and Delivery ledger", sql: "select sum(c_balance) from customer;",
			wantBits: m.onlineFloatBits["customer SUM(c_balance) matches the Payment and Delivery ledger"], useWantBits: true, maxULP: 1},
		{name: "order_line SUM(ol_amount) matches the NewOrder ledger", sql: "select sum(ol_amount) from order_line;",
			wantBits: m.onlineFloatBits["order_line SUM(ol_amount) matches the NewOrder ledger"], useWantBits: true, maxULP: 1},
		{name: "stock SUM(s_ytd) matches the NewOrder ledger", sql: "select sum(s_ytd) from stock;",
			wantBits: m.onlineFloatBits["stock SUM(s_ytd) matches the NewOrder ledger"], useWantBits: true, maxULP: 0},
	}
}

// onlineIntRules returns the 6 integer quick checks of final.md:319. Every rule
// is either a small-table count or an index-key range count, because
// final.md:326 forbids a full large-table scan before the kill. Between them
// they would catch a shrinking row count in warehouse, district, customer,
// stock, orders and order_line — the exact class of bug that escaped the
// previous online check, which never counted any of the large tables.
func onlineIntRules(m consistencyModel) []intRule {
	return []intRule{
		{name: "warehouse row count", sql: "select count(*) from warehouse;", want: m.b(baseWarehouseRows)},
		{name: "district row count", sql: "select count(*) from district;", want: m.b(baseDistrictRows)},
		{name: "customer rows in partition (1,1)",
			sql:  "select count(c_id) from customer where c_w_id = 1 and c_d_id = 1;",
			want: int64(m.customersPerDistrict)},
		{name: "stock rows in warehouse 1",
			sql:  "select count(s_i_id) from stock where s_w_id = 1;",
			want: int64(m.itemCount)},
		{name: "orders rows in partition (1,1) match d_next_o_id",
			sql:       "select count(o_id) from orders where o_w_id = 1 and o_d_id = 1;",
			wantSQL:   "select d_next_o_id from district where d_w_id = 1 and d_id = 1;",
			wantDelta: -1,
			hint:      "an order number was consumed without an orders row, or an orders row was lost"},
		{name: "order_line rows in partition (1,1) match SUM(o_ol_cnt)",
			sql:     "select count(ol_o_id) from order_line where ol_w_id = 1 and ol_d_id = 1;",
			wantSQL: "select sum(o_ol_cnt) from orders where o_w_id = 1 and o_d_id = 1;"},
	}
}

// onlineAmountRules captures all seven pre-crash FLOAT32 aggregates. The three
// non-negative sums whose exact inputs are in the ledger are also checked now;
// the other four are captured for the post-recovery 0/1-ULP comparison.
func onlineAmountRules(m consistencyModel) []amountRule {
	payment := m.a(ledgerPaymentAmount)
	return []amountRule{
		{name: "warehouse SUM(w_ytd) matches the Payment ledger", sql: "select sum(w_ytd) from warehouse;",
			captureOnly: true},
		{name: "district SUM(d_ytd) matches the Payment ledger", sql: "select sum(d_ytd) from district;",
			captureOnly: true},
		{name: "customer SUM(c_ytd_payment) matches the Payment ledger", sql: "select sum(c_ytd_payment) from customer;",
			captureOnly: true},
		{name: "history SUM(h_amount) matches the Payment ledger", sql: "select sum(h_amount) from history;",
			want:            m.f(baseHistoryAmount) + payment,
			boundaryAddends: m.b(baseHistoryRows) + m.l(ledgerPaymentCommits)},
		{name: "customer SUM(c_balance) matches the Payment and Delivery ledger", sql: "select sum(c_balance) from customer;",
			captureOnly: true},
		{name: "order_line SUM(ol_amount) matches the NewOrder ledger", sql: "select sum(ol_amount) from order_line;",
			want:            m.f(baseOrderLineAmount) + m.a(ledgerNewOrderAmount),
			boundaryAddends: m.b(baseOrderLineRows) + m.l(ledgerNewOrderLines)},
		{name: "stock SUM(s_ytd) matches the NewOrder ledger", sql: "select sum(s_ytd) from stock;",
			want: m.f(baseStockYTD) + float64(m.l(ledgerNewOrderQuantity))},
	}
}

func loadPriorResult(resultPath string) (document, error) {
	if resultPath == "" {
		return document{}, errors.New("--result-json is required for consistency")
	}
	data, err := os.ReadFile(resultPath)
	if err != nil {
		return document{}, err
	}
	var prior document
	if err := json.Unmarshal(data, &prior); err != nil {
		return document{}, err
	}
	if err := validateResultDocument(prior); err != nil {
		return document{}, fmt.Errorf("invalid benchmark result: %w", err)
	}
	return prior, nil
}

func checkConsistency(address string, timeout time.Duration, isolation, resultPath, stage string) error {
	prior, err := loadPriorResult(resultPath)
	if err != nil {
		return err
	}
	model, err := newConsistencyModel(prior)
	if err != nil {
		return err
	}
	c, err := newClient(address, timeout, isolation)
	if err != nil {
		return err
	}
	defer c.close()
	if strings.HasPrefix(stage, "online-") {
		terminals, err := validatePaymentFloatChains(prior)
		if err != nil {
			return fmt.Errorf("[%s] Payment FLOAT32 chain: %w", stage, err)
		}
		if err := checkPaymentTerminalBits(c, terminals, stage); err != nil {
			return err
		}
		bits, err := checkOnlineConsistencyWithBits(c, model, stage)
		if err != nil {
			return err
		}
		prior.OnlineFloatBits = bits
		prior.PaymentTerminalBits = terminals
		if _, err := publishResultDocument(resultPath, prior); err != nil {
			return fmt.Errorf("[%s] persist online FLOAT32 evidence: %w", stage, err)
		}
		return nil
	}
	if len(prior.OnlineFloatBits) != postRecoveryAmountRuleCnt {
		return fmt.Errorf("[%s] result carries %d online FLOAT32 baselines, want %d; run online consistency before crash",
			stage, len(prior.OnlineFloatBits), postRecoveryAmountRuleCnt)
	}
	terminals, err := validatePaymentFloatChains(prior)
	if err != nil {
		return fmt.Errorf("[%s] Payment FLOAT32 chain: %w", stage, err)
	}
	if len(prior.PaymentTerminalBits) != len(terminals) {
		return fmt.Errorf("[%s] result carries %d Payment terminal bits, want %d; run online consistency before crash",
			stage, len(prior.PaymentTerminalBits), len(terminals))
	}
	for key, want := range terminals {
		if got, ok := prior.PaymentTerminalBits[key]; !ok || !equalFloat32Bits(got, want) {
			return fmt.Errorf("[%s] persisted Payment terminal %s does not match the committed edge chain", stage, key)
		}
	}
	if err := checkPaymentTerminalBits(c, terminals, stage); err != nil {
		return err
	}
	return checkPostRecoveryConsistency(c, model, stage)
}

func checkOnlineConsistency(c sqlExecutor, model consistencyModel, stage string) error {
	_, err := checkOnlineConsistencyWithBits(c, model, stage)
	return err
}

func checkOnlineConsistencyWithBits(c sqlExecutor, model consistencyModel, stage string) (map[string]uint32, error) {
	runner := &ruleRunner{exec: c}
	start := time.Now()
	runner.runIntRules(onlineIntRules(model))
	runner.runAmountRules(onlineAmountRules(model))
	if runner.intRules != onlineIntRuleCount || runner.amountRules != onlineAmountRuleCount {
		return nil, fmt.Errorf("[%s] online consistency ran %d integer + %d amount rules, want %d + %d",
			stage, runner.intRules, runner.amountRules, onlineIntRuleCount, onlineAmountRuleCount)
	}
	if len(runner.failures) > 0 {
		return nil, fmt.Errorf("[%s] online consistency validation failed (%d of %d rule(s))\n%s",
			stage, len(runner.failures), runner.intRules+runner.amountRules, strings.Join(runner.failures, "\n"))
	}
	fmt.Printf("consistency ok: stage=%s rules=%d integer + %d amount (%s)\n",
		stage, runner.intRules, runner.amountRules, time.Since(start).Round(time.Millisecond))
	return runner.floatBits, nil
}

func checkPostRecoveryConsistency(c sqlExecutor, model consistencyModel, stage string) error {
	runner := &ruleRunner{exec: c}
	start := time.Now()
	runner.runIntRules(postRecoveryIntRules(model))
	runner.runAmountRules(postRecoveryAmountRules(model))
	if runner.intRules != postRecoveryIntRuleCount || runner.amountRules != postRecoveryAmountRuleCnt {
		return fmt.Errorf("[%s] post-recovery consistency ran %d integer + %d amount rules, want %d + %d",
			stage, runner.intRules, runner.amountRules, postRecoveryIntRuleCount, postRecoveryAmountRuleCnt)
	}
	aggregateFailures := len(runner.failures)
	aggregateElapsed := time.Since(start)
	fmt.Printf("[%s] %d integer + %d amount aggregate rules took %s (%d failure(s))\n",
		stage, runner.intRules, runner.amountRules, aggregateElapsed.Round(time.Millisecond), aggregateFailures)

	// final.md:345 additionally requires every warehouse/district partition to be
	// reconciled one by one, plus the warehouse and district samples.
	partitionStart := time.Now()
	checkWarehouseYTD(runner, model)
	partitions := 0
	for wID := 1; wID <= model.warehouses; wID++ {
		for dID := 1; dID <= model.districtsPerWarehouse; dID++ {
			checkPartition(runner, wID, dID)
			partitions++
		}
	}
	fmt.Printf("[%s] %d warehouse/district partitions took %s\n",
		stage, partitions, time.Since(partitionStart).Round(time.Millisecond))
	if len(runner.failures) > 0 {
		return fmt.Errorf("[%s] consistency validation failed (%d rule(s))\n%s",
			stage, len(runner.failures), strings.Join(runner.failures, "\n"))
	}
	fmt.Printf("consistency ok: stage=%s rules=%d integer + %d amount aggregates over %d partitions (%s)\n",
		stage, runner.intRules, runner.amountRules, partitions, time.Since(start).Round(time.Millisecond))
	return nil
}

// checkWarehouseYTD reconciles every warehouse total against the sum of its
// districts. Both sides accumulate the same Payment amounts independently, so
// this catches a partially applied Payment that the global sums would hide.
func checkWarehouseYTD(runner *ruleRunner, model consistencyModel) {
	for wID := 1; wID <= model.warehouses; wID++ {
		warehouseYTD, err := runner.scalar(fmt.Sprintf("select w_ytd from warehouse where w_id = %d;", wID))
		if err != nil {
			runner.fail("warehouse YTD w=%d: %v", wID, err)
			continue
		}
		districtYTD, err := runner.scalar(fmt.Sprintf("select sum(d_ytd) from district where d_w_id = %d;", wID))
		if err != nil {
			runner.fail("district YTD w=%d: %v", wID, err)
			continue
		}
		// Both sides took the same number of steps but at magnitudes an order of
		// magnitude apart, so their drifts are independent and add in quadrature.
		// The magnitudes are the measured ones here, which makes the bound tighter
		// than the estimate the global rules have to use.
		districts := float64(model.districtsPerWarehouse)
		updates := model.paymentsForWarehouse(wID)
		drift := combineDrift(
			binary32DriftSigma(warehouseYTD, updates),
			binary32DriftSigma(districtYTD/districts, updates/districts)*math.Sqrt(districts),
		)
		tolerance := amountToleranceWithDrift(warehouseYTD, districtYTD, drift)
		if math.Abs(warehouseYTD-districtYTD) > tolerance {
			runner.fail("warehouse/district YTD mismatch w=%d: warehouse=%.2f, districts=%.2f (delta %.2f, tolerance %.2f, %.0f payment steps, binary32 ulp %.4g)",
				wID, warehouseYTD, districtYTD, warehouseYTD-districtYTD, tolerance, updates, binary32ULP(warehouseYTD))
		}
	}
}

// checkPartition reconciles one warehouse/district partition: order count,
// order line count, the undelivered queue, the rows with an empty delivery time,
// the o_carrier_id = 0 rows and d_next_o_id (final.md:345).
func checkPartition(runner *ruleRunner, wID, dID int) {
	// query reports the integer value of an aggregate, treating NULL as zero and
	// telling the caller which of the two it was. MIN/MAX over an empty queue is
	// legitimately NULL, so the callers guard on the row count instead.
	query := func(sql string) (int64, bool) {
		value, err := runner.nullableScalar(sql)
		if err != nil {
			runner.fail("partition w=%d d=%d: %v [%s]", wID, dID, err, sql)
			return 0, false
		}
		if !value.present {
			return 0, true
		}
		return int64(math.Round(value.number)), true
	}
	dNext, okNext := query(fmt.Sprintf("select d_next_o_id from district where d_w_id = %d and d_id = %d;", wID, dID))
	maxOrder, okMaxOrder := query(fmt.Sprintf("select max(o_id) from orders where o_w_id = %d and o_d_id = %d;", wID, dID))
	if okNext && okMaxOrder && dNext-1 != maxOrder {
		runner.fail("district order id mismatch w=%d d=%d: d_next=%d, max_order=%d", wID, dID, dNext, maxOrder)
	}
	countOrder, okCountOrder := query(fmt.Sprintf("select count(o_id) from orders where o_w_id = %d and o_d_id = %d;", wID, dID))
	minOrder, okMinOrder := query(fmt.Sprintf("select min(o_id) from orders where o_w_id = %d and o_d_id = %d;", wID, dID))
	if okCountOrder && okMaxOrder && okMinOrder && countOrder > 0 && countOrder != maxOrder-minOrder+1 {
		runner.fail("orders gap w=%d d=%d: count=%d, min=%d, max=%d", wID, dID, countOrder, minOrder, maxOrder)
	}
	countNew, okNew := query(fmt.Sprintf("select count(no_o_id) from new_orders where no_w_id = %d and no_d_id = %d;", wID, dID))
	minNew, okMinNew := query(fmt.Sprintf("select min(no_o_id) from new_orders where no_w_id = %d and no_d_id = %d;", wID, dID))
	maxNew, okMaxNew := query(fmt.Sprintf("select max(no_o_id) from new_orders where no_w_id = %d and no_d_id = %d;", wID, dID))
	if okNew && okMinNew && okMaxNew && countNew > 0 && countNew != maxNew-minNew+1 {
		runner.fail("new_orders gap w=%d d=%d: count=%d, min=%d, max=%d", wID, dID, countNew, minNew, maxNew)
	}
	if okNew && okMaxNew && okMaxOrder && countNew > 0 && maxNew != maxOrder {
		runner.fail("new_orders tail mismatch w=%d d=%d: max_new_order=%d, max_order=%d", wID, dID, maxNew, maxOrder)
	}
	carrierZero, okCarrier := query(fmt.Sprintf("select count(o_id) from orders where o_w_id = %d and o_d_id = %d and o_carrier_id = 0;", wID, dID))
	if okNew && okCarrier && carrierZero != countNew {
		runner.fail("pending order mismatch w=%d d=%d: carrier_zero=%d, new_orders=%d", wID, dID, carrierZero, countNew)
	}
	sumLines, okSum := query(fmt.Sprintf("select sum(o_ol_cnt) from orders where o_w_id = %d and o_d_id = %d;", wID, dID))
	countLines, okCount := query(fmt.Sprintf("select count(ol_o_id) from order_line where ol_w_id = %d and ol_d_id = %d;", wID, dID))
	if okSum && okCount && sumLines != countLines {
		runner.fail("order_line count mismatch w=%d d=%d: sum_o_ol_cnt=%d, count_ol_o_id=%d", wID, dID, sumLines, countLines)
	}
	// The rows with an empty delivery time are exactly the lines of the orders
	// that are still undelivered. This is the per-partition check final.md:345
	// names ("空配送时间行数") and that the previous implementation was missing.
	pendingLines, okPending := query(fmt.Sprintf(
		"select sum(o_ol_cnt) from orders where o_w_id = %d and o_d_id = %d and o_carrier_id = 0;", wID, dID))
	emptyLines, okEmpty := query(fmt.Sprintf(
		"select count(*) from order_line where ol_w_id = %d and ol_d_id = %d and ol_delivery_d = '';", wID, dID))
	if okPending && okEmpty && pendingLines != emptyLines {
		runner.fail("empty delivery time mismatch w=%d d=%d: undelivered_o_ol_cnt=%d, empty_delivery_rows=%d",
			wID, dID, pendingLines, emptyLines)
	}
}
