package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
)

// scriptedAggregateExecutor answers each aggregate query with a canned scalar and
// records the SQL it was asked to run. Anything the rule tables ask for that the
// test did not script is an error, so a renamed or added query cannot silently
// pass.
type scriptedAggregateExecutor struct {
	answers    map[string]float64
	statements []string
}

func (e *scriptedAggregateExecutor) exec(sql string) (string, error) {
	e.statements = append(e.statements, sql)
	value, ok := e.answers[sql]
	if !ok {
		return "", fmt.Errorf("unexpected SQL %q", sql)
	}
	text := strconv.FormatFloat(value, 'f', -1, 64)
	return fmt.Sprintf("+---+\n| n |\n+---+\n| %s |\n+---+\nTotal record(s): 1\n", text), nil
}

func testConsistencyModel() consistencyModel {
	return consistencyModel{
		warehouses:            2,
		districtsPerWarehouse: districtsPerWarehouse,
		customersPerDistrict:  3000,
		itemCount:             100,
		baseline: map[string]float64{
			baseWarehouseRows:          2,
			baseWarehouseYTD:           600000,
			baseDistrictRows:           20,
			baseDistrictYTD:            600000,
			baseDistrictNextOID:        60020,
			baseCustomerRows:           60000,
			baseCustomerBalance:        -600000,
			baseCustomerYTDPayment:     600000,
			baseCustomerPaymentCnt:     60000,
			baseCustomerDeliveryCnt:    0,
			baseHistoryRows:            60000,
			baseHistoryAmount:          600000,
			baseNewOrdersRows:          18000,
			baseOrdersRows:             60000,
			baseOrdersOlCnt:            600000,
			baseOrdersCarrierZeroRows:  18000,
			baseOrderLineRows:          600000,
			baseOrderLineAmount:        900000,
			baseOrderLineQuantity:      3000000,
			baseOrderLineDeliveryNulls: 180000,
			baseItemRows:               100,
			baseStockRows:              200,
			baseStockQuantity:          11000,
			baseStockYTD:               0,
			baseStockOrderCnt:          0,
			baseStockRemoteCnt:         0,
		},
		ledger: map[string]float64{
			ledgerNewOrderCommits:    700,
			ledgerNewOrderRollbacks:  7,
			ledgerNewOrderLines:      7000,
			ledgerNewOrderQuantity:   38500,
			ledgerNewOrderStockDelta: -12000,
			ledgerNewOrderRemote:     70,
			ledgerNewOrderAmount:     123456.78,
			ledgerPaymentCommits:     900,
			ledgerPaymentAmount:      2250000.55,
			ledgerDeliveryOrders:     300,
			ledgerDeliveryCustomers:  299,
			ledgerDeliveryAmount:     150000.25,
		},
	}
}

// derivedRuleAnswers holds the pair of values used by the rules whose expected
// value is a second aggregate of the same database rather than a ledger figure.
const (
	testUndeliveredLines = 191000
	testDeliveredLines   = 416000
	testPartitionOrders  = 3100
	testPartitionDNext   = testPartitionOrders + 1
	testPartitionLines   = 31000
)

func passingIntAnswers(m consistencyModel, rules []intRule) map[string]float64 {
	answers := make(map[string]float64, len(rules)+4)
	for _, rule := range rules {
		if rule.wantSQL == "" {
			answers[rule.sql] = float64(rule.want)
		}
	}
	// The derived rules compare two aggregates of the recovered database; scripting
	// them as an agreeing pair is what "the rule holds" means.
	answers["select sum(o_ol_cnt) from orders where o_carrier_id = 0;"] = testUndeliveredLines
	answers["select count(*) from order_line where ol_delivery_d is null;"] = testUndeliveredLines
	answers["select sum(o_ol_cnt) from orders where o_carrier_id <> 0;"] = testDeliveredLines
	answers["select count(*) from order_line where ol_delivery_d is not null;"] = testDeliveredLines
	answers["select count(o_id) from orders where o_w_id = 1 and o_d_id = 1;"] = testPartitionOrders
	answers["select d_next_o_id from district where d_w_id = 1 and d_id = 1;"] = testPartitionDNext
	answers["select count(ol_o_id) from order_line where ol_w_id = 1 and ol_d_id = 1;"] = testPartitionLines
	answers["select sum(o_ol_cnt) from orders where o_w_id = 1 and o_d_id = 1;"] = testPartitionLines
	return answers
}

func passingAmountAnswers(answers map[string]float64, rules []amountRule) map[string]float64 {
	for _, rule := range rules {
		if rule.useWantBits {
			answers[rule.sql] = float64(math.Float32frombits(rule.wantBits))
		} else {
			answers[rule.sql] = rule.want
		}
	}
	return answers
}

// warehouseYTDAnswers scripts the per-warehouse reconciliation that runs after
// the aggregate rules.
func warehouseYTDAnswers(answers map[string]float64, m consistencyModel) map[string]float64 {
	perWarehouse := (m.f(baseWarehouseYTD) + m.a(ledgerPaymentAmount)) / float64(m.warehouses)
	for wID := 1; wID <= m.warehouses; wID++ {
		answers[fmt.Sprintf("select w_ytd from warehouse where w_id = %d;", wID)] = perWarehouse
		answers[fmt.Sprintf("select sum(d_ytd) from district where d_w_id = %d;", wID)] = perWarehouse
	}
	return answers
}

func partitionAnswers(answers map[string]float64, m consistencyModel) map[string]float64 {
	for wID := 1; wID <= m.warehouses; wID++ {
		for dID := 1; dID <= m.districtsPerWarehouse; dID++ {
			for sql, value := range onePartitionAnswers(wID, dID) {
				answers[sql] = value
			}
		}
	}
	return answers
}

// onePartitionAnswers describes a healthy partition: 3100 gap-free orders, the
// last 900 still queued in new_orders with o_carrier_id = 0, and exactly the
// lines of those 900 orders carrying no delivery time.
func onePartitionAnswers(wID, dID int) map[string]float64 {
	const orders, lines, pending, pendingLines = 3100, 31000, 900, 9000
	return map[string]float64{
		fmt.Sprintf("select d_next_o_id from district where d_w_id = %d and d_id = %d;", wID, dID):                              orders + 1,
		fmt.Sprintf("select max(o_id) from orders where o_w_id = %d and o_d_id = %d;", wID, dID):                                orders,
		fmt.Sprintf("select count(o_id) from orders where o_w_id = %d and o_d_id = %d;", wID, dID):                              orders,
		fmt.Sprintf("select min(o_id) from orders where o_w_id = %d and o_d_id = %d;", wID, dID):                                1,
		fmt.Sprintf("select count(no_o_id) from new_orders where no_w_id = %d and no_d_id = %d;", wID, dID):                     pending,
		fmt.Sprintf("select min(no_o_id) from new_orders where no_w_id = %d and no_d_id = %d;", wID, dID):                       orders - pending + 1,
		fmt.Sprintf("select max(no_o_id) from new_orders where no_w_id = %d and no_d_id = %d;", wID, dID):                       orders,
		fmt.Sprintf("select count(o_id) from orders where o_w_id = %d and o_d_id = %d and o_carrier_id = 0;", wID, dID):         pending,
		fmt.Sprintf("select sum(o_ol_cnt) from orders where o_w_id = %d and o_d_id = %d;", wID, dID):                            lines,
		fmt.Sprintf("select count(ol_o_id) from order_line where ol_w_id = %d and ol_d_id = %d;", wID, dID):                     lines,
		fmt.Sprintf("select sum(o_ol_cnt) from orders where o_w_id = %d and o_d_id = %d and o_carrier_id = 0;", wID, dID):       pendingLines,
		fmt.Sprintf("select count(*) from order_line where ol_w_id = %d and ol_d_id = %d and ol_delivery_d is null;", wID, dID): pendingLines,
	}
}

func passingPostRecoveryAnswers(m consistencyModel) map[string]float64 {
	answers := passingIntAnswers(m, postRecoveryIntRules(m))
	answers = passingAmountAnswers(answers, postRecoveryAmountRules(m))
	answers = warehouseYTDAnswers(answers, m)
	answers = partitionAnswers(answers, m)
	return answers
}

func TestConsistencyRuleCountsMatchTheOfficialShape(t *testing.T) {
	// final.md:319 fixes the shape: 6 integer + 2 amount before the kill, 37
	// integer + 7 amount after the restart.
	m := testConsistencyModel()
	if got := len(onlineIntRules(m)); got != onlineIntRuleCount {
		t.Errorf("online integer rules = %d, want %d", got, onlineIntRuleCount)
	}
	if got := len(onlineAmountRules(m)); got != onlineAmountRuleCount {
		t.Errorf("online amount rules = %d, want %d", got, onlineAmountRuleCount)
	}
	if got := len(postRecoveryIntRules(m)); got != postRecoveryIntRuleCount {
		t.Errorf("post-recovery integer rules = %d, want %d", got, postRecoveryIntRuleCount)
	}
	if got := len(postRecoveryAmountRules(m)); got != postRecoveryAmountRuleCnt {
		t.Errorf("post-recovery amount rules = %d, want %d", got, postRecoveryAmountRuleCnt)
	}
}

func TestConsistencyRuleNamesAndQueriesAreUnique(t *testing.T) {
	m := testConsistencyModel()
	for _, set := range []struct {
		label string
		ints  []intRule
		amts  []amountRule
	}{
		{"online", onlineIntRules(m), onlineAmountRules(m)},
		{"post-recovery", postRecoveryIntRules(m), postRecoveryAmountRules(m)},
	} {
		names, queries := map[string]bool{}, map[string]bool{}
		for _, rule := range set.ints {
			if names[rule.name] {
				t.Errorf("%s: duplicate rule name %q", set.label, rule.name)
			}
			if queries[rule.sql] {
				t.Errorf("%s: duplicate rule query %q", set.label, rule.sql)
			}
			names[rule.name], queries[rule.sql] = true, true
		}
		for _, rule := range set.amts {
			if names[rule.name] {
				t.Errorf("%s: duplicate rule name %q", set.label, rule.name)
			}
			names[rule.name] = true
		}
	}
}

func TestOnlineRulesCaptureSevenDistinctFloatAggregates(t *testing.T) {
	m := testConsistencyModel()
	queries := make(map[string]struct{}, onlineAmountRuleCount)
	for _, rule := range onlineAmountRules(m) {
		queries[rule.sql] = struct{}{}
	}
	if len(queries) != onlineAmountRuleCount {
		t.Fatalf("online FLOAT rules use %d distinct queries, want %d", len(queries), onlineAmountRuleCount)
	}
}

func TestPostRecoveryConsistencyAcceptsAReconciledDatabase(t *testing.T) {
	m := testConsistencyModel()
	executor := &scriptedAggregateExecutor{answers: passingPostRecoveryAnswers(m)}
	if err := checkPostRecoveryConsistency(executor, m, "unit"); err != nil {
		t.Fatal(err)
	}
}

func TestOnlineConsistencyAcceptsAReconciledDatabase(t *testing.T) {
	m := testConsistencyModel()
	answers := passingAmountAnswers(passingIntAnswers(m, onlineIntRules(m)), onlineAmountRules(m))
	if err := checkOnlineConsistency(&scriptedAggregateExecutor{answers: answers}, m, "online-unit"); err != nil {
		t.Fatal(err)
	}
}

// ruleViolations maps every integer rule's query to the rule name that must
// appear in the failure when that query's answer is corrupted.
func ruleViolations(rules []intRule) map[string]string {
	violations := make(map[string]string, len(rules))
	for _, rule := range rules {
		violations[rule.sql] = rule.name
	}
	return violations
}

func TestPostRecoveryConsistencyRejectsEveryViolation(t *testing.T) {
	m := testConsistencyModel()
	base := passingPostRecoveryAnswers(m)
	violations := ruleViolations(postRecoveryIntRules(m))
	if len(violations) != postRecoveryIntRuleCount {
		t.Fatalf("perturbing %d queries covers fewer than the %d integer rules", len(violations), postRecoveryIntRuleCount)
	}
	for sql, name := range violations {
		answers := make(map[string]float64, len(base))
		for key, value := range base {
			answers[key] = value
		}
		answers[sql] = base[sql] + 1
		err := checkPostRecoveryConsistency(&scriptedAggregateExecutor{answers: answers}, m, "unit")
		if err == nil {
			t.Errorf("violating %q was accepted", sql)
			continue
		}
		if !strings.Contains(err.Error(), name) {
			t.Errorf("violating %q reported %v, want the rule name %q", sql, err, name)
		}
	}
	for _, rule := range postRecoveryAmountRules(m) {
		answers := make(map[string]float64, len(base))
		for key, value := range base {
			answers[key] = value
		}
		answers[rule.sql] = rule.want + 2*amountToleranceWithDrift(rule.want, rule.want, rule.drift) + 1
		err := checkPostRecoveryConsistency(&scriptedAggregateExecutor{answers: answers}, m, "unit")
		if err == nil || !strings.Contains(err.Error(), rule.name) {
			t.Errorf("violating %q reported %v, want the rule name %q", rule.sql, err, rule.name)
		}
	}
}

func TestOnlineConsistencyRejectsEveryViolation(t *testing.T) {
	m := testConsistencyModel()
	base := passingAmountAnswers(passingIntAnswers(m, onlineIntRules(m)), onlineAmountRules(m))
	for _, rule := range onlineIntRules(m) {
		answers := make(map[string]float64, len(base))
		for key, value := range base {
			answers[key] = value
		}
		answers[rule.sql] = base[rule.sql] + 1
		err := checkOnlineConsistency(&scriptedAggregateExecutor{answers: answers}, m, "online-unit")
		if err == nil || !strings.Contains(err.Error(), rule.name) {
			t.Errorf("violating %q reported %v, want the rule name %q", rule.sql, err, rule.name)
		}
	}
	for _, rule := range onlineAmountRules(m) {
		if rule.captureOnly {
			continue
		}
		answers := make(map[string]float64, len(base))
		for key, value := range base {
			answers[key] = value
		}
		answers[rule.sql] = rule.want + 2*amountToleranceWithDrift(rule.want, rule.want, rule.drift) + 1
		err := checkOnlineConsistency(&scriptedAggregateExecutor{answers: answers}, m, "online-unit")
		if err == nil || !strings.Contains(err.Error(), rule.name) {
			t.Errorf("violating %q reported %v, want the rule name %q", rule.sql, err, rule.name)
		}
	}
}

// TestOnlineConsistencyDetectsShrunkLargeTables reproduces the class of bug that
// escaped the previous online check: a fraction of the committed customer and
// stock rows became invisible after the crash while every small-table check kept
// passing.
func TestOnlineConsistencyDetectsShrunkLargeTables(t *testing.T) {
	m := testConsistencyModel()
	base := passingAmountAnswers(passingIntAnswers(m, onlineIntRules(m)), onlineAmountRules(m))
	cases := map[string]string{
		"select count(c_id) from customer where c_w_id = 1 and c_d_id = 1;":        "customer rows in partition (1,1)",
		"select count(s_i_id) from stock where s_w_id = 1;":                        "stock rows in warehouse 1",
		"select count(ol_o_id) from order_line where ol_w_id = 1 and ol_d_id = 1;": "order_line rows in partition (1,1) match SUM(o_ol_cnt)",
		"select count(o_id) from orders where o_w_id = 1 and o_d_id = 1;":          "orders rows in partition (1,1) match d_next_o_id",
	}
	for sql, name := range cases {
		answers := make(map[string]float64, len(base))
		for key, value := range base {
			answers[key] = value
		}
		// Lose 0.7% of the rows, the same order of magnitude as the real incident.
		answers[sql] = math.Floor(base[sql] * 0.993)
		err := checkOnlineConsistency(&scriptedAggregateExecutor{answers: answers}, m, "online-unit")
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("shrinking %q reported %v, want the rule name %q", sql, err, name)
		}
	}
}

// TestPostRecoveryConsistencyDetectsLeakedOrderNumber is the guard for the trap
// recorded in PLAN.md item 1.12. An aborted NewOrder must not consume an order
// number here, because this driver increments d_next_o_id inside the transaction
// it aborts. If the engine leaks it, the surplus is exactly the expected-rollback
// count and the diagnostic has to say so.
func TestPostRecoveryConsistencyDetectsLeakedOrderNumber(t *testing.T) {
	m := testConsistencyModel()
	answers := passingPostRecoveryAnswers(m)
	const sql = "select sum(d_next_o_id) from district;"
	answers[sql] += float64(m.l(ledgerNewOrderRollbacks))
	err := checkPostRecoveryConsistency(&scriptedAggregateExecutor{answers: answers}, m, "unit")
	if err == nil {
		t.Fatal("a leaked order number was accepted")
	}
	if !strings.Contains(err.Error(), "district SUM(d_next_o_id)") || !strings.Contains(err.Error(), "expected-rollback") {
		t.Fatalf("leaked order number reported %v, want the d_next_o_id rule and its hint", err)
	}
}

func TestPartitionCheckRejectsEveryPartitionViolation(t *testing.T) {
	base := onePartitionAnswers(3, 7)
	// A healthy partition must pass.
	runner := &ruleRunner{exec: &scriptedAggregateExecutor{answers: base}}
	checkPartition(runner, 3, 7)
	if len(runner.failures) != 0 {
		t.Fatalf("healthy partition failed: %v", runner.failures)
	}
	for sql := range base {
		answers := make(map[string]float64, len(base))
		for key, value := range base {
			answers[key] = value
		}
		answers[sql] = base[sql] + 1
		runner := &ruleRunner{exec: &scriptedAggregateExecutor{answers: answers}}
		checkPartition(runner, 3, 7)
		if len(runner.failures) == 0 {
			t.Errorf("perturbing %q in a partition was accepted", sql)
		}
	}
}

// TestPartitionCheckCoversEmptyDeliveryTime pins the per-partition item
// final.md:345 names and the previous implementation lacked.
func TestPartitionCheckCoversEmptyDeliveryTime(t *testing.T) {
	base := onePartitionAnswers(2, 4)
	const sql = "select count(*) from order_line where ol_w_id = 2 and ol_d_id = 4 and ol_delivery_d is null;"
	if _, ok := base[sql]; !ok {
		t.Fatalf("the partition check does not count rows with an empty delivery time")
	}
	answers := make(map[string]float64, len(base))
	for key, value := range base {
		answers[key] = value
	}
	answers[sql] = base[sql] - 5
	runner := &ruleRunner{exec: &scriptedAggregateExecutor{answers: answers}}
	checkPartition(runner, 2, 4)
	joined := strings.Join(runner.failures, "\n")
	if !strings.Contains(joined, "empty delivery time mismatch") {
		t.Fatalf("failures = %v, want an empty delivery time mismatch", runner.failures)
	}
}

func TestPartitionCheckAcceptsAnEmptyNewOrderQueue(t *testing.T) {
	// MIN/MAX over an empty queue is legitimately NULL and must not be reported as
	// a broken partition.
	answers := onePartitionAnswers(1, 1)
	answers["select count(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1;"] = 0
	answers["select count(o_id) from orders where o_w_id = 1 and o_d_id = 1 and o_carrier_id = 0;"] = 0
	answers["select sum(o_ol_cnt) from orders where o_w_id = 1 and o_d_id = 1 and o_carrier_id = 0;"] = 0
	answers["select count(*) from order_line where ol_w_id = 1 and ol_d_id = 1 and ol_delivery_d is null;"] = 0
	executor := &nullableAggregateExecutor{scriptedAggregateExecutor{answers: answers}, map[string]bool{
		"select min(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1;": true,
		"select max(no_o_id) from new_orders where no_w_id = 1 and no_d_id = 1;": true,
	}}
	runner := &ruleRunner{exec: executor}
	checkPartition(runner, 1, 1)
	if len(runner.failures) != 0 {
		t.Fatalf("an empty new_orders queue was rejected: %v", runner.failures)
	}
}

type nullableAggregateExecutor struct {
	scriptedAggregateExecutor
	nulls map[string]bool
}

func (e *nullableAggregateExecutor) exec(sql string) (string, error) {
	if e.nulls[sql] {
		e.statements = append(e.statements, sql)
		return "+------+\n| n    |\n+------+\n| NULL |\n+------+\nTotal record(s): 1\n", nil
	}
	return e.scriptedAggregateExecutor.exec(sql)
}

func TestNewConsistencyModelRejectsResultsWithoutReconciliationData(t *testing.T) {
	base := document{Config: config{BaselineWarehouseTotal: 50, BaselineDistrictTotal: 500,
		BaselineCustomerTotal: 1500000, BaselineItemTotal: 100000}}
	if _, err := newConsistencyModel(base); err == nil || !strings.Contains(err.Error(), "aggregate snapshot") {
		t.Fatalf("missing baseline error = %v", err)
	}
	withBaseline := base
	withBaseline.Baselines = testConsistencyModel().baseline
	if _, err := newConsistencyModel(withBaseline); err == nil || !strings.Contains(err.Error(), "transaction ledger") {
		t.Fatalf("missing ledger error = %v", err)
	}
	partialLedger := withBaseline
	partialLedger.Ledger = map[string]float64{ledgerPaymentCommits: 1}
	if _, err := newConsistencyModel(partialLedger); err == nil || !strings.Contains(err.Error(), ledgerNewOrderCommits) {
		t.Fatalf("incomplete ledger error = %v", err)
	}
	complete := withBaseline
	complete.Ledger = testConsistencyModel().ledger
	if _, err := newConsistencyModel(complete); err != nil {
		t.Fatal(err)
	}
	missingBaselineKey := complete
	missingBaselineKey.Baselines = make(map[string]float64, len(complete.Baselines))
	for key, value := range complete.Baselines {
		missingBaselineKey.Baselines[key] = value
	}
	delete(missingBaselineKey.Baselines, baseStockOrderCnt)
	if _, err := newConsistencyModel(missingBaselineKey); err == nil || !strings.Contains(err.Error(), baseStockOrderCnt) {
		t.Fatalf("missing baseline key error = %v", err)
	}
}

// multiColumnExecutor answers each baseline query with the number of columns it
// selected, so captureBaselines can be checked without a live server.
type multiColumnExecutor struct {
	values map[string][]float64
}

func (e *multiColumnExecutor) exec(sql string) (string, error) {
	values, ok := e.values[sql]
	if !ok {
		return "", fmt.Errorf("unexpected SQL %q", sql)
	}
	cells := make([]string, len(values))
	for i, value := range values {
		cells[i] = strconv.FormatFloat(value, 'f', -1, 64)
	}
	return fmt.Sprintf("| %s |\n| %s |\nTotal record(s): 1\n",
		strings.Repeat("h |", len(values)), strings.Join(cells, " | ")), nil
}

func TestCaptureBaselinesReadsEveryDeclaredAggregate(t *testing.T) {
	values := make(map[string][]float64, len(baselineQueries))
	next := 1.0
	want := make(map[string]float64)
	for _, query := range baselineQueries {
		row := make([]float64, len(query.keys))
		for i, key := range query.keys {
			row[i] = next
			want[key] = next
			next++
		}
		values[query.sql] = row
	}
	got, err := captureBaselines(&multiColumnExecutor{values: values})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("captured %d baselines, want %d", len(got), len(want))
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("baseline %s = %v, want %v", key, got[key], value)
		}
	}
}

func TestCaptureBaselinesRejectsANullAggregate(t *testing.T) {
	values := make(map[string][]float64, len(baselineQueries))
	for _, query := range baselineQueries {
		values[query.sql] = make([]float64, len(query.keys))
	}
	executor := &nullBaselineExecutor{multiColumnExecutor{values: values}, baselineQueries[0].sql}
	if _, err := captureBaselines(executor); err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Fatalf("NULL baseline error = %v", err)
	}
}

type nullBaselineExecutor struct {
	multiColumnExecutor
	nullSQL string
}

func (e *nullBaselineExecutor) exec(sql string) (string, error) {
	if sql == e.nullSQL {
		columns := len(e.values[sql])
		cells := make([]string, columns)
		for i := range cells {
			cells[i] = "NULL"
		}
		return fmt.Sprintf("| %s |\n| %s |\nTotal record(s): 1\n",
			strings.Repeat("h |", columns), strings.Join(cells, " | ")), nil
	}
	return e.multiColumnExecutor.exec(sql)
}

func TestAmountToleranceIsScaleAdaptive(t *testing.T) {
	if got := amountTolerance(0, 0); got != amountToleranceFloor {
		t.Errorf("amountTolerance(0, 0) = %v, want the floor %v", got, amountToleranceFloor)
	}
	// A binary32 aggregate of 6.5e7 has an ULP of 8, so the tolerance has to be
	// larger than that but far smaller than a single TPC-C payment.
	const scale = 6.5e7
	got := amountTolerance(scale, scale)
	if got <= 8 {
		t.Errorf("amountTolerance(%v) = %v, want more than one binary32 ULP", scale, got)
	}
	if got >= 1000 {
		t.Errorf("amountTolerance(%v) = %v, want less than a single payment amount", scale, got)
	}
}

func TestBinary32ULPReportsTheRecordableStep(t *testing.T) {
	if got := binary32ULP(0); got != 0 {
		t.Errorf("binary32ULP(0) = %v, want 0", got)
	}
	// 2^24 is where binary32 stops being able to represent every integer.
	if got := binary32ULP(1 << 24); got != 2 {
		t.Errorf("binary32ULP(2^24) = %v, want 2", got)
	}
	if got := binary32ULP(-6.5e7); got != 4 {
		t.Errorf("binary32ULP(-6.5e7) = %v, want 4", got)
	}
}

func TestValidateOfficialWarehousesRequiresTheOfficialScale(t *testing.T) {
	// PLAN.md item N6: requiring 25 warehouses was looser than the evaluator,
	// which uses a 50 warehouse data set (final.md:47) and scores a wrong data
	// scale as zero (final.md:226).
	if err := validateOfficialWarehouses(officialWarehouses, false); err != nil {
		t.Fatal(err)
	}
	for _, warehouses := range []int{1, 24, 25, 49, 51} {
		if err := validateOfficialWarehouses(warehouses, false); err == nil {
			t.Errorf("official run against %d warehouses was accepted", warehouses)
		}
	}
	// Explicit smoke runs may deviate, but still need enough terminal homes.
	if err := validateOfficialWarehouses(officialMinWarehouses, true); err != nil {
		t.Fatal(err)
	}
	if err := validateOfficialWarehouses(officialMinWarehouses-1, true); err == nil {
		t.Error("a smoke run without enough terminal homes was accepted")
	}
}

// officialScalePaymentModel mirrors the real numbers measured on a four by 60 s
// run: w_ytd reaches about 19.5 M and each warehouse absorbed about 1e5 Payments.
func officialScalePaymentModel() consistencyModel {
	const (
		warehouses   = 50
		perWarehouse = 100000.0
		average      = 192.0
	)
	m := testConsistencyModel()
	m.warehouses = warehouses
	m.baseline[baseWarehouseYTD] = 300000 * warehouses
	m.baseline[baseDistrictYTD] = 30000 * warehouses * districtsPerWarehouse
	m.ledger[ledgerPaymentCommits] = perWarehouse * warehouses
	m.ledger[ledgerPaymentAmount] = perWarehouse * warehouses * average
	for wID := 1; wID <= warehouses; wID++ {
		m.ledger[ledgerPaymentWarehousePrefix+strconv.Itoa(wID)] = perWarehouse
	}
	return m
}

// TestBinary32DriftSigmaReproducesTheMeasuredDivergence pins the tolerance model
// against the real observation: w_ytd and SUM(d_ytd) diverged by 50 to 66 on a
// long run with no atomicity fault, because w_ytd accumulates ten times the
// amount and its binary32 ULP is therefore sixteen times coarser.
func TestBinary32DriftSigmaReproducesTheMeasuredDivergence(t *testing.T) {
	const (
		warehouseYTD = 19500918.0
		districtYTD  = 1950091.8
		steps        = 100000.0
	)
	if ulp := binary32ULP(warehouseYTD); ulp != 2 {
		t.Fatalf("binary32 ULP at %.0f = %v, want 2", warehouseYTD, ulp)
	}
	if ulp := binary32ULP(districtYTD); ulp != 0.125 {
		t.Fatalf("binary32 ULP at %.0f = %v, want 0.125", districtYTD, ulp)
	}
	warehouseSigma := binary32DriftSigma(warehouseYTD, steps)
	districtSigma := binary32DriftSigma(districtYTD, steps/districtsPerWarehouse) * math.Sqrt(districtsPerWarehouse)
	if warehouseSigma < 150 || warehouseSigma > 220 {
		t.Fatalf("warehouse drift sigma = %.1f, want the ~183 the model predicts", warehouseSigma)
	}
	// The district side has to be more than an order of magnitude smaller; that
	// asymmetry is the whole reason the two sides diverge at all.
	if districtSigma > warehouseSigma/10 {
		t.Fatalf("district drift sigma = %.1f, want far below the warehouse sigma %.1f", districtSigma, warehouseSigma)
	}
	drift := combineDrift(warehouseSigma, districtSigma)
	tolerance := amountToleranceWithDrift(warehouseYTD, districtYTD+18*districtsPerWarehouse, drift)
	// The measured divergence must be inside the window ...
	for _, observed := range []float64{50, 60, 66} {
		if observed > tolerance {
			t.Errorf("a measured binary32 divergence of %.0f exceeds the tolerance %.1f", observed, tolerance)
		}
	}
	// ... and a single average Payment must still be outside it.
	const averagePayment = 2500.0
	if averagePayment <= tolerance {
		t.Errorf("tolerance %.1f would hide a lost Payment of %.0f", tolerance, averagePayment)
	}
}

func TestWarehouseYTDReconciliationSeparatesDriftFromALostPayment(t *testing.T) {
	m := officialScalePaymentModel()
	perWarehouse := (m.f(baseWarehouseYTD) + m.a(ledgerPaymentAmount)) / float64(m.warehouses)
	build := func(delta float64) map[string]float64 {
		answers := map[string]float64{}
		for wID := 1; wID <= m.warehouses; wID++ {
			answers[fmt.Sprintf("select w_ytd from warehouse where w_id = %d;", wID)] = perWarehouse + delta
			answers[fmt.Sprintf("select sum(d_ytd) from district where d_w_id = %d;", wID)] = perWarehouse
		}
		return answers
	}
	// Accumulated binary32 rounding of the size actually measured must pass.
	runner := &ruleRunner{exec: &scriptedAggregateExecutor{answers: build(66)}}
	checkWarehouseYTD(runner, m)
	if len(runner.failures) != 0 {
		t.Fatalf("a 66 unit binary32 divergence was rejected: %v", runner.failures[0])
	}
	// A whole Payment going missing must not.
	runner = &ruleRunner{exec: &scriptedAggregateExecutor{answers: build(-2500)}}
	checkWarehouseYTD(runner, m)
	if len(runner.failures) != m.warehouses {
		t.Fatalf("a lost Payment produced %d failures, want %d", len(runner.failures), m.warehouses)
	}
	if !strings.Contains(runner.failures[0], "payment steps") || !strings.Contains(runner.failures[0], "binary32 ulp") {
		t.Errorf("failure %q lacks the drift diagnostics", runner.failures[0])
	}
}

func TestRecoveryFloatRulesUseFinalV2ULPClasses(t *testing.T) {
	m := officialScalePaymentModel()
	strict := map[string]bool{
		"select sum(w_ytd) from warehouse;": true,
		"select sum(d_ytd) from district;":  true,
		"select sum(s_ytd) from stock;":     true,
	}
	for _, rule := range postRecoveryAmountRules(m) {
		want := uint32(1)
		if strict[rule.sql] {
			want = 0
		}
		if !rule.useWantBits || rule.maxULP != want {
			t.Errorf("%s uses wantBits=%v maxULP=%d, want %d", rule.name, rule.useWantBits, rule.maxULP, want)
		}
	}
	for _, rule := range onlineAmountRules(m) {
		if !rule.captureOnly && rule.boundaryAddends == 0 && rule.sql != "select sum(s_ytd) from stock;" {
			t.Errorf("online rule %s has no finalv2 exact comparison", rule.name)
		}
	}
}

func TestAmountToleranceWithDriftNeverShrinksTheWindow(t *testing.T) {
	const value = 1e7
	base := amountTolerance(value, value)
	if got := amountToleranceWithDrift(value, value, 0); got != base {
		t.Errorf("a zero drift changed the tolerance from %v to %v", base, got)
	}
	if got := amountToleranceWithDrift(value, value, base); got < base {
		t.Errorf("a drift allowance shrank the tolerance to %v", got)
	}
}

func TestFinalV2FloatULPAndBoundaryRules(t *testing.T) {
	if distance := float32ULPDistance(math.Float32bits(0), math.Float32bits(float32(math.Copysign(0, -1)))); distance != 0 {
		t.Fatalf("+0/-0 distance = %d", distance)
	}
	low := float32(1)
	high := math.Nextafter32(low, float32(math.Inf(1)))
	midpoint := (float64(low) + float64(high)) / 2
	if !boundaryAwareFloat32Match(math.Float32bits(low), midpoint, 1) {
		t.Fatal("lower rounding candidate at a binary32 midpoint was rejected")
	}
	if !boundaryAwareFloat32Match(math.Float32bits(high), midpoint, 1) {
		t.Fatal("upper rounding candidate at a binary32 midpoint was rejected")
	}
	beyond := math.Nextafter32(high, float32(math.Inf(1)))
	if boundaryAwareFloat32Match(math.Float32bits(beyond), midpoint, 1) {
		t.Fatal("a value beyond the two boundary candidates was accepted")
	}
}

func TestPaymentFloatChainsLinkWithoutClientCompletionOrder(t *testing.T) {
	start := float32(300000)
	a1, a2 := float32(10.25), float32(20.5)
	middle, end := start+a1, start+a1+a2
	doc := document{
		Ledger: map[string]float64{ledgerPaymentCommits: 2},
		PaymentEdges: []paymentFloatEdge{
			{Kind: "warehouse", Warehouse: 1, BeforeBits: math.Float32bits(middle),
				AmountBits: math.Float32bits(a2), AfterBits: math.Float32bits(end)},
			{Kind: "district", Warehouse: 1, District: 1, BeforeBits: math.Float32bits(float32(30000)),
				AmountBits: math.Float32bits(a1), AfterBits: math.Float32bits(float32(30000) + a1)},
			{Kind: "warehouse", Warehouse: 1, BeforeBits: math.Float32bits(start),
				AmountBits: math.Float32bits(a1), AfterBits: math.Float32bits(middle)},
			{Kind: "district", Warehouse: 2, District: 3, BeforeBits: math.Float32bits(float32(30000)),
				AmountBits: math.Float32bits(a2), AfterBits: math.Float32bits(float32(30000) + a2)},
		},
	}
	terminals, err := validatePaymentFloatChains(doc)
	if err != nil {
		t.Fatal(err)
	}
	if got := terminals["warehouse:1"]; got != math.Float32bits(end) {
		t.Fatalf("warehouse terminal = 0x%08x, want 0x%08x", got, math.Float32bits(end))
	}
}

func TestPaymentFloatChainsRejectALostUpdateFork(t *testing.T) {
	start := float32(300000)
	amount := float32(10)
	doc := document{
		Ledger: map[string]float64{ledgerPaymentCommits: 1},
		PaymentEdges: []paymentFloatEdge{
			{Kind: "warehouse", Warehouse: 1, BeforeBits: math.Float32bits(start),
				AmountBits: math.Float32bits(amount), AfterBits: math.Float32bits(start + amount)},
			{Kind: "warehouse", Warehouse: 1, BeforeBits: math.Float32bits(start),
				AmountBits: math.Float32bits(amount), AfterBits: math.Float32bits(start + amount)},
		},
	}
	if _, err := validatePaymentFloatChains(doc); err == nil || !strings.Contains(err.Error(), "forks") {
		t.Fatalf("lost-update fork reported %v", err)
	}
}
