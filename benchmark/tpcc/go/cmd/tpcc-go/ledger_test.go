package main

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// sqlRecordingBatcher records the raw SQL a ranking transaction emitted, so the
// ledger it published can be checked against the statements it actually sent.
// This is the guard that keeps the reconciliation honest: a ledger that drifts
// from the emitted SQL would make the post-crash validation compare the database
// against a fiction.
type sqlRecordingBatcher struct {
	*fakeRankingBatcher
	sqls []string
}

type sqlRecordingBackend struct {
	sqls []string
}

func (r *sqlRecordingBackend) exec(sql string) (string, error) {
	r.sqls = append(r.sqls, sql)
	switch {
	case strings.HasPrefix(sql, "select d_next_o_id"):
		return "2", nil
	case strings.HasPrefix(sql, "select i_price"):
		return "1.0", nil
	case strings.HasPrefix(sql, "select s_quantity"):
		return "15", nil
	default:
		return "1", nil
	}
}

func (r *sqlRecordingBackend) begin() error  { return nil }
func (r *sqlRecordingBackend) commit() error { return nil }
func (r *sqlRecordingBackend) rollback()     {}
func (r *sqlRecordingBackend) close()        {}

func newSQLRecordingBatcher(t *testing.T) *sqlRecordingBatcher {
	t.Helper()
	return &sqlRecordingBatcher{fakeRankingBatcher: newFakeRankingBatcher(t)}
}

func (r *sqlRecordingBatcher) batchOperation(sql string) (batchOperation, error) {
	r.sqls = append(r.sqls, sql)
	return r.fakeRankingBatcher.batchOperation(sql)
}

func (r *sqlRecordingBatcher) count(substring string) float64 {
	total := 0.0
	for _, sql := range r.sqls {
		if strings.Contains(sql, substring) {
			total++
		}
	}
	return total
}

func (r *sqlRecordingBatcher) sum(pattern string) float64 {
	re := regexp.MustCompile(pattern)
	total := 0.0
	for _, sql := range r.sqls {
		for _, match := range re.FindAllStringSubmatch(sql, -1) {
			value, err := strconv.ParseFloat(match[1], 64)
			if err != nil {
				continue
			}
			total += value
		}
	}
	return total
}

func ledgerContext(ledger *txnLedger) txnContext {
	ctx := rankingTestContext()
	ctx.ledger = ledger
	return ctx
}

func closeEnough(got, want float64) bool { return math.Abs(got-want) < 1e-6 }

// TestNewOrderLedgerMatchesTheEmittedSQL checks every NewOrder ledger entry
// against the statements the transaction sent, over many seeds so both the
// committing and the invalid-item path are covered.
func TestNewOrderLedgerMatchesTheEmittedSQL(t *testing.T) {
	sawCommit, sawRollback := false, false
	for seed := int64(1); seed <= 200; seed++ {
		batcher := newSQLRecordingBatcher(t)
		ledger := newTxnLedger()
		err := rankingNewOrder(batcher, ledgerContext(ledger), rand.New(rand.NewSource(seed)))
		if err != nil && !errors.Is(err, errInvalidItem) {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if errors.Is(err, errInvalidItem) {
			sawRollback = true
			if ledger.values[ledgerNewOrderRollbacks] != 1 {
				t.Fatalf("seed %d: an aborted NewOrder recorded %v expected rollbacks, want 1",
					seed, ledger.values[ledgerNewOrderRollbacks])
			}
			if ledger.values[ledgerNewOrderCommits] != 0 || ledger.values[ledgerNewOrderLines] != 0 {
				t.Fatalf("seed %d: an aborted NewOrder recorded committed effects: %v", seed, ledger.values)
			}
			continue
		}
		sawCommit = true
		if ledger.values[ledgerNewOrderCommits] != 1 {
			t.Fatalf("seed %d: committed NewOrder recorded %v commits", seed, ledger.values[ledgerNewOrderCommits])
		}
		if ledger.values[ledgerNewOrderRollbacks] != 0 {
			t.Fatalf("seed %d: committed NewOrder recorded an expected rollback", seed)
		}
		checks := []struct {
			key  string
			got  float64
			want float64
		}{
			{ledgerNewOrderLines, ledger.values[ledgerNewOrderLines], batcher.count("insert into order_line")},
			{ledgerNewOrderQuantity, ledger.values[ledgerNewOrderQuantity], batcher.sum(`s_ytd = s_ytd \+ (\d+)`)},
			{ledgerNewOrderRemote, ledger.values[ledgerNewOrderRemote], batcher.sum(`s_remote_cnt = s_remote_cnt \+ (\d+)`)},
			{ledgerNewOrderStockDelta, ledger.values[ledgerNewOrderStockDelta],
				batcher.sum(`s_quantity = s_quantity \+ (\d+)`) - batcher.sum(`s_quantity = s_quantity - (\d+)`)},
			{ledgerNewOrderAmount, ledger.values[ledgerNewOrderAmount],
				batcher.sum(`insert into order_line values \([^)]*, ([0-9]+\.[0-9]+), 'dist'\)`)},
		}
		for _, check := range checks {
			if !closeEnough(check.got, check.want) {
				t.Fatalf("seed %d: ledger %s = %v, emitted SQL implies %v", seed, check.key, check.got, check.want)
			}
		}
		// The order row must claim exactly as many lines as were inserted.
		if declared := batcher.sum(`insert into orders values \(\d+, \d+, \d+, \d+, '[^']*', 0, (\d+), 1\)`); !closeEnough(declared, ledger.values[ledgerNewOrderLines]) {
			t.Fatalf("seed %d: orders.o_ol_cnt = %v but %v order_line rows were inserted",
				seed, declared, ledger.values[ledgerNewOrderLines])
		}
	}
	if !sawCommit || !sawRollback {
		t.Fatalf("the seed range did not cover both paths (commit=%v rollback=%v)", sawCommit, sawRollback)
	}
}

func TestNewOrderLocksSortedUniqueStockKeysBeforeReading(t *testing.T) {
	batcher := newSQLRecordingBatcher(t)
	ctx := ledgerContext(newTxnLedger())
	ctx.hotItemIDs = []int{1, 2, 3, 4}
	if err := rankingNewOrder(batcher, ctx, rand.New(rand.NewSource(9))); err != nil && !errors.Is(err, errInvalidItem) {
		t.Fatal(err)
	}
	lockPattern := regexp.MustCompile(`update stock set s_ytd = s_ytd where s_w_id = (\d+) and s_i_id = (\d+);`)
	type key struct{ wID, iID int }
	locks := make([]key, 0)
	firstRead, lastLock := len(batcher.sqls), -1
	for index, sql := range batcher.sqls {
		if strings.HasPrefix(sql, "select s_quantity") && firstRead == len(batcher.sqls) {
			firstRead = index
		}
		if match := lockPattern.FindStringSubmatch(sql); match != nil {
			wID, _ := strconv.Atoi(match[1])
			iID, _ := strconv.Atoi(match[2])
			locks = append(locks, key{wID, iID})
			lastLock = index
		}
	}
	if len(locks) == 0 || lastLock >= firstRead {
		t.Fatalf("stock locks=%v lastLock=%d firstRead=%d", locks, lastLock, firstRead)
	}
	for i := 1; i < len(locks); i++ {
		if locks[i-1].wID > locks[i].wID ||
			locks[i-1].wID == locks[i].wID && locks[i-1].iID >= locks[i].iID {
			t.Fatalf("stock locks are not strictly sorted and unique: %v", locks)
		}
	}
}

func TestProjectedStockQuantityDeltasFollowOriginalLineOrderForDuplicateKey(t *testing.T) {
	deltas := projectedStockQuantityDeltas(
		[]int{7, 7},
		[]int{1, 1},
		[]int{5, 5},
		[]int{15, 15},
	)
	wantDeltas := []int{-5, 86}
	quantity := 15
	wantAfter := []int{10, 96}
	for i, delta := range deltas {
		if delta != wantDeltas[i] {
			t.Fatalf("line %d delta = %d, want %d", i+1, delta, wantDeltas[i])
		}
		quantity += delta
		if quantity != wantAfter[i] {
			t.Fatalf("line %d projected quantity = %d, want %d", i+1, quantity, wantAfter[i])
		}
	}
}

func TestNewOrderUsesEmptyStringForUndeliveredOrderLines(t *testing.T) {
	assertSentinel := func(t *testing.T, sqls []string) {
		t.Helper()
		inserts := 0
		for _, sql := range sqls {
			if !strings.HasPrefix(sql, "insert into order_line") {
				continue
			}
			inserts++
			if strings.Contains(sql, "NULL") || !strings.Contains(sql, ", '', ") {
				t.Fatalf("order_line insert does not use the empty-string delivery sentinel: %s", sql)
			}
		}
		if inserts == 0 {
			t.Fatal("NewOrder emitted no order_line inserts")
		}
	}

	t.Run("ranking", func(t *testing.T) {
		batcher := newSQLRecordingBatcher(t)
		err := rankingNewOrder(batcher, ledgerContext(newTxnLedger()), rand.New(rand.NewSource(1)))
		if err != nil && !errors.Is(err, errInvalidItem) {
			t.Fatal(err)
		}
		assertSentinel(t, batcher.sqls)
	})

	t.Run("non-ranking", func(t *testing.T) {
		backend := &sqlRecordingBackend{}
		ctx := rankingTestContext()
		if err := newOrder(backend, ctx, rand.New(rand.NewSource(1))); err != nil {
			t.Fatal(err)
		}
		assertSentinel(t, backend.sqls)
	})
}

func TestPaymentLedgerMatchesTheEmittedSQL(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		batcher := newSQLRecordingBatcher(t)
		ledger := newTxnLedger()
		if err := rankingPayment(batcher, ledgerContext(ledger), rand.New(rand.NewSource(seed))); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if ledger.values[ledgerPaymentCommits] != 1 {
			t.Fatalf("seed %d: recorded %v payment commits, want 1", seed, ledger.values[ledgerPaymentCommits])
		}
		amount := ledger.values[ledgerPaymentAmount]
		if amount <= 0 {
			t.Fatalf("seed %d: recorded a non-positive payment amount %v", seed, amount)
		}
		// The same amount has to reach w_ytd, d_ytd, c_ytd_payment and history.
		for _, pattern := range []string{
			`w_ytd = w_ytd \+ ([0-9.]+)`,
			`d_ytd = d_ytd \+ ([0-9.]+)`,
			`c_ytd_payment = c_ytd_payment \+ ([0-9.]+)`,
			`insert into history values \([^)]*, ([0-9]+\.[0-9]+), 'payment'\)`,
		} {
			if got := batcher.sum(pattern); !closeEnough(float64(float32(got)), amount) {
				t.Fatalf("seed %d: %s applied %v, ledger recorded %v", seed, pattern, got, amount)
			}
		}
		if got := batcher.sum(`c_balance = c_balance - ([0-9.]+)`); !closeEnough(float64(float32(got)), amount) {
			t.Fatalf("seed %d: c_balance was reduced by %v, ledger recorded %v", seed, got, amount)
		}
	}
}

func TestDeliveryLedgerMatchesTheEmittedSQL(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		batcher := newSQLRecordingBatcher(t)
		ledger := newTxnLedger()
		if err := rankingDelivery(batcher, ledgerContext(ledger), rand.New(rand.NewSource(seed))); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if got, want := ledger.values[ledgerDeliveryOrders], batcher.count("delete from new_orders"); !closeEnough(got, want) {
			t.Fatalf("seed %d: ledger delivered %v orders, SQL deleted %v new_orders rows", seed, got, want)
		}
		if got, want := ledger.values[ledgerDeliveryCustomers], batcher.sum(`c_delivery_cnt = c_delivery_cnt \+ (\d+)`); !closeEnough(got, want) {
			t.Fatalf("seed %d: ledger credited %v customers, SQL incremented %v", seed, got, want)
		}
		if got, want := ledger.values[ledgerDeliveryAmount], batcher.sum(`c_balance = c_balance \+ ([0-9.]+)`); !closeEnough(got, want) {
			t.Fatalf("seed %d: ledger credited %v, SQL credited %v", seed, got, want)
		}
	}
}

func TestReadOnlyTransactionsRecordNothing(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(rankingBatcher, txnContext, *rand.Rand) error
	}{
		{"order_status", rankingOrderStatus},
		{"stock_level", rankingStockLevel},
	} {
		ledger := newTxnLedger()
		if err := test.run(newSQLRecordingBatcher(t), ledgerContext(ledger), rand.New(rand.NewSource(3))); err != nil {
			t.Fatalf("%s: %v", test.name, err)
		}
		for key, value := range ledger.values {
			if value != 0 {
				t.Errorf("%s recorded %s = %v, want a read-only transaction to record nothing", test.name, key, value)
			}
		}
	}
}

func TestDeliveryLedgerRecordsNothingWhenEveryQueueIsEmpty(t *testing.T) {
	batcher := newSQLRecordingBatcher(t)
	batcher.emptyResults = true
	ledger := newTxnLedger()
	if err := rankingDelivery(batcher, ledgerContext(ledger), rand.New(rand.NewSource(1))); err != nil {
		t.Fatal(err)
	}
	for key, value := range ledger.values {
		if value != 0 {
			t.Errorf("an empty warehouse recorded %s = %v", key, value)
		}
	}
}

func TestTxnLedgerCarriesEveryDeclaredKey(t *testing.T) {
	ledger := newTxnLedger()
	snapshot := ledger.snapshot()
	if len(snapshot) != len(ledgerKeys) {
		t.Fatalf("a fresh ledger holds %d keys, want %d", len(snapshot), len(ledgerKeys))
	}
	for _, key := range ledgerKeys {
		if _, ok := snapshot[key]; !ok {
			t.Errorf("a fresh ledger is missing %s", key)
		}
	}
	other := newTxnLedger()
	other.add(ledgerPaymentAmount, 12.5)
	ledger.merge(other)
	other.reset()
	if ledger.values[ledgerPaymentAmount] != 12.5 {
		t.Errorf("merge lost the amount: %v", ledger.values[ledgerPaymentAmount])
	}
	if other.values[ledgerPaymentAmount] != 0 {
		t.Errorf("reset left %v behind", other.values[ledgerPaymentAmount])
	}
	// A nil ledger is the "not reconciling" case and must be inert.
	var absent *txnLedger
	absent.add(ledgerPaymentAmount, 1)
	absent.reset()
	absent.merge(ledger)
	if absent.snapshot() != nil {
		t.Error("a nil ledger produced a snapshot")
	}
}

// deliveryFakeBatcher answers each Delivery query with a value that identifies
// which statement it came from, so the operation indices the transaction reads
// back are pinned. The generic fakeRankingBatcher answers every query with "1",
// which cannot detect a shifted operation index.
type deliveryFakeBatcher struct {
	*fakeRankingBatcher
	pending []string
	all     []string
	oID     int
	cID     int
	amount  float64
	balance float32
	// claimLost models a row a concurrent Delivery already removed. The engine
	// answers `SELECT MIN(no_o_id) ... WHERE no_o_id = X` with exactly one row
	// holding NULL in that case — never with zero rows — so the fixture has to
	// reproduce that shape or it cannot detect a confirmation check that only
	// tests for an empty result.
	claimLost bool
}

func newDeliveryFakeBatcher(t *testing.T) *deliveryFakeBatcher {
	t.Helper()
	return &deliveryFakeBatcher{
		fakeRankingBatcher: newFakeRankingBatcher(t), oID: 2718, cID: 1234, amount: 3456.78, balance: 1,
	}
}

func (d *deliveryFakeBatcher) batchOperation(sql string) (batchOperation, error) {
	d.pending = append(d.pending, sql)
	return d.fakeRankingBatcher.batchOperation(sql)
}

func (d *deliveryFakeBatcher) execBatch(operations []batchOperation) (batchResult, error) {
	sqls := d.pending
	d.pending = nil
	d.all = append(d.all, sqls...)
	result := batchResult{executedOperations: uint16(len(operations)), failedOperation: 0xffff}
	for index, operation := range operations {
		if strings.HasPrefix(sqls[index], "update customer set c_balance = c_balance +") {
			d.balance += fakeFloatArgument(operation.args[0])
		}
		if !operation.statement.query {
			continue
		}
		sql := sqls[index]
		rows := make([][]string, 0, 1)
		switch {
		case strings.Contains(sql, "select min(no_o_id)") && strings.Contains(sql, "and no_o_id ="):
			// Aggregate over an empty group: one row, NULL. decodeRow renders a
			// present=0 cell (final.md:763) as the literal string "NULL".
			if d.claimLost {
				rows = append(rows, []string{"NULL"})
			} else {
				rows = append(rows, []string{strconv.Itoa(d.oID)})
			}
		case strings.Contains(sql, "select min(no_o_id)"):
			rows = append(rows, []string{strconv.Itoa(d.oID)})
		case strings.Contains(sql, "select o_c_id"):
			rows = append(rows, []string{strconv.Itoa(d.cID)})
		case strings.Contains(sql, "select sum(ol_amount)"):
			rows = append(rows, []string{strconv.FormatFloat(d.amount, 'f', 2, 64)})
		case strings.HasPrefix(sql, "select c_balance from customer"):
			rows = append(rows, []string{float32SQL(d.balance)})
		default:
			row := make([]string, 0, len(operation.statement.columns))
			for _, column := range operation.statement.columns {
				if column.sqlType == wireTypeChar {
					row = append(row, "x")
				} else {
					row = append(row, "1")
				}
			}
			rows = append(rows, row)
		}
		result.results = append(result.results, batchOperationResult{operationIndex: uint16(index), rows: rows})
	}
	return result, nil
}

func (d *deliveryFakeBatcher) emitted(substring string) int {
	total := 0
	for _, sql := range d.all {
		if strings.Contains(sql, substring) {
			total++
		}
	}
	return total
}

// TestDeliveryReadsTheRightOperationIndices is the regression guard for the
// zero-based batch cursor. With the cursor off by one the customer id was read
// from SUM(ol_amount) and no customer was credited at all, while the claim
// confirmation read a column that is never empty.
func TestDeliveryReadsTheRightOperationIndices(t *testing.T) {
	batcher := newDeliveryFakeBatcher(t)
	ledger := newTxnLedger()
	if err := rankingDelivery(batcher, ledgerContext(ledger), rand.New(rand.NewSource(1))); err != nil {
		t.Fatal(err)
	}
	districts := rankingTestContext().districtsPerWarehouse
	if got := batcher.emitted("delete from new_orders"); got != districts {
		t.Fatalf("Delivery removed %d new_orders rows, want one per district (%d)", got, districts)
	}
	want := fmt.Sprintf("update customer set c_balance = c_balance + %s, c_delivery_cnt = c_delivery_cnt + 1 where c_id = %d",
		float32SQL(float32(batcher.amount)), batcher.cID)
	if got := batcher.emitted(want); got != districts {
		t.Fatalf("Delivery credited the customer %d times with %q, want %d\nemitted: %v",
			got, want, districts, batcher.all)
	}
	expectedAmount := float64(float32(batcher.amount)) * float64(districts)
	if got := ledger.values[ledgerDeliveryAmount]; !closeEnough(got, expectedAmount) {
		t.Fatalf("ledger recorded %v credited, want %v", got, expectedAmount)
	}
	if got := ledger.values[ledgerDeliveryOrders]; got != float64(districts) {
		t.Fatalf("ledger recorded %v delivered orders, want %v", got, districts)
	}
	if got := ledger.values[ledgerDeliveryCustomers]; got != float64(districts) {
		t.Fatalf("ledger recorded %v credited customers, want %v", got, districts)
	}
}

// TestDeliverySkipsAnOrderItDidNotClaim proves the confirmation read really is
// the "did my claim stick" query: when the row is gone the order must not be
// delivered, which is what stops two concurrent Delivery transactions from both
// counting the same new_orders row.
//
// The subtlety this pins down: a lost claim is *one row holding NULL*, not an
// empty result. Testing the confirmation with `== ""` therefore never fired, so
// every lost claim was treated as won — the driver deleted a queue entry it had
// not claimed and credited the customer twice. Measured at 0.99% of claims with
// two clients on one home warehouse, which surfaced as `new_orders row count`
// and `orders with o_carrier_id = 0` both overshooting the ledger by the same
// amount. The engine is not at fault: after the lock hand-off the loser's
// READ COMMITTED statement snapshot does contain the winner's committed DELETE
// (see test/transaction/delivery_claim_test.cpp).
func TestDeliverySkipsAnOrderItDidNotClaim(t *testing.T) {
	batcher := newDeliveryFakeBatcher(t)
	batcher.claimLost = true
	ledger := newTxnLedger()
	if err := rankingDelivery(batcher, ledgerContext(ledger), rand.New(rand.NewSource(1))); err != nil {
		t.Fatal(err)
	}
	if got := batcher.emitted("delete from new_orders"); got != 0 {
		t.Fatalf("Delivery removed %d new_orders rows it had not claimed", got)
	}
	for key, value := range ledger.values {
		if value != 0 {
			t.Errorf("an unclaimed order recorded %s = %v", key, value)
		}
	}
}
