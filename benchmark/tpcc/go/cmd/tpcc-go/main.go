package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
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
)

var (
	errAbort       = errors.New("rmdb transaction aborted")
	errInvalidItem = errors.New("invalid item")
	integerRE      = regexp.MustCompile(`-?\d+`)
	floatRE        = regexp.MustCompile(`-?\d+(?:\.\d+)?`)
	oracleSeq      atomic.Uint64
	oracleAckMu    sync.Mutex
	oracleAckFile  string
	oracleIDPrefix int
)

type client struct {
	address string
	timeout time.Duration
	conn    net.Conn
	reader  *bufio.Reader
	txnType string
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
	c.reader = bufio.NewReaderSize(conn, 64*1024)
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

func (c *client) exec(sql string) (string, error) {
	if c.conn == nil {
		return "", errors.New("rmdb connection is closed")
	}
	deadline := time.Now().Add(c.timeout)
	if err := c.conn.SetDeadline(deadline); err != nil {
		return "", err
	}
	if _, err := c.conn.Write(append([]byte(sql), 0)); err != nil {
		c.close()
		return "", err
	}
	response, err := c.reader.ReadBytes(0)
	if err != nil {
		c.close()
		return "", err
	}
	text := strings.TrimRight(string(response[:len(response)-1]), "\n")
	if text == "abort" {
		return "", errAbort
	}
	if strings.HasPrefix(text, "Error:") || strings.HasPrefix(text, "Parser Error") {
		return "", errors.New(text)
	}
	return text, nil
}

func (c *client) close() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
		c.reader = nil
	}
}

func (c *client) begin() error {
	_, err := c.exec("begin;")
	return err
}

func (c *client) commit() error {
	oracleID := 0
	if oracleAckFile != "" {
		seq := oracleSeq.Add(1)
		if seq >= 1_000_000 {
			return errors.New("crash oracle transaction sequence exhausted")
		}
		oracleID = oracleIDPrefix*1_000_000 + int(seq)
		payloadHash := 0
		for _, ch := range c.txnType {
			payloadHash = (payloadHash*131 + int(ch)) & 0x7fffffff
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
	_, _ = c.exec("rollback;")
}

type profile struct {
	warehouses            int
	districtsPerWarehouse int
	customersPerDistrict  int
	itemCount             int
}

type txnContext struct {
	wID int
	dID int
	profile
}

type result struct {
	MeasureSeconds int                                  `json:"measure_seconds"`
	TPMC           float64                              `json:"tpmc"`
	AbortRate      float64                              `json:"abort_rate"`
	Counts         map[string]map[string]map[string]int `json:"counts"`
	LatencyMS      map[string]latencySummary            `json:"latency_ms"`
	Errors         map[string]map[string]map[string]int `json:"errors"`
	latencies      map[string][]float64
}

type latencySummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

type liveStats struct {
	commits         [2]atomic.Uint64
	aborts          [2]atomic.Uint64
	newOrderCommits [2]atomic.Uint64
	newOrderAborts  [2]atomic.Uint64
}

func phaseIndex(phase string) int {
	if phase == "measure" {
		return 1
	}
	return 0
}

func (s *liveStats) record(phase, txnType, outcome string) {
	index := phaseIndex(phase)
	if outcome == "commit" {
		s.commits[index].Add(1)
		if txnType == "new_order" {
			s.newOrderCommits[index].Add(1)
		}
		return
	}
	s.aborts[index].Add(1)
	if txnType == "new_order" {
		s.newOrderAborts[index].Add(1)
	}
}

func printProgress(round, rounds int, phase string, elapsed, total int, stats *liveStats) {
	index := phaseIndex(phase)
	commits := stats.commits[index].Load()
	aborts := stats.aborts[index].Load()
	newOrderCommits := stats.newOrderCommits[index].Load()
	newOrderAborts := stats.newOrderAborts[index].Load()
	totalTransactions := commits + aborts
	abortRate := 0.0
	if totalTransactions > 0 {
		abortRate = float64(aborts) / float64(totalTransactions) * 100
	}
	tpmc := 0.0
	if phase == "measure" && elapsed > 0 {
		tpmc = float64(newOrderCommits) / (float64(elapsed) / 60.0)
	}
	fmt.Printf("[round %d/%d %s %d/%ds] commits=%d aborts=%d new_order_commit=%d new_order_abort=%d tpmC=%.2f abort_rate=%.2f%%\n",
		round, rounds, phase, elapsed, total, commits, aborts, newOrderCommits, newOrderAborts, tpmc, abortRate)
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
		MeasureSeconds: measureSeconds,
		Counts:         make(map[string]map[string]map[string]int),
		LatencyMS:      make(map[string]latencySummary),
		Errors:         make(map[string]map[string]map[string]int),
		latencies:      make(map[string][]float64),
	}
}

func (r *result) record(phase, txnType, outcome string, latency float64, detail string) {
	if r.Counts[phase] == nil {
		r.Counts[phase] = make(map[string]map[string]int)
	}
	if r.Counts[phase][txnType] == nil {
		r.Counts[phase][txnType] = make(map[string]int)
	}
	r.Counts[phase][txnType][outcome]++
	if outcome == "commit" {
		r.latencies[txnType] = append(r.latencies[txnType], latency)
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
}

func (r *result) finalize() {
	committed, aborted := 0, 0
	newOrderCommitted := r.Counts["measure"]["new_order"]["commit"]
	for _, txns := range r.Counts {
		for _, outcomes := range txns {
			committed += outcomes["commit"]
			for outcome, count := range outcomes {
				if outcome != "commit" {
					aborted += count
				}
			}
		}
	}
	if r.MeasureSeconds > 0 {
		r.TPMC = float64(newOrderCommitted) / (float64(r.MeasureSeconds) / 60.0)
	}
	if committed+aborted > 0 {
		r.AbortRate = float64(aborted) / float64(committed+aborted)
	}
	for txnType, values := range r.latencies {
		if len(values) == 0 {
			continue
		}
		sort.Float64s(values)
		r.LatencyMS[txnType] = latencySummary{
			P50: percentile(values, 50), P95: percentile(values, 95), P99: percentile(values, 99), Max: values[len(values)-1],
		}
	}
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

func nowText() string { return time.Now().Format("2006-01-02 15:04:05") }

func surname(number int) string {
	sy := []string{"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"}
	return sy[number/100] + sy[(number/10)%10] + sy[number%10]
}

func newOrder(c *client, ctx txnContext, rng *rand.Rand) error {
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
		if _, err := c.exec(fmt.Sprintf("insert into order_line values (%d, %d, %d, %d, %d, %d, '%s', %d, %.2f, 'dist');", dNext, ctx.dID, ctx.wID, number, itemID, supplyWID, nowText(), qty, amount)); err != nil {
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

func payment(c *client, ctx txnContext, rng *rand.Rand) error {
	cID := rng.Intn(ctx.customersPerDistrict) + 1
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
	queries := []string{
		fmt.Sprintf("update warehouse set w_ytd = w_ytd + %.2f where w_id = %d;", amount, ctx.wID),
		fmt.Sprintf("select w_street_1, w_street_2, w_city, w_state, w_zip, w_name from warehouse where w_id = %d;", ctx.wID),
		fmt.Sprintf("update district set d_ytd = d_ytd + %.2f where d_w_id = %d and d_id = %d;", amount, ctx.wID, ctx.dID),
		fmt.Sprintf("select d_street_1, d_street_2, d_city, d_state, d_zip, d_name from district where d_w_id = %d and d_id = %d;", ctx.wID, ctx.dID),
		fmt.Sprintf("update customer set c_balance = c_balance - %.2f, c_ytd_payment = c_ytd_payment + %.2f, c_payment_cnt = c_payment_cnt + 1 where c_w_id = %d and c_d_id = %d and c_id = %d;", amount, amount, ctx.wID, ctx.dID, cID),
		fmt.Sprintf("select c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since from customer where c_w_id = %d and c_d_id = %d and c_id = %d;", ctx.wID, ctx.dID, cID),
		fmt.Sprintf("insert into history values (%d, %d, %d, %d, %d, '%s', %.2f, 'payment');", cID, ctx.dID, ctx.wID, ctx.dID, ctx.wID, nowText(), amount),
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

func orderStatus(c *client, ctx txnContext, rng *rand.Rand) error {
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
		if _, err := c.exec(fmt.Sprintf("select count(c_id) as count_c_id from customer where c_w_id = %d and c_d_id = %d and c_last = '%s';", ctx.wID, ctx.dID, last)); err != nil {
			return err
		}
		if _, err := c.exec(fmt.Sprintf("select c_balance, c_first, c_middle, c_last from customer where c_w_id = %d and c_d_id = %d and c_last = '%s' order by c_first;", ctx.wID, ctx.dID, last)); err != nil {
			return err
		}
	} else if _, err := c.exec(fmt.Sprintf("select c_balance, c_first, c_middle, c_last from customer where c_w_id = %d and c_d_id = %d and c_id = %d;", ctx.wID, ctx.dID, cID)); err != nil {
		return err
	}
	oID := rng.Intn(initialOrdersPerDist) + 1
	if _, err := c.exec(fmt.Sprintf("select o_id, o_entry_d, o_carrier_id from orders where o_w_id = %d and o_d_id = %d and o_c_id = %d and o_id = %d;", ctx.wID, ctx.dID, cID, oID)); err != nil {
		return err
	}
	if _, err := c.exec(fmt.Sprintf("select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from order_line where ol_w_id = %d and ol_d_id = %d and ol_o_id = %d;", ctx.wID, ctx.dID, oID)); err != nil {
		return err
	}
	err := c.commit()
	rollback = err != nil
	return err
}

func delivery(c *client, ctx txnContext, rng *rand.Rand) error {
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
		if _, err := c.exec(fmt.Sprintf("update district set d_next_o_id = d_next_o_id + 0 where d_w_id = %d and d_id = %d;", ctx.wID, dID)); err != nil {
			return err
		}
		oIDText, err := c.exec(fmt.Sprintf("select min(no_o_id) from new_orders where no_w_id = %d and no_d_id = %d;", ctx.wID, dID))
		if err != nil {
			return err
		}
		oID := scalarInt(oIDText, 0)
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

func stockLevel(c *client, ctx txnContext, rng *rand.Rand) error {
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
	dNext := scalarInt(dNextText, 0)
	itemsText, err := c.exec(fmt.Sprintf("select ol_i_id from order_line where ol_w_id = %d and ol_d_id = %d and ol_o_id < %d and ol_o_id >= %d;", ctx.wID, ctx.dID, dNext, max(1, dNext-20)))
	if err != nil {
		return err
	}
	for _, row := range parseRows(itemsText) {
		if len(row) == 0 {
			continue
		}
		itemID := scalarInt(row[0], 0)
		if itemID <= 0 {
			continue
		}
		if _, err := c.exec(fmt.Sprintf("select count(*) as count_stock from stock where s_w_id = %d and s_i_id = %d and s_quantity < %d;", ctx.wID, itemID, threshold)); err != nil {
			return err
		}
	}
	err = c.commit()
	rollback = err != nil
	return err
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func chooseTxn(rng *rand.Rand) string {
	n := rng.Intn(100)
	if n < 45 {
		return "new_order"
	}
	if n < 88 {
		return "payment"
	}
	if n < 92 {
		return "order_status"
	}
	if n < 96 {
		return "delivery"
	}
	return "stock_level"
}

func chooseContext(p profile, workerID int, policy string, rng *rand.Rand) txnContext {
	wID := workerID%p.warehouses + 1
	if policy == "random-per-txn" {
		wID = rng.Intn(p.warehouses) + 1
	}
	return txnContext{wID: wID, dID: rng.Intn(p.districtsPerWarehouse) + 1, profile: p}
}

func runTxn(c *client, txnType string, ctx txnContext, rng *rand.Rand) error {
	c.txnType = txnType
	switch txnType {
	case "new_order":
		return newOrder(c, ctx, rng)
	case "payment":
		return payment(c, ctx, rng)
	case "order_status":
		return orderStatus(c, ctx, rng)
	case "delivery":
		return delivery(c, ctx, rng)
	default:
		return stockLevel(c, ctx, rng)
	}
}

func runWorker(workerID int, p profile, policy, address, isolation string, timeout time.Duration, warmupEnd, measureEnd time.Time, measureSeconds int, think time.Duration, reconnectEachTxn bool, stats *liveStats, output chan<- *result) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*7919))
	local := newResult(measureSeconds)
	c, err := newClient(address, timeout, isolation)
	if err != nil {
		local.record("warmup", "connect", "backend-error", 0, err.Error())
		output <- local
		return
	}
	defer c.close()
	for {
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
		start := time.Now()
		err := runTxn(c, txnType, chooseContext(p, workerID, policy, rng), rng)
		latency := float64(time.Since(start).Microseconds()) / 1000.0
		if err == nil {
			local.record(phase, txnType, "commit", latency, "")
			stats.record(phase, txnType, "commit")
		} else if errors.Is(err, errInvalidItem) {
			local.record(phase, txnType, "invalid-item-rollback", latency, err.Error())
			stats.record(phase, txnType, "invalid-item-rollback")
		} else if errors.Is(err, errAbort) {
			c.rollback()
			local.record(phase, txnType, "server-abort", latency, err.Error())
			stats.record(phase, txnType, "server-abort")
		} else {
			c.rollback()
			local.record(phase, txnType, "backend-error", latency, err.Error())
			stats.record(phase, txnType, "backend-error")
			if err := c.reconnect(isolation); err != nil {
				local.record(phase, txnType, "backend-error", 0, err.Error())
				break
			}
		}
		if reconnectEachTxn {
			if err := c.reconnect(isolation); err != nil {
				local.record(phase, txnType, "backend-error", 0, err.Error())
				break
			}
		}
		if think > 0 {
			time.Sleep(think)
		}
	}
	output <- local
}

func inspectProfile(c *client) (profile, error) {
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
	Backend                string `json:"backend"`
	Isolation              string `json:"isolation"`
	Warehouses             int    `json:"warehouses"`
	Workers                int    `json:"workers"`
	Warmup                 int    `json:"warmup"`
	Measure                int    `json:"measure"`
	Rounds                 int    `json:"rounds"`
	ProgressInterval       int    `json:"progress_interval"`
	Seed                   int    `json:"seed"`
	WarehousePolicy        string `json:"warehouse_policy"`
	BaselineWarehouseTotal int    `json:"baseline_warehouse_total"`
	BaselineDistrictTotal  int    `json:"baseline_district_total"`
	BaselineCustomerTotal  int    `json:"baseline_customer_total"`
	BaselineItemTotal      int    `json:"baseline_item_total"`
	BaselineStockTotal     int    `json:"baseline_stock_total"`
	BaselineOrdersTotal    int    `json:"baseline_orders_total"`
}

type document struct {
	Config     config    `json:"config"`
	MedianTPMC float64   `json:"median_tpmc"`
	Rounds     []*result `json:"rounds"`
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

func main() {
	command := flag.String("command", "run", "run, data-ready, datagen, load, consistency, oracle-init, oracle-verify, wait-port, or merge-results")
	host := flag.String("host", "127.0.0.1", "RMDB host")
	port := flag.Int("port", 8765, "RMDB port")
	warehouses := flag.Int("warehouses", 1, "warehouses for data generation")
	workers := flag.Int("workers", 16, "concurrent workers")
	warmup := flag.Int("warmup", 30, "warmup seconds")
	measure := flag.Int("measure", 360, "measurement seconds")
	rounds := flag.Int("rounds", 1, "benchmark rounds")
	isolation := flag.String("isolation", "read-committed", "read-committed or snapshot-isolation")
	policy := flag.String("warehouse-policy", "terminal-home", "terminal-home or random-per-txn")
	timeout := flag.Duration("timeout", 30*time.Second, "RMDB connection timeout")
	jsonOut := flag.String("json-out", "benchmark/tpcc/result.json", "result JSON path")
	progress := flag.Int("progress-interval", 5, "seconds between live progress lines; 0 disables")
	think := flag.Duration("think", 0, "delay between transactions")
	reconnectEachTxn := flag.Bool("reconnect-each-txn", false, "reconnect after every transaction")
	dataDir := flag.String("data-dir", "benchmark/tpcc/data", "TPC-C CSV directory")
	schemaDir := flag.String("schema-dir", "benchmark/tpcc/schema", "RMDB schema directory")
	rmdbDBDir := flag.String("rmdb-db-dir", "", "RMDB database directory, used to resolve load paths")
	seed := flag.Int64("seed", 1, "data generation seed")
	overwriteData := flag.Bool("overwrite-data-dir", false, "overwrite existing CSV files")
	reuseData := flag.Bool("reuse-data-dir", false, "skip data generation when all CSV files exist")
	resultJSON := flag.String("result-json", "", "existing result JSON used by consistency")
	consistencyStage := flag.String("consistency-stage", "standalone", "consistency stage label")
	waitTimeout := flag.Duration("wait-timeout", 30*time.Second, "wait-port timeout")
	resultInputs := flag.String("result-inputs", "", "comma-separated per-round result JSON files for merge-results")
	oracleAck := flag.String("oracle-ack-file", "", "host-side fsynced ACK file for crash oracle")
	oraclePrefix := flag.Int("oracle-id-prefix", 0, "positive per-run crash oracle ID prefix")
	flag.Parse()
	if *command == "data-ready" {
		if completeCSVSet(*dataDir) {
			return
		}
		os.Exit(1)
	}
	if *command == "datagen" {
		if *reuseData && completeCSVSet(*dataDir) {
			fmt.Printf("[tpcc] datagen skipped, reusing CSV files in %s\n", *dataDir)
			return
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
	if *command == "merge-results" {
		if err := mergeResultFiles(*jsonOut, *resultInputs); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *workers < 1 || *warmup < 0 || *measure < 1 || *rounds < 1 {
		fmt.Fprintln(os.Stderr, "workers must be positive, warmup non-negative, measure and rounds positive")
		os.Exit(2)
	}
	if *policy != "terminal-home" && *policy != "random-per-txn" {
		fmt.Fprintln(os.Stderr, "unsupported warehouse policy")
		os.Exit(2)
	}
	address := net.JoinHostPort(*host, strconv.Itoa(*port))
	if *command == "oracle-init" {
		c, err := newClient(address, *timeout, *isolation)
		if err == nil {
			_, err = c.exec("create table crash_txn_log (txn_id int, txn_type char(16), payload_hash int);")
			if err == nil {
				_, err = c.exec("create index crash_txn_log(txn_id);")
			}
			c.close()
		}
		if err != nil {
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
	if *command == "load" {
		if err := loadData(address, *timeout, *isolation, *dataDir, *schemaDir, *rmdbDBDir); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *command == "consistency" {
		if err := checkConsistency(address, *timeout, *isolation, *resultJSON, *consistencyStage); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *command != "run" {
		fmt.Fprintf(os.Stderr, "unsupported command: %s\n", *command)
		os.Exit(2)
	}
	if *oracleAck != "" {
		if *oraclePrefix <= 0 || *oraclePrefix > 2000 {
			fmt.Fprintln(os.Stderr, "oracle run requires --oracle-id-prefix in 1..2000")
			os.Exit(2)
		}
		oracleAckFile = *oracleAck
		oracleIDPrefix = *oraclePrefix
		oracleSeq.Store(0)
	}
	probe, err := newClient(address, *timeout, *isolation)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_, _ = probe.exec("set output_file off")
	p, err := inspectProfile(probe)
	if err != nil {
		probe.close()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ordersText, err := probe.exec("select count(*) from orders;")
	probe.close()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	baseOrders := scalarInt(ordersText, 0)
	doc := document{Config: config{Backend: "rmdb", Isolation: *isolation, Warehouses: p.warehouses, Workers: *workers, Warmup: *warmup, Measure: *measure, Rounds: *rounds, ProgressInterval: *progress, Seed: 1, WarehousePolicy: *policy, BaselineWarehouseTotal: p.warehouses, BaselineDistrictTotal: p.warehouses * p.districtsPerWarehouse, BaselineCustomerTotal: p.warehouses * p.districtsPerWarehouse * p.customersPerDistrict, BaselineItemTotal: p.itemCount, BaselineStockTotal: p.warehouses * p.itemCount, BaselineOrdersTotal: baseOrders}}
	for round := 1; round <= *rounds; round++ {
		warmupEnd := time.Now().Add(time.Duration(*warmup) * time.Second)
		measureEnd := warmupEnd.Add(time.Duration(*measure) * time.Second)
		stats := &liveStats{}
		printProgress(round, *rounds, "warmup", 0, *warmup, stats)
		monitorStop := make(chan struct{})
		monitorDone := make(chan struct{})
		go monitorProgress(round, *rounds, *warmup, *measure, *progress, warmupEnd, measureEnd, stats, monitorStop, monitorDone)
		partials := make(chan *result, *workers)
		var wg sync.WaitGroup
		for workerID := 0; workerID < *workers; workerID++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				runWorker(id, p, *policy, address, *isolation, *timeout, warmupEnd, measureEnd, *measure, *think, *reconnectEachTxn, stats, partials)
			}(workerID)
		}
		go func() { wg.Wait(); close(partials) }()
		combined := newResult(*measure)
		for partial := range partials {
			combined.merge(partial)
		}
		close(monitorStop)
		<-monitorDone
		combined.finalize()
		doc.Rounds = append(doc.Rounds, combined)
		fmt.Printf("[round %d/%d] tpmC=%.2f abort_rate=%.2f%%\n", round, *rounds, combined.TPMC, combined.AbortRate*100)
	}
	values := make([]float64, len(doc.Rounds))
	for i, round := range doc.Rounds {
		values[i] = round.TPMC
	}
	sort.Float64s(values)
	doc.MedianTPMC = values[len(values)/2]
	encoded, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(*jsonOut, encoded, 0644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(encoded))
}
