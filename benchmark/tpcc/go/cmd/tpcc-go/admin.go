package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	customersPerDistrict        = 3000
	itemCount                   = 100000
	initialNewOrdersPerDistrict = 900
	fixedTimestamp              = "2026-06-29 00:00:00"
	datasetManifestName         = "tpcc-manifest.json"
)

var tpccTables = []string{"warehouse", "district", "customer", "history", "new_orders", "orders", "order_line", "item", "stock"}

type datasetManifest struct {
	Warehouses int                   `json:"warehouses"`
	Seed       int64                 `json:"seed"`
	Files      map[string]fileRecord `json:"files"`
}

type fileRecord struct {
	Size   int64  `json:"size"`
	Rows   int64  `json:"rows"`
	Header string `json:"header"`
}

func inspectCSVFile(path string) (fileRecord, error) {
	info, err := os.Stat(path)
	if err != nil {
		return fileRecord{}, err
	}
	if info.Size() <= 0 {
		return fileRecord{}, fmt.Errorf("dataset file %s is empty", path)
	}
	file, err := os.Open(path)
	if err != nil {
		return fileRecord{}, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return fileRecord{}, fmt.Errorf("read dataset header %s: %w", path, err)
	}
	rows := int64(0)
	for {
		if _, err := reader.Read(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fileRecord{}, fmt.Errorf("read dataset %s: %w", path, err)
		}
		rows++
	}
	return fileRecord{Size: info.Size(), Rows: rows, Header: strings.Join(header, ",")}, nil
}

func expectedCSVRows(warehouses int, table string) int64 {
	w := int64(warehouses)
	districts := w * districtsPerWarehouse
	customers := districts * customersPerDistrict
	switch table {
	case "warehouse":
		return w
	case "district":
		return districts
	case "customer", "history", "orders":
		return customers
	case "item":
		return itemCount
	case "stock":
		return w * itemCount
	case "new_orders":
		return districts * initialNewOrdersPerDistrict
	case "order_line":
		rows := int64(0)
		for wID := 1; wID <= warehouses; wID++ {
			for dID := 1; dID <= districtsPerWarehouse; dID++ {
				for oID := 1; oID <= initialOrdersPerDist; oID++ {
					rows += int64(initialOrderLineCount(wID, dID, oID))
				}
			}
		}
		return rows
	default:
		return 0
	}
}

func completeCSVSet(dataDir string) bool {
	for _, table := range tpccTables {
		info, err := os.Stat(filepath.Join(dataDir, table+".csv"))
		if err != nil || info.IsDir() {
			return false
		}
	}
	return true
}

func writeDatasetManifest(dataDir string, warehouses int, seed int64) error {
	files := make(map[string]fileRecord, len(tpccTables))
	for _, table := range tpccTables {
		path := filepath.Join(dataDir, table+".csv")
		record, err := inspectCSVFile(path)
		if err != nil {
			return fmt.Errorf("inspect generated dataset file %s: %w", path, err)
		}
		if record.Rows != expectedCSVRows(warehouses, table) {
			return fmt.Errorf("generated dataset file %s has %d rows, want %d", path, record.Rows, expectedCSVRows(warehouses, table))
		}
		files[table] = record
	}
	data, err := json.MarshalIndent(datasetManifest{Warehouses: warehouses, Seed: seed, Files: files}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dataDir, datasetManifestName), append(data, '\n'), 0644)
}

// readDatasetManifest returns the row counts recorded when the CSV set was
// generated. The load phase compares COUNT(*) against these numbers, so the
// dynamic order_line total is the one produced by this run, exactly like the
// official loader (final.md:241,277).
func readDatasetManifest(dataDir string) (datasetManifest, error) {
	path := filepath.Join(dataDir, datasetManifestName)
	data, err := os.ReadFile(path)
	if err != nil {
		return datasetManifest{}, fmt.Errorf("read dataset manifest %s: %w", path, err)
	}
	var manifest datasetManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return datasetManifest{}, fmt.Errorf("parse dataset manifest %s: %w", path, err)
	}
	if manifest.Warehouses < 1 {
		return datasetManifest{}, fmt.Errorf("dataset manifest %s has no warehouse count", path)
	}
	for _, table := range tpccTables {
		record, ok := manifest.Files[table]
		if !ok || record.Rows < 1 {
			return datasetManifest{}, fmt.Errorf("dataset manifest %s is missing the row count for %s", path, table)
		}
	}
	return manifest, nil
}

func validateDataset(dataDir string, warehouses int, seed int64) error {
	if !completeCSVSet(dataDir) {
		return fmt.Errorf("TPC-C CSV set is incomplete in %s", dataDir)
	}
	path := filepath.Join(dataDir, datasetManifestName)
	manifest, err := readDatasetManifest(dataDir)
	if err != nil {
		return err
	}
	if manifest.Warehouses != warehouses {
		return fmt.Errorf("dataset warehouses mismatch: manifest has %d, requested %d", manifest.Warehouses, warehouses)
	}
	if manifest.Seed != seed {
		return fmt.Errorf("dataset seed mismatch: manifest has %d, requested %d", manifest.Seed, seed)
	}
	if len(manifest.Files) != len(tpccTables) {
		return fmt.Errorf("dataset manifest %s has incomplete file records", path)
	}
	for _, table := range tpccTables {
		record, ok := manifest.Files[table]
		if !ok || record.Size <= 0 {
			return fmt.Errorf("dataset manifest %s is missing file record for %s", path, table)
		}
		info, err := inspectCSVFile(filepath.Join(dataDir, table+".csv"))
		if err != nil {
			return fmt.Errorf("inspect dataset file %s: %w", table, err)
		}
		if info.Size != record.Size || info.Rows != record.Rows || info.Header != record.Header {
			return fmt.Errorf("dataset file %s changed since manifest", table)
		}
		if info.Rows != expectedCSVRows(warehouses, table) {
			return fmt.Errorf("dataset file %s has %d rows, want %d", table, info.Rows, expectedCSVRows(warehouses, table))
		}
	}
	return nil

}

func randomString(rng *rand.Rand, length int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	bytes := make([]byte, length)
	for i := range bytes {
		bytes[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(bytes)
}

func randomDigits(rng *rand.Rand, length int) string {
	bytes := make([]byte, length)
	for i := range bytes {
		bytes[i] = byte('0' + rng.Intn(10))
	}
	return string(bytes)
}

func decimal(value float64, digits int) string {
	value = math.Round(value*math.Pow10(digits)) / math.Pow10(digits)
	return fmt.Sprintf("%.*f", digits, value)
}

func initialOrderLineCount(wID, dID, oID int) int { return 5 + (oID+dID+wID)%11 }

func writeCSV(path string, header []string, write func(*csv.Writer) error) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	if err := writer.Write(header); err != nil {
		return err
	}
	if err := write(writer); err != nil {
		return err
	}
	writer.Flush()
	return writer.Error()
}

func generateData(warehouses int, dataDir string, seed int64, overwrite bool) error {
	if warehouses < 1 {
		return errors.New("warehouses must be positive")
	}
	if matches, _ := filepath.Glob(filepath.Join(dataDir, "*.csv")); len(matches) > 0 && !overwrite {
		return fmt.Errorf("refusing to overwrite existing CSV files in %s", dataDir)
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(dataDir, datasetManifestName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	rng := rand.New(rand.NewSource(seed))
	if err := writeCSV(filepath.Join(dataDir, "warehouse.csv"), []string{"w_id", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip", "w_tax", "w_ytd"}, func(w *csv.Writer) error {
		for wID := 1; wID <= warehouses; wID++ {
			if err := w.Write([]string{fmt.Sprint(wID), randomString(rng, 10), randomString(rng, 20), randomString(rng, 20), randomString(rng, 20), randomString(rng, 2), randomString(rng, 9), decimal(rng.Float64()*0.2, 4), "300000.0"}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(dataDir, "district.csv"), []string{"d_id", "d_w_id", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip", "d_tax", "d_ytd", "d_next_o_id"}, func(w *csv.Writer) error {
		for wID := 1; wID <= warehouses; wID++ {
			for dID := 1; dID <= districtsPerWarehouse; dID++ {
				if err := w.Write([]string{fmt.Sprint(dID), fmt.Sprint(wID), randomString(rng, 10), randomString(rng, 20), randomString(rng, 20), randomString(rng, 20), randomString(rng, 2), randomString(rng, 9), decimal(rng.Float64()*0.2, 4), "30000.0", fmt.Sprint(initialOrdersPerDist + 1)}); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(dataDir, "customer.csv"), []string{"c_id", "c_d_id", "c_w_id", "c_first", "c_middle", "c_last", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip", "c_phone", "c_since", "c_credit", "c_credit_lim", "c_discount", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt", "c_data"}, func(w *csv.Writer) error {
		for wID := 1; wID <= warehouses; wID++ {
			for dID := 1; dID <= districtsPerWarehouse; dID++ {
				for cID := 1; cID <= customersPerDistrict; cID++ {
					last := ""
					if cID <= 1000 {
						last = surname(cID - 1)
					} else {
						last = surname(rng.Intn(1000))
					}
					if err := w.Write([]string{fmt.Sprint(cID), fmt.Sprint(dID), fmt.Sprint(wID), randomString(rng, 16), "OE", last, randomString(rng, 20), randomString(rng, 20), randomString(rng, 20), randomString(rng, 2), randomString(rng, 9), randomDigits(rng, 16), fixedTimestamp, map[bool]string{true: "BC", false: "GC"}[cID%10 == 0], "50000.0", decimal(rng.Float64()*0.5, 4), "-10.0", "10.0", "1", "0", randomString(rng, 40)}); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(dataDir, "history.csv"), []string{"h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data"}, func(w *csv.Writer) error {
		for wID := 1; wID <= warehouses; wID++ {
			for dID := 1; dID <= districtsPerWarehouse; dID++ {
				for cID := 1; cID <= customersPerDistrict; cID++ {
					if err := w.Write([]string{fmt.Sprint(cID), fmt.Sprint(dID), fmt.Sprint(wID), fmt.Sprint(dID), fmt.Sprint(wID), fixedTimestamp, "10.0", randomString(rng, 24)}); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(dataDir, "item.csv"), []string{"i_id", "i_im_id", "i_name", "i_price", "i_data"}, func(w *csv.Writer) error {
		for iID := 1; iID <= itemCount; iID++ {
			data := randomString(rng, 44)
			if iID%10 == 0 {
				data = "ORIGINAL" + data[:36]
			}
			if err := w.Write([]string{fmt.Sprint(iID), fmt.Sprint(rng.Intn(10000) + 1), randomString(rng, 24), decimal(1+rng.Float64()*99, 2), data}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(dataDir, "stock.csv"), []string{"s_i_id", "s_w_id", "s_quantity", "s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05", "s_dist_06", "s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10", "s_ytd", "s_order_cnt", "s_remote_cnt", "s_data"}, func(w *csv.Writer) error {
		for wID := 1; wID <= warehouses; wID++ {
			for iID := 1; iID <= itemCount; iID++ {
				data := randomString(rng, 44)
				if iID%10 == 0 {
					data = "ORIGINAL" + data[:36]
				}
				row := []string{fmt.Sprint(iID), fmt.Sprint(wID), fmt.Sprint(rng.Intn(91) + 10)}
				for i := 0; i < 10; i++ {
					row = append(row, randomString(rng, 24))
				}
				row = append(row, "0", "0", "0", data)
				if err := w.Write(row); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(dataDir, "orders.csv"), []string{"o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_carrier_id", "o_ol_cnt", "o_all_local"}, func(w *csv.Writer) error {
		customers := make([]int, customersPerDistrict)
		for wID := 1; wID <= warehouses; wID++ {
			for dID := 1; dID <= districtsPerWarehouse; dID++ {
				for i := range customers {
					customers[i] = i + 1
				}
				rng.Shuffle(len(customers), func(i, j int) { customers[i], customers[j] = customers[j], customers[i] })
				for oID, cID := range customers {
					carrier := 0
					if oID < 2100 {
						carrier = rng.Intn(10) + 1
					}
					if err := w.Write([]string{fmt.Sprint(oID + 1), fmt.Sprint(dID), fmt.Sprint(wID), fmt.Sprint(cID), fixedTimestamp, fmt.Sprint(carrier), fmt.Sprint(initialOrderLineCount(wID, dID, oID+1)), "1"}); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(dataDir, "new_orders.csv"), []string{"no_o_id", "no_d_id", "no_w_id"}, func(w *csv.Writer) error {
		start := initialOrdersPerDist - initialNewOrdersPerDistrict + 1
		for wID := 1; wID <= warehouses; wID++ {
			for dID := 1; dID <= districtsPerWarehouse; dID++ {
				for oID := start; oID <= initialOrdersPerDist; oID++ {
					if err := w.Write([]string{fmt.Sprint(oID), fmt.Sprint(dID), fmt.Sprint(wID)}); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(dataDir, "order_line.csv"), []string{"ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount", "ol_dist_info"}, func(w *csv.Writer) error {
		for wID := 1; wID <= warehouses; wID++ {
			for dID := 1; dID <= districtsPerWarehouse; dID++ {
				for oID := 1; oID <= initialOrdersPerDist; oID++ {
					for number := 1; number <= initialOrderLineCount(wID, dID, oID); number++ {
						deliveryDate, amount := fixedTimestamp, "0.0"
						if oID > 2100 {
							deliveryDate = ""
							amount = decimal(0.01+rng.Float64()*999.98, 2)
						}
						if err := w.Write([]string{fmt.Sprint(oID), fmt.Sprint(dID), fmt.Sprint(wID), fmt.Sprint(number), fmt.Sprint((oID*17+number)%itemCount + 1), fmt.Sprint(wID), deliveryDate, "5", amount, randomString(rng, 24)}); err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return writeDatasetManifest(dataDir, warehouses, seed)
}

func executeSQLFile(c *client, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for _, statement := range strings.Split(string(data), ";") {
		if sql := strings.TrimSpace(statement); sql != "" {
			if _, err := c.exec(sql + ";"); err != nil {
				return err
			}
		}
	}
	return nil
}

func loadPath(dataDir, dbDir, table string) (string, error) {
	path, err := filepath.Abs(filepath.Join(dataDir, table+".csv"))
	if err != nil {
		return "", err
	}
	if dbDir == "" {
		return filepath.ToSlash(path), nil
	}
	absDBDir, err := filepath.Abs(dbDir)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(absDBDir, path)
	if err != nil {
		return "", err
	}
	rel = filepath.ToSlash(rel)
	if !strings.HasPrefix(rel, ".") {
		rel = "./" + rel
	}
	return rel, nil
}

// expectedUndeliveredOrderLines counts the generated order_line rows that carry
// no delivery time. generateData leaves the delivery time unset for the last
// initialNewOrdersPerDistrict orders of every district, which are exactly the
// orders still queued in new_orders.
func expectedUndeliveredOrderLines(warehouses int) int64 {
	firstUndelivered := initialOrdersPerDist - initialNewOrdersPerDistrict + 1
	rows := int64(0)
	for wID := 1; wID <= warehouses; wID++ {
		for dID := 1; dID <= districtsPerWarehouse; dID++ {
			for oID := firstUndelivered; oID <= initialOrdersPerDist; oID++ {
				rows += int64(initialOrderLineCount(wID, dID, oID))
			}
		}
	}
	return rows
}

// verifyLoadIntegrity implements the publicly defined post-load integrity
// semantics of final.md:285-292. Every rule is compared against the exact
// numbers recorded for the dataset generated by this run, so a mismatch is a
// real `LOAD integrity mismatch` and never a tolerance question.
func verifyLoadIntegrity(c sqlExecutor, manifest datasetManifest) error {
	orderLineRows := manifest.Files["order_line"].Rows
	newOrderRows := manifest.Files["new_orders"].Rows
	checks := []struct {
		label string
		sql   string
		want  int64
	}{
		{"orders SUM(o_ol_cnt) equals the order_line row count",
			"select sum(o_ol_cnt) from orders;", orderLineRows},
		{"stock.s_quantity below the 10..100 range",
			"select count(*) from stock where s_quantity < 10;", 0},
		{"stock.s_quantity above the 10..100 range",
			"select count(*) from stock where s_quantity > 100;", 0},
		{"orders.o_ol_cnt below the 5..15 range",
			"select count(*) from orders where o_ol_cnt < 5;", 0},
		{"orders.o_ol_cnt above the 5..15 range",
			"select count(*) from orders where o_ol_cnt > 15;", 0},
		{"orders with o_carrier_id = 0 equals the new_orders row count",
			"select count(*) from orders where o_carrier_id = 0;", newOrderRows},
		{"order_line rows with an empty delivery time equals the generated count",
			"select count(*) from order_line where ol_delivery_d is null;", expectedUndeliveredOrderLines(manifest.Warehouses)},
		{"stock.s_ytd is initially zero",
			"select count(*) from stock where s_ytd <> 0.0;", 0},
		{"stock.s_order_cnt is initially zero",
			"select count(*) from stock where s_order_cnt <> 0;", 0},
		{"stock.s_remote_cnt is initially zero",
			"select count(*) from stock where s_remote_cnt <> 0;", 0},
	}
	failures := make([]string, 0)
	for _, check := range checks {
		text, err := c.exec(check.sql)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", check.label, err))
			continue
		}
		got, err := scalarIntStrict(text)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", check.label, err))
			continue
		}
		if int64(got) != check.want {
			failures = append(failures, fmt.Sprintf("%s: got %d, want %d", check.label, got, check.want))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("LOAD integrity mismatch (%d of %d rule(s))\n%s",
			len(failures), len(checks), strings.Join(failures, "\n"))
	}
	fmt.Printf("[load] integrity validation passed %d rules\n", len(checks))
	return nil
}

func loadData(address string, timeout time.Duration, isolation, dataDir, schemaDir, dbDir string) error {
	manifest, err := readDatasetManifest(dataDir)
	if err != nil {
		return err
	}
	c, err := newClient(address, timeout, isolation)
	if err != nil {
		return err
	}
	defer c.close()
	// The official SQL budget starts once the connection is established
	// (final.md:245); the harness enforces it around this command.
	start := time.Now()
	phase := func(label string, since time.Time) {
		fmt.Printf("[load] %s took %s (%s since connect)\n", label,
			time.Since(since).Round(time.Millisecond), time.Since(start).Round(time.Millisecond))
	}
	// Fixed official order: CREATE TABLE x9 -> CREATE INDEX x10 -> LOAD x9 ->
	// COUNT(*) x9 -> integrity validation (final.md:251-261). Creating the
	// indexes before loading is deliberate: that is the path the official run
	// exercises, so index maintenance during bulk load must be measured here.
	if err := executeSQLFile(c, filepath.Join(schemaDir, "rmdb_schema.sql")); err != nil {
		return fmt.Errorf("table creation: %w", err)
	}
	phase("table creation", start)
	indexStart := time.Now()
	if err := executeSQLFile(c, filepath.Join(schemaDir, "rmdb_indexes.sql")); err != nil {
		return fmt.Errorf("index creation: %w", err)
	}
	phase("index creation", indexStart)
	loadStart := time.Now()
	for ordinal, table := range tpccTables {
		path, err := loadPath(dataDir, dbDir, table)
		if err != nil {
			return err
		}
		if _, err := c.exec(fmt.Sprintf("load %s into %s;", path, table)); err != nil {
			return fmt.Errorf("CSV loading (table ordinal %d): %w", ordinal+1, err)
		}
	}
	phase("CSV loading", loadStart)
	countStart := time.Now()
	for ordinal, table := range tpccTables {
		text, err := c.exec(fmt.Sprintf("select count(*) from %s;", table))
		if err != nil {
			return fmt.Errorf("row-count validation (table ordinal %d): %w", ordinal+1, err)
		}
		rows, err := scalarIntStrict(text)
		if err != nil {
			return fmt.Errorf("row-count validation (table ordinal %d): %w", ordinal+1, err)
		}
		if want := manifest.Files[table].Rows; int64(rows) != want {
			return fmt.Errorf("LOAD row-count mismatch (table ordinal %d): counted %d, generated %d", ordinal+1, rows, want)
		}
	}
	phase("row-count validation", countStart)
	integrityStart := time.Now()
	if err := verifyLoadIntegrity(c, manifest); err != nil {
		return err
	}
	phase("integrity validation", integrityStart)
	return nil
}

func waitForPort(address string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("server did not become ready on %s within %s: %v", address, timeout, lastErr)
}

func waitForReady(address string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		client, err := newClient(address, 500*time.Millisecond, "read-committed")
		if err == nil {
			_, err = client.exec("show tables;")
			client.close()
			if err == nil {
				return nil
			}
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("server did not become SQL-ready on %s within %s: %v", address, timeout, lastErr)
}

func checkConsistency(address string, timeout time.Duration, isolation, resultPath, stage string) error {
	if resultPath == "" {
		return errors.New("--result-json is required for consistency")
	}
	data, err := os.ReadFile(resultPath)
	if err != nil {
		return err
	}
	var prior document
	if err := json.Unmarshal(data, &prior); err != nil {
		return err
	}
	if err := validateResultDocument(prior); err != nil {
		return fmt.Errorf("invalid benchmark result: %w", err)
	}
	c, err := newClient(address, timeout, isolation)
	if err != nil {
		return err
	}
	defer c.close()
	if strings.HasPrefix(stage, "online-") {
		return checkOnlineConsistency(c, prior, stage)
	}
	failures := make([]string, 0)
	queryInt := func(sql string, fallback int) int {
		text, err := c.exec(sql)
		if err != nil {
			failures = append(failures, err.Error())
			return fallback
		}
		return scalarInt(text, fallback)
	}
	warehouseTotal, districtTotal := prior.Config.BaselineWarehouseTotal, prior.Config.BaselineDistrictTotal
	if actual := queryInt("select count(*) from warehouse;", -1); actual != warehouseTotal {
		failures = append(failures, fmt.Sprintf("warehouse count: expected %d, got %d", warehouseTotal, actual))
	}
	if actual := queryInt("select count(*) from district;", -1); actual != districtTotal {
		failures = append(failures, fmt.Sprintf("district count: expected %d, got %d", districtTotal, actual))
	}
	staticCounts := []struct {
		table    string
		expected int
		label    string
	}{
		{"customer", prior.Config.BaselineCustomerTotal, "customer"},
		{"item", prior.Config.BaselineItemTotal, "item"},
		{"stock", prior.Config.BaselineStockTotal, "stock"},
	}
	for _, check := range staticCounts {
		if check.expected <= 0 {
			continue // Backward-compatible with result files written before these fields existed.
		}
		if actual := queryInt(fmt.Sprintf("select count(*) from %s;", check.table), -1); actual != check.expected {
			failures = append(failures, fmt.Sprintf("%s count: expected %d, got %d", check.label, check.expected, actual))
		}
	}
	for wID := 1; wID <= warehouseTotal; wID++ {
		warehouseText, err := c.exec(fmt.Sprintf("select w_ytd from warehouse where w_id = %d;", wID))
		if err != nil {
			failures = append(failures, err.Error())
			continue
		}
		districtText, err := c.exec(fmt.Sprintf("select sum(d_ytd) from district where d_w_id = %d;", wID))
		if err != nil {
			failures = append(failures, err.Error())
			continue
		}
		warehouseYTD, districtYTD := scalarFloat(warehouseText, 0), scalarFloat(districtText, 0)
		// RMDB stores FLOAT values as binary32. Warehouse and district totals
		// are accumulated independently, so their rounded sums need a scale-
		// aware tolerance after a high-volume run.
		tolerance := math.Max(0.05, 2e-6*math.Max(math.Abs(warehouseYTD), math.Abs(districtYTD)))
		if math.Abs(warehouseYTD-districtYTD) > tolerance {
			failures = append(failures, fmt.Sprintf("warehouse/district YTD mismatch w=%d: warehouse=%.2f, districts=%.2f", wID, warehouseYTD, districtYTD))
		}
	}
	for wID := 1; wID <= warehouseTotal; wID++ {
		for dID := 1; dID <= districtsPerWarehouse; dID++ {
			checkDistrict(c, wID, dID, &failures)
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("[%s] consistency validation failed (%d rule(s))\n%s", stage, len(failures), strings.Join(failures, "\n"))
	}
	fmt.Printf("consistency ok: stage=%s\n", stage)
	return nil
}

func checkOnlineConsistency(c *client, prior document, stage string) error {
	failures := make([]string, 0)
	queryInt := func(sql string, fallback int) int {
		text, err := c.exec(sql)
		if err != nil {
			failures = append(failures, err.Error())
			return fallback
		}
		return scalarInt(text, fallback)
	}
	queryFloat := func(sql string, fallback float64) float64 {
		text, err := c.exec(sql)
		if err != nil {
			failures = append(failures, err.Error())
			return fallback
		}
		return scalarFloat(text, fallback)
	}

	warehouseTotal := prior.Config.BaselineWarehouseTotal
	districtTotal := prior.Config.BaselineDistrictTotal
	if actual := queryInt("select count(*) from warehouse;", -1); actual != warehouseTotal {
		failures = append(failures, fmt.Sprintf("warehouse count: expected %d, got %d", warehouseTotal, actual))
	}
	if actual := queryInt("select count(*) from district;", -1); actual != districtTotal {
		failures = append(failures, fmt.Sprintf("district count: expected %d, got %d", districtTotal, actual))
	}
	if actual := queryInt("select count(w_id) from warehouse where w_id = 1;", -1); actual != 1 {
		failures = append(failures, fmt.Sprintf("warehouse key lookup: expected 1, got %d", actual))
	}
	if actual := queryInt("select count(d_id) from district where d_w_id = 1 and d_id = 1;", -1); actual != 1 {
		failures = append(failures, fmt.Sprintf("district key lookup: expected 1, got %d", actual))
	}
	dNext := queryInt("select d_next_o_id from district where d_w_id = 1 and d_id = 1;", -1)
	if dNext < initialOrdersPerDist+1 {
		failures = append(failures, fmt.Sprintf("district next order id is out of range: %d", dNext))
	}
	maxOrder := queryInt("select max(o_id) from orders where o_w_id = 1 and o_d_id = 1;", -1)
	if maxOrder != dNext-1 {
		failures = append(failures, fmt.Sprintf("district order id mismatch: d_next=%d max_order=%d", dNext, maxOrder))
	}

	warehouseYTD := queryFloat("select w_ytd from warehouse where w_id = 1;", -1)
	districtYTD := queryFloat("select sum(d_ytd) from district where d_w_id = 1;", -1)
	if warehouseYTD < 300000 {
		failures = append(failures, fmt.Sprintf("warehouse YTD is out of range: %.2f", warehouseYTD))
	}
	if districtYTD < 300000 {
		failures = append(failures, fmt.Sprintf("district YTD is out of range: %.2f", districtYTD))
	}
	tolerance := math.Max(0.05, 2e-6*math.Max(math.Abs(warehouseYTD), math.Abs(districtYTD)))
	if math.Abs(warehouseYTD-districtYTD) > tolerance {
		failures = append(failures,
			fmt.Sprintf("warehouse/district YTD mismatch: warehouse=%.2f districts=%.2f", warehouseYTD, districtYTD))
	}
	if len(failures) > 0 {
		return fmt.Errorf("[%s] online consistency validation failed (%d rule(s))\n%s",
			stage, len(failures), strings.Join(failures, "\n"))
	}
	fmt.Printf("consistency ok: stage=%s\n", stage)
	return nil
}

func checkDistrict(c *client, wID, dID int, failures *[]string) {
	query := func(sql string, fallback int) int {
		text, err := c.exec(sql)
		if err != nil {
			*failures = append(*failures, err.Error())
			return fallback
		}
		return scalarInt(text, fallback)
	}
	dNext := query(fmt.Sprintf("select d_next_o_id from district where d_w_id = %d and d_id = %d;", wID, dID), -1)
	maxOrder := query(fmt.Sprintf("select max(o_id) from orders where o_w_id = %d and o_d_id = %d;", wID, dID), -1)
	maxNew := query(fmt.Sprintf("select max(no_o_id) from new_orders where no_w_id = %d and no_d_id = %d;", wID, dID), -1)
	if dNext-1 != maxOrder {
		*failures = append(*failures, fmt.Sprintf("district order id mismatch w=%d d=%d: d_next=%d, max_order=%d", wID, dID, dNext, maxOrder))
	}
	countOrder := query(fmt.Sprintf("select count(o_id) from orders where o_w_id = %d and o_d_id = %d;", wID, dID), 0)
	minOrder := query(fmt.Sprintf("select min(o_id) from orders where o_w_id = %d and o_d_id = %d;", wID, dID), -1)
	if countOrder > 0 && countOrder != maxOrder-minOrder+1 {
		*failures = append(*failures, fmt.Sprintf("orders gap w=%d d=%d: count=%d, min=%d, max=%d", wID, dID, countOrder, minOrder, maxOrder))
	}
	countNew := query(fmt.Sprintf("select count(no_o_id) from new_orders where no_w_id = %d and no_d_id = %d;", wID, dID), 0)
	minNew := query(fmt.Sprintf("select min(no_o_id) from new_orders where no_w_id = %d and no_d_id = %d;", wID, dID), -1)
	if countNew > 0 && countNew != maxNew-minNew+1 {
		*failures = append(*failures, fmt.Sprintf("new_orders gap w=%d d=%d: count=%d, min=%d, max=%d", wID, dID, countNew, minNew, maxNew))
	}
	if countNew > 0 && maxNew != maxOrder {
		*failures = append(*failures, fmt.Sprintf("new_orders tail mismatch w=%d d=%d: max_new_order=%d, max_order=%d", wID, dID, maxNew, maxOrder))
	}
	carrierZero := query(fmt.Sprintf("select count(o_id) from orders where o_w_id = %d and o_d_id = %d and o_carrier_id = 0;", wID, dID), 0)
	if carrierZero != countNew {
		*failures = append(*failures, fmt.Sprintf("pending order mismatch w=%d d=%d: carrier_zero=%d, new_orders=%d", wID, dID, carrierZero, countNew))
	}
	sumLines := query(fmt.Sprintf("select sum(o_ol_cnt) from orders where o_w_id = %d and o_d_id = %d;", wID, dID), 0)
	countLines := query(fmt.Sprintf("select count(ol_o_id) from order_line where ol_w_id = %d and ol_d_id = %d;", wID, dID), 0)
	if sumLines != countLines {
		*failures = append(*failures, fmt.Sprintf("order_line count mismatch w=%d d=%d: sum_o_ol_cnt=%d, count_ol_o_id=%d", wID, dID, sumLines, countLines))
	}
}

func mergeComparableConfig(value config) config {
	value.Rounds = 0
	value.ProgressInterval = 0
	return value
}

func mergeResultFiles(outputPath, inputs string) error {
	paths := strings.Split(inputs, ",")
	if len(paths) == 0 || strings.TrimSpace(paths[0]) == "" {
		return errors.New("--result-inputs is required")
	}
	documents := make([]document, 0, len(paths))
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			return errors.New("--result-inputs contains an empty path")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var doc document
		if err := json.Unmarshal(data, &doc); err != nil {
			return err
		}
		if len(doc.Rounds) != 1 {
			return fmt.Errorf("%s has %d rounds, want exactly 1", path, len(doc.Rounds))
		}
		if err := validateResultWindow(doc.Rounds[0], doc.Config.Measure, doc.Config.Mode, 1); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if len(documents) > 0 && mergeComparableConfig(documents[0].Config) != mergeComparableConfig(doc.Config) {
			return fmt.Errorf("%s has a materially different benchmark config", path)
		}
		documents = append(documents, doc)
	}
	merged := documents[0]
	merged.Config.Rounds = len(documents)
	merged.Rounds = make([]*result, 0, len(documents))
	values := make([]float64, 0, len(documents))
	for _, doc := range documents {
		merged.Rounds = append(merged.Rounds, doc.Rounds[0])
		values = append(values, doc.Rounds[0].TPMC)
	}
	merged.MedianTPMC = median(values)
	encoded, err := publishResultDocument(outputPath, merged)
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}
