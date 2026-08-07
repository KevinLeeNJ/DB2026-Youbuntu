package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMaxProvenanceRecordsOnlyNewSuccessfulMaxima(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	path := filepath.Join(t.TempDir(), "max.jsonl")
	observer, err := newMaxProvenanceObserver(true, path, start)
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range []struct {
		at       time.Time
		txnType  string
		latency  float64
		attempts int
	}{
		{start.Add(10 * time.Millisecond), "new_order", 10, 1},
		{start.Add(20 * time.Millisecond), "new_order", 10, 2},
		{start.Add(30 * time.Millisecond), "payment", 5, 1},
		{start.Add(40 * time.Millisecond), "new_order", 11, 2},
	} {
		observer.recordMeasureCommit(sample.at, sample.txnType, sample.latency, sample.attempts)
	}
	if err := observer.close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var events []maxProvenanceEvent
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event maxProvenanceEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(events) != 5 {
		t.Fatalf("events = %#v, want five new-max records", events)
	}
	if events[0].Scope != "txn_type" || events[1].Scope != "global" || events[2].Scope != "txn_type" ||
		events[3].Scope != "txn_type" || events[4].Scope != "global" {
		t.Fatalf("event scopes = %#v", events)
	}
	if events[2].TxnType != "payment" || events[2].LatencyMS != 5 || events[2].RetrySuccess {
		t.Fatalf("payment event = %#v", events[2])
	}
	if events[3].TxnType != "new_order" || events[3].LatencyMS != 11 || events[3].AttemptCount != 2 || !events[3].RetrySuccess {
		t.Fatalf("retried max event = %#v", events[3])
	}
	for _, event := range events {
		if event.UnixNS == 0 || event.MonotonicMS < 0 || event.Outcome != "commit" {
			t.Fatalf("event lacks provenance fields: %#v", event)
		}
	}
}

func TestMaxProvenanceEnvironmentOptIn(t *testing.T) {
	if !maxProvenanceEnabledFromEnv(" TrUe ") || maxProvenanceEnabledFromEnv("0") || maxProvenanceEnabledFromEnv("anything") {
		t.Fatal("unexpected max provenance environment parsing")
	}
	if observer, err := newMaxProvenanceObserver(false, "", time.Now()); err != nil || observer != nil {
		t.Fatalf("disabled observer = %#v, %v", observer, err)
	}
}
