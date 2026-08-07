package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// maxProvenanceObserver writes a deliberately sparse trace of measurement
// successes that establish a new per-type or global maximum. It is opt-in and
// kept out of result.json so the ranking result schema remains stable.
type maxProvenanceObserver struct {
	mu        sync.Mutex
	start     time.Time
	file      *os.File
	encoder   *json.Encoder
	typeMax   map[string]float64
	globalMax float64
	err       error
}

type maxProvenanceEvent struct {
	UnixNS       int64   `json:"unix_ns"`
	MonotonicMS  float64 `json:"monotonic_ms"`
	Scope        string  `json:"scope"`
	TxnType      string  `json:"txn_type"`
	LatencyMS    float64 `json:"latency_ms"`
	AttemptCount int     `json:"attempt_count"`
	RetrySuccess bool    `json:"retry_success"`
	Outcome      string  `json:"outcome"`
}

func newMaxProvenanceObserver(enabled bool, path string, start time.Time) (*maxProvenanceObserver, error) {
	if !enabled {
		return nil, nil
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create max provenance sidecar: %w", err)
	}
	return &maxProvenanceObserver{
		start:   start,
		file:    file,
		encoder: json.NewEncoder(file),
		typeMax: make(map[string]float64),
	}, nil
}

func (o *maxProvenanceObserver) recordMeasureCommit(finish time.Time, txnType string, latencyMS float64, attempts int) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		return
	}
	base := maxProvenanceEvent{
		UnixNS:       finish.UnixNano(),
		MonotonicMS:  float64(finish.Sub(o.start).Microseconds()) / 1000.0,
		TxnType:      txnType,
		LatencyMS:    latencyMS,
		AttemptCount: attempts,
		RetrySuccess: attempts > 1,
		Outcome:      "commit",
	}
	if latencyMS > o.typeMax[txnType] {
		base.Scope = "txn_type"
		if err := o.encoder.Encode(base); err != nil {
			o.err = fmt.Errorf("write max provenance sidecar: %w", err)
			return
		}
		o.typeMax[txnType] = latencyMS
	}
	if latencyMS > o.globalMax {
		base.Scope = "global"
		if err := o.encoder.Encode(base); err != nil {
			o.err = fmt.Errorf("write max provenance sidecar: %w", err)
			return
		}
		o.globalMax = latencyMS
	}
}

func (o *maxProvenanceObserver) close() error {
	if o == nil || o.file == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.file == nil {
		return o.err
	}
	priorErr := o.err
	if err := o.file.Sync(); err != nil {
		_ = o.file.Close()
		o.file = nil
		return fmt.Errorf("sync max provenance sidecar: %w", err)
	}
	err := o.file.Close()
	o.file = nil
	if err != nil {
		return fmt.Errorf("close max provenance sidecar: %w", err)
	}
	return priorErr
}

func maxProvenanceEnabledFromEnv(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
