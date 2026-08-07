package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// latencyPhaseObserver is an opt-in, sampled client-side view of one logical
// transaction. It intentionally stays outside result.json: ranking metrics
// continue to use the unmodified, complete transaction population.
//
// Sampling is per connection. After every N final successful logical
// transactions, the next logical transaction is traced; if it aborts, tracing
// remains armed until a successful one is observed. Thus every retained sample
// is a final success at ordinal N, 2N, ... for that connection.
type latencyPhaseObserver struct {
	sampleEvery uint64

	mu      sync.Mutex
	samples map[string]map[string][]latencyPhaseSample
}

type latencyPhaseSample struct {
	EndToEnd    time.Duration
	Encode      time.Duration
	SocketWrite time.Duration
	ReadWait    time.Duration
	Decode      time.Duration
}

type latencyPhaseSampler struct {
	observer  *latencyPhaseObserver
	successes uint64
	active    bool
	started   time.Time
	sample    latencyPhaseSample
}

type latencyTxnBackend interface {
	beginLatencyPhaseTxn()
	finishLatencyPhaseTxn(phase string, committed, retried bool)
}

func (c *rankingClient) beginLatencyPhaseTxn() {
	if c.latency != nil {
		c.latency.begin()
	}
}

func (c *rankingClient) finishLatencyPhaseTxn(phase string, committed, retried bool) {
	if c.latency != nil {
		c.latency.finish(phase, committed, retried)
	}
}

type latencyPhaseSummary struct {
	Samples     int            `json:"samples"`
	EndToEndMS  latencySummary `json:"end_to_end_ms"`
	EncodeMS    latencySummary `json:"encode_ms"`
	SocketWrite latencySummary `json:"socket_write_ms"`
	ReadWaitMS  latencySummary `json:"read_wait_ms"`
	DecodeMS    latencySummary `json:"decode_ms"`
}

type latencyPhaseReport struct {
	SampleEvery uint64                                    `json:"sample_every_final_success_per_connection"`
	Phases      map[string]map[string]latencyPhaseSummary `json:"phases"`
}

func newLatencyPhaseObserver(enabled bool, sampleEvery uint64) *latencyPhaseObserver {
	if !enabled {
		return nil
	}
	if sampleEvery == 0 {
		sampleEvery = 1024
	}
	return &latencyPhaseObserver{sampleEvery: sampleEvery, samples: make(map[string]map[string][]latencyPhaseSample)}
}

func (o *latencyPhaseObserver) newSampler() *latencyPhaseSampler {
	if o == nil {
		return nil
	}
	return &latencyPhaseSampler{observer: o}
}

func (s *latencyPhaseSampler) begin() {
	if s == nil || s.observer == nil {
		return
	}
	s.active = s.successes%s.observer.sampleEvery == s.observer.sampleEvery-1
	if !s.active {
		return
	}
	s.started = time.Now()
	s.sample = latencyPhaseSample{}
}

func (s *latencyPhaseSampler) addEncode(elapsed time.Duration) {
	if s != nil && s.active {
		s.sample.Encode += elapsed
	}
}

func (s *latencyPhaseSampler) addSocketWrite(elapsed time.Duration) {
	if s != nil && s.active {
		s.sample.SocketWrite += elapsed
	}
}

func (s *latencyPhaseSampler) addReadWait(elapsed time.Duration) {
	if s != nil && s.active {
		s.sample.ReadWait += elapsed
	}
}

func (s *latencyPhaseSampler) addDecode(elapsed time.Duration) {
	if s != nil && s.active {
		s.sample.Decode += elapsed
	}
}

func (s *latencyPhaseSampler) finish(phase string, committed, retried bool) {
	if s == nil || s.observer == nil {
		return
	}
	if s.active && committed {
		s.sample.EndToEnd = time.Since(s.started)
		classification := "first_try_committed"
		if retried {
			classification = "retry_success"
		}
		s.observer.record(phase, classification, s.sample)
	}
	if committed {
		s.successes++
	}
	s.active = false
}

func (o *latencyPhaseObserver) record(phase, classification string, sample latencyPhaseSample) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.samples[phase] == nil {
		o.samples[phase] = make(map[string][]latencyPhaseSample)
	}
	o.samples[phase][classification] = append(o.samples[phase][classification], sample)
}

func durationSummary(samples []latencyPhaseSample, selectDuration func(latencyPhaseSample) time.Duration) latencySummary {
	if len(samples) == 0 {
		return latencySummary{}
	}
	values := make([]float64, len(samples))
	for i, sample := range samples {
		values[i] = float64(selectDuration(sample).Microseconds()) / 1000.0
	}
	sort.Float64s(values)
	return latencySummary{P50: percentile(values, 50), P95: percentile(values, 95), P99: percentile(values, 99), Max: values[len(values)-1]}
}

func summarizeLatencyPhase(samples []latencyPhaseSample) latencyPhaseSummary {
	return latencyPhaseSummary{
		Samples:     len(samples),
		EndToEndMS:  durationSummary(samples, func(sample latencyPhaseSample) time.Duration { return sample.EndToEnd }),
		EncodeMS:    durationSummary(samples, func(sample latencyPhaseSample) time.Duration { return sample.Encode }),
		SocketWrite: durationSummary(samples, func(sample latencyPhaseSample) time.Duration { return sample.SocketWrite }),
		ReadWaitMS:  durationSummary(samples, func(sample latencyPhaseSample) time.Duration { return sample.ReadWait }),
		DecodeMS:    durationSummary(samples, func(sample latencyPhaseSample) time.Duration { return sample.Decode }),
	}
}

func (o *latencyPhaseObserver) report() latencyPhaseReport {
	report := latencyPhaseReport{SampleEvery: o.sampleEvery, Phases: make(map[string]map[string]latencyPhaseSummary)}
	o.mu.Lock()
	defer o.mu.Unlock()
	for phase, classes := range o.samples {
		report.Phases[phase] = make(map[string]latencyPhaseSummary, len(classes))
		for classification, samples := range classes {
			report.Phases[phase][classification] = summarizeLatencyPhase(samples)
		}
	}
	return report
}

func (o *latencyPhaseObserver) emit(path string) error {
	if o == nil {
		return nil
	}
	payload, err := json.MarshalIndent(o.report(), "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "[latency-phase] sample_every_final_success_per_connection=%d report=%s\n", o.sampleEvery, string(payload))
	if path == "" {
		return nil
	}
	if err := os.WriteFile(path, append(payload, '\n'), 0o644); err != nil {
		return fmt.Errorf("write latency phase report: %w", err)
	}
	return nil
}

func latencyPhaseEnabledFromEnv(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func latencyPhaseSampleEveryFromEnv(value string) (uint64, error) {
	if strings.TrimSpace(value) == "" {
		return 1024, nil
	}
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("RMDB_CLIENT_LATENCY_PHASE_SAMPLE_EVERY must be a positive integer")
	}
	return parsed, nil
}
