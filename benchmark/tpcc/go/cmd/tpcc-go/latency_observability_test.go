package main

import (
	"testing"
	"time"
)

func TestSummarizeLatencyPhasePercentiles(t *testing.T) {
	samples := []latencyPhaseSample{
		{EndToEnd: time.Millisecond, Encode: 2 * time.Millisecond, SocketWrite: 3 * time.Millisecond, ReadWait: 4 * time.Millisecond, Decode: 5 * time.Millisecond},
		{EndToEnd: 100 * time.Millisecond, Encode: 20 * time.Millisecond, SocketWrite: 30 * time.Millisecond, ReadWait: 40 * time.Millisecond, Decode: 50 * time.Millisecond},
		{EndToEnd: 3 * time.Millisecond, Encode: 4 * time.Millisecond, SocketWrite: 5 * time.Millisecond, ReadWait: 6 * time.Millisecond, Decode: 7 * time.Millisecond},
	}
	summary := summarizeLatencyPhase(samples)
	if summary.Samples != 3 || summary.EndToEndMS.P50 != 3 || summary.EndToEndMS.P99 != 100 || summary.EndToEndMS.Max != 100 {
		t.Fatalf("end-to-end summary = %#v", summary)
	}
	if summary.EncodeMS.P50 != 4 || summary.SocketWrite.P50 != 5 || summary.ReadWaitMS.P50 != 6 || summary.DecodeMS.P50 != 7 {
		t.Fatalf("phase summary = %#v", summary)
	}
}

func TestLatencyPhaseSamplerSamplesFinalSuccessesAndClassifiesRetries(t *testing.T) {
	observer := newLatencyPhaseObserver(true, 2)
	sampler := observer.newSampler()

	// First success is not sampled. The next sample remains armed after an
	// aborted logical transaction, then records the second final success.
	sampler.begin()
	if sampler.active {
		t.Fatal("first final success should not be sampled")
	}
	sampler.finish("measure", true, false)
	sampler.begin()
	if !sampler.active {
		t.Fatal("second final success should be sampled")
	}
	sampler.finish("measure", false, true)
	sampler.begin()
	if !sampler.active {
		t.Fatal("aborted trace must remain armed for the next final success")
	}
	sampler.started = time.Now().Add(-7 * time.Millisecond)
	sampler.sample.Encode = time.Millisecond
	sampler.finish("measure", true, true)

	report := observer.report()
	retry := report.Phases["measure"]["retry_success"]
	if retry.Samples != 1 || retry.EncodeMS.P50 != 1 || retry.EndToEndMS.Max < 7 {
		t.Fatalf("retry report = %#v", retry)
	}
	if _, ok := report.Phases["measure"]["first_try_committed"]; ok {
		t.Fatalf("unexpected first-try sample: %#v", report)
	}
}

func TestLatencyPhaseEnvironmentOptInAndSampleValidation(t *testing.T) {
	if !latencyPhaseEnabledFromEnv(" TrUe ") || latencyPhaseEnabledFromEnv("0") || latencyPhaseEnabledFromEnv("anything") {
		t.Fatal("unexpected opt-in environment parsing")
	}
	if every, err := latencyPhaseSampleEveryFromEnv(""); err != nil || every != 1024 {
		t.Fatalf("default sample every = (%d, %v)", every, err)
	}
	if every, err := latencyPhaseSampleEveryFromEnv("8"); err != nil || every != 8 {
		t.Fatalf("explicit sample every = (%d, %v)", every, err)
	}
	if _, err := latencyPhaseSampleEveryFromEnv("0"); err == nil {
		t.Fatal("zero sample interval unexpectedly accepted")
	}
}
