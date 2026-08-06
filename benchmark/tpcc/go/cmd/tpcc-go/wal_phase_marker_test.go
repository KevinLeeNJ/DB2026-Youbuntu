package main

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestWalPhaseMarkerDeadlinesAndOrder(t *testing.T) {
	start := time.Unix(100, 0)
	warmup := 3 * time.Second
	measure := 5 * time.Second
	want := walPhaseMarkerDeadlines(start, warmup, measure, 3)
	cancel := make(chan struct{})
	var waited []time.Time
	var sent []walPhaseMarker
	nowIndex := 0
	done := runWalPhaseMarkers(start, warmup, measure, 3, cancel,
		func(deadline time.Time, _ <-chan struct{}) bool {
			waited = append(waited, deadline)
			return true
		}, func() time.Time {
			actual := want[nowIndex].planned.Add(time.Millisecond)
			nowIndex++
			return actual
		}, func(marker walPhaseMarker) (walPhaseMarker, error) {
			sent = append(sent, marker)
			return marker, nil
		})
	result := <-done
	if result.err != nil || result.canceled || result.sent != len(want) {
		t.Fatalf("result = %+v, want %d successful sends", result, len(want))
	}
	for i := range want {
		if !waited[i].Equal(want[i].planned) || sent[i].phase != want[i].phase ||
			sent[i].window != want[i].window || !sent[i].planned.Equal(want[i].planned) {
			t.Fatalf("marker %d = waited:%s sent:%+v, want %+v", i, waited[i], sent[i], want[i])
		}
	}
}

func TestWalPhaseMarkerNormalCompletionWaitsForFinalDeadline(t *testing.T) {
	start := time.Unix(200, 0)
	finalDeadline := start.Add(2 * time.Second)
	reachedFinal := make(chan struct{})
	releaseFinal := make(chan struct{})
	current := start.Add(time.Second)
	done := runWalPhaseMarkers(start, time.Second, time.Second, 1, make(chan struct{}),
		func(deadline time.Time, _ <-chan struct{}) bool {
			current = deadline.Add(time.Nanosecond)
			if deadline.Equal(finalDeadline) {
				close(reachedFinal)
				<-releaseFinal
			}
			return true
		}, func() time.Time { return current }, func(marker walPhaseMarker) (walPhaseMarker, error) {
			return marker, nil
		})
	<-reachedFinal
	select {
	case result := <-done:
		t.Fatalf("coordinator completed before final deadline send: %+v", result)
	default:
	}
	close(releaseFinal)
	if result := <-done; result.err != nil || result.canceled || result.sent != 2 {
		t.Fatalf("result = %+v, want both markers after final release", result)
	}
}

func TestWalPhaseMarkerEarlyCancel(t *testing.T) {
	cancel := make(chan struct{})
	close(cancel)
	done := runWalPhaseMarkers(time.Now(), time.Hour, time.Hour, 2, cancel,
		func(_ time.Time, cancel <-chan struct{}) bool {
			<-cancel
			return false
		}, time.Now, func(marker walPhaseMarker) (walPhaseMarker, error) { return marker, nil })
	if result := <-done; !result.canceled || result.sent != 0 || result.err != nil {
		t.Fatalf("result = %+v, want clean early cancellation", result)
	}
}

func TestWalPhaseMarkerSendFailureContinues(t *testing.T) {
	wantErr := errors.New("socket unavailable")
	start := time.Unix(300, 0)
	markers := walPhaseMarkerDeadlines(start, time.Second, time.Second, 2)
	nowIndex := 0
	attempts := 0
	done := runWalPhaseMarkers(start, time.Second, time.Second, 2, make(chan struct{}),
		func(time.Time, <-chan struct{}) bool { return true }, func() time.Time {
			actual := markers[nowIndex].planned.Add(time.Millisecond)
			nowIndex++
			return actual
		}, func(marker walPhaseMarker) (walPhaseMarker, error) {
			attempts++
			if attempts == 1 {
				return marker, wantErr
			}
			return marker, nil
		})
	result := <-done
	if attempts != 3 || result.sent != 3 || result.canceled || !errors.Is(result.err, wantErr) {
		t.Fatalf("attempts=%d result=%+v, want all sends and retained first failure", attempts, result)
	}
}

func TestWalPhaseMarkerValidation(t *testing.T) {
	valid := walPhaseMarker{
		phase: "window_end", window: 2, planned: time.Unix(400, 0), actual: time.Unix(400, 1), lateness: time.Nanosecond,
	}
	payload, err := encodeWalPhaseMarker(valid)
	if err != nil || len(payload) > walPhaseMarkerPayloadMax || !strings.HasPrefix(string(payload), "v1 phase=window_end window=2 ") {
		t.Fatalf("payload=%q err=%v", payload, err)
	}
	invalid := valid
	invalid.phase = "unknown"
	if err := validateWalPhaseMarker(invalid); err == nil {
		t.Fatal("validateWalPhaseMarker() accepted an unknown phase")
	}
	for _, socketName := range []string{"", "phase", "not-abstract", "@", "@" + strings.Repeat("x", 108)} {
		if err := validateWalPhaseMarkerSocketName(socketName); err == nil {
			t.Fatalf("validateWalPhaseMarkerSocketName(%q) accepted an invalid abstract socket name", socketName)
		}
	}
	if err := validateWalPhaseMarkerSocketName("@rmdb-wal-phase-test"); err != nil {
		t.Fatalf("validateWalPhaseMarkerSocketName() rejected a valid abstract socket name: %v", err)
	}
	if err := validateWalPhaseMarkerFlagScope("consistency", "rmdb", "official-equivalent", "@phase"); err == nil {
		t.Fatal("validateWalPhaseMarkerFlagScope() accepted a non-run command")
	}
}
