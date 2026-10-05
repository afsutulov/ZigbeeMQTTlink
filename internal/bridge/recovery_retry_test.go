package bridge

import (
	"context"
	"testing"
	"time"
	"zigbeemqttlink/internal/config"
)

func TestRetryDoesNotCarryFramesAcrossAddressEpoch(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	a := putPlug(t, b, 5)
	replacement := putPlug(t, b, 6)
	b.safeEvent(context.Background(), reportFrame(0x4321, 0x66))
	b.processJob(context.Background(), <-b.queue)
	b.handleAddressResult(context.Background(), <-b.recoveryResults)
	if err := b.store.Remove(a.IEEE); err != nil {
		t.Fatal(err)
	}
	b.addressLookups[0x4321].retryAfter = time.Now().Add(-time.Second)
	b.radio = resolvingRadio{fakeRadio{done: make(chan struct{})}, replacement.IEEE}
	b.safeEvent(context.Background(), reportFrame(0x4321, 0x67))
	b.processJob(context.Background(), <-b.queue)
	b.handleAddressResult(context.Background(), <-b.recoveryResults)
	if b.diagnostics.recoveryReplayed.Load() != 1 {
		t.Fatalf("old report attributed to replacement: replayed=%d", b.diagnostics.recoveryReplayed.Load())
	}
	p := <-b.reports
	if string(p.payload) == "" {
		t.Fatal("missing new report")
	}
	select {
	case <-b.reports:
		t.Fatal("old sender's report queued for replacement")
	default:
	}
}

func TestRetryBackoffLeavesTimeForBufferedReport(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	b.safeEvent(context.Background(), reportFrame(0x4321, 0x66))
	j := <-b.queue
	l := b.addressLookups[j.resolve]
	l.started = time.Now().Add(-10 * time.Second)
	j.received = l.started
	b.processJob(context.Background(), j)
	b.handleAddressResult(context.Background(), <-b.recoveryResults)
	if l.retryAfter.After(l.started.Add(lookupRetryBackoff + 100*time.Millisecond)) {
		t.Fatal("backoff starts after timeout, consuming the buffer's lifetime")
	}
}

func TestPendingLookupCannotAssignFrameAfterMembershipChange(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	old := putPlug(t, b, 5)
	other := putPlug(t, b, 6)
	b.radio = resolvingRadio{fakeRadio{done: make(chan struct{})}, other.IEEE}
	b.safeEvent(context.Background(), reportFrame(0x4321, 0x66))
	j := <-b.queue
	if err := b.store.Remove(old.IEEE); err != nil {
		t.Fatal(err)
	}
	b.processJob(context.Background(), j)
	b.handleAddressResult(context.Background(), <-b.recoveryResults)
	got, _ := b.store.ByIEEE(other.IEEE)
	if got.Network != other.Network || len(b.store.State(other.IEEE)) != 0 {
		t.Fatal("ambiguous report assigned to another device after removal")
	}
}
