package bridge

import (
	"context"
	"testing"
	"time"

	"zigbeemqttlink/internal/config"
	"zigbeemqttlink/internal/store"
	"zigbeemqttlink/internal/znp"
)

type resolvingRadio struct {
	fakeRadio
	ieee string
}

func (r resolvingRadio) IEEEAddress(context.Context, uint16) (string, error) { return r.ieee, nil }

func TestUnknownAddressIsRecoveredForPairedDevice(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	d := putPlug(t, b, 5)
	b.radio = resolvingRadio{fakeRadio{done: make(chan struct{})}, d.IEEE}
	b.safeEvent(context.Background(), reportFrame(0x4321, 0))
	var j job
	select {
	case j = <-b.queue:
	default:
		t.Fatal("no address recovery queued")
	}
	if j.resolve != 0x4321 || b.jobKey(j) != "address-recovery" {
		t.Fatalf("unexpected job %+v", j)
	}
	b.processJob(context.Background(), j)
	b.handleAddressResult(context.Background(), <-b.recoveryResults)
	got, _ := b.store.ByIEEE(d.IEEE)
	if got.Network != 0x4321 || got.Name != d.Name {
		t.Fatalf("address not recovered: %+v", got)
	}
	// Rate limited: a second frame does not queue another lookup immediately.
	b.store.Put(store.Device{IEEE: "0x00000000000000aa", Name: "other", Network: 77, Type: "Router"})
	b.safeEvent(context.Background(), reportFrame(0x9999, 0))
	<-b.queue
	b.safeEvent(context.Background(), reportFrame(0x9999, 0))
	select {
	case <-b.queue:
		t.Fatal("lookup not rate limited")
	default:
	}
}

func TestTrustCenterIndicationUpdatesKnownDeviceOnly(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	d := putPlug(t, b, 6)
	ind := func(n uint16, ieee uint64) znp.Frame {
		data := []byte{byte(n), byte(n >> 8)}
		for i := 0; i < 8; i++ {
			data = append(data, byte(ieee>>(8*i)))
		}
		return znp.Frame{Cmd0: 0x45, Cmd1: 0xca, Data: append(data, 0, 0)}
	}
	b.safeEvent(context.Background(), ind(0x2222, 6))
	if got, _ := b.store.ByIEEE(d.IEEE); got.Network != 0x2222 {
		t.Fatalf("known device not updated: %+v", got)
	}
	b.safeEvent(context.Background(), ind(0x3333, 0xdead))
	if _, ok := b.store.ByIEEE("0x000000000000dead"); ok || len(b.store.Devices()) != 1 {
		t.Fatal("unknown device added without announce/permit_join")
	}
}

func TestEventPanicIsContained(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	b.addressLookups = nil // writing to a nil map panics inside event()
	done := make(chan struct{})
	go func() { defer close(done); b.safeEvent(context.Background(), reportFrame(0x5555, 0)) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stalled")
	}
	if b.diagnostics.panics.Load() != 1 {
		t.Fatalf("panic not counted")
	}
}
