package bridge

import (
	"context"
	"testing"
	"time"
	"zigbeemqttlink/internal/config"
	"zigbeemqttlink/internal/device"
)

func TestFirstUnknownReportIsNotLost(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	d := putPlug(t, b, 5)
	b.radio = resolvingRadio{fakeRadio{done: make(chan struct{})}, d.IEEE}
	b.safeEvent(context.Background(), reportFrame(0x4321, 0x66))
	b.processJob(context.Background(), <-b.queue)
	b.handleAddressResult(context.Background(), <-b.recoveryResults)
	if state := b.store.State(d.IEEE); state["temperature"] != 21.5 {
		t.Fatalf("first report lost: %v", state)
	}
}

func TestDelayedRecoveryDoesNotOverrideAnnounce(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	d := putPlug(t, b, 5)
	b.radio = resolvingRadio{fakeRadio{done: make(chan struct{})}, d.IEEE}
	b.safeEvent(context.Background(), reportFrame(0x4321, 0x66))
	j := <-b.queue
	d.Network = 0x7777
	if _, err := b.store.Announce(d); err != nil {
		t.Fatal(err)
	}
	b.processJob(context.Background(), j)
	b.handleAddressResult(context.Background(), <-b.recoveryResults)
	got, _ := b.store.ByIEEE(d.IEEE)
	if got.Network != 0x7777 {
		t.Fatalf("stale lookup restored address: %+v", got)
	}
	if len(b.store.State(d.IEEE)) != 0 {
		t.Fatal("stale sender report replayed")
	}
}

func TestRecoveryBufferBoundedAndExpired(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	d := putPlug(t, b, 5)
	b.radio = resolvingRadio{fakeRadio{done: make(chan struct{})}, d.IEEE}
	for i := 0; i < 20; i++ {
		b.safeEvent(context.Background(), reportFrame(0x4321, byte(i)))
	}
	if b.bufferedFrames != maxFramesPerAddress {
		t.Fatal(b.bufferedFrames)
	}
	j := <-b.queue
	b.addressLookups[0x4321].started = time.Now().Add(-recoveryTTL - time.Second)
	for i := range b.addressLookups[0x4321].frames {
		b.addressLookups[0x4321].frames[i].received = b.addressLookups[0x4321].started
	}
	j.received = b.addressLookups[0x4321].started
	b.processJob(context.Background(), j)
	b.handleAddressResult(context.Background(), <-b.recoveryResults)
	if b.bufferedFrames != 0 || len(b.store.State(d.IEEE)) != 0 {
		t.Fatal("expired reports replayed")
	}
}

func TestOldRecoveryResultCannotCompleteNewLookup(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	d := putPlug(t, b, 5)
	b.radio = resolvingRadio{fakeRadio{done: make(chan struct{})}, d.IEEE}
	b.safeEvent(context.Background(), reportFrame(0x4321, 0x66))
	old := <-b.queue
	b.addressLookups[0x4321].started = time.Now().Add(-3 * time.Minute)
	b.addressLookups[0x4321].frames[0].received = b.addressLookups[0x4321].started
	b.safeEvent(context.Background(), reportFrame(0x4321, 0x67))
	current := <-b.queue
	if b.bufferedFrames != 1 {
		t.Fatalf("expired buffer leaked: %d", b.bufferedFrames)
	}
	b.processJob(context.Background(), old)
	b.handleAddressResult(context.Background(), <-b.recoveryResults)
	if len(b.store.State(d.IEEE)) != 0 {
		t.Fatal("old reply accepted")
	}
	b.processJob(context.Background(), current)
	b.handleAddressResult(context.Background(), <-b.recoveryResults)
	if b.store.State(d.IEEE)["temperature"] != 21.51 {
		t.Fatal("new report lost")
	}
	if b.diagnostics.incomingFrames.Load() != 2 {
		t.Fatal("replay double-counted radio frames")
	}
}

func TestSingleLeakEventSurvivesAddressRecovery(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	r, err := device.Load("../../device-definitions.json")
	if err != nil {
		t.Fatal(err)
	}
	b.definitions = r
	d := putPlug(t, b, 5)
	d.Model = "lumi.sensor_wleak.aq1"
	d.Manufacturer = "LUMI"
	d.Type = "EndDevice"
	if err := b.store.Put(d); err != nil {
		t.Fatal(err)
	}
	b.radio = resolvingRadio{fakeRadio{done: make(chan struct{})}, d.IEEE}
	f := reportFrame(0x4321, 0)
	f.Data[2], f.Data[3], f.Data[16] = 0, 5, 9
	f.Data = append(f.Data[:17], []byte{0x19, 1, 0, 1, 0, 0, 0, 0, 0}...)
	b.safeEvent(context.Background(), f)
	b.processJob(context.Background(), <-b.queue)
	b.handleAddressResult(context.Background(), <-b.recoveryResults)
	if state := b.store.State(d.IEEE); state["water_leak"] != true {
		t.Fatalf("single leak lost: %v", state)
	}
	select {
	case p := <-b.reports:
		if p.ieee != d.IEEE {
			t.Fatal("wrong MQTT target")
		}
	default:
		t.Fatal("no MQTT publication queued")
	}
}

// A failed lookup (sleeping device missed the request) must not discard the
// only leak report: the next frame retries after a short backoff and both
// reports are replayed once the address is resolved.
func TestFailedLookupKeepsReportForRetry(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	d := putPlug(t, b, 5)
	b.safeEvent(context.Background(), reportFrame(0x4321, 0x66))
	first := <-b.queue
	b.processJob(context.Background(), first) // fakeRadio cannot resolve: error result
	b.handleAddressResult(context.Background(), <-b.recoveryResults)
	if b.bufferedFrames != 1 {
		t.Fatalf("report discarded after failed lookup: %d", b.bufferedFrames)
	}
	// Within the backoff the next frame is buffered, no new lookup is queued.
	b.safeEvent(context.Background(), reportFrame(0x4321, 0x67))
	select {
	case <-b.queue:
		t.Fatal("retry not rate limited")
	default:
	}
	b.addressLookups[0x4321].retryAfter = time.Now().Add(-time.Second)
	b.radio = resolvingRadio{fakeRadio{done: make(chan struct{})}, d.IEEE}
	b.safeEvent(context.Background(), reportFrame(0x4321, 0x68))
	b.processJob(context.Background(), <-b.queue)
	b.handleAddressResult(context.Background(), <-b.recoveryResults)
	if got, _ := b.store.ByIEEE(d.IEEE); got.Network != 0x4321 {
		t.Fatalf("address not recovered on retry: %+v", got)
	}
	if b.bufferedFrames != 0 || b.diagnostics.recoveryReplayed.Load() != 3 {
		t.Fatalf("buffered=%d replayed=%d", b.bufferedFrames, b.diagnostics.recoveryReplayed.Load())
	}
	if b.store.State(d.IEEE)["temperature"] != 21.52 {
		t.Fatalf("replay order/content wrong: %v", b.store.State(d.IEEE))
	}
}
