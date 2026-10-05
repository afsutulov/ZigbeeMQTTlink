package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"zigbeemqttlink/internal/config"
	"zigbeemqttlink/internal/store"
	"zigbeemqttlink/internal/zcl"
	"zigbeemqttlink/internal/znp"
)

type blockedToken struct{ released <-chan struct{} }

func (t blockedToken) Wait() bool { <-t.released; return true }
func (t blockedToken) WaitTimeout(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-t.released:
		return true
	case <-timer.C:
		return false
	}
}
func (t blockedToken) Done() <-chan struct{} { return t.released }
func (t blockedToken) Error() error          { return nil }

type brokerStub struct {
	mqtt.Client
	started chan string
	release chan struct{}
}

func (c *brokerStub) IsConnectionOpen() bool { return true }
func (c *brokerStub) Publish(topic string, qos byte, retain bool, payload interface{}) mqtt.Token {
	c.started <- topic
	return blockedToken{c.release}
}

func reportFrame(n uint16, temp byte) znp.Frame {
	data := []byte{0, 0, 2, 4, byte(n), byte(n >> 8), 1, 1, 0, 150, 0, 0, 0, 0, 0, 1, 8, 0x08, 1, 0x0a, 0, 0, 0x29, temp, 8}
	return znp.Frame{Cmd0: 0x44, Cmd1: 0x81, Data: data}
}
func putPlug(t *testing.T, b *Bridge, id int) store.Device {
	t.Helper()
	d := store.Device{IEEE: fmt.Sprintf("0x%016x", id), Name: fmt.Sprintf("p%d", id), Network: uint16(id), Type: "Router", Endpoints: []store.Endpoint{{ID: 1, Profile: 0x104, In: []uint16{0, 6}}}}
	if err := b.store.Put(d); err != nil {
		t.Fatal(err)
	}
	return d
}
func await[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(time.Second):
		t.Fatal("operation stalled")
		var zero T
		return zero
	}
}

func TestSlowBrokerDoesNotBlockReports(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	d := putPlug(t, b, 1)
	broker := &brokerStub{started: make(chan string, 8), release: make(chan struct{})}
	b.client = broker
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { defer close(finished); b.publicationWorker(ctx) }()
	defer func() { close(broker.release); cancel(); await(t, finished) }()
	first := make(chan struct{})
	go func() { b.event(ctx, reportFrame(d.Network, 0x66)); close(first) }()
	await(t, broker.started)
	await(t, first)
	second := make(chan struct{})
	go func() { b.event(ctx, reportFrame(d.Network, 0x67)); close(second) }()
	await(t, second)
	if state := b.store.State(d.IEEE); state["temperature"] != 21.51 {
		t.Fatalf("latest radio state not processed: %v", state)
	}
}

type slowWriter struct {
	header           http.Header
	started, release chan struct{}
}

func (w *slowWriter) Header() http.Header         { return w.header }
func (w *slowWriter) WriteHeader(int)             {}
func (w *slowWriter) Write(p []byte) (int, error) { close(w.started); <-w.release; return len(p), nil }
func TestSlowHTTPClientDoesNotHoldAdminLock(t *testing.T) {
	b := newTestBridge(t, config.Web{Enabled: true, Listen: "0.0.0.0:8080", User: "admin", Password: "secret"})
	d := putPlug(t, b, 1)
	srv, err := b.HTTP()
	if err != nil {
		t.Fatal(err)
	}
	w := &slowWriter{header: make(http.Header), started: make(chan struct{}), release: make(chan struct{})}
	req := httptest.NewRequest("GET", "/api/admin", nil)
	req.SetBasicAuth("admin", "secret")
	finished := make(chan struct{})
	go func() { defer close(finished); srv.Handler.ServeHTTP(w, req) }()
	defer func() { close(w.release); await(t, finished) }()
	await(t, w.started)
	reported := make(chan struct{})
	go func() { b.event(context.Background(), reportFrame(d.Network, 0x66)); close(reported) }()
	await(t, reported)
}

type sent struct {
	network uint16
	command byte
}
type delayedRadio struct {
	fakeRadio
	sent    chan sent
	release chan struct{}
}

func (r *delayedRadio) Send(ctx context.Context, n uint16, ep byte, cluster uint16, payload []byte) error {
	r.sent <- sent{n, payload[2]}
	if n == 1 {
		select {
		case <-r.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func TestWorkerParallelDevicesAndFIFO(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	putPlug(t, b, 1)
	putPlug(t, b, 2)
	r := &delayedRadio{fakeRadio: fakeRadio{make(chan struct{})}, sent: make(chan sent, 8), release: make(chan struct{})}
	b.radio = r
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { defer close(finished); b.worker(ctx) }()
	defer func() { cancel(); await(t, finished) }()
	b.queue <- job{topic: "p1/set", payload: []byte("ON")}
	if s := await(t, r.sent); s.network != 1 || s.command != 1 {
		t.Fatalf("first command: %v", s)
	}
	b.queue <- job{topic: "p1/set", payload: []byte("OFF")}
	b.queue <- job{topic: "p2/set", payload: []byte("ON")}
	if s := await(t, r.sent); s.network != 2 {
		t.Fatalf("sleeping device blocks others or FIFO broken: %v", s)
	}
	close(r.release)
	if s := await(t, r.sent); s.network != 1 || s.command != 0 {
		t.Fatalf("OFF overtook ON: %v", s)
	}
}

func TestCompoundCommandDoesNotUseReassignedAddress(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	d := putPlug(t, b, 1)
	r := &delayedRadio{fakeRadio: fakeRadio{make(chan struct{})}, sent: make(chan sent, 8), release: make(chan struct{})}
	b.radio = r
	finished := make(chan error, 1)
	go func() {
		finished <- b.sendCommands(context.Background(), d, []zcl.Command{{Endpoint: 1, Cluster: 6, Control: 0x11, ID: 1}, {Endpoint: 1, Cluster: 6, Control: 0x11, ID: 0}})
	}()
	await(t, r.sent)
	b.event(context.Background(), announceFrame(2, 1))
	close(r.release)
	if err := await(t, finished); err == nil {
		t.Fatal("stale destination accepted")
	}
	if len(r.sent) != 0 {
		t.Fatal("second command sent to obsolete short address")
	}
}

func TestHTTPRejectsDuplicateKeys(t *testing.T) {
	b := newTestBridge(t, config.Web{Enabled: true, Listen: "0.0.0.0:8080", User: "admin", Password: "secret"})
	srv, _ := b.HTTP()
	req := httptest.NewRequest("POST", "/api/request", strings.NewReader(`{"request":"permit_join","data":{"time":0,"time":60}}`))
	req.SetBasicAuth("admin", "secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)
	if rec.Code != 400 || b.joinUntil.Load() != 0 {
		t.Fatalf("ambiguous mutation accepted: %d", rec.Code)
	}
}
func TestMQTTRejectsDuplicateKeys(t *testing.T) {
	if _, _, _, err := parseCommand("p/set", []byte(`{"state":"OFF","state":"ON"}`)); err == nil {
		t.Fatal("duplicate MQTT keys accepted")
	}
}
func TestQueuedReportDoesNotRestoreRenamedTopic(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	d := putPlug(t, b, 1)
	broker := &brokerStub{started: make(chan string, 8), release: make(chan struct{})}
	b.client = broker
	close(broker.release)
	b.queueReport(d, map[string]any{"state": "ON"}, true)
	if _, err := b.store.Rename(d.IEEE, "renamed"); err != nil {
		t.Fatal(err)
	}
	current, _ := b.store.ByIEEE(d.IEEE)
	b.queueReport(current, map[string]any{"state": "OFF"}, true)
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { defer close(finished); b.publicationWorker(ctx) }()
	defer func() { cancel(); await(t, finished) }()
	if topic := await(t, broker.started); topic != "z/renamed" {
		t.Fatalf("obsolete retained topic restored: %q", topic)
	}
}

func TestCompoundValidationBeforeAnyWrite(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	d := putPlug(t, b, 1)
	r := &delayedRadio{fakeRadio: fakeRadio{make(chan struct{})}, sent: make(chan sent, 8), release: make(chan struct{})}
	b.radio = r
	err := b.sendCommands(context.Background(), d, []zcl.Command{{Endpoint: 1, Cluster: 6, Control: 0x11, ID: 1}, {Endpoint: 1, Cluster: 6, Control: 0x11, Payload: make([]byte, 238)}})
	if err == nil || len(r.sent) != 0 {
		t.Fatal("invalid second frame caused a partial radio write")
	}
}

func TestExpiredQueuedCommandIsNotExecuted(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	putPlug(t, b, 1)
	r := &delayedRadio{fakeRadio: fakeRadio{make(chan struct{})}, sent: make(chan sent, 8), release: make(chan struct{})}
	b.radio = r
	b.processJob(context.Background(), job{topic: "p1/set", payload: []byte("ON"), received: time.Now().Add(-time.Minute)})
	if len(r.sent) != 0 {
		t.Fatal("old queued command executed")
	}
}

// Keep health output JSON-safe after concurrent workers add queue counters.
func TestHealthJSON(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	if _, err := json.Marshal(b.Health()); err != nil {
		t.Fatal(err)
	}
}
