package bridge

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"zigbeemqttlink/internal/config"
	"zigbeemqttlink/internal/device"
	"zigbeemqttlink/internal/store"
	"zigbeemqttlink/internal/znp"
)

type fakeRadio struct{ done chan struct{} }

func (f fakeRadio) Events() <-chan znp.Frame { return nil }
func (f fakeRadio) Done() <-chan struct{}    { return f.done }
func (f fakeRadio) Err() error               { return nil }
func (f fakeRadio) Send(context.Context, uint16, byte, uint16, []byte) error {
	return nil
}
func (f fakeRadio) PermitJoin(context.Context, byte) error { return nil }

func newTestBridge(t *testing.T, web config.Web) *Bridge {
	dir := t.TempDir()
	defs := filepath.Join(dir, "defs.json")
	os.WriteFile(defs, []byte(`{"version":1,"devices":[]}`), 0600)
	r, err := device.Load(defs)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := store.New(filepath.Join(dir, "db.json"))
	cfg := config.Config{Web: web, MQTT: config.MQTT{BaseTopic: "z"}}
	return New(cfg, s, fakeRadio{done: make(chan struct{})}, slog.New(slog.NewTextHandler(io.Discard, nil)), r)
}

func TestBasicAuth(t *testing.T) {
	b := newTestBridge(t, config.Web{Enabled: true, Listen: "0.0.0.0:0", User: "admin", Password: "secret"})
	srv, err := b.HTTP()
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/api/devices", nil))
	if rec.Code != 401 {
		t.Fatalf("unauthenticated request got %d", rec.Code)
	}
	req := httptest.NewRequest("GET", "/api/devices", nil)
	req.SetBasicAuth("admin", "secret")
	rec = httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("authenticated request got %d", rec.Code)
	}
}

func TestParseCommandTopic(t *testing.T) {
	target, op, p, err := parseCommand("kitchen/lamp/set", []byte("ON"))
	if err != nil || target != "kitchen/lamp" || op != "set" || p["state"] != "ON" {
		t.Fatalf("%q %q %v %v", target, op, p, err)
	}
	target, _, p, err = parseCommand("lamp/set/brightness", []byte("120"))
	if err != nil || target != "lamp" || p["brightness"] != float64(120) {
		t.Fatalf("%q %v %v", target, p, err)
	}
}

func announceFrame(nwk uint16, ieee byte) znp.Frame {
	d := []byte{0, 0, byte(nwk), byte(nwk >> 8), ieee, 0, 0, 0, 0, 0, 0, 0, 0x80}
	return znp.Frame{Cmd0: 0x45, Cmd1: 0xc1, Data: d}
}

func TestAnnounceFlow(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	ctx := context.Background()
	b.event(ctx, announceFrame(0x1111, 1))
	if len(b.queue) != 0 || len(b.store.Devices()) != 0 {
		t.Fatal("device accepted while pairing closed")
	}
	b.joinUntil.Store(1 << 40)
	b.event(ctx, announceFrame(0x1111, 1))
	if len(b.queue) != 1 || len(b.store.Devices()) != 1 {
		t.Fatal("new device not queued for interview")
	}
	<-b.queue
	// Interviewed device rejoins with a new address: no re-interview.
	d := b.store.Devices()[0]
	d.Model = "X"
	d.Endpoints = []store.Endpoint{{ID: 1, Profile: 0x104}}
	b.store.Refresh(d)
	b.event(ctx, announceFrame(0x2222, 1))
	if len(b.queue) != 0 {
		t.Fatal("known device re-interviewed")
	}
	// Another device takes the old address: must be accepted.
	b.event(ctx, announceFrame(0x2222, 2))
	if len(b.store.Devices()) != 2 {
		t.Fatal("device with reused address rejected")
	}
}

func TestReportUpdatesState(t *testing.T) {
	b := newTestBridge(t, config.Web{})
	b.store.Put(store.Device{IEEE: "0x0000000000000001", Name: "t", Network: 0x1234, Type: "EndDevice", Endpoints: []store.Endpoint{{ID: 1, Profile: 0x104, In: []uint16{0x402}}}})
	zcl := []byte{0x08, 1, 0x0a, 0, 0, 0x29, 0x66, 0x08}
	data := append([]byte{0, 0, 0x02, 0x04, 0x34, 0x12, 1, 1, 0, 150, 0, 0, 0, 0, 0, 1, byte(len(zcl))}, zcl...)
	b.event(context.Background(), znp.Frame{Cmd0: 0x44, Cmd1: 0x81, Data: data})
	if st := b.store.State("0x0000000000000001"); st["temperature"] != 21.5 {
		t.Fatalf("state %v", st)
	}
	if len(b.acks) != 1 {
		t.Fatal("default response not queued on the protocol worker")
	}
}
