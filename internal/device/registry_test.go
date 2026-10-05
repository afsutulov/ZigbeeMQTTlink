package device

import (
	"bytes"
	"testing"

	"zigbeemqttlink/internal/store"
)

const manifest = `{"version":1,"devices":[{"id":"plug","description":"Test plug","models":["PLUG1"],"protocol":"zcl","endpoint":1,
"properties":{"state":{"type":"enum","access":"rw","cluster":6,"attribute":0,"wire_type":16,"values":{"OFF":0,"ON":1},"command_ids":{"OFF":0,"ON":1}},
"power":{"type":"number","access":"r","cluster":2820,"attribute":1291,"wire_type":41}},
"configure":[{"kind":"bind","cluster":6},{"kind":"report","cluster":6,"attribute":0,"wire_type":16,"min_interval":0,"max_interval":300},
{"kind":"report","cluster":2820,"attribute":1291,"wire_type":41,"min_interval":5,"max_interval":600,"change":10}]}]}`

func TestBindAndReportConfigure(t *testing.T) {
	m, err := Parse([]byte(manifest))
	if err != nil {
		t.Fatal(err)
	}
	r := &Registry{manifest: m}
	d := store.Device{IEEE: "0x0000000000000001", Name: "p", Network: 5, Type: "Router", Model: "PLUG1",
		Endpoints: []store.Endpoint{{ID: 1, Profile: 0x104, In: []uint16{0, 6, 0xb04}}}}
	cs, err := r.Configure(d)
	if err != nil || len(cs) != 3 {
		t.Fatalf("configure: %v %v", cs, err)
	}
	if !cs[0].Bind || cs[0].Cluster != 6 || cs[0].Endpoint != 1 {
		t.Fatalf("bind step wrong: %+v", cs[0])
	}
	// Boolean: discrete type, no reportable change.
	if want := []byte{0, 0, 0, 0x10, 0, 0, 0x2c, 0x01}; cs[1].ID != 6 || !bytes.Equal(cs[1].Payload, want) {
		t.Fatalf("discrete report payload %x", cs[1].Payload)
	}
	// int16: analog, change appended.
	if want := []byte{0, 0x0b, 0x05, 0x29, 5, 0, 0x58, 0x02, 10, 0}; !bytes.Equal(cs[2].Payload, want) {
		t.Fatalf("analog report payload %x", cs[2].Payload)
	}
	if r.Description(d)["description"] != "Test plug" {
		t.Fatal("description not exposed")
	}
}

func TestReportValidation(t *testing.T) {
	bad := `{"version":1,"devices":[{"id":"x","models":["X"],"protocol":"zcl","events":[{"cluster":6,"command":1,"state":{"a":1}}],
"configure":[{"kind":"report","cluster":6,"attribute":0,"wire_type":16,"min_interval":0,"max_interval":300,"change":1}]}]}`
	if _, err := Parse([]byte(bad)); err == nil {
		t.Fatal("change on a discrete type must be rejected")
	}
}
