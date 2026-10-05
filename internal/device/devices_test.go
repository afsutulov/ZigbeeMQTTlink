package device

import (
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"

	"zigbeemqttlink/internal/store"
	"zigbeemqttlink/internal/zcl"
)

func shippedRegistry(t *testing.T) *Registry {
	t.Helper()
	r, err := Load(filepath.Join("..", "..", "device-definitions.json"))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestInstalledModelsReportFixtures(t *testing.T) {
	r := shippedRegistry(t)
	for _, tc := range []struct {
		name, model, maker string
		ep                 byte
		cluster            uint16
		wire, key          string
		want               any
	}{
		{"aqara-e1-right", "lumi.switch.b2nc01", "LUMI", 2, 6, "18010a00001001", "state_right", "ON"},
		{"aqara-e1-button", "lumi.switch.b2nc01", "LUMI", 42, 0x12, "18010a5500210200", "action", "double_right"},
		{"aqara-relay-l2", "lumi.relay.c2acn01", "LUMI", 2, 6, "18010a00001000", "state_l2", "OFF"},
		{"aqara-weather-standard", "lumi.weather", "LUMI", 1, 0x402, "18010a0000296608", "temperature", 21.5},
		// Binary FF01 data must never be decoded as UTF-8 CHAR_STR.
		{"aqara-weather-binary", "lumi.weather", "LUMI", 1, 0, "18010a01ff42040121b80b64296608652194116623cd8b0100", "temperature", 21.5},
		{"aqara-leak", "lumi.sensor_wleak.aq1", "LUMI", 1, 0x500, "190100010000000000", "water_leak", true},
		{"aqara-button", "lumi.remote.b186acn02", "LUMI", 1, 0x12, "18010a5500210200", "action", "double"},
		{"tuya-plug-state", "TS011F", "_TZ3000_ew3ldmgx", 1, 6, "18010a00001000", "state", "OFF"},
		{"tuya-plug-timing-quirk", "TS011F", "_TZ3000_ew3ldmgx", 1, 0xe000, "08010a01d048020006", "random_timing_raw", "0006"},
		{"tuya-siren", "TS0601", "_TZE200_t1blo2bj", 1, 0xef00, "19010201000d01000101", "alarm", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := hex.DecodeString(tc.wire)
			if err != nil {
				t.Fatal(err)
			}
			f, err := zcl.Parse(data)
			if err != nil {
				t.Fatal(err)
			}
			state, err := r.State(store.Device{Model: tc.model, Manufacturer: tc.maker}, tc.ep, tc.cluster, f)
			if err != nil || state[tc.key] != tc.want {
				t.Fatalf("state=%v err=%v, want %s=%v", state, err, tc.key, tc.want)
			}
		})
	}
}

func TestSirenOnOffWireAndSafeOrdering(t *testing.T) {
	r := shippedRegistry(t)
	d := store.Device{Model: "TS0601", Manufacturer: "_TZE200_t1blo2bj", Endpoints: []store.Endpoint{{ID: 1, Profile: 0x104, In: []uint16{0, 0xef00}}}}
	for _, alarm := range []bool{true, false} {
		t.Run(map[bool]string{true: "on", false: "off"}[alarm], func(t *testing.T) {
			cs, err := r.Commands(d, map[string]any{"alarm": alarm, "duration": float64(60), "melody": "7", "volume": "high"}, false)
			if err != nil {
				t.Fatal(err)
			}
			want := []byte{21, 5, 7, 13}
			if !alarm {
				want = []byte{13, 21, 5, 7}
			}
			if len(cs) != len(want) {
				t.Fatal("missing siren property")
			}
			for i, c := range cs {
				if c.Cluster != 0xef00 || c.Endpoint != 1 || c.Payload[2] != want[i] {
					t.Fatalf("unsafe order: %+v", cs)
				}
			}
			index := 3
			if !alarm {
				index = 0
			}
			f, err := zcl.Parse(zcl.WireTransaction(cs[index], 0x1234))
			if err != nil {
				t.Fatal(err)
			}
			if f.Payload[0] != 0x34 || f.Payload[1] != 0x12 {
				t.Fatal("Tuya transaction lost")
			}
			dp, err := zcl.DecodeDatapoints(f.Payload)
			if err != nil || len(dp) != 1 || dp[0].ID != 13 || dp[0].Value != alarm {
				t.Fatalf("alarm wire value %+v %v", dp, err)
			}
		})
	}
	d.Manufacturer = "unknown_variant"
	if _, err := r.Commands(d, map[string]any{"alarm": true}, false); err == nil {
		t.Fatal("guessed siren commands sent to unknown TS0601 variant")
	}
}

func TestSignedReportingChangeCannotBeNegative(t *testing.T) {
	bad := strings.Replace(manifest, `"change":10`, `"change":-1`, 1)
	if _, err := Parse([]byte(bad)); err == nil {
		t.Fatal("negative reportable change accepted")
	}
}
