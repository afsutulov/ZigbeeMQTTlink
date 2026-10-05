package store

import (
	"path/filepath"
	"testing"
	"time"
)

// A radio-loop copy taken before a rename must not revert the rename on announce.
func TestAnnounceDoesNotRevertConcurrentRename(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "db.json"))
	if err != nil {
		t.Fatal(err)
	}
	d := Device{IEEE: "0x0000000000000001", Name: "old", Network: 10, Type: "EndDevice", Model: "lumi.sensor_wleak.aq1", Endpoints: []Endpoint{{ID: 1}}}
	if err = s.Put(d); err != nil {
		t.Fatal(err)
	}
	stale, _ := s.ByIEEE(d.IEEE)
	if _, err = s.Rename(d.IEEE, "kitchen_leak"); err != nil {
		t.Fatal(err)
	}
	if err = s.SetOptions(d.IEEE, map[string]float64{"temperature_calibration": 1}); err != nil {
		t.Fatal(err)
	}
	stale.Network = 20
	stale.Model = ""
	stale.Endpoints = nil
	stale.LastSeen = time.Now().UTC()
	if _, err = s.Announce(stale); err != nil {
		t.Fatal(err)
	}
	got, _ := s.ByIEEE(d.IEEE)
	if got.Name != "kitchen_leak" || got.Network != 20 || got.Model != d.Model || len(got.Endpoints) != 1 || got.Options["temperature_calibration"] != 1 {
		t.Fatalf("announce overwrote live metadata: %+v", got)
	}
}
