package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func dev(ieee, name string, n uint16) Device {
	return Device{IEEE: ieee, Name: name, Network: n, Type: "EndDevice", Endpoints: []Endpoint{{ID: 1, Profile: 0x104, In: []uint16{0, 6}}}}
}

func TestAnnounceTakesOverReusedAddress(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "db.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put(dev("0x0000000000000001", "a", 0x1111)); err != nil {
		t.Fatal(err)
	}
	displaced, err := s.Announce(dev("0x0000000000000002", "b", 0x1111))
	if err != nil || len(displaced) != 1 || displaced[0] != "a" {
		t.Fatalf("announce must win: %v %v", displaced, err)
	}
	if d, _ := s.ByIEEE("0x0000000000000001"); d.Network != UnknownNetwork {
		t.Fatalf("old entry keeps address %x", d.Network)
	}
	if d, ok := s.ByNetwork(0x1111); !ok || d.Name != "b" {
		t.Fatalf("address resolves to %q", d.Name)
	}
	if _, ok := s.ByNetwork(UnknownNetwork); ok {
		t.Fatal("unknown address must not resolve")
	}
	// Database with an unknown-address device must reload.
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	if _, err := New(s.path); err != nil {
		t.Fatalf("reload failed: %v", err)
	}
}

func TestSaveIfDirty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.json")
	s, _ := New(path)
	if err := s.SaveIfDirty(0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("clean store must not be written")
	}
	s.Put(dev("0x0000000000000001", "a", 0x1111))
	if err := s.SaveIfDirty(time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("dirty store must be written")
	}
	s.Update("0x0000000000000001", map[string]any{"temperature": 20.5})
	before, _ := os.Stat(path)
	s.SaveIfDirty(time.Hour)
	after, _ := os.Stat(path)
	if !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("write rate limit ignored")
	}
}

func TestCloneDeviceIsDeep(t *testing.T) {
	d := dev("0x0000000000000001", "a", 1)
	d.Options = map[string]float64{"temperature_calibration": 1}
	c := cloneDevice(d)
	c.Endpoints[0].In[0] = 99
	c.Options["temperature_calibration"] = 5
	if d.Endpoints[0].In[0] == 99 || d.Options["temperature_calibration"] == 5 {
		t.Fatal("clone shares memory")
	}
}
