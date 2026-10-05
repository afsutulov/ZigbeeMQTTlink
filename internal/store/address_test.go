package store

import (
	"path/filepath"
	"testing"
)

func TestRecoveryRejectsDeletedReaddedAndSupersededDevices(t *testing.T) {
	for _, action := range []string{"removed", "readded", "announced", "displaced"} {
		t.Run(action, func(t *testing.T) {
			s, err := New(filepath.Join(t.TempDir(), "db.json"))
			if err != nil {
				t.Fatal(err)
			}
			d := Device{IEEE: "0x0000000000000001", Name: "leak", Network: 10, Type: "EndDevice"}
			if err = s.Put(d); err != nil {
				t.Fatal(err)
			}
			epoch := s.AddressEpoch()
			switch action {
			case "removed", "readded":
				if err = s.Remove(d.IEEE); err != nil {
					t.Fatal(err)
				}
				if action == "readded" {
					d.Network = 30
					if err = s.Put(d); err != nil {
						t.Fatal(err)
					}
				}
			case "announced":
				d.Network = 30
				if _, err = s.Announce(d); err != nil {
					t.Fatal(err)
				}
			case "displaced":
				other := Device{IEEE: "0x0000000000000002", Name: "new", Network: 20, Type: "Router"}
				if _, err = s.Announce(other); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err = s.RecoverAddress(d.IEEE, 20, epoch); err == nil {
				t.Fatal("stale recovery accepted")
			}
			got, exists := s.ByIEEE(d.IEEE)
			if action == "removed" && exists {
				t.Fatal("removed device resurrected")
			}
			if (action == "readded" || action == "announced") && got.Network != 30 {
				t.Fatal(got)
			}
			if action == "displaced" {
				other, _ := s.ByIEEE("0x0000000000000002")
				if other.Network != 20 {
					t.Fatal(other)
				}
			}
		})
	}
}

func TestRecoveryPreservesMetadataWithoutAddingDevices(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "db.json"))
	if err != nil {
		t.Fatal(err)
	}
	d := Device{IEEE: "0x0000000000000001", Name: "old", Network: 10, Type: "EndDevice"}
	if err = s.Put(d); err != nil {
		t.Fatal(err)
	}
	epoch := s.AddressEpoch()
	if _, err = s.Rename(d.IEEE, "new"); err != nil {
		t.Fatal(err)
	}
	if err = s.SetOptions(d.IEEE, map[string]float64{"temperature_calibration": 2}); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.RecoverAddress(d.IEEE, 20, epoch)
	if err != nil || got.Network != 20 || got.Name != "new" || got.Options["temperature_calibration"] != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	if _, _, err = s.RecoverAddress("0x00000000000000ff", 30, s.AddressEpoch()); err == nil {
		t.Fatal("unknown device admitted")
	}
}

func TestKnownAnnounceDoesNotResurrectRemovedDevice(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "db.json"))
	if err != nil {
		t.Fatal(err)
	}
	d := Device{IEEE: "0x0000000000000001", Name: "old", Network: 10, Type: "EndDevice"}
	if err = s.Put(d); err != nil {
		t.Fatal(err)
	}
	stale, _ := s.ByIEEE(d.IEEE)
	if err = s.Remove(d.IEEE); err != nil {
		t.Fatal(err)
	}
	stale.Network = 20
	if _, err = s.AnnounceKnown(stale); err == nil {
		t.Fatal("removed device restored by stale announce")
	}
	if len(s.Devices()) != 0 {
		t.Fatal("record restored")
	}
}
