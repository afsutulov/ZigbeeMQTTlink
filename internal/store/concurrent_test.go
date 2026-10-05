package store

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestAdminDiskWritePreservesConcurrentRadioUpdates(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			s, _ := New(filepath.Join(t.TempDir(), "db.json"))
			d := dev("0x0000000000000001", "old", 1)
			if err := s.Put(d); err != nil {
				t.Fatal(err)
			}
			if err := s.Save(); err != nil {
				t.Fatal(err)
			}
			started, release := make(chan struct{}), make(chan struct{})
			s.persist = func(path string, b []byte) error {
				close(started)
				<-release
				if fail {
					return fmt.Errorf("simulated disk failure")
				}
				return AtomicWrite(path, b)
			}
			finished := make(chan error, 1)
			go func() { _, err := s.Rename(d.IEEE, "new"); finished <- err }()
			<-started
			radioDone := make(chan error, 1)
			seen := time.Now().UTC()
			go func() {
				d.Network = 2
				d.LastSeen = seen
				_, err := s.Announce(d)
				s.Update(d.IEEE, map[string]any{"temperature": 22.5})
				radioDone <- err
			}()
			select {
			case err := <-radioDone:
				if err != nil {
					close(release)
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				close(release)
				t.Fatal("disk persistence blocked radio updates")
			}
			close(release)
			err := <-finished
			if (err != nil) != fail {
				t.Fatalf("commit status %v", err)
			}
			current, _ := s.ByIEEE(d.IEEE)
			wantName := "new"
			if fail {
				wantName = "old"
			}
			if current.Name != wantName || current.Network != 2 || !current.LastSeen.Equal(seen) || s.State(d.IEEE)["temperature"] != 22.5 {
				t.Fatalf("concurrent update lost: %+v %v", current, s.State(d.IEEE))
			}
			s.persist = AtomicWrite
			if err := s.SaveIfDirty(0); err != nil {
				t.Fatal(err)
			}
			reloaded, err := New(s.path)
			if err != nil {
				t.Fatal(err)
			}
			stored, _ := reloaded.ByIEEE(d.IEEE)
			if stored.Name != wantName || stored.Network != 2 || reloaded.State(d.IEEE)["temperature"] != 22.5 {
				t.Fatalf("merge was not saved: %+v", stored)
			}
		})
	}
}

func TestInterviewAndTouchPreserveLiveMetadata(t *testing.T) {
	s, _ := New(filepath.Join(t.TempDir(), "db.json"))
	d := dev("0x0000000000000001", "a", 1)
	d.Model = "PLUG"
	d.Manufacturer = "VENDOR"
	d.LastSeen = time.Now().UTC()
	d.Endpoints[0].Scales = map[string]float64{"current_divisor": 1000}
	s.Put(d)
	seen := d.LastSeen.Add(time.Minute)
	if err := s.Touch(d.IEEE, 1, seen); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetEndpoints(d.IEEE, 1, []Endpoint{{ID: 1, Profile: 0x104, In: []uint16{0, 6, 0xb04}}}); err != nil {
		t.Fatal(err)
	}
	d.LastSeen = seen.Add(time.Minute)
	if err := s.RefreshReport(d); err != nil {
		t.Fatal(err)
	}
	current, _ := s.ByIEEE(d.IEEE)
	if current.Model != "PLUG" || len(current.Endpoints[0].In) != 3 || current.Endpoints[0].Scales["current_divisor"] != 1000 || !current.LastSeen.Equal(d.LastSeen) {
		t.Fatalf("stale report/interview replaced live metadata: %+v", current)
	}
}

func TestRejectInvalidAnnounceWithoutChangingDatabase(t *testing.T) {
	s, _ := New(filepath.Join(t.TempDir(), "db.json"))
	s.Put(dev("0x0000000000000001", "a", 1))
	if _, err := s.Announce(dev("0x0000000000000002", "b", UnknownNetwork)); err == nil {
		t.Fatal("invalid announce accepted")
	}
	if len(s.Devices()) != 1 {
		t.Fatal("invalid announce mutated the database")
	}
}

func TestRetainedCleanupSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.json")
	s, _ := New(path)
	d := dev("0x0000000000000001", "old", 1)
	if err := s.Put(d); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Rename(d.IEEE, "new"); err != nil {
		t.Fatal(err)
	}
	s, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if names := s.CleanupTopics(); len(names) != 1 || names[0] != "old" {
		t.Fatalf("cleanup forgotten on restart: %v", names)
	}
	if err := s.AcknowledgeCleanup([]string{"old"}); err != nil {
		t.Fatal(err)
	}
	s, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.CleanupTopics()) != 0 {
		t.Fatal("broker acknowledgement not persisted")
	}
	if err := s.Remove(d.IEEE); err != nil {
		t.Fatal(err)
	}
	s, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
	if names := s.CleanupTopics(); len(names) != 1 || names[0] != "new" {
		t.Fatalf("removed retained topic forgotten: %v", names)
	}
}
