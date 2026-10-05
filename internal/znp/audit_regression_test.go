package znp

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestAuditRestoreCounterOverflow(t *testing.T) {
	fastTimings(t)
	for _, link := range []bool{false, true} {
		b, _ := ParseBackup([]byte(herdsmanBackup))
		if link {
			b.Devices[1].LinkKey.TxCounter = math.MaxUint32
		} else {
			b.FrameCounter = math.MaxUint32
		}
		e, c := newEmulator(t, ProductZStack3x0, 1)
		err := RestoreNetwork(testCtx(t), c, b, quietLog())
		e.mu.Lock()
		writes := e.writes
		e.mu.Unlock()
		if err == nil || writes != 0 {
			t.Errorf("link=%v: unsafe restore err=%v NV writes=%d", link, err, writes)
		}
	}
}

func TestAuditCapacityFailurePreservesNetwork(t *testing.T) {
	fastTimings(t)
	e, c := newEmulator(t, ProductZStack3x0, 1)
	ctx := testCtx(t)
	if _, err := FormNetwork(ctx, c, fixedNetwork(), quietLog()); err != nil {
		t.Fatal(err)
	}
	b, _ := ParseBackup([]byte(herdsmanBackup))
	for i := 0; i < 20; i++ {
		b.Devices = append(b.Devices, BackupDevice{IEEE: []byte{0, 0x15, 0x8d, 0, 0, 0, 0, byte(i + 1)}, IsDirectChild: true})
	}
	e.mu.Lock()
	before := e.writes
	nib := append([]byte(nil), e.nv[nvNIB]...)
	e.mu.Unlock()
	err := RestoreNetwork(ctx, c, b, quietLog())
	e.mu.Lock()
	defer e.mu.Unlock()
	if err == nil || before != e.writes || !bytes.Equal(nib, e.nv[nvNIB]) {
		t.Fatalf("capacity rejection destroyed network: err=%v writes %d->%d", err, before, e.writes)
	}
}

func TestAuditFailedRestoreNotMarkedConfigured(t *testing.T) {
	fastTimings(t)
	e, c := newEmulator(t, ProductZStack3x0, 1)
	ctx := testCtx(t)
	if _, err := FormNetwork(ctx, c, fixedNetwork(), quietLog()); err != nil {
		t.Fatal(err)
	}
	// Inject a real NV write failure after temporary formation.
	e.mu.Lock()
	e.failWriteID = nvExtAddr
	e.mu.Unlock()
	b, _ := ParseBackup([]byte(herdsmanBackup))
	if err := RestoreNetwork(ctx, c, b, quietLog()); err == nil {
		t.Fatal("expected failure")
	}
	a := &Adapter{C: c, inFlight: make(chan struct{}, maxInFlight)}
	if err := a.Start(ctx); err == nil {
		t.Fatal("service accepted a temporary/partial network after restore failure")
	}

}

func TestAuditParseRejectsMissingSecurityFields(t *testing.T) {
	var f map[string]any
	_ = json.Unmarshal([]byte(herdsmanBackup), &f)
	delete(f["network_key"].(map[string]any), "frame_counter")
	raw, _ := json.Marshal(f)
	if _, err := ParseBackup(raw); err == nil {
		t.Fatal("missing counter accepted as zero")
	}
}

func TestAuditValidHighPAN(t *testing.T) {
	var f ocbFile
	_ = json.Unmarshal([]byte(herdsmanBackup), &f)
	f.PanID = "fffe"
	raw, _ := json.Marshal(f)
	if _, err := ParseBackup(raw); err != nil {
		t.Fatal(err)
	}
}

func TestAuditExistingNetworkWithoutMarker(t *testing.T) {
	fastTimings(t)
	e, c := newEmulator(t, ProductZStack3x0, 1)
	ctx := testCtx(t)
	if _, err := FormNetwork(ctx, c, fixedNetwork(), quietLog()); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.nv[nvHasConfiguredZStack3] = []byte{0}
	e.mu.Unlock()
	st, err := Inspect(ctx, c)
	if err != nil || !st.Configured {
		t.Fatalf("existing network mistaken for empty stick: %+v %v", st, err)
	}
}

func TestAuditRestoreVerificationRejectsPartialSuccess(t *testing.T) {
	b, _ := ParseBackup([]byte(herdsmanBackup))
	for _, mutate := range []func(*Backup){
		func(g *Backup) { g.Channel = 20 },
		func(g *Backup) { g.FrameCounter = 0 },
		func(g *Backup) { g.KeySequence++ },
		func(g *Backup) { g.TCLKSeed = make([]byte, 16) },
		func(g *Backup) { g.NetworkKey = make([]byte, 16) },
	} {
		g, _ := ParseBackup([]byte(herdsmanBackup))
		g.FrameCounter += frameCounterRestoreMargin
		for _, d := range g.Devices {
			if d.LinkKey != nil {
				d.LinkKey.TxCounter += frameCounterRestoreMargin
			}
		}
		mutate(g)
		if err := b.VerifyRestored(g); err == nil {
			t.Fatal("partial restore accepted")
		}
	}
}

func TestAuditAlternateBackupUnreadableIsPreserved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "backup.json")
	old, _ := ParseBackup([]byte(herdsmanBackup))
	if _, err := SaveBackupFile(path, old, nil, false); err != nil {
		t.Fatal(err)
	}
	next, _ := ParseBackup([]byte(herdsmanBackup))
	next.NetworkKey[0] ^= 1
	alt := alternatePath(path, next, "")
	if err := os.WriteFile(alt, []byte("keep me"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := SaveBackupFile(path, next, nil, false); err == nil {
		t.Fatal("overwrote unreadable alternate backup")
	}
	if raw, _ := os.ReadFile(alt); string(raw) != "keep me" {
		t.Fatal("alternate contents lost")
	}
}

func TestAuditRestoringOldBackupDoesNotRewindCounters(t *testing.T) {
	fastTimings(t)
	_, c := newEmulator(t, ProductZStack3x0, 1)
	ctx := testCtx(t)
	b, _ := ParseBackup([]byte(herdsmanBackup))
	if err := RestoreNetwork(ctx, c, b, quietLog()); err != nil {
		t.Fatal(err)
	}
	before, err := CreateBackup(ctx, c, "before")
	if err != nil {
		t.Fatal(err)
	}
	old, _ := ParseBackup([]byte(herdsmanBackup))
	if err := RestoreNetwork(ctx, c, old, quietLog()); err != nil {
		t.Fatal(err)
	}
	after, err := CreateBackup(ctx, c, "after")
	if err != nil {
		t.Fatal(err)
	}
	if after.FrameCounter < before.FrameCounter+frameCounterRestoreMargin {
		t.Fatal("network counter rewound")
	}
	for i, d := range before.Devices {
		if d.LinkKey != nil && after.Devices[i].LinkKey.TxCounter < d.LinkKey.TxCounter+frameCounterRestoreMargin {
			t.Fatal("device counter rewound")
		}
	}
}

// Address/missing-device differences remain nonfatal. Existing-key counter
// rollback is separately rejected by the safety tests.
func TestRestoreDeviceDifferencesReported(t *testing.T) {
	b, _ := ParseBackup([]byte(herdsmanBackup))
	exact := func() *Backup {
		g, _ := ParseBackup([]byte(herdsmanBackup))
		g.FrameCounter += frameCounterRestoreMargin
		for _, d := range g.Devices {
			if d.LinkKey != nil {
				d.LinkKey.TxCounter += frameCounterRestoreMargin
			}
		}
		return g
	}
	if g := exact(); b.VerifyRestored(g) != nil || len(b.DeviceDifferences(g)) != 0 {
		t.Fatalf("exact restore flagged: %v %v", b.VerifyRestored(g), b.DeviceDifferences(g))
	}
	for name, mutate := range map[string]func(*Backup){
		"key": func(g *Backup) { g.Devices[1].LinkKey.Key[0] ^= 1 },

		"missing": func(g *Backup) { g.Devices = g.Devices[:2] },
		"child":   func(g *Backup) { g.Devices[0].IsDirectChild = false },
		"address": func(g *Backup) { n := uint16(1); g.Devices[2].NetworkAddress = &n },
		"extra": func(g *Backup) {
			g.Devices = append(g.Devices, BackupDevice{IEEE: []byte{1, 2, 3, 4, 5, 6, 7, 8}, IsDirectChild: true})
		},
	} {
		g := exact()
		mutate(g)
		if err := b.VerifyRestored(g); err != nil {
			t.Fatalf("%s: device difference blocked restore: %v", name, err)
		}
		if len(b.DeviceDifferences(g)) == 0 {
			t.Fatalf("%s: device difference not reported", name)
		}
	}
}

// After an interrupted restore the stick holds the right network without the
// ready marker: Inspect must tell this apart so -restore can finish the job.
func TestInterruptedRestoreDetectable(t *testing.T) {
	fastTimings(t)
	e, c := newEmulator(t, ProductZStack3x0, 1)
	ctx := testCtx(t)
	b, _ := ParseBackup([]byte(herdsmanBackup))
	if err := RestoreNetwork(ctx, c, b, quietLog()); err != nil {
		t.Fatal(err)
	}
	if st, err := Inspect(ctx, c); err != nil || !st.Configured || !st.Ready {
		t.Fatalf("completed restore: %+v %v", st, err)
	}
	e.mu.Lock()
	e.nv[nvHasConfiguredZStack3] = []byte{0} // power lost before the final marker write
	e.mu.Unlock()
	st, err := Inspect(ctx, c)
	if err != nil || !st.Configured || st.Ready {
		t.Fatalf("interrupted restore not distinguishable: %+v %v", st, err)
	}
	cur, err := CreateBackup(ctx, c, "x")
	if err != nil || !cur.SameNetwork(b) {
		t.Fatalf("interrupted network unreadable: %v", err)
	}
	// Running the restore again completes it.
	if err = RestoreNetwork(ctx, c, b, quietLog()); err != nil {
		t.Fatal(err)
	}
	a := &Adapter{C: c, inFlight: make(chan struct{}, maxInFlight), ExpectedIEEE: b.IEEEString()}
	if err = a.Start(ctx); err != nil {
		t.Fatalf("service start after completing interrupted restore: %v", err)
	}
}
