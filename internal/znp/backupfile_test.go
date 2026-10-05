package znp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveBackupFileNeverOverwritesAnotherNetwork(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "coordinator_backup.json")
	first, _ := ParseBackup([]byte(herdsmanBackup))
	if res, err := SaveBackupFile(path, first, nil, false); err != nil || res.Path != path || res.Preserved != "" {
		t.Fatalf("first save %+v %v", res, err)
	}
	if st, _ := os.Stat(path); st.Mode().Perm() != 0o600 {
		t.Fatalf("backup permissions %v", st.Mode().Perm())
	}
	other, _ := ParseBackup([]byte(herdsmanBackup))
	other.ExtendedPanID = []byte{1, 2, 3, 4, 5, 6, 7, 8}
	// Automatic backup: the existing file stays, the new one goes next to it.
	res, err := SaveBackupFile(path, other, nil, false)
	if err != nil || res.Path == path || !strings.Contains(res.Path, "0102030405060708") {
		t.Fatalf("automatic save of another network %+v %v", res, err)
	}
	if kept, _ := ReadBackupFile(path); !kept.SameNetwork(first) {
		t.Fatal("automatic backup overwrote another network")
	}
	// Deliberate replacement: the old file is moved aside under its own name.
	res, err = SaveBackupFile(path, other, nil, true)
	if err != nil || res.Path != path || !strings.Contains(res.Preserved, "00124b0009d69f77") {
		t.Fatalf("replacement %+v %v", res, err)
	}
	if moved, _ := ReadBackupFile(res.Preserved); !moved.SameNetwork(first) {
		t.Fatal("previous network not preserved")
	}
	// An unreadable file is kept, never silently replaced.
	os.WriteFile(path, []byte("garbage"), 0o600)
	res, err = SaveBackupFile(path, first, nil, false)
	if err == nil {
		t.Fatal("automatic backup should leave invalid file untouched")
	}
	if raw, _ := os.ReadFile(path); string(raw) != "garbage" {
		t.Fatal("automatic backup moved invalid file")
	}
	res, err = SaveBackupFile(path, first, nil, true)
	if err != nil || !strings.Contains(res.Preserved, "unreadable") {
		t.Fatalf("unreadable %+v %v", res, err)
	}
	if b, _ := os.ReadFile(res.Preserved); string(b) != "garbage" {
		t.Fatal("unreadable file lost")
	}
}

func TestSaveBackupFileMergesLostLinkKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.json")
	full, _ := ParseBackup([]byte(herdsmanBackup))
	if _, err := SaveBackupFile(path, full, nil, false); err != nil {
		t.Fatal(err)
	}
	partial, _ := ParseBackup([]byte(herdsmanBackup))
	partial.Devices = partial.Devices[:1]
	res, err := SaveBackupFile(path, partial, func(string) bool { return true }, false)
	if err != nil || len(res.Merged) != 6 {
		t.Fatalf("merge %+v %v", res, err)
	}
	if b, _ := ReadBackupFile(path); len(b.Devices) != 7 {
		t.Fatalf("merged file has %d devices", len(b.Devices))
	}
}

func TestReformingClearsOldDevices(t *testing.T) {
	fastTimings(t)
	e, c := newEmulator(t, ProductZStack3x0, 1)
	ctx := testCtx(t)
	if _, err := FormNetwork(ctx, c, fixedNetwork(), quietLog()); err != nil {
		t.Fatal(err)
	}
	e.pair(t, testSeed)
	n2, _ := RandomNetwork(25)
	if _, err := FormNetwork(ctx, c, n2, quietLog()); err != nil {
		t.Fatal(err)
	}
	b, err := CreateBackup(ctx, c, "x")
	if err != nil || len(b.Devices) != 0 || b.PanID != n2.PanID || b.Channel != 25 {
		t.Fatalf("new network %+v %v", b, err)
	}
}
