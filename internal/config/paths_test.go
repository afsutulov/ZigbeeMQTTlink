package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSamePathAliases(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real")
	os.Mkdir(real, 0700)
	alias := filepath.Join(dir, "alias")
	if err := os.Symlink(real, alias); err != nil {
		t.Fatal(err)
	}
	if !SamePath(filepath.Join(real, "missing.json"), filepath.Join(alias, "missing.json")) {
		t.Fatal("symlinked parent not detected")
	}
	a := filepath.Join(real, "a")
	os.WriteFile(a, []byte("x"), 0600)
	b := filepath.Join(real, "b")
	if err := os.Link(a, b); err != nil {
		t.Fatal(err)
	}
	if !SamePath(a, b) {
		t.Fatal("hardlink not detected")
	}
	if SamePath(a, filepath.Join(real, "other")) {
		t.Fatal("different path rejected")
	}
}
