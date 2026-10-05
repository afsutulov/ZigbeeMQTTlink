package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStrictJSONDuplicateKey(t *testing.T) {
	var v map[string]any
	if err := StrictJSON([]byte(`{"a":1,"a":2}`), &v); err == nil {
		t.Fatal("duplicate key accepted")
	}
}

func TestWebAuthPair(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.json")
	os.WriteFile(path, []byte(`{"version":1,"serial":{"port":"/dev/null","baudrate":115200},"web":{"enabled":true,"listen":"0.0.0.0:8080","user":"admin"}}`), 0600)
	if _, err := Load(path); err == nil {
		t.Fatal("user without password accepted")
	}
	os.WriteFile(path, []byte(`{"version":1,"serial":{"port":"/dev/null","baudrate":115200},"web":{"enabled":true,"listen":"127.0.0.1:8080"}}`), 0600)
	c, err := Load(path)
	if err != nil || !c.Web.Loopback() {
		t.Fatalf("loopback config: %v", err)
	}
}

func TestRefuseUnprotectedNetworkAndExamplePasswords(t *testing.T) {
	for _, raw := range []string{
		`{"serial":{"port":"/dev/null"},"web":{"listen":"0.0.0.0:8080"}}`,
		`{"serial":{"port":"/dev/null"},"web":{"listen":"0.0.0.0:8080","user":"admin","password":"CHANGE_ME"}}`,
		`{"serial":{"port":"/dev/null"},"web":{"listen":"127.0.0.1:8080"},"mqtt":{"password":"CHANGE_ME"}}`,
	} {
		path := filepath.Join(t.TempDir(), "c.json")
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil {
			t.Fatalf("unsafe example accepted: %s", raw)
		}
	}
}

func TestNamesDoNotOverlapCommandTopics(t *testing.T) {
	for _, name := range []string{"kitchen/set/state", "kitchen/get/value", "set/lamp", "get/lamp"} {
		if Name(name) {
			t.Fatalf("command-ambiguous name accepted: %s", name)
		}
	}
	if !Name("kitchen/lamp") {
		t.Fatal("ordinary nested name refused")
	}
}

func TestCoordinatorBackupPath(t *testing.T) {
	dir := t.TempDir()
	write := func(extra string) error {
		p := filepath.Join(dir, "c.json")
		os.WriteFile(p, []byte(`{"version":1,"mqtt":{"password":"x"},"serial":{"port":"/dev/null"},"web":{"enabled":false}`+extra+`}`), 0o600)
		_, err := Load(p)
		return err
	}
	if err := write(""); err != nil {
		t.Fatal(err)
	}
	c, _ := Load(filepath.Join(dir, "c.json"))
	if c.CoordinatorBackup != filepath.Join(dir, "coordinator_backup.json") {
		t.Fatalf("default backup path %q", c.CoordinatorBackup)
	}
	for _, bad := range []string{`,"coordinator_backup":"devices.json"`, `,"coordinator_backup":"c.json"`, `,"coordinator_backup":""`} {
		if write(bad) == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}
