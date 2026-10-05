package znp

import (
	"encoding/json"
	"testing"
)

func FuzzBackupParse(f *testing.F) {
	f.Add([]byte(herdsmanBackup))
	f.Add([]byte(`{"network_key":null}`))
	f.Add([]byte(`null`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		b, err := ParseBackup(raw)
		if err != nil {
			return
		}
		if err = b.Validate(); err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = ParseBackup(encoded); err != nil {
			t.Fatal(err)
		}
	})
}
