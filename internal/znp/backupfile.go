package znp

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"zigbeemqttlink/internal/store"
)

// SaveResult describes where a backup ended up.
type SaveResult struct {
	Path      string   // file written
	Preserved string   // a backup of another network moved/kept here, if any
	Merged    []string // devices kept from the previous backup of this network
}

// alternatePath names a backup file after its network.
func alternatePath(path string, b *Backup, suffix string) string {
	base := strings.TrimSuffix(path, filepath.Ext(path))
	name := base + "-" + hex.EncodeToString(b.ExtendedPanID)
	if suffix != "" {
		name += "-" + suffix
	}
	return name + ".json"
}

// SaveBackupFile writes b atomically with owner-only permissions. A backup of
// a different network found at path is never overwritten:
//   - replaceOther=false (automatic backups): b is written next to it under a
//     name containing its extended PAN ID, the existing file stays as is;
//   - replaceOther=true (after deliberate formation/restore): the existing
//     file is first moved to such a name, then b takes its place.
//
// Devices with link keys that disappeared from the coordinator tables but are
// still paired are carried over from the previous backup of the same network.
func SaveBackupFile(path string, b *Backup, paired func(ieee string) bool, replaceOther bool) (SaveResult, error) {
	res := SaveResult{Path: path}
	if err := b.Validate(); err != nil {
		return res, err
	}
	old, err := ReadBackupFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		old = nil
	case err != nil:
		if !replaceOther {
			return res, fmt.Errorf("existing backup %s is unreadable; left untouched: %w", path, err)
		}
		keep, rerr := preserveBackup(path, strings.TrimSuffix(path, filepath.Ext(path))+"-unreadable")
		if rerr != nil {
			return res, rerr
		}
		res.Preserved, old = keep, nil
	case !old.SameNetwork(b):
		if replaceOther {
			keep, rerr := preserveBackup(path, strings.TrimSuffix(alternatePath(path, old, ""), ".json"))
			if rerr != nil {
				return res, fmt.Errorf("could not preserve the backup of the previous network: %w", rerr)
			}
			res.Preserved, old = keep, nil
		} else {
			res.Preserved = path
			res.Path = alternatePath(path, b, "")
			var aerr error
			old, aerr = ReadBackupFile(res.Path)
			if aerr != nil && !errors.Is(aerr, os.ErrNotExist) {
				return res, fmt.Errorf("alternate backup %s is unreadable; left untouched: %w", res.Path, aerr)
			}
			if old != nil && !old.SameNetwork(b) {
				return res, fmt.Errorf("refusing to overwrite %s: it holds yet another network", res.Path)
			}
		}
	}
	b.RaiseCountersFrom(old)
	if paired != nil {
		res.Merged = b.MergeMissingDevices(old, paired)
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return res, err
	}
	return res, store.AtomicWrite(res.Path, append(raw, '\n'))
}

// Reserve a unique destination: second-resolution timestamps could replace
// an already preserved backup. Rename overwrites only our own empty file.
func preserveBackup(path, base string) (string, error) {
	f, err := os.CreateTemp(filepath.Dir(base), filepath.Base(base)+"-*.json")
	if err != nil {
		return "", err
	}
	keep := f.Name()
	if err = f.Close(); err != nil {
		os.Remove(keep)
		return "", err
	}
	if err = os.Rename(path, keep); err != nil {
		os.Remove(keep)
		return "", err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return keep, err
	}
	defer dir.Close()
	return keep, dir.Sync()
}
