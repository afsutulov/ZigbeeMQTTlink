package store

import (
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
)

// Lock excludes a second native process from writing the same state directory.
func Lock(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("device database is already in use: %w", err)
	}
	return f, nil
}
