package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"zigbeemqttlink/internal/config"
)

type File struct {
	mu        sync.Mutex
	path      string
	max, size int64
	backups   int
	f         *os.File
}

func Open(c config.Logging) (*slog.Logger, io.Closer, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(c.Level)); err != nil {
		return nil, nil, err
	}
	var writers []io.Writer
	if c.Console {
		writers = append(writers, os.Stderr)
	}
	var file *File
	if c.File != "" {
		file = &File{path: c.File, max: int64(c.MaxSizeMB) << 20, backups: c.Backups}
		if err := os.MkdirAll(filepath.Dir(c.File), 0700); err != nil {
			return nil, nil, err
		}
		if err := file.open(); err != nil {
			return nil, nil, err
		}
		writers = append(writers, file)
	}
	return slog.New(slog.NewJSONHandler(io.MultiWriter(writers...), &slog.HandlerOptions{Level: level})), file, nil
}
func (f *File) open() error {
	var err error
	f.f, err = os.OpenFile(f.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	st, err := f.f.Stat()
	if err != nil {
		f.f.Close()
		return err
	}
	f.size = st.Size()
	return nil
}
func (f *File) rotate() error {
	if err := f.f.Sync(); err != nil {
		return err
	}
	if err := f.f.Close(); err != nil {
		return err
	}
	f.f = nil
	// Reopen on failure, so a failed rotation cannot leave logging disabled.
	if err := f.moveBackups(); err != nil {
		_ = f.open()
		return err
	}
	return f.open()
}
func (f *File) moveBackups() error {
	if f.backups == 0 {
		return remove(f.path)
	}
	if err := remove(fmt.Sprintf("%s.%d", f.path, f.backups)); err != nil {
		return err
	}
	for i := f.backups - 1; i >= 1; i-- {
		if err := os.Rename(fmt.Sprintf("%s.%d", f.path, i), fmt.Sprintf("%s.%d", f.path, i+1)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return os.Rename(f.path, f.path+".1")
}
func remove(path string) error {
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
func (f *File) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.f == nil {
		return 0, fmt.Errorf("log file closed")
	}
	if f.size > 0 && f.size+int64(len(p)) > f.max {
		if err := f.rotate(); err != nil {
			fmt.Fprintln(os.Stderr, "ZigbeeMQTTlink log rotation:", err)
			return 0, err
		}
	}
	n, err := f.f.Write(p)
	f.size += int64(n)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ZigbeeMQTTlink log write:", err)
	}
	return n, err
}
func (f *File) Close() error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.f == nil {
		return nil
	}
	err := f.f.Sync()
	e := f.f.Close()
	f.f = nil
	if err != nil {
		return err
	}
	return e
}
