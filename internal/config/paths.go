package config

import (
	"os"
	"path/filepath"
)

// SamePath includes symlinks, symlinked parent directories and existing hard
// links. This is a local configuration guard, not a hostile-filesystem sandbox.
func SamePath(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	if canonicalPath(a) == canonicalPath(b) {
		return true
	}
	x, err := os.Stat(a)
	if err != nil {
		return false
	}
	y, err := os.Stat(b)
	return err == nil && os.SameFile(x, y)
}
func canonicalPath(p string) string {
	p, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	tail := ""
	for {
		if resolved, err := filepath.EvalSymlinks(p); err == nil {
			return filepath.Join(resolved, tail)
		}
		parent := filepath.Dir(p)
		if parent == p {
			return filepath.Join(p, tail)
		}
		tail = filepath.Join(filepath.Base(p), tail)
		p = parent
	}
}
