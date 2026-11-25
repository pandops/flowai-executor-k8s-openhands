// Package seqfile keeps the Executor's publisher_seq counter in a state file
// next to its YAML configuration. Every number is written and fsynced before
// it is returned, so a number is never reused after a crash or restart.
package seqfile

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Counter is the persisted publisher_seq counter.
type Counter struct {
	mu   sync.Mutex
	path string
	file *os.File
	last int64
}

// Open opens or creates the state file at path (mode 0600).
func Open(path string) (*Counter, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("seqfile: open %s: %w", path, err)
	}
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return nil, err
	}
	last, err := Read(path)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Counter{path: path, file: f, last: last}, nil
}

// Next returns the next publisher_seq after persisting it with fsync.
func (c *Counter) Next() (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.last + 1
	text := []byte(strconv.FormatInt(n, 10) + "\n")
	if err := c.file.Truncate(0); err != nil {
		return 0, err
	}
	if _, err := c.file.WriteAt(text, 0); err != nil {
		return 0, err
	}
	if err := c.file.Sync(); err != nil {
		return 0, err
	}
	c.last = n
	return n, nil
}

// Last returns the last persisted number (0 when none).
func (c *Counter) Last() int64 { c.mu.Lock(); defer c.mu.Unlock(); return c.last }

// Path returns the state file path.
func (c *Counter) Path() string { return c.path }

// Close closes the state file.
func (c *Counter) Close() error { return c.file.Close() }

// Read returns the number persisted in the state file at path without
// opening a counter.
func Read(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	text := strings.TrimSpace(string(data))
	if text == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("seqfile: corrupt state file %s", path)
	}
	return n, nil
}
