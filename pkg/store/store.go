// Package store provides an abstraction over S3 and local file I/O
// for reading and writing backup objects.
package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
)

// ObjectStore is the interface for reading and writing backup objects.
type ObjectStore interface {
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	Size(ctx context.Context, key string) (int64, error)
	NewWriter(ctx context.Context, key string) (io.WriteCloser, error)
}

// LocalStore implements ObjectStore using local file paths.
type LocalStore struct{}

// NewLocalStore initializes a new LocalStore.
func NewLocalStore() *LocalStore {
	return &LocalStore{}
}

// GetRange retrieves a specific byte range of a local file.
func (s *LocalStore) GetRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
	f, err := os.Open(key)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", key, err)
	}
	defer func() { _ = f.Close() }()

	buf := make([]byte, length)
	n, err := f.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("store: read %s at %d: %w", key, offset, err)
	}
	return buf[:n], nil
}

// Size returns the total size of the local file in bytes.
func (s *LocalStore) Size(_ context.Context, key string) (int64, error) {
	info, err := os.Stat(key)
	if err != nil {
		return 0, fmt.Errorf("store: stat %s: %w", key, err)
	}
	return info.Size(), nil
}

// NewWriter initializes a new writer for a local file.
func (s *LocalStore) NewWriter(_ context.Context, key string) (io.WriteCloser, error) {
	f, err := os.Create(key)
	if err != nil {
		return nil, fmt.Errorf("store: create %s: %w", key, err)
	}
	return f, nil
}
