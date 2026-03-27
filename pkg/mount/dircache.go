// Package mount implements the FUSE filesystem for mounting S3 backup objects.
package mount

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"

	"github.com/karthikeyansura/s3-backup-go/pkg/s3fs"
)

// DirCache provides zero-copy access to cached directory data using mmap.
//
// Instead of loading all directory data into heap memory, it is written to a
// temp file and mmap'd. This ensures:
//   - Directory data does not consume Go heap memory.
//   - The OS can page it out when under memory pressure.
//   - Lookups use zero-copy pointer arithmetic into the mmap'd region.
type DirCache struct {
	data  []byte
	index map[uint64]dirCacheEntry
	file  *os.File
}

type dirCacheEntry struct {
	offset int
	length int
}

// NewDirCache creates a directory cache from directory locations and packed
// directory data read from an S3 backup object.
func NewDirCache(locs []s3fs.DirLoc, dirData []byte) (*DirCache, error) {
	f, err := os.CreateTemp("", "s3mount-dircache-*")
	if err != nil {
		return nil, fmt.Errorf("dircache: create temp file: %w", err)
	}

	if _, err := f.Write(dirData); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, fmt.Errorf("dircache: write temp file: %w", err)
	}

	// Unlink the file immediately to ensure the OS cleans it up once the file descriptor is closed.
	_ = os.Remove(f.Name())

	size := len(dirData)
	if size == 0 {
		_ = f.Close()
		return &DirCache{
			index: make(map[uint64]dirCacheEntry),
		}, nil
	}

	mapped, err := unix.Mmap(int(f.Fd()), 0, size, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("dircache: mmap: %w", err)
	}

	// Directory data is packed contiguously without sector-boundary padding.
	// The index maps the S3Offset to the absolute byte offset within the mmap'd region.
	index := make(map[uint64]dirCacheEntry, len(locs))
	byteOffset := 0
	for _, loc := range locs {
		if loc.Bytes > 0 {
			index[loc.Offset.Raw()] = dirCacheEntry{
				offset: byteOffset,
				length: int(loc.Bytes),
			}
			byteOffset += int(loc.Bytes)
		}
	}

	return &DirCache{
		data:  mapped,
		index: index,
		file:  f,
	}, nil
}

// FindDir returns the directory data for the given offset, or nil if not found.
// The returned slice points directly into the mmap'd region to avoid allocations.
func (dc *DirCache) FindDir(off s3fs.S3Offset, nbytes uint64) []byte {
	if nbytes == 0 {
		return nil
	}
	entry, ok := dc.index[off.Raw()]
	if !ok {
		return nil
	}
	return dc.data[entry.offset : entry.offset+entry.length]
}

// Close unmaps the memory and closes the backing file descriptor.
func (dc *DirCache) Close() error {
	if dc.data != nil {
		if err := unix.Munmap(dc.data); err != nil {
			return fmt.Errorf("dircache: munmap: %w", err)
		}
		dc.data = nil
	}
	if dc.file != nil {
		_ = dc.file.Close()
		dc.file = nil
	}
	return nil
}
