package mount

import (
	"context"
	"sync"

	"github.com/karthikeyansura/s3-backup-go/pkg/store"
)

const (
	// CacheSize is the number of blocks in the LRU cache.
	CacheSize = 16

	// CacheBlockSize is the size of each cached block (16 MiB).
	CacheBlockSize = 16 * 1024 * 1024
)

// DataCache implements an LRU block cache for file data read from S3.
// It caches aligned 16MB blocks and serves reads from cached data when possible.
type DataCache struct {
	mu      sync.Mutex
	entries [CacheSize]cacheEntry
	seq     int64
	store   store.ObjectStore
}

type cacheEntry struct {
	valid   bool
	seq     int64
	version int
	base    int64
	length  int64
	data    []byte
}

// NewDataCache creates a new LRU data block cache.
func NewDataCache(st store.ObjectStore) *DataCache {
	return &DataCache{store: st}
}

// Read reads data from the specified version's object at the given byte offset.
// It transparently caches 16MB aligned blocks, using LRU eviction.
func (dc *DataCache) Read(ctx context.Context, key string, version int,
	buf []byte, offset int64, maxOffset int64) error {

	remaining := len(buf)
	pos := 0

	for remaining > 0 {
		data, blockOffset, err := dc.getBlock(ctx, key, version, offset, maxOffset)
		if err != nil {
			return err
		}

		inBlockOffset := offset - blockOffset
		available := int64(len(data)) - inBlockOffset
		toCopy := int64(remaining)
		if toCopy > available {
			toCopy = available
		}

		// Prevent infinite loop if EOF is reached within or at the end of a block
		if toCopy <= 0 {
			break
		}

		copy(buf[pos:], data[inBlockOffset:inBlockOffset+toCopy])
		pos += int(toCopy)
		offset += toCopy
		remaining -= int(toCopy)
	}

	return nil
}

// getBlock returns cached data for the block containing the given offset.
// Returns the block data and the block's base offset.
func (dc *DataCache) getBlock(ctx context.Context, key string, version int,
	offset int64, maxOffset int64) ([]byte, int64, error) {

	base := offset & ^(int64(CacheBlockSize) - 1)

	dc.mu.Lock()
	defer dc.mu.Unlock()

	for i := range dc.entries {
		e := &dc.entries[i]
		if e.valid && e.base == base && e.version == version {
			e.seq = dc.seq
			dc.seq++
			return e.data[:e.length], e.base, nil
		}
	}

	slot := -1
	for i := range dc.entries {
		if !dc.entries[i].valid {
			slot = i
			break
		}
	}

	if slot == -1 {
		minSeq := dc.seq
		for i := range dc.entries {
			if dc.entries[i].seq < minSeq {
				minSeq = dc.entries[i].seq
				slot = i
			}
		}
	}

	e := &dc.entries[slot]

	if e.data == nil {
		e.data = make([]byte, CacheBlockSize)
	}

	readLen := int64(CacheBlockSize)
	if base+readLen > maxOffset {
		readLen = maxOffset - base
	}
	if readLen <= 0 {
		return nil, base, nil
	}

	// Lock is strictly held during I/O to prevent slot-reservation races.
	// For a production FUSE mount, this requires a refactor to a concurrent
	// reservation pattern (e.g., using an 'inflight' status channel per block)
	// to avoid stalling parallel reads.
	data, err := dc.store.GetRange(ctx, key, base, readLen)
	if err != nil {
		return nil, 0, err
	}

	copy(e.data, data)
	e.valid = true
	e.version = version
	e.base = base
	e.length = int64(len(data))
	e.seq = dc.seq
	dc.seq++

	return e.data[:e.length], e.base, nil
}

// ReadDirect reads data without caching. Used when cache is disabled.
func ReadDirect(ctx context.Context, st store.ObjectStore, key string,
	buf []byte, offset int64) error {

	data, err := st.GetRange(ctx, key, offset, int64(len(buf)))
	if err != nil {
		return err
	}
	copy(buf, data)
	return nil
}
