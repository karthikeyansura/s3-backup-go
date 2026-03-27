// Package s3fs implements the on-disk binary format for S3 backup objects.
//
// The format is a simplified log-structured filesystem stored in a single S3
// object, with 512-byte sectors. All multi-byte integers are little-endian.
// Structures are packed (no alignment padding) to match the C implementation.
package s3fs

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// SectorSize defines the standard block size used in the filesystem.
const SectorSize = 512

// Magic is the S3BU file signature: 'S' | ('3' << 8) | ('B' << 16) | ('U' << 24).
const Magic uint32 = 0x55423353

const (
	// FlagDirLoc identifies a directory location entry.
	FlagDirLoc = 4
	// FlagDirData identifies a directory data entry.
	FlagDirData = 8
	// FlagDirPacked identifies a packed directory entry.
	FlagDirPacked = 16
)

// S3Offset encodes a location within a chain of S3 objects.
// Bits 0-47: sector offset within the object.
// Bits 48-63: object index (0 = oldest version).
type S3Offset uint64

// NewS3Offset creates a valid S3Offset from a sector and object index.
func NewS3Offset(sector uint64, object uint16) S3Offset {
	return S3Offset((sector & 0x0000FFFFFFFFFFFF) | (uint64(object) << 48))
}

// Sector extracts the 48-bit sector offset.
func (o S3Offset) Sector() uint64 { return uint64(o) & 0x0000FFFFFFFFFFFF }

// Object extracts the 16-bit object index.
func (o S3Offset) Object() uint16 { return uint16(o >> 48) }

// IsZero returns true if the offset is completely zero.
func (o S3Offset) IsZero() bool { return o == 0 }

// Raw returns the underlying uint64 representation.
func (o S3Offset) Raw() uint64 { return uint64(o) }

// PackBytesXattr packs a 52-bit byte count and 12-bit xattr length into uint64.
func PackBytesXattr(bytes uint64, xattr uint16) uint64 {
	return (bytes & 0x000FFFFFFFFFFFFF) | (uint64(xattr) << 52)
}

// UnpackBytes extracts the 52-bit byte count.
func UnpackBytes(v uint64) uint64 { return v & 0x000FFFFFFFFFFFFF }

// UnpackXattr extracts the 12-bit xattr length.
func UnpackXattr(v uint64) uint16 { return uint16(v >> 52) }

// Dirent represents a single directory entry in the backup format.
type Dirent struct {
	Mode    uint16
	UID     uint16
	GID     uint16
	Ctime   uint32
	Offset  S3Offset
	Bytes   uint64
	Xattr   uint16
	NameLen uint8
	Name    string
}

// DirentFixedSize is the size of the fixed fields before the variable-length name.
const DirentFixedSize = 27

// Size returns the total serialized size of this directory entry.
func (d *Dirent) Size() int {
	return DirentFixedSize + int(d.NameLen)
}

// ParseDirent reads a single directory entry from a byte slice.
func ParseDirent(data []byte) (Dirent, int, error) {
	if len(data) < DirentFixedSize {
		return Dirent{}, 0, errors.New("s3fs: buffer too small for dirent")
	}

	d := Dirent{
		Mode:   binary.LittleEndian.Uint16(data[0:2]),
		UID:    binary.LittleEndian.Uint16(data[2:4]),
		GID:    binary.LittleEndian.Uint16(data[4:6]),
		Ctime:  binary.LittleEndian.Uint32(data[6:10]),
		Offset: S3Offset(binary.LittleEndian.Uint64(data[10:18])),
	}

	bx := binary.LittleEndian.Uint64(data[18:26])
	d.Bytes = UnpackBytes(bx)
	d.Xattr = UnpackXattr(bx)
	d.NameLen = data[26]

	total := DirentFixedSize + int(d.NameLen)
	if len(data) < total {
		return Dirent{}, 0, fmt.Errorf("s3fs: buffer too small for dirent name (need %d, have %d)", total, len(data))
	}

	d.Name = string(data[27:total])
	return d, total, nil
}

// MarshalDirent serializes a directory entry into a byte slice.
func (d *Dirent) MarshalDirent(buf []byte) int {
	binary.LittleEndian.PutUint16(buf[0:2], d.Mode)
	binary.LittleEndian.PutUint16(buf[2:4], d.UID)
	binary.LittleEndian.PutUint16(buf[4:6], d.GID)
	binary.LittleEndian.PutUint32(buf[6:10], d.Ctime)
	binary.LittleEndian.PutUint64(buf[10:18], uint64(d.Offset))
	binary.LittleEndian.PutUint64(buf[18:26], PackBytesXattr(d.Bytes, d.Xattr))
	buf[26] = d.NameLen
	copy(buf[27:], d.Name)
	return d.Size()
}

// IterDirents iterates over directory entries in a byte slice.
func IterDirents(data []byte, fn func(d Dirent) bool) error {
	offset := 0
	for offset < len(data) {
		allZero := true
		for _, b := range data[offset:] {
			if b != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			break
		}

		d, n, err := ParseDirent(data[offset:])
		if err != nil {
			return fmt.Errorf("s3fs: at offset %d: %w", offset, err)
		}
		if !fn(d) {
			break
		}
		offset += n
	}
	return nil
}

// LookupDirent finds a directory entry by name within a directory's data.
func LookupDirent(dirData []byte, name string) (Dirent, bool) {
	var found Dirent
	var ok bool
	_ = IterDirents(dirData, func(d Dirent) bool {
		if d.Name == name {
			found = d
			ok = true
			return false
		}
		return true
	})
	return found, ok
}

// DirLoc records the location and size of a directory's data.
type DirLoc struct {
	Offset S3Offset
	Bytes  uint32
}

// DirLocSize defines the size of a marshaled DirLoc.
const DirLocSize = 12

// ParseDirLoc decodes a DirLoc from bytes.
func ParseDirLoc(data []byte) DirLoc {
	return DirLoc{
		Offset: S3Offset(binary.LittleEndian.Uint64(data[0:8])),
		Bytes:  binary.LittleEndian.Uint32(data[8:12]),
	}
}

// Marshal encodes a DirLoc into bytes.
func (dl *DirLoc) Marshal(buf []byte) {
	binary.LittleEndian.PutUint64(buf[0:8], uint64(dl.Offset))
	binary.LittleEndian.PutUint32(buf[8:12], dl.Bytes)
}

// ParseDirLocs reads an array of DirLoc entries from a byte slice.
func ParseDirLocs(data []byte) []DirLoc {
	n := len(data) / DirLocSize
	locs := make([]DirLoc, n)
	for i := 0; i < n; i++ {
		locs[i] = ParseDirLoc(data[i*DirLocSize:])
	}
	return locs
}

// MarshalDirLocs serializes an array of DirLoc entries.
func MarshalDirLocs(locs []DirLoc) []byte {
	buf := make([]byte, len(locs)*DirLocSize)
	for i, dl := range locs {
		dl.Marshal(buf[i*DirLocSize:])
	}
	return buf
}

// StatFS represents filesystem statistics.
type StatFS struct {
	TotalSectors uint64
	Files        uint32
	FileSectors  uint64
	Dirs         uint32
	DirSectors   uint64
	DirBytes     uint64
	Symlinks     uint32
	SymSectors   uint64
}

// StatFSSize defines the size of a marshaled StatFS.
const StatFSSize = 52

// ParseStatFS decodes a StatFS from bytes.
func ParseStatFS(data []byte) StatFS {
	return StatFS{
		TotalSectors: binary.LittleEndian.Uint64(data[0:8]),
		Files:        binary.LittleEndian.Uint32(data[8:12]),
		FileSectors:  binary.LittleEndian.Uint64(data[12:20]),
		Dirs:         binary.LittleEndian.Uint32(data[20:24]),
		DirSectors:   binary.LittleEndian.Uint64(data[24:32]),
		DirBytes:     binary.LittleEndian.Uint64(data[32:40]),
		Symlinks:     binary.LittleEndian.Uint32(data[40:44]),
		SymSectors:   binary.LittleEndian.Uint64(data[44:52]),
	}
}

// Marshal encodes a StatFS into bytes.
func (s *StatFS) Marshal(buf []byte) {
	binary.LittleEndian.PutUint64(buf[0:8], s.TotalSectors)
	binary.LittleEndian.PutUint32(buf[8:12], s.Files)
	binary.LittleEndian.PutUint64(buf[12:20], s.FileSectors)
	binary.LittleEndian.PutUint32(buf[20:24], s.Dirs)
	binary.LittleEndian.PutUint64(buf[24:32], s.DirSectors)
	binary.LittleEndian.PutUint64(buf[32:40], s.DirBytes)
	binary.LittleEndian.PutUint32(buf[40:44], s.Symlinks)
	binary.LittleEndian.PutUint64(buf[44:52], s.SymSectors)
}

// RoundUp rounds a up to the next multiple of b.
func RoundUp(a, b int64) int64 {
	return b * ((a + b - 1) / b)
}
