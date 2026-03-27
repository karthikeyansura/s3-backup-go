package s3fs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/google/uuid"
)

// Version represents a single entry in the superblock's version chain.
// Versions are stored newest-first in the superblock, but numbered
// with 0 being the oldest (for constructing S3Offset.Object values).
type Version struct {
	UUID    uuid.UUID
	NameLen uint16
	Name    string
}

// VersionFixedSize is the serialized size of the UUID and NameLen fields.
const VersionFixedSize = 18

// Size returns the total serialized size of this version entry.
func (v *Version) Size() int {
	return VersionFixedSize + int(v.NameLen)
}

// ParseVersion reads a single version entry from a byte slice.
func ParseVersion(data []byte) (Version, int, error) {
	if len(data) < VersionFixedSize {
		return Version{}, 0, errors.New("s3fs: buffer too small for version")
	}

	var v Version
	copy(v.UUID[:], data[0:16])
	v.NameLen = binary.LittleEndian.Uint16(data[16:18])

	total := VersionFixedSize + int(v.NameLen)
	if len(data) < total {
		return Version{}, 0, fmt.Errorf("s3fs: buffer too small for version name (need %d, have %d)", total, len(data))
	}

	v.Name = string(data[18:total])
	return v, total, nil
}

// Marshal serializes a version entry into a byte slice.
func (v *Version) Marshal(buf []byte) int {
	copy(buf[0:16], v.UUID[:])
	binary.LittleEndian.PutUint16(buf[16:18], v.NameLen)
	copy(buf[18:], v.Name)
	return v.Size()
}

// Superblock is the header of an S3 backup object.
type Superblock struct {
	Magic    uint32
	Version  uint32
	Flags    uint32
	Len      uint32
	NVers    uint32
	Versions []Version
}

// SuperblockFixedSize is the size of the fixed header fields.
const SuperblockFixedSize = 20

// ParseSuperblock reads a superblock from a byte slice (typically 4096 bytes
// read from offset 0 of the S3 object).
func ParseSuperblock(data []byte) (*Superblock, error) {
	if len(data) < SuperblockFixedSize {
		return nil, errors.New("s3fs: buffer too small for superblock")
	}

	sb := &Superblock{
		Magic:   binary.LittleEndian.Uint32(data[0:4]),
		Version: binary.LittleEndian.Uint32(data[4:8]),
		Flags:   binary.LittleEndian.Uint32(data[8:12]),
		Len:     binary.LittleEndian.Uint32(data[12:16]),
		NVers:   binary.LittleEndian.Uint32(data[16:20]),
	}

	if sb.Magic != Magic {
		return nil, fmt.Errorf("s3fs: bad magic 0x%08X (expected 0x%08X)", sb.Magic, Magic)
	}

	offset := SuperblockFixedSize
	sb.Versions = make([]Version, 0, sb.NVers)
	for i := uint32(0); i < sb.NVers; i++ {
		v, n, err := ParseVersion(data[offset:])
		if err != nil {
			return nil, fmt.Errorf("s3fs: parsing version %d: %w", i, err)
		}
		sb.Versions = append(sb.Versions, v)
		offset += n
	}

	return sb, nil
}

// MakeSuperblock creates a new superblock for a backup object.
// Returns the superblock serialized into a sector-aligned buffer.
func MakeSuperblock(newName string, prevVersions []Version) ([]byte, int) {
	buf := make([]byte, 4096)

	binary.LittleEndian.PutUint32(buf[0:4], Magic)
	binary.LittleEndian.PutUint32(buf[4:8], 1)
	binary.LittleEndian.PutUint32(buf[8:12], FlagDirLoc|FlagDirData|FlagDirPacked)

	newUUID := uuid.New()
	offset := SuperblockFixedSize
	nvers := uint32(1)
	baseName := filepath.Base(newName)

	v := Version{
		UUID:    newUUID,
		NameLen: uint16(len(baseName)),
		Name:    baseName,
	}
	offset += v.Marshal(buf[offset:])

	for _, pv := range prevVersions {
		offset += pv.Marshal(buf[offset:])
		nvers++
	}

	binary.LittleEndian.PutUint32(buf[16:20], nvers)

	lenSectors := uint32(RoundUp(int64(offset), SectorSize) / SectorSize)
	binary.LittleEndian.PutUint32(buf[12:16], lenSectors)

	totalBytes := int(lenSectors) * SectorSize
	return buf[:totalBytes], int(lenSectors)
}

// VersionNames returns the object names in oldest-to-newest order.
func (sb *Superblock) VersionNames() []string {
	names := make([]string, len(sb.Versions))
	for i, v := range sb.Versions {
		names[len(names)-1-i] = v.Name
	}
	return names
}

// VersionUUIDs returns UUIDs in oldest-to-newest order.
func (sb *Superblock) VersionUUIDs() []uuid.UUID {
	uuids := make([]uuid.UUID, len(sb.Versions))
	for i, v := range sb.Versions {
		uuids[len(uuids)-1-i] = v.UUID
	}
	return uuids
}
