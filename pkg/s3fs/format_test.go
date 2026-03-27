package s3fs

import (
	"testing"
)

func TestS3Offset(t *testing.T) {
	tests := []struct {
		sector uint64
		object uint16
	}{
		{0, 0},
		{1, 0},
		{0, 1},
		{0xFFFFFFFFFFFF, 0xFFFF}, // max values
		{1024, 3},
		{999999, 7},
	}

	for _, tt := range tests {
		off := NewS3Offset(tt.sector, tt.object)
		if off.Sector() != tt.sector {
			t.Errorf("NewS3Offset(%d, %d).Sector() = %d, want %d",
				tt.sector, tt.object, off.Sector(), tt.sector)
		}
		if off.Object() != tt.object {
			t.Errorf("NewS3Offset(%d, %d).Object() = %d, want %d",
				tt.sector, tt.object, off.Object(), tt.object)
		}
	}
}

func TestBytesXattr(t *testing.T) {
	tests := []struct {
		bytes uint64
		xattr uint16
	}{
		{0, 0},
		{1234567, 0},
		{0, 15},
		{999999999, 4095},        // max xattr (12 bits)
		{0xFFFFFFFFFFFFF, 0xFFF}, // max values
	}

	for _, tt := range tests {
		packed := PackBytesXattr(tt.bytes, tt.xattr)
		gotBytes := UnpackBytes(packed)
		gotXattr := UnpackXattr(packed)

		if gotBytes != tt.bytes {
			t.Errorf("PackBytesXattr(%d, %d): UnpackBytes = %d, want %d",
				tt.bytes, tt.xattr, gotBytes, tt.bytes)
		}
		if gotXattr != tt.xattr {
			t.Errorf("PackBytesXattr(%d, %d): UnpackXattr = %d, want %d",
				tt.bytes, tt.xattr, gotXattr, tt.xattr)
		}
	}
}

func TestDirentRoundTrip(t *testing.T) {
	original := Dirent{
		Mode:    0o40755, // directory
		UID:     1000,
		GID:     1000,
		Ctime:   1700000000,
		Offset:  NewS3Offset(42, 1),
		Bytes:   8192,
		Xattr:   0,
		NameLen: 7,
		Name:    "testdir",
	}

	buf := make([]byte, original.Size()+10) // extra space
	n := original.MarshalDirent(buf)
	if n != original.Size() {
		t.Fatalf("MarshalDirent returned %d, want %d", n, original.Size())
	}

	parsed, consumed, err := ParseDirent(buf)
	if err != nil {
		t.Fatalf("ParseDirent: %v", err)
	}
	if consumed != original.Size() {
		t.Errorf("ParseDirent consumed %d bytes, want %d", consumed, original.Size())
	}

	if parsed.Mode != original.Mode {
		t.Errorf("Mode: got %o, want %o", parsed.Mode, original.Mode)
	}
	if parsed.UID != original.UID {
		t.Errorf("UID: got %d, want %d", parsed.UID, original.UID)
	}
	if parsed.GID != original.GID {
		t.Errorf("GID: got %d, want %d", parsed.GID, original.GID)
	}
	if parsed.Ctime != original.Ctime {
		t.Errorf("Ctime: got %d, want %d", parsed.Ctime, original.Ctime)
	}
	if parsed.Offset.Sector() != original.Offset.Sector() {
		t.Errorf("Offset.Sector: got %d, want %d", parsed.Offset.Sector(), original.Offset.Sector())
	}
	if parsed.Offset.Object() != original.Offset.Object() {
		t.Errorf("Offset.Object: got %d, want %d", parsed.Offset.Object(), original.Offset.Object())
	}
	if parsed.Bytes != original.Bytes {
		t.Errorf("Bytes: got %d, want %d", parsed.Bytes, original.Bytes)
	}
	if parsed.Xattr != original.Xattr {
		t.Errorf("Xattr: got %d, want %d", parsed.Xattr, original.Xattr)
	}
	if parsed.Name != original.Name {
		t.Errorf("Name: got %q, want %q", parsed.Name, original.Name)
	}
}

func TestDirentIteration(t *testing.T) {
	// Create a buffer with 3 directory entries
	entries := []Dirent{
		{Mode: 0o100644, UID: 1000, GID: 1000, Ctime: 1700000000,
			Offset: NewS3Offset(10, 0), Bytes: 100, NameLen: 5, Name: "file1"},
		{Mode: 0o100644, UID: 1000, GID: 1000, Ctime: 1700000001,
			Offset: NewS3Offset(20, 0), Bytes: 200, NameLen: 5, Name: "file2"},
		{Mode: 0o40755, UID: 1000, GID: 1000, Ctime: 1700000002,
			Offset: NewS3Offset(30, 0), Bytes: 4096, NameLen: 6, Name: "subdir"},
	}

	// Calculate total size
	totalSize := 0
	for _, e := range entries {
		totalSize += e.Size()
	}

	buf := make([]byte, totalSize+SectorSize) // extra for padding
	offset := 0
	for _, e := range entries {
		offset += e.MarshalDirent(buf[offset:])
	}

	// Iterate and verify
	var found []string
	err := IterDirents(buf[:offset], func(d Dirent) bool {
		found = append(found, d.Name)
		return true
	})
	if err != nil {
		t.Fatalf("IterDirents: %v", err)
	}

	if len(found) != 3 {
		t.Fatalf("IterDirents found %d entries, want 3", len(found))
	}
	for i, name := range []string{"file1", "file2", "subdir"} {
		if found[i] != name {
			t.Errorf("entry %d: got %q, want %q", i, found[i], name)
		}
	}
}

func TestLookupDirent(t *testing.T) {
	entries := []Dirent{
		{Mode: 0o100644, UID: 1000, GID: 1000, Ctime: 1700000000,
			Offset: NewS3Offset(10, 0), Bytes: 100, NameLen: 5, Name: "file1"},
		{Mode: 0o100644, UID: 1000, GID: 1000, Ctime: 1700000001,
			Offset: NewS3Offset(20, 0), Bytes: 200, NameLen: 5, Name: "file2"},
	}

	buf := make([]byte, 1024)
	offset := 0
	for _, e := range entries {
		offset += e.MarshalDirent(buf[offset:])
	}

	// Found
	d, ok := LookupDirent(buf[:offset], "file2")
	if !ok {
		t.Fatal("LookupDirent(file2) not found")
	}
	if d.Bytes != 200 {
		t.Errorf("file2 bytes: got %d, want 200", d.Bytes)
	}

	// Not found
	_, ok = LookupDirent(buf[:offset], "file3")
	if ok {
		t.Error("LookupDirent(file3) should not be found")
	}
}

func TestDirLocRoundTrip(t *testing.T) {
	locs := []DirLoc{
		{Offset: NewS3Offset(100, 2), Bytes: 4096},
		{Offset: NewS3Offset(200, 3), Bytes: 8192},
	}

	data := MarshalDirLocs(locs)
	parsed := ParseDirLocs(data)

	if len(parsed) != len(locs) {
		t.Fatalf("ParseDirLocs: got %d entries, want %d", len(parsed), len(locs))
	}

	for i := range locs {
		if parsed[i].Offset.Raw() != locs[i].Offset.Raw() {
			t.Errorf("loc %d offset: got %d, want %d", i, parsed[i].Offset.Raw(), locs[i].Offset.Raw())
		}
		if parsed[i].Bytes != locs[i].Bytes {
			t.Errorf("loc %d bytes: got %d, want %d", i, parsed[i].Bytes, locs[i].Bytes)
		}
	}
}

func TestStatFSRoundTrip(t *testing.T) {
	original := StatFS{
		TotalSectors: 10000,
		Files:        500,
		FileSectors:  8000,
		Dirs:         50,
		DirSectors:   200,
		DirBytes:     90000,
		Symlinks:     10,
		SymSectors:   5,
	}

	buf := make([]byte, StatFSSize)
	original.Marshal(buf)
	parsed := ParseStatFS(buf)

	if parsed != original {
		t.Errorf("StatFS round-trip failed:\ngot  %+v\nwant %+v", parsed, original)
	}
}

func TestRoundUp(t *testing.T) {
	tests := []struct {
		a, b, want int64
	}{
		{0, 512, 0},
		{1, 512, 512},
		{511, 512, 512},
		{512, 512, 512},
		{513, 512, 1024},
		{1023, 512, 1024},
		{1024, 512, 1024},
	}

	for _, tt := range tests {
		got := RoundUp(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("RoundUp(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestSuperblockRoundTrip(t *testing.T) {
	buf, sectors := MakeSuperblock("backup-2024-01-01", nil)

	if sectors < 1 {
		t.Fatalf("MakeSuperblock returned %d sectors", sectors)
	}
	if len(buf) != sectors*SectorSize {
		t.Fatalf("buffer size %d != sectors*512 (%d)", len(buf), sectors*SectorSize)
	}

	sb, err := ParseSuperblock(buf)
	if err != nil {
		t.Fatalf("ParseSuperblock: %v", err)
	}

	if sb.Magic != Magic {
		t.Errorf("magic: got 0x%08X, want 0x%08X", sb.Magic, Magic)
	}
	if sb.NVers != 1 {
		t.Errorf("nvers: got %d, want 1", sb.NVers)
	}
	if sb.Versions[0].Name != "backup-2024-01-01" {
		t.Errorf("version name: got %q, want %q", sb.Versions[0].Name, "backup-2024-01-01")
	}

	names := sb.VersionNames()
	if len(names) != 1 || names[0] != "backup-2024-01-01" {
		t.Errorf("VersionNames: got %v, want [backup-2024-01-01]", names)
	}
}
