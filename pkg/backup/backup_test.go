package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/karthikeyansura/s3-backup-go/pkg/s3fs"
	"github.com/karthikeyansura/s3-backup-go/pkg/store"
)

// TestFullBackupLocal creates a test directory, backs it up in local mode,
// then reads back the backup object and verifies its structure.
func TestFullBackupLocal(t *testing.T) {
	srcDir := t.TempDir()

	writeFile(t, filepath.Join(srcDir, "hello.txt"), "Hello, world!\n")
	writeFile(t, filepath.Join(srcDir, "data.bin"), "Some binary data here\x00\x01\x02\x03")

	subDir := filepath.Join(srcDir, "subdir")
	if err := os.Mkdir(subDir, 0755); err != nil {
		t.Fatalf("failed to create subdir: %v", err)
	}
	writeFile(t, filepath.Join(subDir, "nested.txt"), "Nested file content\n")

	if err := os.Symlink("hello.txt", filepath.Join(srcDir, "link.txt")); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	outFile := filepath.Join(t.TempDir(), "backup.img")

	st := store.NewLocalStore()
	cfg := Config{
		Store:      st,
		NewName:    outFile,
		Dir:        srcDir,
		Tag:        "--root--",
		Verbose:    false,
		NoIO:       false,
		VersionIdx: 0,
	}

	result, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("backup failed: %v", err)
	}

	if result.Stats.Files < 3 {
		t.Errorf("expected >= 3 files, got %d", result.Stats.Files)
	}
	if result.Stats.Dirs < 1 {
		t.Errorf("expected >= 1 directory, got %d", result.Stats.Dirs)
	}
	if result.Stats.Symlinks < 1 {
		t.Errorf("expected >= 1 symlink, got %d", result.Stats.Symlinks)
	}

	info, err := os.Stat(outFile)
	if err != nil {
		t.Fatalf("backup output not found: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("backup output is empty")
	}
	if info.Size()%512 != 0 {
		t.Errorf("backup size %d is not sector-aligned", info.Size())
	}

	sbData, err := st.GetRange(context.Background(), outFile, 0, 4096)
	if err != nil {
		t.Fatalf("read superblock: %v", err)
	}

	sb, err := s3fs.ParseSuperblock(sbData)
	if err != nil {
		t.Fatalf("parse superblock: %v", err)
	}

	if sb.Magic != s3fs.Magic {
		t.Errorf("bad magic: 0x%08X", sb.Magic)
	}
	if sb.NVers != 1 {
		t.Errorf("expected 1 version, got %d", sb.NVers)
	}
	if sb.Versions[0].Name != filepath.Base(outFile) {
		t.Errorf("version name: got %q, want %q", sb.Versions[0].Name, filepath.Base(outFile))
	}

	trailerData, err := st.GetRange(context.Background(), outFile, info.Size()-512, 512)
	if err != nil {
		t.Fatalf("read trailer: %v", err)
	}

	rootDE, rootN, err := s3fs.ParseDirent(trailerData)
	if err != nil {
		t.Fatalf("parse root dirent: %v", err)
	}
	if rootDE.Name != "--root--" {
		t.Errorf("root name: got %q, want %q", rootDE.Name, "--root--")
	}
	if rootDE.Mode&0040000 == 0 {
		t.Error("root dirent should be a directory")
	}

	dirlocDE, dirlocN, err := s3fs.ParseDirent(trailerData[rootN:])
	if err != nil {
		t.Fatalf("parse dirloc dirent: %v", err)
	}
	if dirlocDE.Name != "_dirloc_" {
		t.Errorf("dirloc name: got %q, want %q", dirlocDE.Name, "_dirloc_")
	}

	dirdatDE, _, err := s3fs.ParseDirent(trailerData[rootN+dirlocN:])
	if err != nil {
		t.Fatalf("parse dirdat dirent: %v", err)
	}
	if dirdatDE.Name != "_dirdat_" {
		t.Errorf("dirdat name: got %q, want %q", dirdatDE.Name, "_dirdat_")
	}

	statfs := s3fs.ParseStatFS(trailerData[512-s3fs.StatFSSize:])
	if statfs.TotalSectors == 0 {
		t.Error("statfs.TotalSectors should be > 0")
	}

	rootDirData, err := st.GetRange(context.Background(), outFile,
		int64(rootDE.Offset.Sector())*512, int64(rootDE.Bytes))
	if err != nil {
		t.Fatalf("read root dir: %v", err)
	}

	var foundNames []string
	if err := s3fs.IterDirents(rootDirData, func(d s3fs.Dirent) bool {
		foundNames = append(foundNames, d.Name)
		return true
	}); err != nil {
		t.Fatalf("iterate dirents failed: %v", err)
	}

	expectedNames := map[string]bool{
		"hello.txt": false,
		"data.bin":  false,
		"subdir":    false,
		"link.txt":  false,
	}
	for _, name := range foundNames {
		expectedNames[name] = true
	}
	for name, found := range expectedNames {
		if !found {
			t.Errorf("missing directory entry: %s (found: %v)", name, foundNames)
		}
	}

	t.Logf("Backup OK: %d files, %d dirs, %d symlinks, %d sectors, %d bytes",
		result.Stats.Files, result.Stats.Dirs, result.Stats.Symlinks,
		result.Stats.TotalSectors, info.Size())
}

func TestParseSize(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"1024", 1024},
		{"1K", 1024},
		{"1M", 1024 * 1024},
		{"1G", 1024 * 1024 * 1024},
		{"5M", 5 * 1024 * 1024},
		{"100G", 100 * 1024 * 1024 * 1024},
		{"", 0},
	}

	for _, tt := range tests {
		got, err := ParseSize(tt.input)
		if err != nil {
			t.Errorf("ParseSize(%q): %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseSize(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
