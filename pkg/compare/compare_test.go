package compare

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/karthikeyansura/s3-backup-go/pkg/backup"
	"github.com/karthikeyansura/s3-backup-go/pkg/mount"
	"github.com/karthikeyansura/s3-backup-go/pkg/store"
)

func TestCompareMatches(t *testing.T) {
	srcDir := t.TempDir()

	writeFile(t, filepath.Join(srcDir, "hello.txt"), "Hello, world!\n")
	writeFile(t, filepath.Join(srcDir, "data.bin"), "Some binary data\x00\x01")

	subDir := filepath.Join(srcDir, "subdir")
	if err := os.Mkdir(subDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeFile(t, filepath.Join(subDir, "nested.txt"), "Nested content\n")

	if err := os.Symlink("hello.txt", filepath.Join(srcDir, "link.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	outFile := filepath.Join(t.TempDir(), "backup.img")
	st := store.NewLocalStore()
	ctx := context.Background()

	_, err := backup.Run(ctx, backup.Config{
		Store:      st,
		NewName:    outFile,
		Dir:        srcDir,
		Tag:        "--root--",
		VersionIdx: 0,
	})
	if err != nil {
		t.Fatalf("backup: %v", err)
	}

	arch, err := mount.OpenArchive(ctx, st, outFile, false)
	if err != nil {
		t.Fatalf("open archive: %v", err)
	}
	defer func() { _ = arch.Close() }()

	rep, err := Tree(ctx, arch, srcDir)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}

	if !rep.OK() {
		for _, m := range rep.MissingInBackup {
			t.Errorf("missing in backup: %s", m)
		}
		for _, m := range rep.MissingInLocal {
			t.Errorf("missing locally: %s", m)
		}
		for _, m := range rep.Mismatches {
			t.Errorf("mismatch: %s", m)
		}
		t.FailNow()
	}

	t.Logf("compare OK: %d entries compared", rep.Compared)
}

func TestCompareDetectsModification(t *testing.T) {
	srcDir := t.TempDir()
	writeFile(t, filepath.Join(srcDir, "mutable.txt"), "original content")

	outFile := filepath.Join(t.TempDir(), "backup.img")
	st := store.NewLocalStore()
	ctx := context.Background()

	_, err := backup.Run(ctx, backup.Config{
		Store:      st,
		NewName:    outFile,
		Dir:        srcDir,
		Tag:        "--root--",
		VersionIdx: 0,
	})
	if err != nil {
		t.Fatalf("backup: %v", err)
	}

	writeFile(t, filepath.Join(srcDir, "mutable.txt"), "modified content!")

	arch, err := mount.OpenArchive(ctx, st, outFile, false)
	if err != nil {
		t.Fatalf("open archive: %v", err)
	}
	defer func() { _ = arch.Close() }()

	rep, err := Tree(ctx, arch, srcDir)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}

	if rep.OK() {
		t.Fatal("expected comparison to detect modification, but got OK")
	}

	found := false
	for _, m := range rep.Mismatches {
		t.Logf("detected: %s", m)
		found = true
	}
	if !found {
		t.Error("expected at least one mismatch for modified file")
	}
}

func TestCompareDetectsNewFile(t *testing.T) {
	srcDir := t.TempDir()
	writeFile(t, filepath.Join(srcDir, "original.txt"), "original")

	outFile := filepath.Join(t.TempDir(), "backup.img")
	st := store.NewLocalStore()
	ctx := context.Background()

	_, err := backup.Run(ctx, backup.Config{
		Store:      st,
		NewName:    outFile,
		Dir:        srcDir,
		Tag:        "--root--",
		VersionIdx: 0,
	})
	if err != nil {
		t.Fatalf("backup: %v", err)
	}

	writeFile(t, filepath.Join(srcDir, "new_file.txt"), "new content")

	arch, err := mount.OpenArchive(ctx, st, outFile, false)
	if err != nil {
		t.Fatalf("open archive: %v", err)
	}
	defer func() { _ = arch.Close() }()

	rep, err := Tree(ctx, arch, srcDir)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}

	if rep.OK() {
		t.Fatal("expected comparison to detect new file, but got OK")
	}

	if len(rep.MissingInBackup) == 0 {
		t.Error("expected at least one 'missing in backup' entry")
	}
	for _, m := range rep.MissingInBackup {
		t.Logf("missing in backup: %s", m)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
