package fsck

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/karthikeyansura/s3-backup-go/pkg/backup"
	"github.com/karthikeyansura/s3-backup-go/pkg/store"
)

func TestCheckLocalBackup(t *testing.T) {
	srcDir := t.TempDir()

	writeFile(t, filepath.Join(srcDir, "hello.txt"), "Hello, world!\n")
	writeFile(t, filepath.Join(srcDir, "data.bin"), "Some binary data here\x00\x01\x02\x03")

	subDir := filepath.Join(srcDir, "subdir")
	if err := os.Mkdir(subDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeFile(t, filepath.Join(subDir, "nested.txt"), "Nested file content\n")

	if err := os.Symlink("hello.txt", filepath.Join(srcDir, "link.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	outFile := filepath.Join(t.TempDir(), "backup.img")
	st := store.NewLocalStore()
	ctx := context.Background()

	result, err := backup.Run(ctx, backup.Config{
		Store:      st,
		NewName:    outFile,
		Dir:        srcDir,
		Tag:        "--root--",
		VersionIdx: 0,
	})
	if err != nil {
		t.Fatalf("backup: %v", err)
	}

	// Sanity check the backup file exists.
	if _, err := os.Stat(outFile); err != nil {
		t.Fatalf("backup file missing: %v", err)
	}

	t.Logf("backup: %d files, %d dirs, %d symlinks, %d sectors",
		result.Stats.Files, result.Stats.Dirs, result.Stats.Symlinks, result.Stats.TotalSectors)

	rep, err := Check(ctx, st, outFile)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}

	for _, w := range rep.Warnings {
		t.Logf("fsck warning: %s", w)
	}

	if !rep.OK() {
		for _, e := range rep.Errors {
			t.Errorf("fsck error: %s", e)
		}
		t.FailNow()
	}

	if rep.Directories < 1 {
		t.Errorf("expected >= 1 directory, got %d", rep.Directories)
	}
	if rep.Files < 2 {
		t.Errorf("expected >= 2 files, got %d", rep.Files)
	}
	if rep.Symlinks < 1 {
		t.Errorf("expected >= 1 symlink, got %d", rep.Symlinks)
	}

	t.Logf("fsck OK: %d dirs, %d files, %d symlinks", rep.Directories, rep.Files, rep.Symlinks)
}

func TestCheckEmptyDir(t *testing.T) {
	srcDir := t.TempDir()
	outFile := filepath.Join(t.TempDir(), "empty.img")
	st := store.NewLocalStore()

	_, err := backup.Run(context.Background(), backup.Config{
		Store:      st,
		NewName:    outFile,
		Dir:        srcDir,
		Tag:        "--root--",
		VersionIdx: 0,
	})
	if err != nil {
		t.Fatalf("backup: %v", err)
	}

	rep, err := Check(context.Background(), st, outFile)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}

	if !rep.OK() {
		for _, e := range rep.Errors {
			t.Errorf("fsck error: %s", e)
		}
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
