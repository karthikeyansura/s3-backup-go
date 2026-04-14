// Package fsck validates the on-disk structural consistency of an S3 backup
// object. It checks superblock integrity, version chain UUIDs, directory entry
// bounds, file/symlink data readability, and statfs counter accuracy.
package fsck

import (
	"context"
	"fmt"
	"syscall"

	"github.com/karthikeyansura/s3-backup-go/pkg/mount"
	"github.com/karthikeyansura/s3-backup-go/pkg/s3fs"
	"github.com/karthikeyansura/s3-backup-go/pkg/store"
)

// Report holds the results of a consistency check.
type Report struct {
	Versions    int
	Directories int
	Files       int
	Symlinks    int
	Specials    int
	Warnings    []string
	Errors      []string
}

// OK returns true if no errors were found.
func (r *Report) OK() bool { return len(r.Errors) == 0 }

func (r *Report) errorf(format string, args ...any) {
	r.Errors = append(r.Errors, fmt.Sprintf(format, args...))
}

func (r *Report) warnf(format string, args ...any) {
	r.Warnings = append(r.Warnings, fmt.Sprintf(format, args...))
}

// Check validates structural consistency of a backup object. It opens the
// archive (which already validates superblock magic, version chain UUIDs,
// sector alignment, and packed directory data length), then walks the entire
// directory tree checking every entry.
func Check(ctx context.Context, st store.ObjectStore, objectKey string) (*Report, error) {
	arch, err := mount.OpenArchive(ctx, st, objectKey, false)
	if err != nil {
		return nil, fmt.Errorf("fsck: %w", err)
	}
	defer func() { _ = arch.Close() }()

	rep := &Report{
		Versions: len(arch.Names),
	}

	if arch.RootDE.Mode&syscall.S_IFDIR == 0 {
		rep.errorf("root entry is not a directory (mode 0%o)", arch.RootDE.Mode)
		return rep, nil
	}

	checkBounds(rep, arch, arch.RootDE, "<root>")
	walkDir(ctx, rep, arch, arch.RootDE, "")

	// Cross-check statfs counters. The backup counts _dirloc_ and _dirdat_
	// hidden entries as files, so statfs.Files is typically walkedFiles + 2.
	expectedFiles := rep.Files + 2
	if int(arch.Statfs.Files) != expectedFiles {
		rep.warnf("statfs.Files=%d, expected %d (walked %d + 2 hidden entries)",
			arch.Statfs.Files, expectedFiles, rep.Files)
	}
	if int(arch.Statfs.Dirs) != rep.Directories {
		rep.warnf("statfs.Dirs=%d but walked %d directories", arch.Statfs.Dirs, rep.Directories)
	}
	if int(arch.Statfs.Symlinks) != rep.Symlinks {
		rep.warnf("statfs.Symlinks=%d but walked %d symlinks", arch.Statfs.Symlinks, rep.Symlinks)
	}

	return rep, nil
}

// walkDir iterates over directory children using manual parsing (not
// IterDirents) so errors propagate cleanly instead of being swallowed
// by the callback return value.
func walkDir(ctx context.Context, rep *Report, arch *mount.Archive, de s3fs.Dirent, path string) {
	rep.Directories++

	dirData := arch.DirCache.FindDir(de.Offset, de.Bytes)
	if dirData == nil {
		if de.Bytes > 0 {
			rep.errorf("%s: directory data missing (offset=%v bytes=%d)", path, de.Offset, de.Bytes)
		}
		return
	}

	offset := 0
	for offset < len(dirData) {
		allZero := true
		for _, b := range dirData[offset:] {
			if b != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			break
		}

		child, n, err := s3fs.ParseDirent(dirData[offset:])
		if err != nil {
			rep.errorf("%s: parse dirent at byte %d: %v", path, offset, err)
			return
		}
		offset += n

		childPath := child.Name
		if path != "" {
			childPath = path + "/" + child.Name
		}

		checkBounds(rep, arch, child, childPath)
		checkData(ctx, rep, arch, child, childPath)

		kind := child.Mode & syscall.S_IFMT
		switch kind {
		case syscall.S_IFDIR:
			walkDir(ctx, rep, arch, child, childPath)
		case syscall.S_IFREG:
			rep.Files++
		case syscall.S_IFLNK:
			rep.Symlinks++
		default:
			rep.Specials++
		}
	}
}

// checkBounds verifies the dirent's object index and that the full data span
// (sector offset + ceil(bytes/512) sectors) fits within the version object.
func checkBounds(rep *Report, arch *mount.Archive, de s3fs.Dirent, path string) {
	obj := int(de.Offset.Object())
	if obj < 0 || obj >= len(arch.Names) {
		rep.errorf("%s: object index %d out of range [0, %d)", path, obj, len(arch.Names))
		return
	}

	maxSector := arch.NSectors[obj]
	startSector := int64(de.Offset.Sector())
	if startSector > maxSector {
		rep.errorf("%s: sector %d exceeds object %s size %d sectors",
			path, startSector, arch.Names[obj], maxSector)
		return
	}

	// For files and symlinks, verify the entire data span fits.
	kind := de.Mode & syscall.S_IFMT
	if (kind == syscall.S_IFREG || kind == syscall.S_IFLNK) && de.Bytes > 0 {
		dataSectors := s3fs.RoundUp(int64(de.Bytes), s3fs.SectorSize) / s3fs.SectorSize
		endSector := startSector + dataSectors
		if endSector > maxSector {
			rep.errorf("%s: data span [%d, %d) exceeds object %s size %d sectors",
				path, startSector, endSector, arch.Names[obj], maxSector)
		}
	}
}

// checkData attempts to read file/symlink data and verifies the returned
// length matches the recorded byte count.
func checkData(ctx context.Context, rep *Report, arch *mount.Archive, de s3fs.Dirent, path string) {
	kind := de.Mode & syscall.S_IFMT

	switch kind {
	case syscall.S_IFREG:
		if de.Bytes == 0 {
			return
		}
		data, err := arch.ReadFile(ctx, de)
		if err != nil {
			rep.errorf("%s: read file data: %v", path, err)
			return
		}
		if uint64(len(data)) != de.Bytes {
			rep.errorf("%s: file size mismatch (record=%d read=%d)", path, de.Bytes, len(data))
		}

	case syscall.S_IFLNK:
		target, err := arch.ReadSymlink(ctx, de)
		if err != nil {
			rep.errorf("%s: read symlink: %v", path, err)
			return
		}
		if uint64(len(target)) != de.Bytes {
			rep.errorf("%s: symlink size mismatch (record=%d read=%d)", path, de.Bytes, len(target))
		}
	}
}
