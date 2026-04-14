// Package compare implements a byte-level comparison between a local directory
// tree and an S3 backup archive. It validates directory structure, file
// metadata (mode, uid, gid, ctime, size), symlink targets, and file content
// (via SHA256 hash). It respects cross-device mount point semantics: mount
// point directories are expected to be empty in the backup.
package compare

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/karthikeyansura/s3-backup-go/pkg/compat"
	"github.com/karthikeyansura/s3-backup-go/pkg/mount"
	"github.com/karthikeyansura/s3-backup-go/pkg/s3fs"
)

// Report collects all differences found during comparison.
type Report struct {
	Compared        int
	MissingInBackup []string
	MissingInLocal  []string
	Mismatches      []string
}

// OK returns true if backup and local tree match exactly.
func (r *Report) OK() bool {
	return len(r.MissingInBackup) == 0 &&
		len(r.MissingInLocal) == 0 &&
		len(r.Mismatches) == 0
}

func (r *Report) mismatch(path, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	r.Mismatches = append(r.Mismatches, path+": "+msg)
}

// Tree compares a local directory tree against a loaded backup archive.
// Both are walked in parallel, matching the backup's traversal semantics
// (no cross-device descent, skip FIFOs).
func Tree(ctx context.Context, arch *mount.Archive, localRoot string) (*Report, error) {
	rootInfo, err := os.Lstat(localRoot)
	if err != nil {
		return nil, err
	}
	if !rootInfo.IsDir() {
		return nil, fmt.Errorf("compare: %s is not a directory", localRoot)
	}

	rep := &Report{}
	compareDir(ctx, rep, arch, localRoot, "", arch.RootDE)
	return rep, nil
}

// compareDir walks one directory level, comparing local children against backup.
// Cross-device mount point detection uses this directory's own device ID
// (matching backup.go's storeDir behavior with dirDev).
func compareDir(ctx context.Context, rep *Report, arch *mount.Archive,
	absPath, relPath string, backupDE s3fs.Dirent) {

	rep.Compared++

	// Get this directory's own device ID for passing to children.
	dirInfo, err := os.Lstat(absPath)
	if err != nil {
		rep.mismatch(relPath, "lstat failed: %v", err)
		return
	}
	dirStat := compat.GetStatInfo(dirInfo.Sys().(*syscall.Stat_t))
	thisDev := dirStat.Dev

	// Build lookup of backup children.
	backupChildren := make(map[string]s3fs.Dirent)
	dirData := arch.DirCache.FindDir(backupDE.Offset, backupDE.Bytes)
	if dirData != nil {
		_ = s3fs.IterDirents(dirData, func(d s3fs.Dirent) bool {
			backupChildren[d.Name] = d
			return true
		})
	}

	// Read local children.
	localEntries, err := os.ReadDir(absPath)
	if err != nil {
		rep.mismatch(relPath, "readdir failed: %v", err)
		return
	}

	localNames := make(map[string]struct{}, len(localEntries))
	for _, e := range localEntries {
		localNames[e.Name()] = struct{}{}
	}

	// Check for entries in backup but missing locally.
	for name := range backupChildren {
		if _, ok := localNames[name]; !ok {
			rep.MissingInLocal = append(rep.MissingInLocal, filepath.Join(relPath, name))
		}
	}

	// Walk local entries, comparing against backup.
	for _, entry := range localEntries {
		name := entry.Name()
		childAbs := filepath.Join(absPath, name)
		childRel := filepath.Join(relPath, name)

		info, err := os.Lstat(childAbs)
		if err != nil {
			rep.mismatch(childRel, "lstat failed: %v", err)
			continue
		}

		// Skip FIFOs (backup skips them).
		if info.Mode()&fs.ModeNamedPipe != 0 {
			continue
		}

		bde, inBackup := backupChildren[name]
		if !inBackup {
			rep.MissingInBackup = append(rep.MissingInBackup, childRel)
			continue
		}

		rep.Compared++
		st := compat.GetStatInfo(info.Sys().(*syscall.Stat_t))

		switch {
		case info.Mode().IsDir():
			if bde.Mode&syscall.S_IFDIR == 0 {
				rep.mismatch(childRel, "local is directory, backup is not (mode 0%o)", bde.Mode)
				continue
			}
			compareMeta(rep, childRel, st, info, bde)

			// Cross-device mount point: compare child's dev against THIS
			// directory's dev (not the tree root's dev), matching how
			// backup.go's storeDir compares childStat.Dev to dirDev.
			if st.Dev != thisDev {
				if bde.Bytes != 0 {
					rep.mismatch(childRel, "mount point should be empty in backup (bytes=%d)", bde.Bytes)
				}
				continue
			}
			// Recurse into child directory.
			compareDir(ctx, rep, arch, childAbs, childRel, bde)

		case info.Mode()&fs.ModeSymlink != 0:
			if bde.Mode&syscall.S_IFMT != syscall.S_IFLNK {
				rep.mismatch(childRel, "local is symlink, backup is not (mode 0%o)", bde.Mode)
				continue
			}
			compareMeta(rep, childRel, st, info, bde)

			localTarget, err := os.Readlink(childAbs)
			if err != nil {
				rep.mismatch(childRel, "readlink failed: %v", err)
				continue
			}
			backupTarget, err := arch.ReadSymlink(ctx, bde)
			if err != nil {
				rep.mismatch(childRel, "read backup symlink: %v", err)
				continue
			}
			if localTarget != backupTarget {
				rep.mismatch(childRel, "symlink target differs: local=%q backup=%q", localTarget, backupTarget)
			}

		case info.Mode().IsRegular():
			if bde.Mode&syscall.S_IFMT != syscall.S_IFREG {
				rep.mismatch(childRel, "local is regular file, backup is not (mode 0%o)", bde.Mode)
				continue
			}
			compareMeta(rep, childRel, st, info, bde)

			if uint64(info.Size()) != bde.Bytes {
				continue
			}
			if err := compareContent(ctx, rep, arch, childAbs, childRel, bde); err != nil {
				rep.mismatch(childRel, "content compare error: %v", err)
			}

		default:
			compareMeta(rep, childRel, st, info, bde)
		}
	}
}

func compareMeta(rep *Report, path string, st compat.StatInfo, info fs.FileInfo, de s3fs.Dirent) {
	if uint16(st.Mode) != de.Mode {
		rep.mismatch(path, "mode differs: local=0%o backup=0%o", st.Mode, de.Mode)
	}
	if uint16(st.Uid) != de.UID || uint16(st.Gid) != de.GID {
		rep.mismatch(path, "owner differs: local=%d:%d backup=%d:%d", st.Uid, st.Gid, de.UID, de.GID)
	}
	if uint32(st.Ctime) != de.Ctime {
		rep.mismatch(path, "ctime differs: local=%d backup=%d", st.Ctime, de.Ctime)
	}
	if info.Mode().IsRegular() && uint64(info.Size()) != de.Bytes {
		rep.mismatch(path, "size differs: local=%d backup=%d", info.Size(), de.Bytes)
	}
}

// compareContent hashes both the local file and the backup data using SHA256
// in streaming fashion and reports a mismatch if they differ.
func compareContent(ctx context.Context, rep *Report, arch *mount.Archive,
	localPath, relPath string, de s3fs.Dirent) error {

	localHash, err := hashLocalFile(localPath)
	if err != nil {
		return fmt.Errorf("hash local: %w", err)
	}

	backupHash, err := hashBackupFile(ctx, arch, de)
	if err != nil {
		return fmt.Errorf("hash backup: %w", err)
	}

	if localHash != backupHash {
		rep.mismatch(relPath, "content hash differs: local=%s backup=%s", localHash, backupHash)
	}
	return nil
}

func hashLocalFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

const hashChunkSize = 4 * 1024 * 1024

func hashBackupFile(ctx context.Context, arch *mount.Archive, de s3fs.Dirent) (string, error) {
	obj := int(de.Offset.Object())
	key := arch.Names[obj]
	base := int64(de.Offset.Sector()) * s3fs.SectorSize
	remaining := int64(de.Bytes)

	h := sha256.New()
	cur := base
	for remaining > 0 {
		n := int64(hashChunkSize)
		if remaining < n {
			n = remaining
		}
		data, err := arch.Store.GetRange(ctx, key, cur, n)
		if err != nil {
			return "", err
		}
		if len(data) == 0 {
			break
		}
		h.Write(data)
		cur += int64(len(data))
		remaining -= int64(len(data))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
