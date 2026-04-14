package mount

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/karthikeyansura/s3-backup-go/pkg/s3fs"
	"github.com/karthikeyansura/s3-backup-go/pkg/store"
)

// Archive holds a loaded backup object with its parsed metadata and mmap-backed
// directory cache. It is the shared entry point for the FUSE mount, fsck, and
// compare tools.
type Archive struct {
	Store    store.ObjectStore
	Object   string
	Names    []string // version names, oldest to newest (resolved to full keys)
	NSectors []int64  // per-version object size in sectors
	RootDE   s3fs.Dirent
	Statfs   s3fs.StatFS
	DirCache *DirCache
}

// OpenArchive reads the superblock, verifies the entire version chain, loads
// directory metadata, and builds the mmap-backed directory cache. The caller
// must call arch.Close() when done.
func OpenArchive(ctx context.Context, st store.ObjectStore, objectKey string, verbose bool) (*Archive, error) {
	sbData, err := st.GetRange(ctx, objectKey, 0, 4096)
	if err != nil {
		return nil, fmt.Errorf("archive: read superblock: %w", err)
	}

	sb, err := s3fs.ParseSuperblock(sbData)
	if err != nil {
		return nil, fmt.Errorf("archive: parse superblock: %w", err)
	}

	nvers := int(sb.NVers)
	rawNames := sb.VersionNames() // basenames stored in superblock
	uuids := sb.VersionUUIDs()

	// The superblock stores version names as basenames (filepath.Base).
	// For local file mode the key is an absolute or relative filesystem path,
	// so we must resolve each version name relative to the directory containing
	// the primary object. For S3 mode this is also correct since bucket keys
	// can contain path separators.
	baseDir := filepath.Dir(objectKey)
	names := make([]string, nvers)
	for i, raw := range rawNames {
		if filepath.Base(objectKey) == raw {
			names[i] = objectKey
		} else {
			names[i] = filepath.Join(baseDir, raw)
		}
	}

	nsectors := make([]int64, nvers)
	for i := 0; i < nvers; i++ {
		size, err := st.Size(ctx, names[i])
		if err != nil {
			return nil, fmt.Errorf("archive: size %s: %w", names[i], err)
		}
		if size < s3fs.SectorSize || size%s3fs.SectorSize != 0 {
			return nil, fmt.Errorf("archive: object %s size %d not sector-aligned", names[i], size)
		}
		nsectors[i] = size / s3fs.SectorSize

		vsbData, err := st.GetRange(ctx, names[i], 0, 4096)
		if err != nil {
			return nil, fmt.Errorf("archive: read %s: %w", names[i], err)
		}
		vsb, err := s3fs.ParseSuperblock(vsbData)
		if err != nil {
			return nil, fmt.Errorf("archive: parse %s: %w", names[i], err)
		}
		if len(vsb.Versions) == 0 || vsb.Versions[0].UUID != uuids[i] {
			return nil, fmt.Errorf("archive: UUID mismatch for %s", names[i])
		}
	}

	objSize, err := st.Size(ctx, objectKey)
	if err != nil {
		return nil, fmt.Errorf("archive: size: %w", err)
	}

	trailerData, err := st.GetRange(ctx, objectKey, objSize-s3fs.SectorSize, s3fs.SectorSize)
	if err != nil {
		return nil, fmt.Errorf("archive: read trailer: %w", err)
	}

	rootDE, rootN, err := s3fs.ParseDirent(trailerData)
	if err != nil {
		return nil, fmt.Errorf("archive: parse root dirent: %w", err)
	}

	dirlocDE, dirlocN, err := s3fs.ParseDirent(trailerData[rootN:])
	if err != nil {
		return nil, fmt.Errorf("archive: parse dirloc dirent: %w", err)
	}

	dirdatDE, _, err := s3fs.ParseDirent(trailerData[rootN+dirlocN:])
	if err != nil {
		return nil, fmt.Errorf("archive: parse dirdat dirent: %w", err)
	}

	statfs := s3fs.ParseStatFS(trailerData[s3fs.SectorSize-s3fs.StatFSSize:])

	locData, err := st.GetRange(ctx, objectKey,
		int64(dirlocDE.Offset.Sector())*s3fs.SectorSize, int64(dirlocDE.Bytes))
	if err != nil {
		return nil, fmt.Errorf("archive: read dirlocs: %w", err)
	}
	locs := s3fs.ParseDirLocs(locData)

	dirData, err := st.GetRange(ctx, objectKey,
		int64(dirdatDE.Offset.Sector())*s3fs.SectorSize, int64(dirdatDE.Bytes))
	if err != nil {
		return nil, fmt.Errorf("archive: read dirdata: %w", err)
	}

	var locSum int
	for _, loc := range locs {
		locSum += int(loc.Bytes)
	}
	if locSum != len(dirData) {
		return nil, fmt.Errorf("archive: packed dir data len %d != sum(dirloc) %d", len(dirData), locSum)
	}

	dirCache, err := NewDirCache(locs, dirData)
	if err != nil {
		return nil, fmt.Errorf("archive: create dircache: %w", err)
	}

	if verbose {
		fmt.Printf("loaded archive: %d versions, %d directories\n", nvers, len(locs))
	}

	return &Archive{
		Store:    st,
		Object:   objectKey,
		Names:    names,
		NSectors: nsectors,
		RootDE:   rootDE,
		Statfs:   statfs,
		DirCache: dirCache,
	}, nil
}

// Close releases the mmap-backed directory cache.
func (a *Archive) Close() error {
	if a.DirCache != nil {
		return a.DirCache.Close()
	}
	return nil
}

// LookupPath resolves a slash-separated path relative to the backup root.
// An empty string or "/" returns the root directory entry.
func (a *Archive) LookupPath(rel string) (s3fs.Dirent, error) {
	rel = strings.Trim(rel, "/")
	if rel == "" {
		return a.RootDE, nil
	}

	de := a.RootDE
	for _, seg := range strings.Split(rel, "/") {
		if seg == "" || seg == "." {
			continue
		}
		if de.Mode&syscall.S_IFDIR == 0 {
			return s3fs.Dirent{}, fmt.Errorf("not a directory at %q in path %q", seg, rel)
		}
		dirData := a.DirCache.FindDir(de.Offset, de.Bytes)
		if dirData == nil {
			return s3fs.Dirent{}, fmt.Errorf("directory data missing for %q", seg)
		}
		child, ok := s3fs.LookupDirent(dirData, seg)
		if !ok {
			return s3fs.Dirent{}, fmt.Errorf("not found: %s", path.Join(rel))
		}
		de = child
	}
	return de, nil
}

// ReadFile reads the raw byte content of a regular file dirent from the
// appropriate version object.
func (a *Archive) ReadFile(ctx context.Context, de s3fs.Dirent) ([]byte, error) {
	if de.Mode&syscall.S_IFREG == 0 {
		return nil, fmt.Errorf("not a regular file (mode 0%o)", de.Mode)
	}
	obj := int(de.Offset.Object())
	if obj < 0 || obj >= len(a.Names) {
		return nil, fmt.Errorf("invalid object index %d", obj)
	}
	key := a.Names[obj]
	base := int64(de.Offset.Sector()) * s3fs.SectorSize
	return a.Store.GetRange(ctx, key, base, int64(de.Bytes))
}

// ReadSymlink returns the symlink target string for a symlink dirent.
func (a *Archive) ReadSymlink(ctx context.Context, de s3fs.Dirent) (string, error) {
	if de.Mode&syscall.S_IFMT != syscall.S_IFLNK {
		return "", fmt.Errorf("not a symlink (mode 0%o)", de.Mode)
	}
	obj := int(de.Offset.Object())
	if obj < 0 || obj >= len(a.Names) {
		return "", fmt.Errorf("invalid object index %d", obj)
	}
	key := a.Names[obj]
	base := int64(de.Offset.Sector()) * s3fs.SectorSize
	data, err := a.Store.GetRange(ctx, key, base, int64(de.Bytes))
	if err != nil {
		return "", err
	}
	return string(data), nil
}
