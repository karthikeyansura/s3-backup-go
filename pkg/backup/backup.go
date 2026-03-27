// Package backup implements the core logic for creating full and incremental
// backups of a directory tree into the S3 backup format.
package backup

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/karthikeyansura/s3-backup-go/pkg/compat"
	"github.com/karthikeyansura/s3-backup-go/pkg/s3fs"
	"github.com/karthikeyansura/s3-backup-go/pkg/store"
)

// Config holds all configuration for a backup run.
type Config struct {
	Store      store.ObjectStore
	Bucket     string
	NewName    string
	OldName    string
	Dir        string
	Tag        string
	Verbose    bool
	NoIO       bool
	Exclude    []string
	StopAfter  int64
	VersionIdx int
}

// Result contains statistics from a completed backup.
type Result struct {
	Stats     s3fs.StatFS
	Truncated bool
}

// dirCache maps S3Offset.Raw() -> directory entry data for incremental lookups.
type dirCache map[uint64][]byte

// Run executes a full or incremental backup.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	if cfg.Tag == "" {
		cfg.Tag = "--root--"
	}
	if cfg.StopAfter == 0 {
		cfg.StopAfter = 1 << 50
	}

	w, err := cfg.Store.NewWriter(ctx, cfg.NewName)
	if err != nil {
		return nil, fmt.Errorf("backup: create output: %w", err)
	}
	defer func() { _ = w.Close() }()

	sw := NewSectorWriter(w, cfg.NoIO)
	cache := make(dirCache)
	var oldRootDE *s3fs.Dirent
	var prevVersions []s3fs.Version

	if cfg.OldName != "" {
		pv, rootDE, err := loadPriorBackup(ctx, cfg, cache)
		if err != nil {
			return nil, fmt.Errorf("backup: load prior: %w", err)
		}
		prevVersions = pv
		oldRootDE = rootDE
	}

	dirInfo, err := os.Lstat(cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("backup: stat %s: %w", cfg.Dir, err)
	}
	if !dirInfo.IsDir() {
		return nil, fmt.Errorf("backup: %s is not a directory", cfg.Dir)
	}

	sbBuf, sbSectors := s3fs.MakeSuperblock(cfg.NewName, prevVersions)
	if err := sw.Write(sbBuf); err != nil {
		return nil, fmt.Errorf("backup: write superblock: %w", err)
	}
	offset := int64(sbSectors)

	// A temporary file is required to buffer the packed directory contents
	// because the directory location table must be written to S3 before the data itself.
	tmpFile, err := os.CreateTemp("", "s3bu-dirdata-*")
	if err != nil {
		return nil, fmt.Errorf("backup: create temp file: %w", err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()
	defer func() { _ = tmpFile.Close() }()

	state := &backupState{
		cfg:     cfg,
		sw:      sw,
		cache:   cache,
		dirLocs: nil,
		tmpFile: tmpFile,
		stats:   s3fs.StatFS{},
		curPath: "",
	}

	trailerDirents := make([]s3fs.Dirent, 0, 3)

	rootDE, offset, err := state.storeDir(offset, cfg.Dir, cfg.Tag, dirInfo, oldRootDE)
	if err != nil {
		return nil, fmt.Errorf("backup: traverse: %w", err)
	}
	trailerDirents = append(trailerDirents, rootDE)

	dirLocData := s3fs.MarshalDirLocs(state.dirLocs)
	dirLocDE, dirLocSectors, err := state.storeData(offset, "_dirloc_", dirLocData)
	if err != nil {
		return nil, fmt.Errorf("backup: write dirlocs: %w", err)
	}
	offset += dirLocSectors
	trailerDirents = append(trailerDirents, dirLocDE)

	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("backup: seek temp: %w", err)
	}
	dirDatDE, dirDatSectors, err := state.storeFileFromReader(offset, "_dirdat_", tmpFile, dirInfo)
	if err != nil {
		return nil, fmt.Errorf("backup: write dirdat: %w", err)
	}
	offset += dirDatSectors
	trailerDirents = append(trailerDirents, dirDatDE)

	state.stats.TotalSectors = uint64(offset + 1)
	trailer := buildTrailer(trailerDirents, state.stats)
	if err := sw.Write(trailer); err != nil {
		return nil, fmt.Errorf("backup: write trailer: %w", err)
	}

	truncated := offset >= cfg.StopAfter

	if cfg.Verbose {
		fmt.Printf("%d files (%d sectors)\n", state.stats.Files, state.stats.FileSectors)
		fmt.Printf("%d directories (%d sectors, %d bytes)\n",
			state.stats.Dirs, state.stats.DirSectors, state.stats.DirBytes)
		fmt.Printf("%d symlinks\n", state.stats.Symlinks)
		fmt.Printf("%d total sectors (%d bytes)\n",
			state.stats.TotalSectors, sw.TotalWritten())
		fmt.Printf("truncated: %v\n", truncated)
	}

	return &Result{
		Stats:     state.stats,
		Truncated: truncated,
	}, nil
}

type backupState struct {
	cfg     Config
	sw      *SectorWriter
	cache   dirCache
	dirLocs []s3fs.DirLoc
	tmpFile *os.File
	stats   s3fs.StatFS
	curPath string
}

func (s *backupState) createDirent(offset, nbytes int64, name string, info fs.FileInfo) s3fs.Dirent {
	stat := compat.GetStatInfo(info.Sys().(*syscall.Stat_t))
	return s3fs.Dirent{
		Mode:    uint16(stat.Mode),
		UID:     uint16(stat.Uid),
		GID:     uint16(stat.Gid),
		Ctime:   uint32(stat.Ctime),
		Offset:  s3fs.NewS3Offset(uint64(offset), uint16(s.cfg.VersionIdx)),
		Bytes:   uint64(nbytes),
		NameLen: uint8(len(name)),
		Name:    name,
	}
}

func (s *backupState) storeFile(offset int64, path string, name string, info fs.FileInfo) (s3fs.Dirent, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return s3fs.Dirent{}, 0, err
	}
	defer func() { _ = f.Close() }()

	return s.storeFileFromReader(offset, name, f, info)
}

func (s *backupState) storeFileFromReader(offset int64, name string, r io.Reader, info fs.FileInfo) (s3fs.Dirent, int64, error) {
	var nbytes int64
	buf := make([]byte, 16*1024)

	if s.cfg.NoIO {
		nbytes = info.Size()
	} else {
		for {
			n, err := r.Read(buf)
			if n > 0 {
				if werr := s.sw.Write(buf[:n]); werr != nil {
					return s3fs.Dirent{}, 0, werr
				}
				nbytes += int64(n)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				return s3fs.Dirent{}, 0, err
			}
		}
	}

	// The custom file system format requires all distinct file and metadata blocks
	// to align strictly to 512-byte sector boundaries.
	padLen := roundUp(nbytes, 512) - nbytes
	if padLen > 0 && !s.cfg.NoIO {
		pad := make([]byte, padLen)
		if err := s.sw.Write(pad); err != nil {
			return s3fs.Dirent{}, 0, err
		}
	}
	if s.cfg.NoIO {
		s.sw.totalWritten += roundUp(nbytes, 512)
	}

	totalSectors := roundUp(nbytes, 512) / 512
	de := s.createDirent(offset, nbytes, name, info)

	s.stats.Files++
	s.stats.FileSectors += uint64(totalSectors)

	if s.cfg.Verbose {
		fmt.Printf("F %d %d %s/%s\n", totalSectors, info.Size(), s.curPath, name)
	}

	return de, totalSectors, nil
}

func (s *backupState) storeLink(offset int64, path string, name string, info fs.FileInfo) (s3fs.Dirent, int64, error) {
	target, err := os.Readlink(path)
	if err != nil {
		return s3fs.Dirent{}, 0, fmt.Errorf("readlink %s: %w", path, err)
	}

	targetBytes := []byte(target)
	nbytes := int64(len(targetBytes))
	padded := make([]byte, roundUp(nbytes, 512))
	copy(padded, targetBytes)

	if err := s.sw.Write(padded); err != nil {
		return s3fs.Dirent{}, 0, err
	}

	totalSectors := int64(len(padded)) / 512
	de := s.createDirent(offset, nbytes, name, info)

	s.stats.Symlinks++
	s.stats.SymSectors += uint64(totalSectors)

	if s.cfg.Verbose {
		fmt.Printf("L %d %d %s/%s\n", totalSectors, nbytes, s.curPath, name)
	}

	return de, totalSectors, nil
}

func (s *backupState) storeData(offset int64, name string, data []byte) (s3fs.Dirent, int64, error) {
	nbytes := int64(len(data))

	if !s.cfg.NoIO {
		padded := make([]byte, roundUp(nbytes, 512))
		copy(padded, data)
		if err := s.sw.Write(padded); err != nil {
			return s3fs.Dirent{}, 0, err
		}
	} else {
		s.sw.totalWritten += roundUp(nbytes, 512)
	}

	totalSectors := roundUp(nbytes, 512) / 512

	de := s3fs.Dirent{
		Mode:    0,
		UID:     0,
		GID:     0,
		Ctime:   0,
		Offset:  s3fs.NewS3Offset(uint64(offset), uint16(s.cfg.VersionIdx)),
		Bytes:   uint64(nbytes),
		NameLen: uint8(len(name)),
		Name:    name,
	}

	s.stats.Files++
	s.stats.FileSectors += uint64(totalSectors)

	if s.cfg.Verbose {
		fmt.Printf("d %d %d %s\n", totalSectors, nbytes, name)
	}

	return de, totalSectors, nil
}

func storeNode(name string, info fs.FileInfo) s3fs.Dirent {
	stat := compat.GetStatInfo(info.Sys().(*syscall.Stat_t))
	de := s3fs.Dirent{
		Mode:    uint16(stat.Mode),
		UID:     uint16(stat.Uid),
		GID:     uint16(stat.Gid),
		Ctime:   uint32(stat.Ctime),
		Offset:  s3fs.S3Offset(0),
		Bytes:   0,
		NameLen: uint8(len(name)),
		Name:    name,
	}

	// For block and character devices, the device number is encoded into the Bytes field
	// to avoid modifying the core metadata struct layout.
	mode := info.Mode()
	if mode&fs.ModeCharDevice != 0 || mode&fs.ModeDevice != 0 {
		de.Bytes = stat.Rdev
	}

	return de
}

func unchanged(old *s3fs.Dirent, info fs.FileInfo) bool {
	if old == nil {
		return false
	}
	stat := compat.GetStatInfo(info.Sys().(*syscall.Stat_t))
	return old.Mode == uint16(stat.Mode) &&
		old.Bytes == uint64(info.Size()) &&
		old.Ctime == uint32(stat.Ctime) &&
		old.UID == uint16(stat.Uid) &&
		old.GID == uint16(stat.Gid)
}

func (s *backupState) isExcluded(dir, file string) bool {
	full := filepath.Join(dir, file)
	for _, pattern := range s.cfg.Exclude {
		if pattern == full {
			return true
		}
	}
	return false
}

func (s *backupState) storeDir(offset int64, dirPath string, name string,
	dirInfo fs.FileInfo, oldDE *s3fs.Dirent) (s3fs.Dirent, int64, error) {

	prevPath := s.curPath
	s.curPath = filepath.Join(s.curPath, name)
	defer func() { s.curPath = prevPath }()

	dirStat := compat.GetStatInfo(dirInfo.Sys().(*syscall.Stat_t))
	dirDev := dirStat.Dev

	var oldDirData []byte
	if oldDE != nil {
		// Validates that the incremental chain is properly ordered to prevent cyclic resolution
		if int(oldDE.Offset.Object()) >= s.cfg.VersionIdx {
			return s3fs.Dirent{}, 0, fmt.Errorf("corrupt ancestor index %d at %s",
				oldDE.Offset.Object(), s.curPath)
		}
		oldDirData = s.cache[oldDE.Offset.Raw()]
	}

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return s3fs.Dirent{}, 0, fmt.Errorf("readdir %s: %w", dirPath, err)
	}

	var childDirents []byte

	for _, entry := range entries {
		childName := entry.Name()
		childPath := filepath.Join(dirPath, childName)

		if s.isExcluded(s.curPath, childName) {
			if s.cfg.Verbose {
				fmt.Printf("excluding %s/%s\n", s.curPath, childName)
			}
			continue
		}

		if offset >= s.cfg.StopAfter {
			break
		}

		info, err := os.Lstat(childPath)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "skipping %s: %s\n", childPath, err)
			continue
		}

		// Named pipes (FIFOs) will block the traversal thread indefinitely
		// if opened without an active reader present.
		if info.Mode()&fs.ModeNamedPipe != 0 {
			_, _ = fmt.Fprintf(os.Stderr, "skipping FIFO %s\n", childPath)
			continue
		}

		var oldChild *s3fs.Dirent
		if oldDirData != nil {
			if found, ok := s3fs.LookupDirent(oldDirData, childName); ok {
				oldChild = &found
			}
		}

		var de s3fs.Dirent
		mode := info.Mode()

		switch {
		case mode.IsRegular():
			if oldChild != nil && unchanged(oldChild, info) {
				de = *oldChild
			} else {
				var sectors int64
				de, sectors, err = s.storeFile(offset, childPath, childName, info)
				if err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "skipping %s: %s\n", childPath, err)
					continue
				}
				offset += sectors
			}

		case mode.IsDir():
			childStat := compat.GetStatInfo(info.Sys().(*syscall.Stat_t))
			if childStat.Dev != dirDev {
				de = storeNode(childName, info)
			} else {
				var newOffset int64
				de, newOffset, err = s.storeDir(offset, childPath, childName, info, oldChild)
				if err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "skipping %s: %s\n", childPath, err)
					continue
				}
				offset = newOffset
			}

		case mode&fs.ModeSymlink != 0:
			var sectors int64
			de, sectors, err = s.storeLink(offset, childPath, childName, info)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "skipping %s: %s\n", childPath, err)
				continue
			}
			offset += sectors

		case mode&fs.ModeCharDevice != 0, mode&fs.ModeDevice != 0:
			de = storeNode(childName, info)

		default:
			_, _ = fmt.Fprintf(os.Stderr, "skipping %s: unsupported file type\n", childPath)
			continue
		}

		buf := make([]byte, de.Size())
		de.MarshalDirent(buf)
		childDirents = append(childDirents, buf...)
	}

	dirDataLen := int64(len(childDirents))
	padded := make([]byte, roundUp(dirDataLen, 512))
	copy(padded, childDirents)

	if err := s.sw.Write(padded); err != nil {
		return s3fs.Dirent{}, 0, fmt.Errorf("write dir %s: %w", s.curPath, err)
	}

	dirSectors := int64(len(padded)) / 512

	loc := s3fs.NewS3Offset(uint64(offset), uint16(s.cfg.VersionIdx))
	thisDirDE := s3fs.Dirent{
		Mode:    uint16(dirStat.Mode),
		UID:     uint16(dirStat.Uid),
		GID:     uint16(dirStat.Gid),
		Ctime:   uint32(dirStat.Ctime),
		Offset:  loc,
		Bytes:   uint64(dirDataLen),
		NameLen: uint8(len(name)),
		Name:    name,
	}

	s.dirLocs = append(s.dirLocs, s3fs.DirLoc{
		Offset: loc,
		Bytes:  uint32(dirDataLen),
	})

	if dirDataLen > 0 {
		if _, err := s.tmpFile.Write(childDirents); err != nil {
			return s3fs.Dirent{}, 0, fmt.Errorf("write temp dir data: %w", err)
		}
	}

	offset += dirSectors

	s.stats.Dirs++
	s.stats.DirSectors += uint64(dirSectors)
	s.stats.DirBytes += uint64(dirDataLen)

	if s.cfg.Verbose {
		fmt.Printf("D %d %d %s\n", dirSectors, dirInfo.Size(), s.curPath)
	}

	return thisDirDE, offset, nil
}

// buildTrailer constructs the final 512-byte sector of the object.
// The file format dictates a strict layout for this block:
// 1. Root Directory Entry
// 2. Directory Location Table Entry
// 3. Packed Directory Data Entry
// 4. File System Statistics (StatFS) anchored to the final bytes of the sector.
func buildTrailer(dirents []s3fs.Dirent, stats s3fs.StatFS) []byte {
	buf := make([]byte, 512)

	offset := 0
	for _, de := range dirents {
		offset += de.MarshalDirent(buf[offset:])
	}

	stats.Marshal(buf[512-s3fs.StatFSSize:])

	return buf
}

func loadPriorBackup(ctx context.Context, cfg Config, cache dirCache) ([]s3fs.Version, *s3fs.Dirent, error) {
	sbData, err := cfg.Store.GetRange(ctx, cfg.OldName, 0, 4096)
	if err != nil {
		return nil, nil, fmt.Errorf("read old superblock: %w", err)
	}

	oldSB, err := s3fs.ParseSuperblock(sbData)
	if err != nil {
		return nil, nil, fmt.Errorf("parse old superblock: %w", err)
	}

	names := oldSB.VersionNames()
	for i, v := range oldSB.Versions {
		name := names[len(names)-1-i]
		if err := verifyUUID(ctx, cfg.Store, name, v.UUID); err != nil {
			return nil, nil, err
		}
		if cfg.Verbose {
			fmt.Printf("verified %s OK\n", name)
		}
	}

	oldSize, err := cfg.Store.Size(ctx, cfg.OldName)
	if err != nil {
		return nil, nil, fmt.Errorf("size old object: %w", err)
	}

	trailerData, err := cfg.Store.GetRange(ctx, cfg.OldName, oldSize-512, 512)
	if err != nil {
		return nil, nil, fmt.Errorf("read old trailer: %w", err)
	}

	rootDE, rootN, err := s3fs.ParseDirent(trailerData)
	if err != nil {
		return nil, nil, fmt.Errorf("parse old root dirent: %w", err)
	}

	dirlocDE, dirlocN, err := s3fs.ParseDirent(trailerData[rootN:])
	if err != nil {
		return nil, nil, fmt.Errorf("parse old dirloc dirent: %w", err)
	}

	dirdatDE, _, err := s3fs.ParseDirent(trailerData[rootN+dirlocN:])
	if err != nil {
		return nil, nil, fmt.Errorf("parse old dirdat dirent: %w", err)
	}

	locData, err := cfg.Store.GetRange(ctx, cfg.OldName,
		int64(dirlocDE.Offset.Sector())*512, int64(dirlocDE.Bytes))
	if err != nil {
		return nil, nil, fmt.Errorf("read old dirlocs: %w", err)
	}
	locs := s3fs.ParseDirLocs(locData)

	dirData, err := cfg.Store.GetRange(ctx, cfg.OldName,
		int64(dirdatDE.Offset.Sector())*512, int64(dirdatDE.Bytes))
	if err != nil {
		return nil, nil, fmt.Errorf("read old dirdata: %w", err)
	}

	byteOffset := 0
	for _, loc := range locs {
		if loc.Bytes > 0 {
			end := byteOffset + int(loc.Bytes)
			if end > len(dirData) {
				end = len(dirData)
			}
			data := make([]byte, end-byteOffset)
			copy(data, dirData[byteOffset:end])
			cache[loc.Offset.Raw()] = data
			byteOffset = end
		}
	}

	if cfg.Verbose {
		fmt.Printf("loaded %d directories from prior backup\n", len(locs))
	}

	return oldSB.Versions, &rootDE, nil
}

func verifyUUID(ctx context.Context, st store.ObjectStore, name string, expected [16]byte) error {
	data, err := st.GetRange(ctx, name, 0, 512)
	if err != nil {
		return fmt.Errorf("verify %s: %w", name, err)
	}

	sb, err := s3fs.ParseSuperblock(data)
	if err != nil {
		return fmt.Errorf("verify %s: %w", name, err)
	}

	if sb.Versions[0].UUID != expected {
		return fmt.Errorf("verify %s: UUID mismatch (got %x, want %x)",
			name, sb.Versions[0].UUID, expected)
	}

	return nil
}

// ParseSize parses a size string with optional K/M/G suffix.
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}

	multiplier := int64(1)
	last := strings.ToUpper(s[len(s)-1:])
	switch last {
	case "G":
		multiplier = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	case "M":
		multiplier = 1024 * 1024
		s = s[:len(s)-1]
	case "K":
		multiplier = 1024
		s = s[:len(s)-1]
	}

	var val int64
	_, err := fmt.Sscanf(s, "%d", &val)
	if err != nil {
		return 0, fmt.Errorf("invalid size: %s", s)
	}

	return val * multiplier, nil
}
