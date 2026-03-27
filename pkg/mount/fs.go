package mount

import (
	"context"
	"fmt"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/karthikeyansura/s3-backup-go/pkg/s3fs"
	"github.com/karthikeyansura/s3-backup-go/pkg/store"
)

// S3FS is the root of the FUSE filesystem. It implements the go-fuse
// InodeEmbedder interface.
type S3FS struct {
	fs.Inode

	store     store.ObjectStore
	names     []string
	nsectors  []int64
	rootDE    s3fs.Dirent
	statfs    s3fs.StatFS
	dirCache  *DirCache
	dataCache *DataCache
	noCache   bool
}

// Config holds the configuration for mounting a backup.
type Config struct {
	Store     store.ObjectStore
	ObjectKey string
	Verbose   bool
	NoCache   bool
}

// NewS3FS creates a new FUSE filesystem from a backup object.
func NewS3FS(ctx context.Context, cfg Config) (*S3FS, error) {
	st := cfg.Store

	sbData, err := st.GetRange(ctx, cfg.ObjectKey, 0, 4096)
	if err != nil {
		return nil, fmt.Errorf("mount: read superblock: %w", err)
	}

	sb, err := s3fs.ParseSuperblock(sbData)
	if err != nil {
		return nil, fmt.Errorf("mount: parse superblock: %w", err)
	}

	nvers := int(sb.NVers)
	names := sb.VersionNames()

	nsectors := make([]int64, nvers)
	uuids := sb.VersionUUIDs()
	for i := 0; i < nvers; i++ {
		size, err := st.Size(ctx, names[i])
		if err != nil {
			return nil, fmt.Errorf("mount: size %s: %w", names[i], err)
		}
		nsectors[i] = size / 512

		vsbData, err := st.GetRange(ctx, names[i], 0, 4096)
		if err != nil {
			return nil, fmt.Errorf("mount: read %s: %w", names[i], err)
		}
		vsb, err := s3fs.ParseSuperblock(vsbData)
		if err != nil {
			return nil, fmt.Errorf("mount: parse %s: %w", names[i], err)
		}
		if vsb.Versions[0].UUID != uuids[i] {
			return nil, fmt.Errorf("mount: UUID mismatch for %s", names[i])
		}
	}

	objSize, err := st.Size(ctx, cfg.ObjectKey)
	if err != nil {
		return nil, fmt.Errorf("mount: size: %w", err)
	}
	trailerData, err := st.GetRange(ctx, cfg.ObjectKey, objSize-512, 512)
	if err != nil {
		return nil, fmt.Errorf("mount: read trailer: %w", err)
	}

	rootDE, rootN, err := s3fs.ParseDirent(trailerData)
	if err != nil {
		return nil, fmt.Errorf("mount: parse root dirent: %w", err)
	}

	dirlocDE, dirlocN, err := s3fs.ParseDirent(trailerData[rootN:])
	if err != nil {
		return nil, fmt.Errorf("mount: parse dirloc dirent: %w", err)
	}

	dirdatDE, _, err := s3fs.ParseDirent(trailerData[rootN+dirlocN:])
	if err != nil {
		return nil, fmt.Errorf("mount: parse dirdat dirent: %w", err)
	}

	statfs := s3fs.ParseStatFS(trailerData[512-s3fs.StatFSSize:])

	locData, err := st.GetRange(ctx, cfg.ObjectKey,
		int64(dirlocDE.Offset.Sector())*512, int64(dirlocDE.Bytes))
	if err != nil {
		return nil, fmt.Errorf("mount: read dirlocs: %w", err)
	}
	locs := s3fs.ParseDirLocs(locData)

	dirData, err := st.GetRange(ctx, cfg.ObjectKey,
		int64(dirdatDE.Offset.Sector())*512, int64(dirdatDE.Bytes))
	if err != nil {
		return nil, fmt.Errorf("mount: read dirdata: %w", err)
	}

	dirCache, err := NewDirCache(locs, dirData)
	if err != nil {
		return nil, fmt.Errorf("mount: create dircache: %w", err)
	}

	var dataCache *DataCache
	if !cfg.NoCache {
		dataCache = NewDataCache(st)
	}

	if cfg.Verbose {
		fmt.Printf("mounted: %d versions, %d directories\n", nvers, len(locs))
	}

	return &S3FS{
		store:     st,
		names:     names,
		nsectors:  nsectors,
		rootDE:    rootDE,
		statfs:    statfs,
		dirCache:  dirCache,
		dataCache: dataCache,
		noCache:   cfg.NoCache,
	}, nil
}

// Close releases resources held by the filesystem.
func (f *S3FS) Close() error {
	if f.dirCache != nil {
		return f.dirCache.Close()
	}
	return nil
}

// S3Node is a node in the FUSE filesystem backed by an s3fs.Dirent.
type S3Node struct {
	fs.Inode
	root *S3FS
	de   s3fs.Dirent
}

var _ = (fs.NodeGetattrer)((*S3Node)(nil))
var _ = (fs.NodeReaddirer)((*S3Node)(nil))
var _ = (fs.NodeLookuper)((*S3Node)(nil))
var _ = (fs.NodeOpener)((*S3Node)(nil))
var _ = (fs.NodeReader)((*S3Node)(nil))
var _ = (fs.NodeReadlinker)((*S3Node)(nil))

// Getattr returns file attributes.
func (n *S3Node) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	dirent2attr(&n.de, &out.Attr)
	return 0
}

// Lookup finds a child entry in a directory.
func (n *S3Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.de.Mode&syscall.S_IFDIR == 0 {
		return nil, syscall.ENOTDIR
	}

	dirData := n.root.dirCache.FindDir(n.de.Offset, n.de.Bytes)
	if dirData == nil {
		return nil, syscall.ENOENT
	}

	child, ok := s3fs.LookupDirent(dirData, name)
	if !ok {
		return nil, syscall.ENOENT
	}

	dirent2attr(&child, &out.Attr)
	out.SetEntryTimeout(1000)
	out.SetAttrTimeout(1000)

	childNode := &S3Node{root: n.root, de: child}
	inode := n.NewInode(ctx, childNode, fs.StableAttr{Mode: uint32(child.Mode)})
	return inode, 0
}

// Readdir lists directory contents.
func (n *S3Node) Readdir(_ context.Context) (fs.DirStream, syscall.Errno) {
	if n.de.Mode&syscall.S_IFDIR == 0 {
		return nil, syscall.ENOTDIR
	}

	dirData := n.root.dirCache.FindDir(n.de.Offset, n.de.Bytes)
	if dirData == nil {
		return nil, 0
	}

	var entries []fuse.DirEntry
	err := s3fs.IterDirents(dirData, func(d s3fs.Dirent) bool {
		entries = append(entries, fuse.DirEntry{
			Name: d.Name,
			Mode: uint32(d.Mode),
		})
		return true
	})
	if err != nil {
		return nil, syscall.EIO
	}

	return fs.NewListDirStream(entries), 0
}

// Open opens a file.
func (n *S3Node) Open(_ context.Context, _ uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

// Read reads data from a file.
func (n *S3Node) Read(ctx context.Context, _ fs.FileHandle, dest []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	if n.de.Mode&syscall.S_IFREG == 0 {
		return nil, syscall.EISDIR
	}

	fileSize := int64(n.de.Bytes)
	if offset >= fileSize {
		return fuse.ReadResultData(nil), 0
	}

	end := offset + int64(len(dest))
	if end > fileSize {
		end = fileSize
	}
	readLen := end - offset

	buf := make([]byte, readLen)
	objectIdx := int(n.de.Offset.Object())
	key := n.root.names[objectIdx]
	baseOffset := int64(n.de.Offset.Sector()) * 512

	if n.root.noCache || n.root.dataCache == nil {
		err := ReadDirect(ctx, n.root.store, key, buf, baseOffset+offset)
		if err != nil {
			return nil, syscall.EIO
		}
	} else {
		maxOffset := n.root.nsectors[objectIdx] * 512
		err := n.root.dataCache.Read(ctx, key, objectIdx, buf, baseOffset+offset, maxOffset)
		if err != nil {
			return nil, syscall.EIO
		}
	}

	return fuse.ReadResultData(buf), 0
}

// Readlink reads a symbolic link target.
func (n *S3Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	if n.de.Mode&syscall.S_IFLNK == 0 {
		return nil, syscall.EINVAL
	}

	linkLen := int64(n.de.Bytes)
	buf := make([]byte, linkLen)
	objectIdx := int(n.de.Offset.Object())
	key := n.root.names[objectIdx]
	baseOffset := int64(n.de.Offset.Sector()) * 512

	data, err := n.root.store.GetRange(ctx, key, baseOffset, linkLen)
	if err != nil {
		return nil, syscall.EIO
	}

	copy(buf, data)
	return buf, 0
}

var _ = (fs.NodeGetattrer)((*S3FS)(nil))
var _ = (fs.NodeReaddirer)((*S3FS)(nil))
var _ = (fs.NodeLookuper)((*S3FS)(nil))
var _ = (fs.NodeStatfser)((*S3FS)(nil))

// Getattr for the root directory.
func (f *S3FS) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	dirent2attr(&f.rootDE, &out.Attr)
	return 0
}

// Lookup for the root directory.
func (f *S3FS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	dirData := f.dirCache.FindDir(f.rootDE.Offset, f.rootDE.Bytes)
	if dirData == nil {
		return nil, syscall.ENOENT
	}

	child, ok := s3fs.LookupDirent(dirData, name)
	if !ok {
		return nil, syscall.ENOENT
	}

	dirent2attr(&child, &out.Attr)
	out.SetEntryTimeout(1000)
	out.SetAttrTimeout(1000)

	childNode := &S3Node{root: f, de: child}
	inode := f.NewInode(ctx, childNode, fs.StableAttr{Mode: uint32(child.Mode)})
	return inode, 0
}

// Readdir for the root directory.
func (f *S3FS) Readdir(_ context.Context) (fs.DirStream, syscall.Errno) {
	dirData := f.dirCache.FindDir(f.rootDE.Offset, f.rootDE.Bytes)
	if dirData == nil {
		return nil, 0
	}

	var entries []fuse.DirEntry
	err := s3fs.IterDirents(dirData, func(d s3fs.Dirent) bool {
		entries = append(entries, fuse.DirEntry{
			Name: d.Name,
			Mode: uint32(d.Mode),
		})
		return true
	})
	if err != nil {
		return nil, syscall.EIO
	}

	return fs.NewListDirStream(entries), 0
}

// Statfs returns filesystem statistics.
func (f *S3FS) Statfs(_ context.Context, out *fuse.StatfsOut) syscall.Errno {
	out.Bsize = 512
	out.Frsize = 512
	out.Blocks = f.statfs.TotalSectors
	for _, ns := range f.nsectors {
		out.Blocks += uint64(ns)
	}
	out.Bfree = 0
	out.Bavail = 0
	out.Files = uint64(f.statfs.Files + f.statfs.Dirs + f.statfs.Symlinks)
	out.Ffree = 0
	out.NameLen = 255
	return 0
}

func dirent2attr(de *s3fs.Dirent, attr *fuse.Attr) {
	attr.Mode = uint32(de.Mode)
	attr.Nlink = 1
	attr.Owner.Uid = uint32(de.UID)
	attr.Owner.Gid = uint32(de.GID)

	isCharDev := de.Mode&syscall.S_IFCHR == syscall.S_IFCHR
	isBlkDev := de.Mode&syscall.S_IFBLK == syscall.S_IFBLK
	if isCharDev || isBlkDev {
		attr.Rdev = uint32(de.Bytes)
		attr.Size = 0
	} else {
		attr.Size = de.Bytes
	}

	attr.Blocks = (de.Bytes + 511) / 512
	attr.Atime = uint64(de.Ctime)
	attr.Mtime = uint64(de.Ctime)
	attr.Ctime = uint64(de.Ctime)
}
