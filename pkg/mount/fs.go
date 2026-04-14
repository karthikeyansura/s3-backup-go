package mount

import (
	"context"
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
func NewS3FS(ctx context.Context, cfg Config) (_ *S3FS, retErr error) {
	arch, err := OpenArchive(ctx, cfg.Store, cfg.ObjectKey, cfg.Verbose)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			_ = arch.Close()
		}
	}()

	var dataCache *DataCache
	if !cfg.NoCache {
		dataCache = NewDataCache(cfg.Store)
	}

	return &S3FS{
		store:     cfg.Store,
		names:     arch.Names,
		nsectors:  arch.NSectors,
		rootDE:    arch.RootDE,
		statfs:    arch.Statfs,
		dirCache:  arch.DirCache,
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

func (n *S3Node) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	dirent2attr(&n.de, &out.Attr)
	return 0
}

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

func (n *S3Node) Open(_ context.Context, _ uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

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

func (f *S3FS) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	dirent2attr(&f.rootDE, &out.Attr)
	return 0
}

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
