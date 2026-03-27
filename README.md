# S3 Incremental Backup and FUSE Client — Go Implementation

**s3backup** performs a full or incremental backup of a directory hierarchy to a single S3 object; **s3mount** mounts a backup as a read-only FUSE file system.

This implementation is a ground-up port of [pjd-nu/s3-backup](https://github.com/pjd-nu/s3-backup) from C to Go, replacing the unmaintained `libs3` library with the [MinIO Go SDK](https://github.com/minio/minio-go), eliminating `libavl` in favor of native Go maps, and implementing the mmap-based directory cache optimization described in the original project's cleanup notes.

The on-disk binary format is fully preserved — backups created by either the C or Go version can be mounted by the other.

Original project by Peter Desnoyers, Northeastern University, Solid-State Storage Lab.

## Usage

Environment variables: `S3_HOSTNAME`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`
(also available as command-line flags)

```
s3backup --bucket BUCKET [--incremental OBJECT] [--max SIZE] OBJECT /path
s3mount [--local] bucket/key /mountpoint
```

## Description

s3backup stores a snapshot of a file system as a single S3 object, using a simplified log-structured file system with 512-byte sectors. It supports incremental backups by chaining a sequence of these objects — each incremental backup stores only files whose metadata (mode, size, ctime, uid, gid) has changed since the previous version.

Although point-in-time snapshots are not supported, the incremental model allows creation of a *fuzzy snapshot* by following a full backup with an incremental one. Inconsistencies are bounded by the time it takes to traverse the local file system during the incremental pass.

The FUSE client aggressively caches data and directories, flags files as cacheable to the kernel, stores symbolic links, and preserves owners, timestamps, and permissions. Directory metadata is cached via `mmap` on a temporary file rather than held in heap memory, allowing the OS to page it in and out as needed — significantly reducing resident memory for large backups.

Mount points are not traversed (equivalent to the `-xdev` flag to `find`); empty directories are stored for any encountered mount points.

### Features

- S3 hostname, access key, and secret key can be provided by flags as well as environment variables
- The `--local` flag forces object names to be interpreted as local file paths, for debugging without an S3 endpoint
- `--max SIZE` stops backup after a given amount of data (K/M/G suffixes accepted), allowing large full backups to be split into a chain of smaller objects. Files are not broken across backups, so the limit is soft.
- `--exclude` skips directories or files matching a path, e.g. excluding `.ssh` to avoid archiving SSH keys

## Building

Requires Go 1.22 or later. No C dependencies are needed for `s3backup`; `s3mount` requires FUSE support ([FUSE-T](https://github.com/macos-fuse-t/fuse-t) on macOS, `libfuse-dev` on Linux).

```bash
go mod tidy
go build ./cmd/s3backup
go build ./cmd/s3mount
```

### Running Tests

```bash
go test ./pkg/s3fs/...      # binary format round-trip tests
go test ./pkg/backup/...    # end-to-end backup + verification (local mode)
```

## What Changed from the C Version

| Component | C Original | Go Implementation |
|-----------|-----------|-------------------|
| S3 library | libs3 (unmaintained, required 64-bit patch) | minio-go v7 (multipart handled automatically) |
| Map / tree | libavl | Go built-in `map` |
| Directory cache | In-memory heap buffers | mmap'd temp file (zero-copy lookups) |
| Data cache | Fixed 16-entry LRU, 16 MiB blocks | Same algorithm, `sync.Mutex`-protected |
| CLI parsing | argp | cobra |
| UUID | libuuid | google/uuid |
| FUSE | libfuse 2.7 (C callbacks) | hanwen/go-fuse/v2 (Go interfaces) |
| Platform compat | `#ifdef` / gcc attributes | Build-tagged `compat_darwin.go` / `compat_linux.go` |
| Build system | Makefile + manual deps | `go build` (single command, no system packages) |

### Key Improvements

- **mmap directory cache**: The original `s3mount` loads all directory data into heap-allocated buffers. This implementation writes packed directory contents to a temp file and `mmap`s it read-only, giving the OS control over paging. This addresses the memory usage concern noted in the original project's "Implementation cleanup" section.
- **No libavl dependency**: The AVL tree used for directory offset lookups is replaced by a Go `map[uint64]` keyed on the raw S3 offset value. This also addresses the original project's note about using binary or interpolation search as an alternative.
- **Automatic multipart upload**: The ~400 lines of libs3 callback machinery, retry logic, and XML assembly for multipart uploads collapse into a single `client.PutObject()` call — minio-go handles chunking and part management internally.

## Project Structure

```
cmd/s3backup/    CLI entry point for backup
cmd/s3mount/     CLI entry point for FUSE mount
pkg/s3fs/        On-disk format: packed struct serialization, dirent/version iteration
pkg/store/       ObjectStore interface with S3 (minio-go) and local file backends
pkg/backup/      Backup engine: directory traversal, incremental diffing, sector-aligned I/O
pkg/mount/       FUSE filesystem, mmap-based directory cache, LRU data block cache
pkg/compat/      Platform abstraction for syscall.Stat_t field differences (darwin vs linux)
```

## Design / On-Disk Format

The binary format is identical to the original C implementation. Objects use a log-structured layout with 512-byte sectors. Describing it from the inside out:

### Offsets

Sectors are addressed by an 8-byte packed offset:

| object# : 16 | sector offset : 48 |
|--------------|-------------------|

### Directory Entries

Variable-sized, packed with no alignment padding:

| mode : 16 | uid : 16 | gid : 16 | ctime : 32 | offset : 64 | bytes:52 + xattr:12 | namelen : 8 | name |
|-----------|---------|---------|-----------|-----------|-------------------|------------|------|

Names are not null-terminated. An iterator (`IterDirents`) walks entries by computing `fixed_size + namelen` to advance.

### Object Header (Superblock)

All fixed fields are 4 bytes:

| magic | version | flags | len | nversions | \<versions\> |
|-------|---------|-------|-----|-----------|-------------|

Magic is `0x55423353` (`S3BU` in little-endian). Versions are ordered newest-first:

| uuid : 128 | namelen : 16 | name |
|-----------|-------------|------|

For constructing offsets, versions are numbered in reverse — the oldest version is numbered 0.

### Object Trailer

The last 512-byte sector of the object contains:

- **First dirent**: points to the root directory
- **Second dirent**: hidden entry pointing to the directory location table (`s3dirloc` array)
- **Third dirent**: hidden entry pointing to packed directory contents
- **Last 52 bytes**: filesystem statistics (`s3statfs`)

The directory location table maps each directory's S3 offset to its byte count. By summing these, the byte offset of each directory's data within the packed contents can be computed, enabling all directories to be loaded at mount time without additional S3 round-trips.

## Dependencies

| Package | Purpose |
|---------|---------|
| `github.com/minio/minio-go/v7` | S3-compatible object storage access |
| `github.com/hanwen/go-fuse/v2` | FUSE filesystem implementation |
| `github.com/google/uuid` | UUID generation for version chain |
| `github.com/spf13/cobra` | Command-line interface |
| `golang.org/x/sys` | mmap and platform-specific syscalls |