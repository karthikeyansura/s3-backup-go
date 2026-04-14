# S3 Incremental Backup and FUSE Client — Go Implementation

**s3backup** performs a full or incremental backup of a directory hierarchy to a single S3 object; **s3mount** mounts a backup as a read-only FUSE file system; **s3check** validates backup integrity and compares backups against source trees.

This implementation is a ground-up port of [pjd-nu/s3-backup](https://github.com/pjd-nu/s3-backup) from C to Go, replacing the unmaintained `libs3` library with the [MinIO Go SDK](https://github.com/minio/minio-go), eliminating `libavl` in favor of native Go maps, and implementing the mmap-based directory cache optimization described in the original project's cleanup notes.

The on-disk binary format is fully preserved — backups created by either the C or Go version can be mounted by the other.

Original project by Peter Desnoyers, Northeastern University, Solid-State Storage Lab.

## Usage

Environment variables: `S3_HOSTNAME`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`
(also available as command-line flags)

```
s3backup --bucket BUCKET [--incremental OBJECT] [--max SIZE] [--exclude PATH] OBJECT /path
s3mount  [--local] bucket/key /mountpoint
s3check  fsck [--local] TARGET
s3check  diff [--local] TARGET DIRECTORY
```

## Description

s3backup stores a snapshot of a file system as a single S3 object, using a simplified log-structured file system with 512-byte sectors. It supports incremental backups by chaining a sequence of these objects — each incremental backup stores only files whose metadata (mode, size, ctime, uid, gid) has changed since the previous version.

Although point-in-time snapshots are not supported, the incremental model allows creation of a *fuzzy snapshot* by following a full backup with an incremental one. Inconsistencies are bounded by the time it takes to traverse the local file system during the incremental pass.

The FUSE client aggressively caches data and directories, flags files as cacheable to the kernel, stores symbolic links, and preserves owners, timestamps, and permissions. Directory metadata is cached via `mmap` on a temporary file rather than held in heap memory, allowing the OS to page it in and out as needed — significantly reducing resident memory for large backups.

### Traversal Semantics

During backup, the directory tree traversal handles these cases:

- **Symbolic links**: stored as data entries containing the link target path string, preserving the original target verbatim (e.g. `mydir -> /opt/xyz/mine` stores `"/opt/xyz/mine"`). Restored faithfully on mount via FUSE `Readlink`.
- **Cross-device mount points**: detected by comparing `st_dev` of child directories against the parent. Mount point directories are stored as empty entries (zero offset, zero bytes) and not descended into, equivalent to `find -xdev`.
- **FIFOs**: skipped entirely to avoid blocking the traversal thread.
- **Block/character devices**: metadata stored (mode, uid, gid, device number encoded in the bytes field), no data.

### Features

- S3 hostname, access key, and secret key can be provided by flags as well as environment variables
- The `--local` flag forces object names to be interpreted as local file paths, for debugging without an S3 endpoint
- `--max SIZE` stops backup after a given amount of data (K/M/G suffixes accepted), allowing large full backups to be split into a chain of smaller objects. Files are not broken across backups, so the limit is soft.
- `--exclude` skips directories or files matching a path, e.g. excluding `.ssh` to avoid archiving SSH keys

## Validation and Comparison Tools

**s3check** provides two subcommands for verifying backup correctness:

### `s3check fsck` — Structural Consistency Check

Validates the internal structure of a backup object without needing the original source tree:

- Parses and verifies the superblock magic, version chain UUIDs, and sector alignment
- Walks the entire directory tree via the packed directory cache
- Validates every dirent's object index and sector offset are within bounds
- Verifies the full data span (sector + ceil(bytes/512)) fits within the version object
- Reads back every regular file and symlink to confirm data length matches the recorded byte count
- Cross-checks `statfs` counters against the walked tree (accounting for hidden `_dirloc_`/`_dirdat_` metadata entries)

```
./s3check fsck --local /path/to/backup.img
./s3check fsck mybucket/backups/full-001
```

### `s3check diff` — Backup vs. Original Comparison

Compares a backup object against a live directory tree, reporting all differences:

- Walks both trees in parallel matching the backup's traversal semantics (no cross-device descent, skip FIFOs)
- Compares metadata: mode, uid, gid, ctime, size
- Compares symlink targets verbatim
- Compares regular file content using streaming SHA-256 hashes (4 MiB chunks, no full file memory load)
- Reports missing-in-backup, missing-locally, and per-field mismatches as a structured report

```
./s3check diff --local /path/to/backup.img /path/to/source
./s3check diff mybucket/backups/full-001 /path/to/source
```

## Building

Requires Go 1.22 or later. No C dependencies are needed for `s3backup` or `s3check`; `s3mount` requires FUSE support ([FUSE-T](https://github.com/macos-fuse-t/fuse-t) on macOS, `libfuse-dev` on Linux).

```bash
go mod tidy
go build ./cmd/s3backup
go build ./cmd/s3mount
go build ./cmd/s3check
```

### Running Tests

```bash
# Unit and integration tests
go test ./...

# Comprehensive local end-to-end smoke test
chmod +x test_local.sh
./test_local.sh
```

The test suite covers:

- Binary format round-trip tests (`pkg/s3fs`)
- End-to-end backup + superblock/trailer verification (`pkg/backup`)
- Structural fsck on local backup images (`pkg/fsck`)
- Backup-vs-original comparison including content mutation, new file, and deleted file detection (`pkg/compare`)
- Local smoke test script exercising all three CLI tools across 10 scenarios

## What Changed from the C Version

| Component       | C Original                                    | Go Implementation                                         |
|-----------------|-----------------------------------------------|-----------------------------------------------------------|
| S3 library      | libs3 (unmaintained, required 64-bit patch)    | minio-go v7 (streaming multipart via `io.Pipe`)           |
| S3 upload       | In-memory buffer + manual multipart callbacks  | Streaming `io.Pipe` + `bufio.Writer` + `PutObject(-1)`    |
| Map / tree      | libavl                                         | Go built-in `map`                                         |
| Directory cache | In-memory heap buffers                         | mmap'd temp file (zero-copy lookups)                      |
| Data cache      | Fixed 16-entry LRU, 16 MiB blocks             | Same algorithm, `sync.Mutex`-protected                    |
| CLI parsing     | argp                                           | cobra                                                     |
| UUID            | libuuid                                        | google/uuid                                               |
| FUSE            | libfuse 2.7 (C callbacks)                      | hanwen/go-fuse/v2 (Go interfaces)                         |
| Platform compat | `#ifdef` / gcc attributes                      | Build-tagged `compat_darwin.go` / `compat_linux.go`       |
| Build system    | Makefile + manual deps                         | `go build` (single command, no system packages)           |
| Validation      | (none)                                         | `s3check fsck` (structural) + `s3check diff` (content)    |

### Key Improvements

- **Streaming multipart upload**: The S3 writer uses `io.Pipe` with a 1 MiB `bufio.Writer` feeding `PutObject` with unknown size (`-1`), so minio-go handles automatic multipart chunking. Backup traversal writes stream directly into the upload pipeline without buffering the entire object in memory. Writer close errors are propagated via named returns to surface upload failures that occur during finalization.
- **mmap directory cache**: The original `s3mount` loads all directory data into heap-allocated buffers. This implementation writes packed directory contents to a temp file and `mmap`s it read-only, giving the OS control over paging. This addresses the memory usage concern noted in the original project's "Implementation cleanup" section.
- **No libavl dependency**: The AVL tree used for directory offset lookups is replaced by a Go `map[uint64]` keyed on the raw S3 offset value. This also addresses the original project's note about using binary or interpolation search as an alternative.
- **Shared archive loader**: The `OpenArchive` function centralizes superblock parsing, version chain verification, trailer loading, and directory cache construction. It is shared across `s3mount`, `s3check fsck`, and `s3check diff`, eliminating duplicated loading logic. Version names stored as basenames in the superblock are resolved relative to the primary object's directory for local file mode compatibility.
- **Structural fsck**: Walks the entire directory tree from the backup object, validates every dirent's object index and data span bounds, reads back all file and symlink data to verify size consistency, and cross-checks statfs counters.
- **Backup-vs-original comparison**: Parallel tree walk comparing metadata and streaming SHA-256 content hashes. Reports all differences (not just the first), handling cross-device mount points and symlink targets correctly.

## Project Structure

```
cmd/s3backup/    CLI entry point for backup
cmd/s3mount/     CLI entry point for FUSE mount
cmd/s3check/     CLI entry point for fsck and diff validation tools
pkg/s3fs/        On-disk format: packed struct serialization, dirent/version iteration
pkg/store/       ObjectStore interface with S3 (minio-go) and local file backends
pkg/backup/      Backup engine: directory traversal, incremental diffing, sector-aligned I/O
pkg/mount/       FUSE filesystem, shared archive loader, mmap-based directory cache, LRU data block cache
pkg/fsck/        Structural consistency checker (fsck)
pkg/compare/     Backup-vs-original tree comparison with streaming SHA-256
pkg/compat/      Platform abstraction for syscall.Stat_t field differences (darwin vs linux)
```

## Design / On-Disk Format

The binary format is identical to the original C implementation. Objects use a log-structured layout with 512-byte sectors. Describing it from the inside out:

### Offsets

Sectors are addressed by an 8-byte packed offset:

| Field          | Bits  |
|----------------|-------|
| object#        | 16    |
| sector offset  | 48    |

### Directory Entries

Variable-sized, packed with no alignment padding:

| Field           | Size (bits) |
|-----------------|-------------|
| mode            | 16          |
| uid             | 16          |
| gid             | 16          |
| ctime           | 32          |
| offset          | 64          |
| bytes + xattr   | 52 + 12     |
| namelen         | 8           |
| name            | variable    |

Names are not null-terminated. An iterator (`IterDirents`) walks entries by computing `fixed_size + namelen` to advance.

### Object Header (Superblock)

All fixed fields are 4 bytes:

| Field      | Size (bytes) |
|------------|--------------|
| magic      | 4            |
| version    | 4            |
| flags      | 4            |
| len        | 4            |
| nversions  | 4            |
| versions   | variable     |

Magic is `0x55423353` (`S3BU` in little-endian). Versions are ordered newest-first:

| Field   | Size (bits) |
|---------|-------------|
| uuid    | 128         |
| namelen | 16          |
| name    | variable    |

For constructing offsets, versions are numbered in reverse — the oldest version is numbered 0.

### Object Trailer

The last 512-byte sector of the object contains:

- **First dirent**: points to the root directory
- **Second dirent**: hidden entry pointing to the directory location table (`s3dirloc` array)
- **Third dirent**: hidden entry pointing to packed directory contents
- **Last 52 bytes**: filesystem statistics (`s3statfs`)

The directory location table maps each directory's S3 offset to its byte count. By summing these, the byte offset of each directory's data within the packed contents can be computed, enabling all directories to be loaded at mount time without additional S3 round-trips.

## Dependencies

| Package                        | Purpose                                  |
|--------------------------------|------------------------------------------|
| `github.com/minio/minio-go/v7` | S3-compatible object storage access      |
| `github.com/hanwen/go-fuse/v2` | FUSE filesystem implementation           |
| `github.com/google/uuid`       | UUID generation for version chain        |
| `github.com/spf13/cobra`       | Command-line interface                   |
| `golang.org/x/sys`             | mmap and platform-specific syscalls      |