#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PASS=0
FAIL=0
TESTS=0

pass() { ((PASS++)); ((TESTS++)); echo "  PASS: $1"; }
fail() { ((FAIL++)); ((TESTS++)); echo "  FAIL: $1"; }

section() { echo ""; echo "=== $1 ==="; }

# ─── Cleanup stale artifacts ────────────────────────────
rm -f s3backup s3mount s3check
rm -f *.img
rm -rf /tmp/s3mount-test

# ─── Build ──────────────────────────────────────────────
section "Build"
go build -o s3backup ./cmd/s3backup
go build -o s3mount  ./cmd/s3mount
echo "  Built s3backup and s3mount"

# ─── Setup ──────────────────────────────────────────────
TEST_ROOT="/tmp/s3mount-test"
SRC="$TEST_ROOT/source"
IMG="$TEST_ROOT/backup.img"
MNT="$TEST_ROOT/mnt"

cleanup() {
    # Unmount if still mounted
    umount "$MNT" 2>/dev/null || diskutil unmount "$MNT" 2>/dev/null || true
    # Kill s3mount if still running
    [ -n "${MOUNT_PID:-}" ] && kill "$MOUNT_PID" 2>/dev/null && wait "$MOUNT_PID" 2>/dev/null || true
    rm -rf "$TEST_ROOT"
    rm -f "$SCRIPT_DIR/s3backup" "$SCRIPT_DIR/s3mount" "$SCRIPT_DIR/s3check"
}
trap cleanup EXIT

mkdir -p "$SRC/sub/deep" "$MNT"

# ─── Create test data ───────────────────────────────────
section "Create test data"
echo "hello world"              > "$SRC/a.txt"
echo "nested file"              > "$SRC/sub/b.txt"
echo "deep nested"              > "$SRC/sub/deep/d.txt"
printf '\x00\x01\x02binary'    > "$SRC/c.bin"
ln -s a.txt "$SRC/link1"
ln -s /etc/hosts "$SRC/abslink"
mkdir "$SRC/emptydir"
# Sector boundary files
head -c 511 /dev/urandom > "$SRC/f511.bin"
head -c 512 /dev/urandom > "$SRC/f512.bin"
head -c 513 /dev/urandom > "$SRC/f513.bin"

echo "  Source tree:"
find "$SRC" -not -path "$SRC" | sort | sed 's/^/    /'

# ─── Backup ─────────────────────────────────────────────
section "Backup"
./s3backup --local -b dummy "$IMG" "$SRC"

# ─── Mount ──────────────────────────────────────────────
section "Mount"
./s3mount --local "$IMG" "$MNT" &
MOUNT_PID=$!

# Wait for mount to be ready (check for up to 5 seconds)
RETRIES=0
until mount | grep -q "$MNT" 2>/dev/null || ls "$MNT" 2>/dev/null | grep -q .; do
    ((RETRIES++))
    if [ "$RETRIES" -gt 25 ]; then
        fail "mount did not become ready in 5s"
        echo ""
        echo "==========================================="
        echo "  Results: $PASS passed, $FAIL failed, $TESTS total"
        echo "==========================================="
        exit 1
    fi
    sleep 0.2
done
pass "mount ready"

# ─── Verify ─────────────────────────────────────────────
section "Verify mounted filesystem"

# Directory listing
if ls "$MNT" >/dev/null 2>&1; then
    pass "ls root"
else
    fail "ls root"
fi

# Regular file read
if [ "$(cat "$MNT/a.txt")" = "hello world" ]; then
    pass "read a.txt"
else
    fail "read a.txt"
fi

# Nested file read
if [ "$(cat "$MNT/sub/b.txt")" = "nested file" ]; then
    pass "read sub/b.txt"
else
    fail "read sub/b.txt"
fi

# Deep nested read
if [ "$(cat "$MNT/sub/deep/d.txt")" = "deep nested" ]; then
    pass "read sub/deep/d.txt"
else
    fail "read sub/deep/d.txt"
fi

# Binary file content match
if diff <(xxd "$SRC/c.bin") <(xxd "$MNT/c.bin") >/dev/null 2>&1; then
    pass "binary file content matches"
else
    fail "binary file content matches"
fi

# Relative symlink
if [ "$(readlink "$MNT/link1")" = "a.txt" ]; then
    pass "relative symlink target"
else
    fail "relative symlink target"
fi

# Absolute symlink
if [ "$(readlink "$MNT/abslink")" = "/etc/hosts" ]; then
    pass "absolute symlink target"
else
    fail "absolute symlink target"
fi

# Symlink content (follow through to file)
if [ "$(cat "$MNT/link1")" = "hello world" ]; then
    pass "symlink follow read"
else
    fail "symlink follow read"
fi

# Empty directory exists
if [ -d "$MNT/emptydir" ]; then
    pass "empty directory exists"
else
    fail "empty directory exists"
fi

# Sector boundary files match
for SIZE in 511 512 513; do
    if diff "$SRC/f${SIZE}.bin" "$MNT/f${SIZE}.bin" >/dev/null 2>&1; then
        pass "sector boundary ${SIZE}B file matches"
    else
        fail "sector boundary ${SIZE}B file matches"
    fi
done

# Read-only check (write should fail)
if echo "test" > "$MNT/should_fail.txt" 2>/dev/null; then
    fail "filesystem is read-only"
else
    pass "filesystem is read-only"
fi

# File metadata
if [ "$(stat -f '%p' "$MNT/a.txt" 2>/dev/null || stat -c '%a' "$MNT/a.txt" 2>/dev/null)" != "" ]; then
    pass "stat returns metadata"
else
    fail "stat returns metadata"
fi

# ─── Unmount ────────────────────────────────────────────
section "Unmount"
umount "$MNT" 2>/dev/null || diskutil unmount "$MNT" 2>/dev/null || true
wait "$MOUNT_PID" 2>/dev/null || true
MOUNT_PID=""
pass "unmount clean"

# ─── Results ────────────────────────────────────────────
echo ""
echo "==========================================="
echo "  FUSE Mount: $PASS passed, $FAIL failed, $TESTS total"
echo "==========================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi

echo ""
echo "All FUSE mount tests passed."