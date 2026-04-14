#!/usr/bin/env bash
set -euo pipefail

# local end-to-end test for s3-backup-go
# Creates temp directories, runs all tools, validates all scenarios, cleans up.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PASS=0
FAIL=0
TESTS=0

pass() { ((PASS++)); ((TESTS++)); echo "  PASS: $1"; }
fail() { ((FAIL++)); ((TESTS++)); echo "  FAIL: $1"; }

run_expect_ok() {
    local desc="$1"; shift
    if "$@" >/dev/null 2>&1; then pass "$desc"; else fail "$desc"; fi
}

run_expect_fail() {
    local desc="$1"; shift
    if "$@" >/dev/null 2>&1; then fail "$desc (expected failure)"; else pass "$desc"; fi
}

echo "=== Building binaries ==="
go build -o s3backup ./cmd/s3backup
go build -o s3check  ./cmd/s3check
echo "OK"
echo ""

echo "=== Running unit tests ==="
go test ./... 2>&1 | tail -20
echo ""

# Setup
TEST_ROOT=$(mktemp -d)
trap "rm -rf $TEST_ROOT" EXIT

SRC="$TEST_ROOT/source"
BACKUP="$TEST_ROOT/backup.img"

# ─────────────────────────────────────────────
echo "=== Test 1: Full backup, fsck, diff match ==="
# ─────────────────────────────────────────────

mkdir -p "$SRC/sub/deep"
echo "hello world"       > "$SRC/a.txt"
echo "second file"       > "$SRC/b.txt"
printf 'binary\x00\x01\x02\x03' > "$SRC/b.bin"
echo "nested"            > "$SRC/sub/n.txt"
echo "deep nested"       > "$SRC/sub/deep/d.txt"
ln -s a.txt "$SRC/link1"
ln -s sub/n.txt "$SRC/link2"
mkdir "$SRC/emptydir"

echo "Source tree:"
find "$SRC" -not -path "$SRC" | sort
echo ""

run_expect_ok "backup creates image" \
    ./s3backup --local -b dummy "$BACKUP" "$SRC"

echo "  Backup size: $(wc -c < "$BACKUP") bytes"

run_expect_ok "fsck passes" \
    ./s3check fsck --local "$BACKUP"

run_expect_ok "diff matches original" \
    ./s3check diff --local "$BACKUP" "$SRC"

echo ""

# ─────────────────────────────────────────────
echo "=== Test 2: Content modification detection ==="
# ─────────────────────────────────────────────

echo "changed content" > "$SRC/a.txt"

run_expect_fail "diff detects content change" \
    ./s3check diff --local "$BACKUP" "$SRC"

# Restore
echo "hello world" > "$SRC/a.txt"
run_expect_ok "diff matches after restore" \
    ./s3check diff --local "$BACKUP" "$SRC"

echo ""

# ─────────────────────────────────────────────
echo "=== Test 3: New file detection ==="
# ─────────────────────────────────────────────

echo "brand new" > "$SRC/new_file.txt"

run_expect_fail "diff detects new file" \
    ./s3check diff --local "$BACKUP" "$SRC"

rm "$SRC/new_file.txt"
run_expect_ok "diff matches after removal" \
    ./s3check diff --local "$BACKUP" "$SRC"

echo ""

# ─────────────────────────────────────────────
echo "=== Test 4: Deleted file detection ==="
# ─────────────────────────────────────────────

rm "$SRC/b.txt"

run_expect_fail "diff detects deleted file" \
    ./s3check diff --local "$BACKUP" "$SRC"

echo "second file" > "$SRC/b.txt"
run_expect_ok "diff matches after re-creation" \
    ./s3check diff --local "$BACKUP" "$SRC"

echo ""

# ─────────────────────────────────────────────
echo "=== Test 5: Symlink target change detection ==="
# ─────────────────────────────────────────────

rm "$SRC/link1"
ln -s b.txt "$SRC/link1"

run_expect_fail "diff detects symlink target change" \
    ./s3check diff --local "$BACKUP" "$SRC"

rm "$SRC/link1"
ln -s a.txt "$SRC/link1"
run_expect_ok "diff matches after symlink restore" \
    ./s3check diff --local "$BACKUP" "$SRC"

echo ""

# ─────────────────────────────────────────────
echo "=== Test 6: Large file backup ==="
# ─────────────────────────────────────────────

LARGE_SRC="$TEST_ROOT/large_source"
LARGE_BACKUP="$TEST_ROOT/large.img"
mkdir -p "$LARGE_SRC"
dd if=/dev/urandom of="$LARGE_SRC/big.bin" bs=1M count=5 2>/dev/null

run_expect_ok "backup handles 5MB file" \
    ./s3backup --local -b dummy "$LARGE_BACKUP" "$LARGE_SRC"

run_expect_ok "fsck passes on large backup" \
    ./s3check fsck --local "$LARGE_BACKUP"

run_expect_ok "diff matches large file" \
    ./s3check diff --local "$LARGE_BACKUP" "$LARGE_SRC"

echo ""

# ─────────────────────────────────────────────
echo "=== Test 7: Empty directory backup ==="
# ─────────────────────────────────────────────

EMPTY_SRC="$TEST_ROOT/empty_source"
EMPTY_BACKUP="$TEST_ROOT/empty.img"
mkdir -p "$EMPTY_SRC"

run_expect_ok "backup handles empty directory" \
    ./s3backup --local -b dummy "$EMPTY_BACKUP" "$EMPTY_SRC"

run_expect_ok "fsck passes on empty backup" \
    ./s3check fsck --local "$EMPTY_BACKUP"

run_expect_ok "diff matches empty directory" \
    ./s3check diff --local "$EMPTY_BACKUP" "$EMPTY_SRC"

echo ""

# ─────────────────────────────────────────────
echo "=== Test 8: Exclude flag ==="
# ─────────────────────────────────────────────

EXCL_SRC="$TEST_ROOT/excl_source"
EXCL_BACKUP="$TEST_ROOT/excl.img"
mkdir -p "$EXCL_SRC/.ssh" "$EXCL_SRC/keep"
echo "secret" > "$EXCL_SRC/.ssh/id_rsa"
echo "public" > "$EXCL_SRC/keep/data.txt"

run_expect_ok "backup with --exclude" \
    ./s3backup --local -b dummy -e ".ssh" "$EXCL_BACKUP" "$EXCL_SRC"

run_expect_ok "fsck passes with exclusions" \
    ./s3check fsck --local "$EXCL_BACKUP"

echo ""

# ─────────────────────────────────────────────
echo "=== Test 9: Verbose fsck output ==="
# ─────────────────────────────────────────────

echo "  fsck verbose output for main backup:"
./s3check fsck --local "$BACKUP" 2>&1 | sed 's/^/    /'

echo ""

# ─────────────────────────────────────────────
echo "=== Test 10: Verbose diff output on mismatch ==="
# ─────────────────────────────────────────────

echo "tampered" > "$SRC/a.txt"
echo "  diff output on tampered tree:"
./s3check diff --local "$BACKUP" "$SRC" 2>&1 | sed 's/^/    /' || true
echo "hello world" > "$SRC/a.txt"

echo ""

# ─────────────────────────────────────────────
echo "==========================================="
echo "  Results: $PASS passed, $FAIL failed, $TESTS total"
echo "==========================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi

echo ""
echo "All tests passed."