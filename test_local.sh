#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PASS=0
FAIL=0
TESTS=0

pass() { ((PASS++)); ((TESTS++)); echo "  PASS: $1"; }
fail() { ((FAIL++)); ((TESTS++)); echo "  FAIL: $1"; }

run_ok() {
    local desc="$1"; shift
    if "$@" >/dev/null 2>&1; then pass "$desc"; else fail "$desc"; fi
}

run_fail() {
    local desc="$1"; shift
    if "$@" >/dev/null 2>&1; then fail "$desc (expected failure)"; else pass "$desc"; fi
}

section() { echo ""; echo "=== $1 ==="; }

# ─── Cleanup stale artifacts ────────────────────────────
rm -f s3backup s3mount s3check
rm -f *.img

# ─── Build ──────────────────────────────────────────────
section "Build"
go build -o s3backup ./cmd/s3backup
go build -o s3check  ./cmd/s3check
echo "  Built s3backup and s3check"

# ─── Unit tests ─────────────────────────────────────────
section "Unit tests"
go test ./... 2>&1 | grep -E "^(ok|FAIL|---)" | sed 's/^/  /'

# ─── Setup temp root ────────────────────────────────────
TEST_ROOT=$(mktemp -d)
trap "rm -rf $TEST_ROOT" EXIT

# Helper: create a fresh source+backup pair, run backup, return paths via globals
fresh() {
    _SRC="$TEST_ROOT/${1}_src"
    _IMG="$TEST_ROOT/${1}.img"
    mkdir -p "$_SRC"
}

backup_it() {
    ./s3backup --local -b dummy "$_IMG" "$_SRC" >/dev/null 2>&1
}

# ─── LOCAL TESTS ────────────────────────────────────────

section "Test 1: Full backup + fsck + diff"
fresh t1
mkdir -p "$_SRC/sub/deep"
echo "hello world"              > "$_SRC/a.txt"
echo "second file"              > "$_SRC/b.txt"
printf 'binary\x00\x01\x02\x03'> "$_SRC/b.bin"
echo "nested"                   > "$_SRC/sub/n.txt"
echo "deep nested"              > "$_SRC/sub/deep/d.txt"
ln -s a.txt "$_SRC/link1"
ln -s sub/n.txt "$_SRC/link2"
mkdir "$_SRC/emptydir"
backup_it
run_ok  "backup creates image"          test -f "$_IMG"
run_ok  "image is sector-aligned"       test $(($(wc -c < "$_IMG") % 512)) -eq 0
run_ok  "fsck passes"                   ./s3check fsck --local "$_IMG"
run_ok  "diff matches original"         ./s3check diff --local "$_IMG" "$_SRC"

section "Test 2: Content modification"
fresh t2
echo "original" > "$_SRC/data.txt"
backup_it
run_ok   "diff matches before change"   ./s3check diff --local "$_IMG" "$_SRC"
echo "modified" > "$_SRC/data.txt"
run_fail "diff detects content change"   ./s3check diff --local "$_IMG" "$_SRC"

section "Test 3: New file"
fresh t3
echo "keep" > "$_SRC/keep.txt"
backup_it
run_ok   "diff matches before add"      ./s3check diff --local "$_IMG" "$_SRC"
echo "new" > "$_SRC/added.txt"
run_fail "diff detects new file"         ./s3check diff --local "$_IMG" "$_SRC"

section "Test 4: Deleted file"
fresh t4
echo "a" > "$_SRC/a.txt"
echo "b" > "$_SRC/b.txt"
backup_it
run_ok   "diff matches before delete"   ./s3check diff --local "$_IMG" "$_SRC"
rm "$_SRC/b.txt"
run_fail "diff detects deleted file"     ./s3check diff --local "$_IMG" "$_SRC"

section "Test 5: Symlink target change"
fresh t5
echo "target a" > "$_SRC/a.txt"
echo "target b" > "$_SRC/b.txt"
ln -s a.txt "$_SRC/mylink"
backup_it
run_ok   "diff matches before change"   ./s3check diff --local "$_IMG" "$_SRC"
rm "$_SRC/mylink" && ln -s b.txt "$_SRC/mylink"
run_fail "diff detects symlink change"   ./s3check diff --local "$_IMG" "$_SRC"

section "Test 6: Empty directory"
fresh t6
backup_it
run_ok  "backup handles empty dir"      test -f "$_IMG"
run_ok  "fsck passes on empty"          ./s3check fsck --local "$_IMG"
run_ok  "diff matches empty dir"        ./s3check diff --local "$_IMG" "$_SRC"

section "Test 7: Exclude flag"
fresh t7
mkdir -p "$_SRC/.ssh" "$_SRC/keep"
echo "secret" > "$_SRC/.ssh/id_rsa"
echo "public" > "$_SRC/keep/data.txt"
./s3backup --local -b dummy -e ".ssh" "$_IMG" "$_SRC" >/dev/null 2>&1
run_ok  "fsck passes with exclusions"   ./s3check fsck --local "$_IMG"

section "Test 8: 1000 small files"
fresh t8
echo "  Creating 1000 files..."
for i in $(seq 1 1000); do
    echo "content $i" > "$_SRC/f$(printf '%04d' $i).txt"
done
backup_it
run_ok  "backup handles 1000 files"     test -f "$_IMG"
run_ok  "fsck passes on 1000 files"     ./s3check fsck --local "$_IMG"
run_ok  "diff matches 1000 files"       ./s3check diff --local "$_IMG" "$_SRC"

section "Test 9: 5 MB file"
fresh t9
dd if=/dev/urandom of="$_SRC/medium.bin" bs=1M count=5 2>/dev/null
backup_it
run_ok  "backup handles 5MB"            test -f "$_IMG"
run_ok  "fsck passes on 5MB"            ./s3check fsck --local "$_IMG"
run_ok  "diff matches 5MB"              ./s3check diff --local "$_IMG" "$_SRC"

section "Test 10: 500 MB scale test"
fresh t10
echo "  Generating 500MB file (this takes a moment)..."
dd if=/dev/urandom of="$_SRC/large.bin" bs=1M count=500 2>/dev/null
echo "  Running backup..."
T0=$(date +%s)
backup_it
T1=$(date +%s)
echo "  Backup: $(wc -c < "$_IMG" | tr -d ' ') bytes in $((T1-T0))s"
run_ok  "backup handles 500MB"          test -f "$_IMG"
run_ok  "fsck passes on 500MB"          ./s3check fsck --local "$_IMG"
echo "  Running diff (streaming SHA-256)..."
T0=$(date +%s)
run_ok  "diff matches 500MB"            ./s3check diff --local "$_IMG" "$_SRC"
T1=$(date +%s)
echo "  Diff: $((T1-T0))s"
rm -f "$_SRC/large.bin" "$_IMG"

section "Test 11: Verbose fsck"
echo "  Output from Test 1 backup:"
./s3check fsck --local "$TEST_ROOT/t1.img" 2>&1 | sed 's/^/    /'

# ─── LOCAL RESULTS ──────────────────────────────────────
echo ""
echo "==========================================="
echo "  Local: $PASS passed, $FAIL failed, $TESTS total"
echo "==========================================="
LOCAL_FAIL=$FAIL

# ─── S3 INTEGRATION (MinIO via Docker Compose) ─────────

section "S3 Integration Tests (MinIO)"

if ! command -v docker >/dev/null 2>&1; then
    echo "  SKIP: docker not found"
    echo ""
    if [ "$LOCAL_FAIL" -gt 0 ]; then exit 1; fi
    echo "All local tests passed."
    exit 0
fi

if ! docker info >/dev/null 2>&1; then
    echo "  SKIP: docker daemon not running (start Docker Desktop)"
    echo ""
    if [ "$LOCAL_FAIL" -gt 0 ]; then exit 1; fi
    echo "All local tests passed."
    exit 0
fi

# Spin up MinIO
echo "  Starting MinIO..."
docker compose up -d --wait 2>/dev/null || docker-compose up -d 2>/dev/null
sleep 3

# Wait for MinIO to be ready
RETRIES=0
until curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; do
    ((RETRIES++))
    if [ "$RETRIES" -gt 15 ]; then
        echo "  SKIP: MinIO failed to start after 30s"
        docker compose down -v 2>/dev/null || docker-compose down -v 2>/dev/null || true
        if [ "$LOCAL_FAIL" -gt 0 ]; then exit 1; fi
        echo "All local tests passed."
        exit 0
    fi
    sleep 2
done

echo "  MinIO ready"

export S3_HOSTNAME=localhost:9000
export S3_ACCESS_KEY_ID=minioadmin
export S3_SECRET_ACCESS_KEY=minioadmin
S3_BUCKET=s3backup-test

s3_cleanup() {
    echo ""
    echo "  Tearing down MinIO..."
    docker compose down -v 2>/dev/null || docker-compose down -v 2>/dev/null || true
}
trap "rm -rf $TEST_ROOT; s3_cleanup" EXIT

section "S3 Test 1: Full backup + fsck + diff (5 MB)"
S3_SRC="$TEST_ROOT/s3_src"
mkdir -p "$S3_SRC/sub"
echo "s3 test file"     > "$S3_SRC/hello.txt"
echo "nested s3"        > "$S3_SRC/sub/deep.txt"
ln -s hello.txt "$S3_SRC/link"
dd if=/dev/urandom of="$S3_SRC/medium.bin" bs=1M count=5 2>/dev/null

run_ok  "s3 backup 5MB" \
    ./s3backup --bucket "$S3_BUCKET" --protocol http backups/full-001 "$S3_SRC"

run_ok  "s3 fsck" \
    ./s3check fsck --http "$S3_BUCKET/backups/full-001"

run_ok  "s3 diff matches" \
    ./s3check diff --http "$S3_BUCKET/backups/full-001" "$S3_SRC"

section "S3 Test 2: Mutation detection over S3"
echo "changed" > "$S3_SRC/hello.txt"
run_fail "s3 diff detects mutation" \
    ./s3check diff --http "$S3_BUCKET/backups/full-001" "$S3_SRC"

section "S3 Test 3: Scale test (500 MB over S3)"
S3_SCALE="$TEST_ROOT/s3_scale"
mkdir -p "$S3_SCALE"
echo "  Generating 500MB file..."
dd if=/dev/urandom of="$S3_SCALE/big.bin" bs=1M count=500 2>/dev/null

echo "  Uploading via streaming multipart..."
T0=$(date +%s)
run_ok  "s3 backup 500MB" \
    ./s3backup --bucket "$S3_BUCKET" --protocol http backups/scale-001 "$S3_SCALE"
T1=$(date +%s)
echo "  Upload: $((T1-T0))s"

run_ok  "s3 fsck 500MB" \
    ./s3check fsck --http "$S3_BUCKET/backups/scale-001"

echo "  Verifying content (streaming SHA-256 over S3)..."
T0=$(date +%s)
run_ok  "s3 diff 500MB" \
    ./s3check diff --http "$S3_BUCKET/backups/scale-001" "$S3_SCALE"
T1=$(date +%s)
echo "  Verify: $((T1-T0))s"

rm -f "$S3_SCALE/big.bin"

# ─── FINAL RESULTS ──────────────────────────────────────
echo ""
echo "==========================================="
echo "  Total: $PASS passed, $FAIL failed, $TESTS total"
echo "==========================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi

echo ""
echo "All tests passed."