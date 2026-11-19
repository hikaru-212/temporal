#!/usr/bin/env bash
# Reproduces the sdk-rust PR #1819 Stream replay activation merge on a real Temporal dev Server.
#
# A passing run means the known bug was OBSERVED: the test's assertions describe the buggy
# behavior of the pinned revision. It does not mean anything is fixed.
#
# Optional environment variables:
#   REPRO_SDK_DIR           local sdk-rust git checkout that contains the pinned commit
#                           (cloned, never modified); default: fetch from GitHub
#   REPRO_TEMPORAL_CLI      path to a Temporal CLI binary of the pinned version;
#                           default: download the official release and check its SHA-256
#   REPRO_WORK_DIR          work directory to create (must not exist); default: mktemp
#   REPRO_CARGO_TARGET_DIR  Cargo target directory; default: <work>/target
#   REPRO_MIN_FREE_GIB      minimum free disk space for the work directory; default: 10
#   REPRO_BUILD_TIMEOUT     build timeout in seconds; default: 10800
#   REPRO_TEST_TIMEOUT      test timeout in seconds; default: 900
#   REPRO_ALLOW_UNTESTED_PLATFORM=1  allow platforms other than linux/arm64
#   CARGO_BUILD_JOBS        default: 1; CARGO_NET_OFFLINE=true is honored by Cargo itself
set -euo pipefail

readonly SDK_REPO_URL="https://github.com/temporalio/sdk-rust.git"
readonly SDK_SHA="f858a1b0901f41255e3d00ca4ce81b86d05044d9"
readonly CLI_VERSION="1.8.3-server-1.32.0-162.0"
readonly PATCH_NAME="sdk-rust-1819-stream-replay-activation.patch"
readonly PATCH_SHA256="b0767b26140962b237fb204d5aaff2ecf1e913ac19aa65c33a1cd0e64a34d15d"
readonly TEST_NAME="core_tests::streams::investigation_1819_real::investigation_real_server_rejected_update_cold_replay"
readonly PATCHED_PATHS="crates/sdk-core/src/core_tests/streams.rs
crates/sdk-core/src/core_tests/streams/investigation_1819_real.rs"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_PID=""
WORK=""

log() { printf '[reproduce] %s\n' "$*" >&2; }
die() {
    log "ERROR: $*"
    exit 1
}
need() { command -v "$1" >/dev/null 2>&1 || die "missing required tool: $1"; }

cleanup() {
    local status=$?
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        log "stopping the Temporal dev Server this script started (pid $SERVER_PID)"
        kill -TERM "$SERVER_PID" 2>/dev/null || true
        for _ in $(seq 1 40); do
            kill -0 "$SERVER_PID" 2>/dev/null || break
            sleep 0.5
        done
        kill -0 "$SERVER_PID" 2>/dev/null && kill -KILL "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    if [[ -n "$WORK" ]]; then
        log "logs and evidence are kept in: $WORK"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

sha256_of() {
    python3 -I -c 'import hashlib, sys
h = hashlib.sha256()
with open(sys.argv[1], "rb") as f:
    for block in iter(lambda: f.read(1 << 20), b""):
        h.update(block)
print(h.hexdigest())' "$1"
}

for tool in git cargo python3 protoc uname; do need "$tool"; done
if command -v timeout >/dev/null 2>&1; then
    TIMEOUT=timeout
elif command -v gtimeout >/dev/null 2>&1; then
    TIMEOUT=gtimeout
else
    die "missing required tool: timeout (GNU coreutils)"
fi

case "$(uname -s)/$(uname -m)" in
Linux/aarch64 | Linux/arm64)
    PLATFORM=linux_arm64
    CLI_ARCHIVE_SHA256=ebf79ea0692965d30d2ae5b95d9a518e4dacf3e43708fcdf87a6fcb0edd58f95
    CLI_BINARY_SHA256=1f1bb3a5965532fcb21dc02053604fee7f6d5b47d8e556e7d4e7298397a45b03
    ;;
Linux/x86_64)
    PLATFORM=linux_amd64
    CLI_ARCHIVE_SHA256=fd26655b40f5a4ebeb3e632676f63c6c939bd2d05aec9d61dc23df190a950909
    CLI_BINARY_SHA256=""
    ;;
Darwin/arm64)
    PLATFORM=darwin_arm64
    CLI_ARCHIVE_SHA256=7052789d55c53ada1c01b73893e7ac72553d11d10fee9469d1405752ebb1b1cd
    CLI_BINARY_SHA256=""
    ;;
Darwin/x86_64)
    PLATFORM=darwin_amd64
    CLI_ARCHIVE_SHA256=10736201a82ad618485f3eb31c82c80afc381797d182f61d3d3f0b822a06e8e4
    CLI_BINARY_SHA256=""
    ;;
*) die "unsupported platform: $(uname -s)/$(uname -m)" ;;
esac
if [[ "$PLATFORM" != linux_arm64 && "${REPRO_ALLOW_UNTESTED_PLATFORM:-}" != 1 ]]; then
    die "only linux/arm64 has been tested; set REPRO_ALLOW_UNTESTED_PLATFORM=1 to try $PLATFORM"
fi

if [[ -n "${REPRO_WORK_DIR:-}" ]]; then
    [[ -e "$REPRO_WORK_DIR" ]] && die "REPRO_WORK_DIR already exists; refusing to overwrite: $REPRO_WORK_DIR"
    mkdir -p "$REPRO_WORK_DIR"
    WORK="$(cd "$REPRO_WORK_DIR" && pwd)"
else
    WORK="$(mktemp -d "${TMPDIR:-/tmp}/sdk-rust-1819-repro.XXXXXX")"
fi
mkdir -p "$WORK/logs" "$WORK/evidence" "$WORK/reproducer" "$WORK/cli"
log "work directory: $WORK"

python3 -I - "$WORK" "${REPRO_MIN_FREE_GIB:-10}" <<'PY'
import shutil, sys
free = shutil.disk_usage(sys.argv[1]).free / 2**30
if free < float(sys.argv[2]):
    sys.exit(f"[reproduce] ERROR: only {free:.1f} GiB free, need {sys.argv[2]} GiB")
print(f"[reproduce] free disk: {free:.1f} GiB", file=sys.stderr)
PY

# Only the expected regular files are read from the bundle, so no path in it can escape the
# work directory.
python3 -I - "$HERE/reproducer.tar.gz" "$WORK/reproducer" "$PATCH_NAME" <<'PY'
import os, sys, tarfile
archive, dest, patch = sys.argv[1:]
expected = {f"reproducer/{patch}", "reproducer/pinned.json", "reproducer/SHA256SUMS"}
with tarfile.open(archive, "r:gz") as tar:
    members = tar.getmembers()
    names = {m.name for m in members}
    if names != expected or not all(m.isfile() for m in members):
        sys.exit(f"[reproduce] ERROR: unexpected reproducer.tar.gz members: {sorted(names)}")
    for m in members:
        with open(os.path.join(dest, os.path.basename(m.name)), "wb") as out:
            out.write(tar.extractfile(m).read())
PY
PATCH="$WORK/reproducer/$PATCH_NAME"
[[ "$(sha256_of "$PATCH")" == "$PATCH_SHA256" ]] || die "patch SHA-256 mismatch"
log "patch verified: $PATCH_SHA256"

SRC="$WORK/sdk-rust"
if [[ -n "${REPRO_SDK_DIR:-}" ]]; then
    log "cloning the local checkout $REPRO_SDK_DIR (it is not modified)"
    git clone --quiet --no-checkout --no-hardlinks "$REPRO_SDK_DIR" "$SRC" >"$WORK/logs/git.log" 2>&1 ||
        die "cannot clone REPRO_SDK_DIR; see $WORK/logs/git.log"
    git -C "$SRC" checkout --quiet --detach "$SDK_SHA" >>"$WORK/logs/git.log" 2>&1 ||
        die "REPRO_SDK_DIR does not contain $SDK_SHA"
else
    log "fetching $SDK_SHA from $SDK_REPO_URL"
    git init --quiet "$SRC"
    git -C "$SRC" remote add origin "$SDK_REPO_URL"
    git -C "$SRC" fetch --quiet --depth 1 origin "$SDK_SHA" >"$WORK/logs/git.log" 2>&1 ||
        die "cannot fetch $SDK_SHA; see $WORK/logs/git.log"
    git -C "$SRC" checkout --quiet --detach FETCH_HEAD >>"$WORK/logs/git.log" 2>&1
fi
[[ "$(git -C "$SRC" rev-parse HEAD)" == "$SDK_SHA" ]] || die "checkout is not $SDK_SHA"
[[ -z "$(git -C "$SRC" status --porcelain)" ]] || die "checkout is not clean"
git -C "$SRC" apply --check "$PATCH" || die "the patch does not apply to $SDK_SHA"
git -C "$SRC" apply "$PATCH"
CHANGED="$(git -C "$SRC" status --porcelain --untracked-files=all | sed 's/^...//' | sort)"
[[ "$CHANGED" == "$(printf '%s\n' "$PATCHED_PATHS" | sort)" ]] ||
    die "the patch changed unexpected paths: $CHANGED"
log "checkout $SDK_SHA with the test-only patch applied"

if [[ -n "${REPRO_TEMPORAL_CLI:-}" ]]; then
    CLI="$REPRO_TEMPORAL_CLI"
    [[ -x "$CLI" ]] || die "REPRO_TEMPORAL_CLI is not executable: $CLI"
else
    need curl
    ARCHIVE_NAME="temporal_cli_${CLI_VERSION}_${PLATFORM}.tar.gz"
    URL="https://github.com/temporalio/cli/releases/download/v${CLI_VERSION}/${ARCHIVE_NAME}"
    log "downloading $URL"
    curl -fsSL --retry 3 -o "$WORK/cli/$ARCHIVE_NAME" "$URL" || die "download failed"
    [[ "$(sha256_of "$WORK/cli/$ARCHIVE_NAME")" == "$CLI_ARCHIVE_SHA256" ]] ||
        die "CLI archive SHA-256 mismatch; refusing to run it"
    log "CLI archive verified: $CLI_ARCHIVE_SHA256"
    python3 -I - "$WORK/cli/$ARCHIVE_NAME" "$WORK/cli/temporal" <<'PY'
import os, sys, tarfile
archive, dest = sys.argv[1:]
with tarfile.open(archive, "r:gz") as tar:
    member = tar.getmember("temporal")
    if not member.isfile():
        sys.exit("[reproduce] ERROR: the CLI archive's temporal member is not a regular file")
    with open(dest, "wb") as out:
        out.write(tar.extractfile(member).read())
os.chmod(dest, 0o755)
PY
    CLI="$WORK/cli/temporal"
fi
if [[ -n "$CLI_BINARY_SHA256" ]]; then
    [[ "$(sha256_of "$CLI")" == "$CLI_BINARY_SHA256" ]] || die "CLI binary SHA-256 mismatch: $CLI"
fi
CLI_REPORTED="$("$CLI" --version)"
[[ "$CLI_REPORTED" == "temporal version ${CLI_VERSION} "* ]] ||
    die "CLI version is not $CLI_VERSION: $CLI_REPORTED"
log "CLI: $CLI_REPORTED"

export CARGO_TARGET_DIR="${REPRO_CARGO_TARGET_DIR:-$WORK/target}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}"
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
log "building the sdk-core lib tests (CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS); this can take a while"
(cd "$SRC" && "$TIMEOUT" "${REPRO_BUILD_TIMEOUT:-10800}" cargo test -p temporalio-sdk-core --locked --lib --no-run) \
    >"$WORK/logs/build.log" 2>&1 || die "build failed; see $WORK/logs/build.log"

read -r GRPC_PORT HTTP_PORT METRICS_PORT < <(python3 -I -c 'import socket
socks = [socket.socket() for _ in range(3)]
for s in socks:
    s.bind(("127.0.0.1", 0))
print(*(s.getsockname()[1] for s in socks))
for s in socks:
    s.close()')
log "starting the Temporal dev Server on 127.0.0.1:$GRPC_PORT (in-memory)"
(cd "$WORK" && exec "$CLI" server start-dev --headless --ip 127.0.0.1 --port "$GRPC_PORT" \
    --http-port "$HTTP_PORT" --metrics-port "$METRICS_PORT" --log-level warn) \
    >"$WORK/logs/server.log" 2>&1 &
SERVER_PID=$!
for attempt in $(seq 1 120); do
    kill -0 "$SERVER_PID" 2>/dev/null || die "the Server exited early; see $WORK/logs/server.log"
    if "$CLI" operator namespace describe --address "127.0.0.1:$GRPC_PORT" -n default >/dev/null 2>&1; then
        break
    fi
    [[ "$attempt" == 120 ]] && die "the Server did not become ready"
    sleep 0.5
done

log "running $TEST_NAME"
set +e
(cd "$SRC" && PR1819_SERVER_TARGET="http://127.0.0.1:$GRPC_PORT" PR1819_REAL_EVIDENCE_DIR="$WORK/evidence" \
    "$TIMEOUT" "${REPRO_TEST_TIMEOUT:-900}" cargo test -p temporalio-sdk-core --locked --lib -- \
    --ignored --exact "$TEST_NAME" --test-threads=1 --nocapture) >"$WORK/logs/test.log" 2>&1
TEST_STATUS=$?
set -e

grep -E '^(RESULT|ASSERTED) ' "$WORK/logs/test.log" | cut -c1-400 >&2 || true
if [[ "$TEST_STATUS" != 0 ]]; then
    tail -n 40 "$WORK/logs/test.log" >&2
    die "the test failed (exit $TEST_STATUS); see $WORK/logs/test.log"
fi
# --exact with a wrong name runs no test and still exits 0.
grep -q '^test result: ok\. 1 passed;' "$WORK/logs/test.log" || die "the test did not run exactly once"
grep -q '^ASSERTED case=plain ' "$WORK/logs/test.log" || die "the plain case did not finish"
grep -q '^ASSERTED case=timer_per_page ' "$WORK/logs/test.log" || die "the timer_per_page case did not finish"
log "PASS: the known bug was observed on $SDK_SHA with Temporal CLI $CLI_VERSION"
log "      live W2 pages [p1] [p2] -> cold replay one read job [p1, p2];"
log "      live timers 2,3 -> replay timer 2 only -> Nondeterminism [TMPRL1100] at TimerStarted event 11"
log "This characterizes the bug. It does NOT mean the bug is fixed."
