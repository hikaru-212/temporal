# Reproducing PR #12772 evidence

These instructions were prepared from historical artifacts and were **not
executed during packaging**. They create fresh disposable source archives;
they do not require switching or editing any existing Temporal working tree.
The original four executed commands remain in
[../observed/corrected/commands.txt](../observed/corrected/commands.txt).
Earlier normal/race commands are preserved in their JSON records.

Prerequisites: Bash, Git, tar, patch, Python 3, a compatible C compiler for
`CGO_ENABLED=1` / `-race`, Go 1.27.0, and a Temporal Git repository containing
the exact BASE and HEAD objects. Network access or a populated module cache
is required. The historical platform was linux/arm64.

## Prepare fresh archives and test-only changes

Set `repo`, `evidence`, and `go127` to absolute paths on your machine.
Use a Go 1.27.0 binary; do not rely on the system Go 1.25.8 seen in the failed
historical setup attempt.

```bash
set -euo pipefail
repo=/absolute/path/to/temporal-source-repository
evidence=/absolute/path/to/temporal-review-evidence/12772
go127=/absolute/path/to/go1.27.0/bin/go

base=fbc3a07d87bdd0c4144188ed394449554aa0875b
head=1681a1054efe5967b5730cb8ffe98c3e9bd5182d
git -C "$repo" cat-file -e "$base^{commit}"
git -C "$repo" cat-file -e "$head^{commit}"
"$go127" version

run=$(mktemp -d /tmp/temporal-12772-reproduce-XXXXXXXX)
mkdir "$run/base" "$run/head" "$run/overlays" "$run/results" "$run/go-build"
git -C "$repo" archive "$base" | tar -x -C "$run/base"
git -C "$repo" archive "$head" | tar -x -C "$run/head"

patch --batch --fuzz=0 -p1 -d "$run/base" < "$evidence/tests/base-A-test-only.diff"
patch --batch --fuzz=0 -p1 -d "$run/base" < "$evidence/tests/base-B-test-only.diff"

python3 - "$run" <<'PY'
import json
from pathlib import Path
import shutil
import sys

root = Path(sys.argv[1]).resolve()
for invariant, rel in {
    "A": "service/history/workflow/context_test.go",
    "B": "service/history/api/respondworkflowtaskcompleted/api_test.go",
}.items():
    original = root / "head" / rel
    overlay = root / "overlays" / rel
    overlay.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(original, overlay)
    mapping = {"Replace": {str(original): str(overlay)}}
    (root / f"head-{invariant}-overlay.json").write_text(json.dumps(mapping, indent=2) + "\n")
PY

patch --batch --fuzz=0 -p1 -d "$run/overlays" < "$evidence/tests/head-A-observation-only.diff"
patch --batch --fuzz=0 -p1 -d "$run/overlays" < "$evidence/tests/head-B-observation-only.diff"

export GIT_OPTIONAL_LOCKS=0 GOTOOLCHAIN=local GOMAXPROCS=2 CGO_ENABLED=1
export GOPROXY=https://proxy.golang.org GOSUMDB=sum.golang.org
export GOCACHE="$run/go-build"

workflow='./service/history/workflow'
handler='./service/history/api/respondworkflowtaskcompleted'
test_a='^TestContextSuite$/^TestTaskCompletionBuffer_BudgetReleasedOnMergeAndClear$'
test_b='^TestWorkflowTaskCompletedHandlerSuite$/^TestWorkflowTaskCompletionPagination$/^Merged_pages_stay_reserved_until_the_transaction_is_persisted$'
flags=(-mod=readonly -tags=test_dep,disable_grpc_modules -count=1 -p=2 -timeout=5m -v)

run_case() {
    local label=$1 cwd=$2 code
    shift 2
    if (cd "$cwd" && "$go127" test "${flags[@]}" "$@") >"$run/results/$label.log" 2>&1; then
        code=0
    else
        code=$?
    fi
    printf '%s\n' "$code" >"$run/results/$label.exit-code"
    cat "$run/results/$label.log"
    printf '%s exit_code=%s\n' "$label" "$code"
}

run_case 01-base-A "$run/base" "$workflow" -run "$test_a"
run_case 02-head-A "$run/head" "-overlay=$run/head-A-overlay.json" "$workflow" -run "$test_a"
run_case 03-base-B "$run/base" "$handler" -run "$test_b"
run_case 04-head-B "$run/head" "-overlay=$run/head-B-overlay.json" "$handler" -run "$test_b"
```

Expected exits are 1, 0, 1, 0. Inspect the failing logs: an environment or build
failure is not a valid RED result. The expected behavioral values are listed
in the case README. The fresh cache may change compilation duration, but is
not a claim to reproduce the historical timings.

The original PR test diff is already present at HEAD and must not be applied
again. Only the BASE-compatible patches belong on BASE. The HEAD patches add
logging to overlay copies, preserving the archived HEAD source text.

## Earlier uninstrumented HEAD normal/race checks

Using the same Bash session and unchanged `"$run/head"` archive, omit overlays:

```bash
workflow_tests='^TestContextSuite$/^TestTaskCompletionBuffer_BudgetReleasedOn(MergeAndClear|FailedMerge)$'
run_case 04-workflow-normal "$run/head" "$workflow" -run "$workflow_tests"
run_case 05-handler-normal "$run/head" "$handler" -run "$test_b"
run_case 06-workflow-race "$run/head" -race "$workflow" -run "$workflow_tests"
run_case 07-handler-race "$run/head" -race "$handler" -run "$test_b"
```

All four historical checks exited 0. These commands are optional future
reproduction, not additional results from package preparation. This document
does not publish or modify the evidence repository's Git branch.
