# Temporal Server PR #12772: pagination limiter reservation lifetime

The retained execution records establish **VERIFIED BASE RED / HEAD GREEN**
for the two focused invariants below. This package was prepared from existing
artifacts; no tests were rerun during packaging.

PR: [temporalio/temporal#12772](https://github.com/temporalio/temporal/pull/12772).

| Revision role | Exact commit |
| --- | --- |
| Tested BASE | `fbc3a07d87bdd0c4144188ed394449554aa0875b` |
| Tested PR HEAD | `1681a1054efe5967b5730cb8ffe98c3e9bd5182d` |
| Recorded PR target snapshot, not the tested BASE | `01766a34880758f7dad6697621777287b777fbd8` |

The tested BASE is both HEAD's direct parent and the merge-base of HEAD with
the recorded target snapshot. The corrected records consistently use that
BASE. No RED/GREEN result is attributed to the target snapshot itself.

## Corrected four-run matrix

A requires a successful merge to retain the byte reservation. HEAD additionally
checks that context reconciliation and Clear preserve the reservation until
the returned release function runs.

B requires the merged pages to remain reserved at the persistence callback,
with the reservation released after the handler returns.

| Invariant / revision | Exit | Observed reservation values | Result |
| --- | ---: | --- | --- |
| A / BASE | 1 | initial=16; after_merge=0 | RED: expected the reservation to remain 16 |
| A / HEAD | 0 | initial=16; after_merge=16; after_context_cleanup=16; after_release=0 | GREEN |
| B / BASE | 1 | initial=171; at_persistence=0; after_handler=0 | RED: expected 171 at persistence |
| B / HEAD | 0 | initial=171; at_persistence=171; after_handler=0 | GREEN |

Each row has an unmodified `.json` execution record and `.log` under
[observed/corrected/](observed/corrected/). Records include argv, environment,
revision, exit code, and elapsed time. The RED logs show assertion failures,
not build failures. [results.json](observed/corrected/results.json) is the
original result summary, independently cross-checked against all four logs.

## Original tests and BASE adaptations

[tests/original-pr-test-only.diff](tests/original-pr-test-only.diff) is generated
from the exact BASE-to-HEAD Git diff restricted to the PR's two changed
`*_test.go` files. It includes the original assertions and API-signature
adjustments. It is archival context, not the patch to apply to BASE: BASE
returns two values from GetMergedTaskCompletionPages, while HEAD returns three,
including the release function.

- [base-A-test-only.diff](tests/base-A-test-only.diff) keeps BASE's two-result
  API call, records the reservation, and changes the post-merge expectation
  to retention. It does not invent a release API on BASE.
- [base-B-test-only.diff](tests/base-B-test-only.diff) adds the persistence
  boundary test with observation logging, using BASE-compatible APIs.
- [head-A-observation-only.diff](tests/head-A-observation-only.diff) and
  [head-B-observation-only.diff](tests/head-B-observation-only.diff) only add
  Logf observations to the existing HEAD tests. Historical HEAD runs used Go
  file overlays; the original HEAD working tree remained clean.

All four historical patches are copied byte-for-byte. Reconstructing each
diff from its Git source revision and the retained executed test file produces
the exact archived patch. The original BASE manifest has 4,094 files; its
comparison with the retained BASE directory finds only the two adapted test
files changed, with no missing or extra files. See
[observed/source-verification.json](observed/source-verification.json).

## Earlier HEAD execution results

These are a separate, earlier group of tests on the uninstrumented HEAD.
They are not extra corrected BASE executions or a BASE/HEAD race comparison.

| Historical record | Focus | Mode | Exit |
| --- | --- | --- | ---: |
| 04-workflow-normal | MergeAndClear and FailedMerge | normal | 0 |
| 05-handler-normal | persistence-boundary subtest | normal | 0 |
| 06-workflow-race | MergeAndClear and FailedMerge | race detector | 0 |
| 07-handler-race | persistence-boundary subtest | race detector | 0 |

The raw logs and JSON records are under [observed/earlier/](observed/earlier/).
The earlier command records do not individually embed HEAD; the retained
[final worktree check](observed/earlier/08-worktree-final.json) identifies
`1681a1054efe5967b5730cb8ffe98c3e9bd5182d` and a clean worktree.

Three preceding environment/setup attempts also exited 1 and are retained in
[observed/setup-attempts/](observed/setup-attempts/): Go 1.25.8 was too old;
automatic Go 1.27.0 selection failed with GOSUMDB disabled; direct use of the
cached toolchain then hit a read-only module-cache lock while a dependency was
missing. None reached the behavioral assertions, so none counts as BASE RED.

## Environment and reproduction

Recorded toolchain: `go version go1.27.0 linux/arm64`.
The successful runs used `GOTOOLCHAIN=local`, `GOMAXPROCS=2`,
`CGO_ENABLED=1`, `GOPROXY=https://proxy.golang.org`,
`GOSUMDB=sum.golang.org`, and the existing Go build cache.
Flags include `-mod=readonly -tags=test_dep,disable_grpc_modules -count=1
-p=2 -timeout=5m -v`; the two earlier race runs also used `-race`.

Historical commands and absolute paths are preserved in
[commands.txt](observed/corrected/commands.txt),
[configuration.json](observed/corrected/configuration.json), and the JSON
execution records. [tests/REPRODUCTION.md](tests/REPRODUCTION.md) provides
portable, explicitly unexecuted commands using fresh source archives and Go
overlays. The source clone, toolchain, module cache, and binaries are not bundled.

## Limitations

- These are focused suite tests with mocks, including mocked persistence.
  They do not establish full-cluster, real-database, crash/restart, high-load,
  or arbitrary-interleaving correctness.
- BASE A fails at its first post-merge retention assertion; its log does not
  establish later context-cleanup or owner-release observations on BASE.
- The corrected matrix has exactly four normal executions and no new race
  executions. Earlier HEAD race passes do not prove the absence of all races.
- The compatibility adaptation and observation hooks are explicit. Original
  HEAD test text cannot be transplanted unchanged onto BASE's older API.
- `disable_grpc_modules` was used; these records do not validate the omitted
  gRPC-module paths. The dependency setup required online module access.
- Full OS/container, C compiler, and installed dependency inventories were not
  recorded in the supplied artifacts. No missing environment facts are inferred.
- The retained paths and timestamps identify historical runs and are not
  portable commands. The portable instructions were not executed here.
- Checksums establish package integrity, not an independent rerun, an external
  signature, exhaustive correctness, or a maintainer approval.

## Integrity and provenance

From the evidence repository root, run:

```bash
sha256sum -c 12772/SHA256SUMS
```

The manifest covers the root README and every case file except itself.
[provenance.json](provenance.json) maps byte-identical copies and generated
derivatives to source artifacts, hashes, and exact Git revisions. Raw execution
records are not rewritten. Large source archives, source trees, caches, the
repository-wide ref dump, and unrelated files are excluded.
