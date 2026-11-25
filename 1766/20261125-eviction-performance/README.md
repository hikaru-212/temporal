# Temporal SDK Core #1835 — Cost of Conservative Legacy-Query Eviction

## Summary

[sdk-rust #1835](https://github.com/temporalio/sdk-rust/pull/1835) fixes
[#1766](https://github.com/temporalio/sdk-rust/issues/1766) by evicting a cached run once a legacy
query has run workflow code. That code ran at a point no workflow task records. An earlier
revision of the PR reused the run when a predicate judged reuse safe (conditional eviction). A
counterexample showed that predicate to be unsound, and the PR now always evicts
(conservative eviction).

This package measures what that costs. It compares the two policies, plus upstream, on identical
workloads.

Main results, measured in Core with a mocked server:

- **Normal workflow tasks and legacy queries that run no workflow code:** no measurable
  difference. Every 95% CI includes 0.
- **The one path where conservative eviction adds work:** an effectful legacy query followed by a
  task with no new work. Here B rebuilds the run, with one full-history fetch and a replay of every
  workflow task. This adds 3.1 ms at 98 events and 269 ms at 9,998 events, growing linearly with the
  number of workflow tasks in the tested range.
- **Paths that already evicted** (a new signal, update or held local-activity result): the
  evictions, activations, fetches and local-activity executions are identical.

**This is a Core-level benchmark.** It runs a sdk-core lib test with a mocked server and a minimal
synthetic language implementation. It is not a real-server or language-SDK measurement, and the
numbers are not production latency estimates.

## Tested versions and environment

| Label | Commit | What it is |
|---|---|---|
| U | `6eaaeebaa5d90e573e7bed5152c9b5dfca75dc08` | upstream `main` (ancestor of current `main`) |
| A | `31a1a905c26034957f3968015de45177849a2b3f` | conditional eviction (first PR commit) |
| B | `9a59ba3c33ac0d64035a197dc1ae103aa670a8a5` | conservative eviction, PR #1835 head |

- **Source identity.** B's production diff against U has sha256 `35f5f0c7ef1d86e1…`. The full
  diff has sha256 `8cc838816b4cbf6c…`.
- **Build trees.** Each tree is a `git archive` of its commit plus one test-only module,
  `perf_bench_1766.rs` (sha256 `b610cefa509996aa7752ee2059d26b17d06c74fd3c6f4ded027e5fb6c60d8055`,
  identical for all three), declared by one appended line in `core_tests/mod.rs`. The identity
  checks are in `evidence/identity/`.
- **Build.**
  - Command: `cargo test -p temporalio-sdk-core --lib --release --locked --no-run`.
  - Profile: release, `opt_level=3`, `debuginfo=0`, no debug assertions, no overflow checks, no
    LTO, no `RUSTFLAGS`.
  - Features: `default, envconfig, ephemeral-server, prometheus, tls-ring`.
    `disable_workflow_task_chunking` is off, so workflow-task chunking is in its default mode.
  - Same tree path and target directory for every version, built A → B → U.
  - Toolchain: rustc 1.100.0 (LLVM 23.1.5).
- **Machine.** QEMU VM with 6 vCPU (Apple, aarch64), 7.7 GiB RAM, Ubuntu 24.04.4, Linux
  6.8.0-149. The benchmark processes were pinned with `taskset -c 1-4`.

## Methodology

**Workloads.** The history is *n* timer cycles followed by a fixed tail: signal another workflow,
receive its acknowledgement, then the next workflow task (WFT). Each cycle is five events and one
WFT. The synthetic workflow upserts a memo when the acknowledgement arrives and never reads history
length or time, so both reuse and rebuild commit replayable histories where the comparison is
valid.

| Scenario | What happens | A | B |
|---|---|---|---|
| S0 | normal WFT, no legacy query | reuse | reuse |
| S1 | legacy query whose preparation runs no workflow code | reuse | reuse |
| S2 | legacy query runs workflow code; next WFT adds only its own `WorkflowTaskStarted` | reuse | **evict + rebuild** |
| S3 | as S2, but the next WFT carries a new signal or an update | evict | evict |
| S4 | the W4 workload from the PR tests: local-activity result held across an effectful legacy query | evict | evict |

**Timed window.** The window starts when the mocked poll stream hands the next real WFT to Core and
ends when the mocked server receives that WFT's completion. In S4 it ends at the completion carrying
`RecordMarker`. The window covers eviction, history fetch, replay, the synthetic language's work,
and completion.

**Delivery.** The next WFT arrives as a sticky incremental history, so a rebuild must fetch the
full history from the mock. S4 keeps W4's full-history delivery.

**Execution.**
- Every process runs one version.
- Each round runs U, A and B in fresh processes, and the order reverses on odd rounds.
- 3 warm-up samples (2 for S4 with a 20 ms local activity) precede the measured ones in each
  process.
- Every sample uses a new worker (`max_cached_workflows=1`) and the same history.

**Sample counts per version and cell.** S2: 100. S0, S1 and S3: 60. S4: 100. S4 with a 20 ms local
activity: 30. In total, 5,742 timed rows (4,410 measurements, 1,332 warm-up) and 189 correctness rows.

**Statistics.** Medians with linear-interpolated percentiles. The 95% CI of the B − A median
difference is a bootstrap (2,000 resamples, `random.Random(1766)`). The p-value is a two-sided
Mann–Whitney U test. With 60–100 samples, p99 rests on one or two values.

**Correctness, run separately.** A separate pass ran first, in separate processes: 3 samples for
every cell. Each committed history was cold-replayed on a fresh replay worker. Timed rows contain no
replay work.

## Results

### S2 — the extra eviction

| History events | WFTs | A median | B median | Additional latency (B − A) | 95% CI | Additional CPU | B replay activations | Full-history fetches | Fetched bytes (median) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 2 | 0.14 ms | 0.48 ms | **0.34 ms** | 0.30 – 0.37 ms | 0.23 ms | 1 | 1 | 286 |
| 98 | 20 | 0.11 ms | 3.2 ms | **3.1 ms** | 3.0 – 3.2 ms | 2.3 ms | 19 | 1 | 2,626 |
| 998 | 200 | 0.11 ms | 26.9 ms | **26.8 ms** | 26.6 – 26.9 ms | 20.9 ms | 199 | 1 | 27,971 |
| 4,998 | 1,000 | 0.12 ms | 134 ms | **134 ms** | 134 – 135 ms | 105 ms | 999 | 1 | 142,243 |
| 9,998 | 2,000 | 0.15 ms | 269 ms | **269 ms** | 266 – 270 ms | 210 ms | 1999 | 1 | 287,241 |

- **A does no work for lang.** The work was already done while preparing the query, so its
  completion needs no lang activation.
- **B's added work.** B adds one eviction, one full-history fetch (the mock's encoded response
  size), WFTs − 1 replay activations, and one live activation.
- **History size.** History sizes vary by up to 7.5% between samples, because event timestamps are
  wall-clock values with variable-length protobuf encoding. `results.json` gives min/median/max.

Fit over the five points (ordinary least squares on B − A medians against WFT count). It applies
**to this workload (5 events per WFT) and to 2–2,000 WFTs only**:

- Δlatency ≈ 134.4 µs × WFTs + 96 µs
- ΔCPU ≈ 104.8 µs × WFTs + 47 µs
- log-log slope 0.97 (≥ 20 WFTs)
- marginal cost 2→20: 153, 20→200: 132, 200→1,000: 134, 1,000→2,000: 135 µs/WFT

The extra cost therefore matches one full rebuild of the run, the same work as a sticky-cache miss.
A marked run is rebuilt once. The rebuilt run starts unmarked and is marked again only by another effectful legacy query.

### S0 / S1 — normal paths

| Scenario | History events | A median µs | B median µs | B − A µs | 95% CI µs | MWU p | n A/B |
|---|---:|---:|---:|---:|---:|---:|---:|
| S0 | 98 | 120.8 | 121.0 | +0.2 | -7.8 … +21.5 | 0.22 | 60/60 |
| S0 | 998 | 110.0 | 114.4 | +4.4 | -2.7 … +9.6 | 0.49 | 60/60 |
| S0 | 4,998 | 118.2 | 123.9 | +5.7 | -1.8 … +12.6 | 0.12 | 60/60 |
| S0 | 9,998 | 138.2 | 139.9 | +1.6 | -5.1 … +11.2 | 0.47 | 60/60 |
| S1 | 98 | 130.3 | 128.4 | -2.0 | -9.7 … +10.4 | 0.61 | 60/60 |
| S1 | 998 | 118.8 | 118.2 | -0.6 | -6.4 … +4.2 | 0.57 | 60/60 |
| S1 | 4,998 | 132.8 | 131.4 | -1.4 | -9.5 … +5.9 | 0.97 | 60/60 |
| S1 | 9,998 | 159.6 | 161.7 | +2.1 | -3.0 … +9.4 | 0.28 | 60/60 |

- No eviction, fetch or replay in either version.
- Query response latency is also unchanged (`results.json`, `query_latency_ns_median`).
- The CIs bound any difference to roughly ±10–20 µs per WFT in this harness.

### S3 / S4 — paths that already evicted

| Scenario | History events | A median (ms) | B median (ms) | B − A (ms) | 95% CI (ms) | MWU p | Evictions A/B | LA executions A/B | n A/B | U median, classification |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| S3-signal | 99 | 3.3 | 3.2 | -0.05 | -0.25 – 0.15 | 0.33 | 1/1 | 0/0 | 60/60 | 0.13 ms, correctness_invalid |
| S3-signal | 999 | 26.7 | 26.9 | 0.22 | -0.03 – 0.48 | 0.024 | 1/1 | 0/0 | 60/60 | 0.12 ms, correctness_invalid |
| S3-signal | 4,999 | 133 | 134 | 1.1 | 0.14 – 1.8 | 0.043 | 1/1 | 0/0 | 60/60 | 0.13 ms, correctness_invalid |
| S3-update | 98 | 3.1 | 3.3 | 0.17 | -0.09 – 0.45 | 0.062 | 1/1 | 0/0 | 60/60 | 0.14 ms, correctness_invalid |
| S3-update | 998 | 26.8 | 26.7 | -0.10 | -0.33 – 0.56 | 0.25 | 1/1 | 0/0 | 60/60 | 0.13 ms, correctness_invalid |
| S3-update | 4,998 | 133 | 135 | 2.0 | 0.82 – 2.8 | 0.0015 | 1/1 | 0/0 | 60/60 | 0.14 ms, correctness_invalid |
| S4 (LA 0 ms) | 13 | 0.69 | 0.70 | 0.01 | -0.04 – 0.06 | 0.58 | 1/1 | 2/2 | 100/100 | 0.18 ms, correctness_invalid |
| S4 (LA 20 ms) | 13 | 27.4 | 30.9 | 3.5 | 0.35 – 6.3 | 0.067 | 1/1 | 2/2 | 30/30 | 0.22 ms, correctness_invalid |

**S3.** A and B take the same eviction path. A evaluates its predicate first, so A does strictly
more work. At about 5k events B was 0.9–1.5% slower (MWU p 0.043 and 0.0015). Nothing in the source
diff explains this, and it is not established as a regression.

**S4 (local activity).** A and B behave the same:
- Both execute the local activity twice: the held result is dropped and the activity re-executes
  once inside the window.
- U executes it once, but its history does not replay.
- With a 20 ms local activity, both distributions span about 21–34 ms and overlap. This
  comparison is inconclusive.

**Local activities on B-only evictions.** A refused reuse whenever a local activity was executing,
had a held result, or had a deferred resolution. So a B-only eviction never re-executes a local
activity. This follows from the source; no B-only workload with a held local activity can exist to
measure it.

## Correctness controls

- **A and B.** Every cell of the 189-row correctness pass shows: the expected number of WFT
  completions, 0 WFT failures, every legacy query answered, progress committed exactly once, and
  a successful cold replay.
- **U.** Its histories replay in S0–S2 but fail with a nondeterminism eviction in S3-signal,
  S3-update and S4.
- **Classification.** U is marked `correctness_invalid` in those scenarios. Its lower latency
  there is not equivalent successful work and is never compared as such.
- **Live behaviour in every timed row.** The eviction count matches the table in
  `results.json` (`expected_window_evictions`), and the activation, replay-activation, fetch and
  local-activity counts match the mechanism described above.

## Verification

From this directory, with Python 3.9+ and no other dependencies:

```bash
python3 verify.py
```

It checks:
- `SHA256SUMS`, archive member checksums and safe paths;
- the original investigation manifest, which covers the raw data, logs and binary hashes;
- the commit and production-diff identities and the identical harness;
- the build profile and features read from Cargo's own records;
- sample counts against the design, and the separation of timing from replay verification;
- per-row eviction, replay, fetch and local-activity counts, and the correctness classification.

It then recomputes every median, percentile, bootstrap CI, p-value, the S2 fit and each README table
row from `evidence/raw/*.jsonl`, and compares them with `results.json`. It exits non-zero on any
mismatch.

This checks the integrity and internal consistency of the saved evidence. It does not build the
SDK, rerun the benchmark, or prove the runs happened.

## Reproduction

```bash
mkdir -p unpacked
tar -xzf reproducer.tar.gz -C unpacked
tar -xzf evidence.tar.gz -C unpacked
cd unpacked/reproducer
git clone https://github.com/temporalio/sdk-rust.git sdk-rust
git -C sdk-rust fetch origin pull/1835/head
SDK_RUST_REPO=$PWD/sdk-rust scripts/build_all.sh
SDK_RUST_REPO=$PWD/sdk-rust scripts/run_all.sh
python3 scripts/analyze.py work/raw/timed.jsonl
```

`reproducer/SETUP.md` covers requirements, settings and expected identity output. The repository
clone is only read. Everything is written under `reproducer/work/`.

Original resources on the 6-vCPU VM:
- Disk: a 1.9 GiB release target and three ~37 MiB binaries.
- Build: 2 m 53 s for the first build with a warm crate cache, then about 1 m 17 s per version.
- Measurement: 36 s for the correctness pass and 10 m 18 s for the timed matrix.

Absolute timings depend on hardware.

## Limitations

- Real-server RPC, persistence, history pagination and protobuf decoding were not measured. The
  mock returns the whole history in one in-memory response.
- Real language-SDK replay cost was not measured. The synthetic language does a few lines of Rust
  per activation. Rebuild cost grows with activation count, so a real SDK adds per-activation cost
  on top.
- Unchunked mode (`disable_workflow_task_chunking`) was not benchmarked.
- Concurrent throughput and cache pressure across many workflows were not measured.
- How often effectful legacy queries occur in production is unknown. The size of the impact
  depends on it, so this package makes **no claim that the production impact is negligible**.
- S4 with a 20 ms local activity is inconclusive.
- The small S3 differences are not established regressions.
- Only one workload shape was measured: timer cycles, 5 events per WFT. The fit is not a general
  replay complexity law.
- These are not production latency estimates.
- Conditional eviction (A) is faster in S2, but it has a known correctness defect outside the
  workloads compared here. A history-length-dependent workflow produced an unreplayable history
  under A.

## Contents

- `results.json`: derived summary with every statistic, identities, the design, classifications
  and archive checksums.
- `evidence.tar.gz`: raw JSONL, logs, build records, identity checks, the original manifest,
  report and scripts.
- `reproducer.tar.gz`: benchmark module, portable scripts, `SETUP.md`.
- `verify.py`: the offline verifier.
- `SHA256SUMS`: checksums of the files above.

Corrections made while packaging are listed in `results.json` under `discrepancies`. They are
`ephemeral-server` missing from the original feature list, and history sizes now reported as
ranges. All 672 numbers in the original analysis output were recomputed and agree.
