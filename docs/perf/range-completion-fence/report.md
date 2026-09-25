**A. Harness implementation summary**

Verdict: **material regression detected in Cassandra cleanup/overlap tail latency under the accelerated contention workload**. No material aggregate foreground throughput or p95 regression was detected. PostgreSQL showed the expected incremental cleanup and exclusive-lock cost without crossing the screening thresholds. This distinction is supported by the numbers below; it is not a claim about production impact.

The identical, opt-in `TestRangeCompletePerf` source runs against detached base `d8f9c6d86b2cd0ea5c27d0694a598da84c6d337d` and patch `44ab5d807396872ed1cb440a217cee7b5f431089`. Reflection only sets the RangeID field when present; its presence is asserted against the selected revision. No production source was changed.

The harness reuses `NewPostgreSQLConfig`, `SetupPostgreSQLDatabase/Schema`, `NewCassandraConfig`, `SetUpCassandraDatabase/Schema`, Temporal SQL/Cassandra factories, and real execution/shard managers. It seeds real `UserTimerTask` instances in batches of 100, validates every fresh range before deletion and verifies emptiness afterward. A ten-row `ActivityTask` smoke case also exercises transfer tasks. These operations are outside timing.

Each isolated size uses the requested warmup/sample counts (5+20 at 1,000; 2+10 at 10,000 per trial). Each mixed case runs one serialized writer for 60 active seconds, with cleanup 100 ms into each 1 s or 5 s segment. Workers finish before refill/validation and restart afterward. Workflow maps stay empty, payload shape stays fixed, and DBRecordVersion advances on each successful update and is checked against persistence after each segment.

PostgreSQL contention uses a read-only observer connection: it detects an active production DELETE in `pg_stat_activity`, starts the real UpdateShard call with an unchanged RangeID, and samples `pg_blocking_pids`. Eight confirmed overlaps per revision/trial yield 24 per revision. Missed overlaps are retained separately. It does not insert an artificial lock-hold delay.

Cassandra uses gocql query/batch observers and five-second operation contexts. There is no application retry wrapper. Observers measure client executions/retry callbacks, not server Paxos rounds. Source formatting, compile checks, PostgreSQL/Cassandra smoke tests, and repository `make lint-code-fast` passed (one initial naming lint was fixed in the temporary harness).

Source references: [RangeID adapter](/tmp/temporal-range-pilot-20260925/harness/range_complete_perf_test.go:158), [bounded population](/tmp/temporal-range-pilot-20260925/harness/range_complete_perf_test.go:187), [workflow writes](/tmp/temporal-range-pilot-20260925/harness/range_complete_perf_test.go:217), [mixed control flow](/tmp/temporal-range-pilot-20260925/harness/range_complete_perf_test.go:256), [controlled SQL overlaps](/tmp/temporal-range-pilot-20260925/harness/range_complete_perf_test.go:315), [backend setup](/tmp/temporal-range-pilot-20260925/harness/range_complete_perf_test.go:388), [SQL fenced cleanup](/tmp/temporal-range-pilot-20260925/patch/common/persistence/sql/execution_tasks.go:66), and [Cassandra conditional cleanup](/tmp/temporal-range-pilot-20260925/patch/common/persistence/cassandra/mutable_state_task_store.go:266).

**B. Exact temporary files created**

All artifacts are beneath `/tmp/temporal-range-pilot-20260925/`:

- [harness/range_complete_perf_test.go](/tmp/temporal-range-pilot-20260925/harness/range_complete_perf_test.go)
- [base/common/persistence/tests/range_complete_perf_test.go](/tmp/temporal-range-pilot-20260925/base/common/persistence/tests/range_complete_perf_test.go)
- [patch/common/persistence/tests/range_complete_perf_test.go](/tmp/temporal-range-pilot-20260925/patch/common/persistence/tests/range_complete_perf_test.go)
- [compile.sh](/tmp/temporal-range-pilot-20260925/compile.sh)
- [smoke.sh](/tmp/temporal-range-pilot-20260925/smoke.sh)
- [run_backend.sh](/tmp/temporal-range-pilot-20260925/run_backend.sh)
- [monitor.py](/tmp/temporal-range-pilot-20260925/monitor.py)
- [monitor-docker-original.py](/tmp/temporal-range-pilot-20260925/monitor-docker-original.py)
- [analyze.py](/tmp/temporal-range-pilot-20260925/analyze.py)
- [report.py](/tmp/temporal-range-pilot-20260925/report.py)
- [base.test](/tmp/temporal-range-pilot-20260925/base.test)
- [patch.test](/tmp/temporal-range-pilot-20260925/patch.test)
- [results/](/tmp/temporal-range-pilot-20260925/results)

`base/` and `patch/` contain detached repository checkouts. `results/` contains each `BACKEND-REVISION-trialN.{samples.jsonl,resources.jsonl,log,state-before.txt,state-after.txt}`, smoke logs/samples, build/lint logs, machine snapshots, aggregate/trial summaries, audits and this report. The excluded first PostgreSQL pass is preserved under `results/excluded-initial-postgresql/`. A complete file inventory is in `results/file-inventory.txt`.

**C. Exact commands executed**

The complete build and run commands are in [compile.sh](/tmp/temporal-range-pilot-20260925/compile.sh), [smoke.sh](/tmp/temporal-range-pilot-20260925/smoke.sh) and [run_backend.sh](/tmp/temporal-range-pilot-20260925/run_backend.sh). The execution sequence is saved in [results/commands-executed.md](/tmp/temporal-range-pilot-20260925/results/commands-executed.md). Key commands:

```bash
git worktree add --detach /tmp/temporal-range-pilot-20260925/base d8f9c6d86b2cd0ea5c27d0694a598da84c6d337d
git worktree add --detach /tmp/temporal-range-pilot-20260925/patch 44ab5d807396872ed1cb440a217cee7b5f431089
bash /tmp/temporal-range-pilot-20260925/compile.sh
bash /tmp/temporal-range-pilot-20260925/smoke.sh PostgreSQL
bash /tmp/temporal-range-pilot-20260925/run_backend.sh PostgreSQL
docker stop temporal-dev-postgresql
docker start temporal-dev-cassandra
bash /tmp/temporal-range-pilot-20260925/smoke.sh Cassandra
bash /tmp/temporal-range-pilot-20260925/run_backend.sh Cassandra
docker stop temporal-dev-cassandra
python3 /tmp/temporal-range-pilot-20260925/analyze.py
python3 /tmp/temporal-range-pilot-20260925/report.py
```

The PostgreSQL command above denotes the included replacement pass; an earlier complete pass was excluded, followed by a container restart and another smoke test. Both passes are preserved. Each backend used exactly three included pairs: base→patch, patch→base, base→patch. Compilation finished before included measurements; no compilation or full functional tests ran concurrently. Build flags were `GOMAXPROCS=2 CGO_ENABLED=0 go test -mod=readonly -p=1 -tags=test_dep,integration,disable_grpc_modules -c`.

**D. Machine/backend state before testing**

6-vCPU ARM64 VM, 7.7 GiB RAM, Go 1.27.0 linux/arm64, GOMAXPROCS=2 for both binaries. Initially 17 GiB disk was available; the post-build snapshot had 15 GiB. Unused Cassandra/MySQL/Grafana/Elasticsearch/UI/Prometheus containers were stopped before PostgreSQL; PostgreSQL was stopped before Cassandra. No backend configuration changed between compared revisions.

PostgreSQL 13.5 (`postgres12` plugin): 16 open/idle connections, plus one observer connection; 128 MB shared_buffers, max_connections=100, synchronous_commit=on, fsync=on. Included PostgreSQL runs had 6.40–6.52 GiB minimum available RAM. Cassandra 5.0: single node, temporary keyspaces explicitly RF=1, two client connections, LOCAL_QUORUM/LOCAL_SERIAL, existing 1 GiB heap setting. Its pretest snapshot had 5.2 GiB available RAM; included-run minima were 5.11–5.23 GiB. This is lower than the PostgreSQL headroom, but container swap, swap-out, OOMs and PSI pressure averages remained zero.

Periodic direct cgroup/host counters recorded DB CPU, container memory/swap/events, disk, host swap and (for Cassandra) memory PSI. PostgreSQL has only a late PSI snapshot (avg10/60/300 zero), so continuous PSI coverage must not be claimed for it. The initial Docker-poll monitor was replaced to reduce probe interference. Both database containers are stopped at the end. Detailed snapshots and counters are in `machine-before.txt`, `cassandra-before.txt`, `resource-audit.json` and per-run resource files.

Observed database CPU (100% = one CPU core): PostgreSQL run means 18.0–18.7%, sampled maximum 33.2%; Cassandra run means 45.3–53.2%, maximum 153.4%, on the six-vCPU VM. Each included test process took 132.1–133.9 seconds, including 120 active mixed-workload seconds plus isolated/fixture work. The twelve included runs therefore consumed about 26.5 minutes of test-process runtime, excluding compilation, startup, orchestration gaps and the discarded first PostgreSQL pass.

**E. Raw sample/result file locations**

[results/summary.csv](/tmp/temporal-range-pilot-20260925/results/summary.csv) and [results/summary.json](/tmp/temporal-range-pilot-20260925/results/summary.json) contain per-trial statistics; [results/aggregate.json](/tmp/temporal-range-pilot-20260925/results/aggregate.json) contains pooled statistics; [results/isolated-deltas.csv](/tmp/temporal-range-pilot-20260925/results/isolated-deltas.csv) contains absolute/percentage deltas for every isolated metric; [results/foreground-tail-counts.json](/tmp/temporal-range-pilot-20260925/results/foreground-tail-counts.json) and [results/resource-audit.json](/tmp/temporal-range-pilot-20260925/results/resource-audit.json) preserve tail counts and validity evidence. Raw samples retain revision/backend/trial, timestamps, duration, expected rows, errors, overlap, and observer counters.

All latencies below are milliseconds. p50 is the median and p95 is nearest-rank. Warmups, refill, validation and teardown are excluded. No p99 is reported. Pooled values are descriptive; repeatability is assessed from paired trials and within-revision variation.

**F. PostgreSQL isolated results**

| Rows | Revision/delta | Count | Mean | p50 | p95 | Max |
| --- | --- | --- | --- | --- | --- | --- |
| 1,000 | base | 60 | 1.046 | 1.027 | 1.271 | 1.392 |
| 1,000 | patch | 60 | 2.383 | 2.396 | 2.712 | 2.792 |
| 1,000 | Δ ms | — | +1.337 | +1.369 | +1.441 | +1.400 |
| 1,000 | Δ % | — | +127.9% | +133.3% | +113.3% | +100.6% |
| 10,000 | base | 30 | 3.208 | 3.196 | 3.763 | 3.921 |
| 10,000 | patch | 30 | 4.496 | 4.503 | 4.901 | 5.030 |
| 10,000 | Δ ms | — | +1.288 | +1.307 | +1.139 | +1.109 |
| 10,000 | Δ % | — | +40.2% | +40.9% | +30.3% | +28.3% |

**G. PostgreSQL mixed-workload results**

**Accelerated contention stress: 1,000 rows roughly every 1 s; 10,000 rows every 5 s. This is not normal production checkpoint cadence.**

Foreground writes:

| Rows | Revision | Writes | Writes/s | p50 | p95 | Max |
| --- | --- | --- | --- | --- | --- | --- |
| 1,000 | base | 54220 | 300.69 | 3.262 | 3.905 | 32.124 |
| 1,000 | patch | 54376 | 301.45 | 3.227 | 3.750 | 281.843 |
| 10,000 | base | 53989 | 299.84 | 3.273 | 3.897 | 25.564 |
| 10,000 | patch | 54361 | 301.91 | 3.245 | 3.855 | 264.377 |

Cleanup:

| Rows | Revision | Count | p50 | p95 | Max |
| --- | --- | --- | --- | --- | --- |
| 1,000 | base | 180 | 1.658 | 2.185 | 5.429 |
| 1,000 | patch | 180 | 3.281 | 4.181 | 38.912 |
| 10,000 | base | 36 | 3.973 | 4.375 | 4.772 |
| 10,000 | patch | 36 | 5.761 | 6.636 | 7.003 |

Foreground writes overlapping cleanup:

| Rows | Revision | Count | Mean | p50 | p95 | Max |
| --- | --- | --- | --- | --- | --- | --- |
| 1,000 | base | 230 | 3.784 | 3.755 | 4.565 | 6.540 |
| 1,000 | patch | 298 | 4.476 | 3.915 | 5.280 | 47.336 |
| 10,000 | base | 71 | 3.657 | 3.564 | 4.469 | 5.622 |
| 10,000 | patch | 79 | 3.863 | 3.770 | 4.900 | 5.007 |

Overlap sample counts differ because longer cleanups include more writes. This conditional subset is not equivalent to overall foreground p95.

**H. PostgreSQL UpdateShard contention results**

| Revision | Operation | Count | Mean | p50 | p95 | Max | Blocking observed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| base | cleanup | 24 | 4.346 | 3.872 | 4.470 | 15.451 | — |
| base | shard_update | 24 | 3.917 | 3.392 | 4.724 | 14.945 | 0 |
| patch | cleanup | 24 | 5.495 | 5.425 | 6.022 | 6.508 | — |
| patch | shard_update | 24 | 4.988 | 5.043 | 5.661 | 5.776 | 24 |

All 24 patched overlaps showed a blocking relationship; baseline showed 0/24. The maximum patched UpdateShard latency was 5.776 ms, below the 100 ms investigation threshold. These are API latencies, not a decomposition of exact time spent waiting. Sampling may miss extremely short baseline waits. Ordinary workflow writes also use shared ownership locks: these results do not show them directly serialized by the shared cleanup lock. Only 10,000 rows were tested here, so size-scaling of exclusive-row waits is unproven.

**I. Cassandra isolated results**

| Rows | Revision/delta | Count | Mean | p50 | p95 | Max |
| --- | --- | --- | --- | --- | --- | --- |
| 1,000 | base | 60 | 2.033 | 2.032 | 2.503 | 3.495 |
| 1,000 | patch | 60 | 2.300 | 2.323 | 2.685 | 2.776 |
| 1,000 | Δ ms | — | +0.268 | +0.291 | +0.182 | -0.719 |
| 1,000 | Δ % | — | +13.2% | +14.3% | +7.3% | -20.6% |
| 10,000 | base | 30 | 1.903 | 1.985 | 2.303 | 2.442 |
| 10,000 | patch | 30 | 2.357 | 2.400 | 2.712 | 2.731 |
| 10,000 | Δ ms | — | +0.455 | +0.415 | +0.409 | +0.289 |
| 10,000 | Δ % | — | +23.9% | +20.9% | +17.8% | +11.8% |

**J. Cassandra mixed-workload results**

**Accelerated contention stress: 1,000 rows roughly every 1 s; 10,000 rows every 5 s. This is not normal production checkpoint cadence.**

Foreground writes:

| Rows | Revision | Writes | Writes/s | p50 | p95 | Max |
| --- | --- | --- | --- | --- | --- | --- |
| 1,000 | base | 80488 | 446.67 | 2.207 | 2.604 | 23.053 |
| 1,000 | patch | 80510 | 446.77 | 2.174 | 2.499 | 101.030 |
| 10,000 | base | 81037 | 450.11 | 2.197 | 2.560 | 26.861 |
| 10,000 | patch | 81347 | 451.83 | 2.183 | 2.522 | 92.301 |

Cleanup:

| Rows | Revision | Count | p50 | p95 | Max |
| --- | --- | --- | --- | --- | --- |
| 1,000 | base | 180 | 2.023 | 2.477 | 2.740 |
| 1,000 | patch | 180 | 2.509 | 82.139 | 113.023 |
| 10,000 | base | 36 | 2.064 | 2.589 | 2.815 |
| 10,000 | patch | 36 | 2.593 | 125.271 | 174.445 |

Foreground writes overlapping cleanup:

| Rows | Revision | Count | Mean | p50 | p95 | Max |
| --- | --- | --- | --- | --- | --- | --- |
| 1,000 | base | 309 | 2.334 | 2.373 | 2.992 | 3.925 |
| 1,000 | patch | 1266 | 5.055 | 2.246 | 14.642 | 101.030 |
| 10,000 | base | 62 | 2.281 | 2.322 | 2.951 | 3.241 |
| 10,000 | patch | 495 | 3.397 | 2.206 | 2.638 | 92.301 |

Overlap sample counts differ because longer cleanups include more writes. This conditional subset is not equivalent to overall foreground p95.

For 1,000-row cleanup, paired cleanup p95 was **2.587→71.217**, **2.431→95.990**, **2.420→66.459 ms**. The increases (68.63, 93.56, 64.04 ms) all exceed the patch/patch p95 span of 29.53 ms and base/base span of 0.17 ms. This tail change is repeatable.

The 1,000-row overlap foreground p95 was **3.170→24.765**, **2.971→3.266**, **2.902→31.705 ms**. Its magnitude is variable. More consistently, overlapping foreground writes above 50 ms were **0→8**, **0→12**, **0→13**; at 10,000 rows they were **0→3**, **0→2**, **0→2**. Every Cassandra foreground write above 50 ms overlapped cleanup. These are descriptive tail counts, not a newly invented SLO.

All observed Cassandra workflow calls used one driver batch; baseline cleanups used one query, patched cleanups one batch. Measured API errors, ownership-loss/condition-failure errors, timeout/unavailable errors, driver-error callbacks and client retry callbacks were all **zero**. Server-internal Paxos rounds/retries were not measured.

**K. Trial-to-trial variability**

| Backend | Rows | Trial | Writes/s base→patch | Δ throughput | p95 base→patch |
| --- | --- | --- | --- | --- | --- |
| PostgreSQL | 1,000 | 1 | 307.40→307.73 | +0.11% | 3.661→3.686 |
| PostgreSQL | 1,000 | 2 | 290.03→292.81 | +0.96% | 4.164→3.781 |
| PostgreSQL | 1,000 | 3 | 304.65→303.83 | -0.27% | 3.763→3.798 |
| PostgreSQL | 10,000 | 1 | 309.63→309.46 | -0.05% | 3.626→3.646 |
| PostgreSQL | 10,000 | 2 | 295.25→294.06 | -0.40% | 4.031→4.062 |
| PostgreSQL | 10,000 | 3 | 294.65→302.21 | +2.57% | 3.940→3.843 |
| Cassandra | 1,000 | 1 | 431.53→445.26 | +3.18% | 2.747→2.526 |
| Cassandra | 1,000 | 2 | 449.05→444.41 | -1.03% | 2.550→2.510 |
| Cassandra | 1,000 | 3 | 459.42→450.63 | -1.91% | 2.470→2.461 |
| Cassandra | 10,000 | 1 | 436.86→450.64 | +3.15% | 2.662→2.513 |
| Cassandra | 10,000 | 2 | 453.93→446.07 | -1.73% | 2.522→2.588 |
| Cassandra | 10,000 | 3 | 459.54→458.79 | -0.16% | 2.461→2.460 |

Within-revision spans (range/mean, not confidence intervals):

| Backend | Rows | Revision | Writes/s range | Span | p95 range ms |
| --- | --- | --- | --- | --- | --- |
| PostgreSQL | 1,000 | base | 290.03–307.40 | 5.77% | 3.661–4.164 |
| PostgreSQL | 1,000 | patch | 292.81–307.73 | 4.95% | 3.686–3.798 |
| PostgreSQL | 10,000 | base | 294.65–309.63 | 4.99% | 3.626–4.031 |
| PostgreSQL | 10,000 | patch | 294.06–309.46 | 5.10% | 3.646–4.062 |
| Cassandra | 1,000 | base | 431.53–459.42 | 6.24% | 2.470–2.747 |
| Cassandra | 1,000 | patch | 444.41–450.63 | 1.39% | 2.461–2.526 |
| Cassandra | 10,000 | base | 436.86–459.54 | 5.04% | 2.461–2.662 |
| Cassandra | 10,000 | patch | 446.07–458.79 | 2.81% | 2.460–2.588 |

No paired aggregate throughput reduction exceeded 1.92%, and no aggregate foreground p95 pair met both >20% and ≥1 ms. Overall throughput differences are not separable from ordinary run variation. PostgreSQL overlap tails are variable (1,000-row patched p95 4.472/8.148/6.029 ms), so a repeatable material PostgreSQL foreground-tail regression is not established. Cassandra cleanup-tail separation is much larger than its within-revision variation. No extra base/base or patch/patch campaign was run; variability comes from these three repeated measurements per revision.

Isolated cleanup mean by trial (ms):

| Backend | Rows | Revision | Trial 1 | Trial 2 | Trial 3 |
| --- | --- | --- | --- | --- | --- |
| PostgreSQL | 1,000 | base | 1.055 | 1.033 | 1.050 |
| PostgreSQL | 1,000 | patch | 2.537 | 2.266 | 2.346 |
| PostgreSQL | 10,000 | base | 3.011 | 3.422 | 3.191 |
| PostgreSQL | 10,000 | patch | 4.502 | 4.561 | 4.425 |
| Cassandra | 1,000 | base | 2.173 | 1.987 | 1.938 |
| Cassandra | 1,000 | patch | 2.333 | 2.292 | 2.276 |
| Cassandra | 10,000 | base | 2.046 | 1.832 | 1.831 |
| Cassandra | 10,000 | patch | 2.448 | 2.417 | 2.207 |

There was an approximately 13-minute orchestration gap between the first Cassandra base process finishing and its patch counterpart starting; it was outside measured intervals. Subsequent pairs remained in the prescribed order. This reinforces treating small aggregate deltas as noise; the cleanup-tail separation occurred in all three pairs.

**L. Errors / invalid trials**

The first PostgreSQL pass (six runs) remains excluded: old container swap and Docker-poll-associated host page-ins made it unsuitable for the conservative initial rule. No excluded result contributes to these tables. The six replacement PostgreSQL and six Cassandra runs are included under the clarified allowance for small background page-ins. Cassandra had lower headroom than PostgreSQL (about 5.1 GiB), but no sustained pressure signal.

| Backend | Revision | Trial | Host page-ins | In active intervals | Min RAM GiB | Swap-out pages | DB swap bytes | Max PSI avg10 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Cassandra | base | 1 | 10 | 10 | 5.109 | 0 | 0 | 0.0 |
| Cassandra | base | 2 | 22 | 22 | 5.187 | 0 | 0 | 0.0 |
| Cassandra | base | 3 | 24 | 16 | 5.188 | 0 | 0 | 0.0 |
| Cassandra | patch | 1 | 23 | 23 | 5.151 | 0 | 0 | 0.0 |
| Cassandra | patch | 2 | 17 | 16 | 5.145 | 0 | 0 | 0.0 |
| Cassandra | patch | 3 | 186 | 186 | 5.183 | 0 | 0 | 0.0 |
| PostgreSQL | base | 1 | 22 | 22 | 6.515 | 0 | 0 | late snapshot only |
| PostgreSQL | base | 2 | 20 | 7 | 6.498 | 0 | 0 | late snapshot only |
| PostgreSQL | base | 3 | 65 | 50 | 6.503 | 0 | 0 | late snapshot only |
| PostgreSQL | patch | 1 | 37 | 30 | 6.515 | 0 | 0 | late snapshot only |
| PostgreSQL | patch | 2 | 246 | 245 | 6.400 | 0 | 0 | late snapshot only |
| PostgreSQL | patch | 3 | 63 | 61 | 6.494 | 0 | 0 | late snapshot only |

Pages are 4 KiB. Host OOM deltas and container OOM events were zero throughout included trials; memory PSI averages were zero in every Cassandra sample. Page-ins were sparse, not sustained pressure. Resource-audit.json contains per-sample maxima and counts. The disk minimum across included runs was 13.50 GiB.

PostgreSQL patch trial 2 contained four foreground outliers above 50 ms, including **281.843 ms** (1,000-row case) and **264.377 ms** (10,000-row case). None overlapped cleanup. There was no observed container swap, swap-out or low-memory event; their cause remains unexplained. They were retained rather than silently trimmed. Continuous PostgreSQL PSI was unavailable, which limits attribution.

Cassandra startup connection refusals occurred only while waiting for node readiness, before smoke tests/measurements. One initial temporary-harness lint naming issue was fixed; final lint passed. There were no measured persistence failures or client retry callbacks.

Cassandra PSI cumulative stalls increased by at most 1.102 ms per approximately 133-second run, with all sampled pressure averages at 0.00. The largest host page-in increments in a one-second sample were 113 pages for PostgreSQL and 164 pages for Cassandra. These sparse background events do not establish sustained memory pressure; raw timing and counters are retained.

**M. Interpretation**

**Material regression detected — localized Cassandra contention-tail latency.** This is not a detected material aggregate throughput regression. With 1,000-row cleanup at 1 Hz, cleanup p95 increased by 64–94 ms in all three pairs, and 8/12/13 foreground writes per patched trial exceeded 50 ms during cleanup versus zero in every baseline trial. Foreground overlap mean increased from 2.437/2.257/2.307 ms to 5.387/4.518/5.572 ms; those paired increases exceed the within-patch mean span. This is a repeatable foreground effect accompanying the cleanup tail increase, not merely a relative increase in isolated delete time.

The likely mechanism is contention introduced by the real conditional cleanup batch sharing the shard partition with workflow LWTs. This is an interpretation of the A/B result and production call paths, not a measurement of Paxos internals. The signal deserves explicit maintainer review; the pilot does not establish a production-wide slowdown or a PR-blocking SLO violation.

PostgreSQL: no material aggregate throughput/p95 or >100 ms ownership-row delay detected at the tested range sizes. Cassandra: no material aggregate throughput/p95 regression and no new client failures/retries detected, alongside the localized tail regression above.

**N. What this pilot does NOT prove**

- RF=1 on one Cassandra node does not represent production quorum/network/Paxos latency, multi-coordinator contention or failover.
- One shard, one serialized workflow writer and one cleanup stream do not cover many shards, overlapping queue categories, production shard I/O admission, checkpoint scheduling or ownership churn. UpdateShard kept RangeID stable.
- Fresh, read-validated 1,000/10,000-row ranges favor warm data. Cold/huge deletes, long-lived tombstone/compaction or vacuum behavior, sustained cleanup lag and disk growth are unproven.
- Minimal fixed workflow metadata excludes history appends, realistic mutable-state maps/payloads and task generation in workflow transactions.
- The writer is closed-loop and refill pauses are excluded. The pilot does not measure externally queued request latency or production cleanup capacity including refill.
- Overlap subsets are length-biased toward longer operations and change size with cleanup duration; their p95 values should not be substituted for global p95.
- The small isolated sample counts and 12 large cleanups per trial do not support meaningful p99. A large-cleanup per-trial p95 with n=12 equals the sample maximum. No statistical significance is inferred from a single run.
- Three trials screen for material differences; they do not prove absence of rare tails, establish production SLOs, or fully explain PostgreSQL’s one-run outliers.

No production source, commit, branch tip, or real worktree file was changed. The final real-worktree status is saved in `results/final-worktree-status.txt`.
