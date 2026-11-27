# Temporal SDK Core: conservative eviction vs bounded legacy queries

Measured 2026-11-27 using the final public revisions of [#1835](https://github.com/temporalio/sdk-rust/pull/1835) (V2) and the V4.2 candidate. These are fresh final-version measurements.

V2 evicts a cached workflow after legacy-query preparation runs workflow code outside a recorded workflow-task (WFT) boundary. The next sticky WFT must fetch and replay the full history. V4.2 bounds preparation at the advertised completed-WFT boundary and defers trailing inputs, allowing cache reuse. Its conservative fallback still evicts when interleaved cancellation requires it.

## Largest histories

Combined latency includes the query and the following WFT. Speedup is V2 median / V4.2 median; below 1 means slower. Confidence intervals resample paired rounds. Evictions are per measured sample.

<!-- BEGIN CSV RESULTS -->
| Scenario | Events | V2 median / P95 (ms) | V4.2 median / P95 (ms) | Speedup (95% CI) | Evictions V2 / V4.2 |
|---|---:|---:|---:|---:|---:|
| Normal WFT | 9,998 | 0.144 / 0.207 | 0.143 / 0.194 | 1.00× (0.9809–1.024) | 0 / 0 |
| Non-effectful legacy query | 9,998 | 1.549 / 1.710 | 1.581 / 1.734 | 0.98× (0.9606–1.014) | 0 / 0 |
| Trailing inputs † | 9,998 | 277.632 / 299.874 | 1.579 / 1.792 | 175.86× (171.2–180.8) | 1 / 0 |
| Interleaved cancellation | 10,004 | 276.328 / 284.693 | 275.662 / 283.087 | 1.00× (0.9958–1.006) | 1 / 1 |
| Held LA result † | 10,003 | 282.287 / 302.678 | 810.065 / 813.481 | 0.35× (0.3471–0.3525) | 1 / 0 |

**Trailing inputs:** 277.632 ms → 1.579 ms, **175.86×**. **Held LA:** 282.287 ms → 810.065 ms, **2.87× slower**.

Held-LA query median is **808.739 ms** in V4.2; its next-WFT median is only **0.945 ms**. The combined interval includes the query wait.
<!-- END CSV RESULTS -->

Normal WFTs and non-effectful queries retain the cache in both versions; neither shows a clear combined-latency benefit here. Interleaved cancellation evicts in both versions and likewise shows no clear combined benefit. Small regressions in individual cells and phases remain visible in [RESULTS.md](RESULTS.md), which includes all four sizes, query/next-WFT/combined median and P95, speedup and delta intervals, operation counts, and round medians.

## Query visibility and Held-LA trade-off

**† Query semantics change.** For trailing inputs and held LA, V2 observes the trailing acknowledgement (`acked=true`); V4.2 answers before applying it (`acked=false`) and applies it on the next WFT. These comparisons measure costs under different query visibility, not an identical-semantics optimization or a general 175× improvement.

For held LA, V4.2 avoids eviction and LA re-execution but waits for the outstanding-LA heartbeat before answering. Formal measurements use a **one-second WFT timeout**, with about **0.8 seconds** of waiting, consistent with the [80%-of-timeout heartbeat deadline](https://github.com/hikaru-212/temporalio-sdk-rust/blob/7f53cc61b066b7320cb384246ddfa7561139affb/crates/sdk-core/src/worker/workflow/managed_run.rs#L1559-L1578). Reporting only next-WFT latency would hide this regression.

**Preliminary observation, excluded from the formal CSVs:** a 60-second-timeout fixture observed approximately 48 seconds of waiting. This was not a controlled timeout sweep; the public CSVs do not substantiate that pilot observation. It motivates testing timeout sensitivity, not extrapolating production latency.

V2 discards the held result and executes the LA again; V4.2 retains it. V2 can commit either a memo or timer branch depending on when the replacement result arrives. Both branches are preserved. Correctness means each tested live history matches its own cold replay, not that the two revisions make identical decisions. LA bodies return immediately; real LA service time and side effects are outside this experiment.

## Evidence and scope

- **4,800 measured samples, 1,200 warm-ups, six rounds:** each version/scenario/size has six fresh processes, each with five warm-ups and 20 measurements. Version order is balanced; cell order is deterministically shuffled. [timing.csv](timing.csv) retains every sample, including warm-ups and slow results. No outliers are removed.
- **40/40 independent correctness cases:** [correctness.csv](correctness.csv) covers both versions × five scenarios × four sizes. The harness commits live commands into history, then checks cold replay and final state/decision on a fresh worker. Both V2 held-LA branches appear and match their own replay. Timing samples do not themselves run cold replay.
- Same release build, locked dependencies, Rust 1.100, opt-level 3, no debug symbols/assertions, no LTO or custom RUSTFLAGS; default WFT chunking. Effective Core features: `default, envconfig, ephemeral-server, otel, prometheus, tls-ring`.
- Same shared Linux/aarch64 VM, six Apple vCPUs, 7.7 GiB RAM; affinity CPUs 1–4. Initial cache replay and correctness validation are outside timing. The **mock server and synthetic Rust language driver** exclude real server/network/storage/queue costs and production language-SDK overhead.
- Nearest-rank P95; 10,000 paired whole-round bootstrap resamples, seed 183542. Only six independent rounds; no multiple-comparison adjustment. These are workload-specific Core latencies, not production throughput or universal performance claims.

## Exact sources and reproduction

| Version | Public commit |
|---|---|
| V2 / #1835 | [`06cb4a97863c0aacda79555fbaabab87a4013cbf`](https://github.com/hikaru-212/temporalio-sdk-rust/commit/06cb4a97863c0aacda79555fbaabab87a4013cbf) |
| V4.2 | [`7f53cc61b066b7320cb384246ddfa7561139affb`](https://github.com/hikaru-212/temporalio-sdk-rust/commit/7f53cc61b066b7320cb384246ddfa7561139affb) |

Verify and recompute the saved reports with Python 3.12+; no build or SDK execution occurs:

```bash
sha256sum --check SHA256SUMS
python3 analyze.py --check
```

To run a **new experiment**, use Linux with CPUs 1–4 available, Git, Rust 1.100 via rustup, Python 3.12+, `protoc` and the repository's build prerequisites. Allow at least 10 GiB free and keep unrelated jobs idle. Use a directory that does not yet exist:

```bash
python3 reproduce.py /tmp/v42-reproduction --plan
python3 reproduce.py /tmp/v42-reproduction
python3 analyze.py --data-dir /tmp/v42-reproduction --output /tmp/v42-reproduction/RESULTS.md
```

[reproduce.py](reproduce.py) fetches the two SHAs from the public repository (or accepts `--repo /path/to/local/checkout`), verifies source blobs, overlays the identical [final_benchmark.rs](final_benchmark.rs), and appends one test-module declaration in disposable exports. It populates dependencies with `cargo fetch --locked`, then builds both with `cargo test -p temporalio-sdk-core --lib --release --locked --offline --no-run -j 3`. Both builds use the same source path and target directory; executables are saved separately. It checks matching profiles/features and correctness fixtures, then runs the original schedule after compilation has finished. Fresh outputs stay in the new directory; existing evidence is never overwritten. Disk guards stop builds without cleanup.

[analyze.py](analyze.py) checks completeness, counters, query answers, replay verdicts, relative links, checksums and every reported statistical table. `--rounds` emits per-round median/P95 as CSV. Verification establishes integrity and internal consistency; it does not independently prove execution. Numerical campaign results are reproducible from the two public CSVs; environment/configuration statements and the explicitly excluded pilot observation are not CSV-derived. Absolute timings will vary with the host.
