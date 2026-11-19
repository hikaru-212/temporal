# sdk-rust PR #1819: cold replay merges two recorded Stream pages into one activation

Evidence for [temporalio/sdk-rust#1819](https://github.com/temporalio/sdk-rust/pull/1819) at
`f858a1b0901f41255e3d00ca4ce81b86d05044d9` (`refs/pull/1819/head` on 2026-11-19).

```bash
python3 verify.py     # checks the saved evidence; no build, no Server, standard library only
bash reproduce.sh     # rebuilds the pinned Core with a test-only patch and reproduces on a real dev Server
```

`reproduce.sh` exits 0 when the **bug is observed**. Its test asserts the buggy behavior of the
pinned revision, so a fix makes it fail by design.

## The contract that breaks

The PR's Core changelog says Core records one `core_stream` marker per Workflow Task and that
"a replay gives those results back in the same activations". The PR's own test comments say the
same: "The marker has each page in its own activation, which a replay gives as it is."

## Trigger

All of these, in one Workflow Task:

1. A Local Activity is still running, so the task is kept open by heartbeats.
2. A Workflow Update arrives on the heartbeat-forced task and is **rejected**. Its activation
   counts as activation 0, but a rejected Update writes no event to History.
3. An open Stream read then delivers two pages, P1 and P2, in two later activations.

The marker records the pages at activations 1 and 2. On replay there is no activation 0, so Core's
idle fallback releases both held pages at once and merges them into one `ResolveStreamRead`.

## What a real Server recorded (Temporal CLI 1.8.3-server-1.32.0-162.0)

`timer_per_page` case. Workflow code starts one timer for each page it receives.

| | Live (first run) | Cold replay of the Server's History |
|---|---|---|
| W2 act 0 | `DoUpdate` (rejected) | absent: not in History |
| W2 pages | act 1: `["p1"]` then act 2: `["p2"]` | **one** activation, **one** read job `["p1","p2"]` |
| Timers sent in W2 | seq 2 (act 1), seq 3 (act 2) | seq 2 only |
| Result | Server accepts; History has TimerStarted 10 (timer 2) and 11 (timer 3) | `Nondeterminism`: `[TMPRL1100] No command scheduled for event HistoryEvent(id: 11, TimerStarted)` |

W2's `core_stream` marker in the Server History: `page_counts=[1,1]`, `page_activations=[1,1]`
(delta-encoded, so activations 1 and 2), records `["p1","p2"]`.

The real Update request was sequenced at `EventId(7)`, W2's `WorkflowTaskScheduled`. History
holds no Update event.

The `plain` control sends no timers. Its pages merge the same way, the replay ends with the
normal `LangRequested` eviction, and Workflow code simply receives one fewer page callback.

## What each command does

`python3 verify.py` reads the two archives in memory, without extracting anything to disk. It
fails with a nonzero exit if any check fails. It:

- checks file hashes, archive member safety, and the agreement of the pins across files;
- decodes the raw protobuf History itself;
- checks WFT ordering and ownership, the W2 marker, and TimerStarted events 10 and 11;
- checks that the live and replay traces agree with the History, and that the TMPRL1100 event id
  is the TimerStarted the live P2 activation sent and the replay did not;
- checks the plain control.

`bash reproduce.sh` (bash, no root) does the following:

1. Fetches the pinned SHA from GitHub and verifies it.
2. Applies the test-only patch after a successful `git apply --check`, then checks that only the
   two test paths changed.
3. Downloads the official CLI release and checks its SHA-256 before running it.
4. Builds with one Cargo job.
5. Starts a dev Server on free localhost ports with in-memory persistence, runs the one ignored
   test, and stops only that Server.

Everything happens in a new temporary directory, and the logs are kept there.

It needs `git`, `cargo` (the checkout pins Rust 1.100 via `rust-toolchain.toml`), `protoc`,
`python3`, `curl` and GNU `timeout`, plus about 10 GiB of free disk. The first build takes a while.

Optional variables:

| Variable | Purpose |
|---|---|
| `REPRO_SDK_DIR` | A local sdk-rust checkout to clone from instead of GitHub |
| `REPRO_TEMPORAL_CLI` | A CLI binary you already have; its version is still checked |
| `REPRO_WORK_DIR` | A work directory to create; it must not already exist |
| `REPRO_CARGO_TARGET_DIR` | The Cargo target directory |
| `CARGO_NET_OFFLINE=true` | Build without network access (honored by Cargo) |

## Contents

| File | What it is |
|---|---|
| `reproducer.tar.gz` | The test-only patch (one new test file and one `mod` line), `pinned.json` and `SHA256SUMS` |
| `evidence.tar.gz` | Data from one clean-checkout run: both Server Histories (raw `.bin` and readable `.txt`), the structured test trace, WFT exchange dumps with task tokens redacted, `provenance.json` and `SHA256SUMS` |
| `results.json` | Machine-readable pins, observations and run results |

## Limitations

- Workflow code is a small test-side simulation that drives Core activations directly. The
  Rust SDK in this revision does not handle `ResolveStreamRead`, and no other language SDK was
  tried.
- Only `records_in_history=true` with the in-memory test Stream Store was exercised.
- Only dev Server 1.32.0-162.0 (in-memory) on linux/arm64 was run. The other platforms in
  `reproduce.sh` are untested and need `REPRO_ALLOW_UNTESTED_PLATFORM=1`.
- The trigger needs a pending Local Activity. With only a read open, a rejection completes the
  task immediately and the pages do not share it.
