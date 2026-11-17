# Temporal SDK Core #1766 — Real-Server Memo/Update Replay Failure

## Summary

A real Temporal Server committed a memo upsert before Update acceptance/completion in one Workflow Task (WFT), after which the unchanged workflow failed cold replay:

```text
TMPRL1100 Update machine does not handle this event: HistoryEvent(id: 10, WorkflowPropertiesModified)
```

This reproduces [issue #1766](https://github.com/temporalio/sdk-rust/issues/1766)'s failure signature under **controlled scheduling**, using **Temporal Server 1.29.1**, CLI **1.5.1**, Python **3.12.3**, SDK **1.34.0 and 1.36.1**, on **Linux aarch64**.

**Invariant:** a Server-committed history should cold-replay with the unchanged workflow on the same SDK, including when a read-only query occurred.

## Failure mechanism

The workflow awaits an outgoing signal acknowledgement, upserts memo `progress=1`, then waits. Its `add(7)` Update returns `7`; its query reads progress.

**Observed in traces:** Legacy Query processing applies trailing signal-acknowledgement history to the cached run: `resolve_signal_external_workflow` emits `modify_workflow_properties`, then a separate `legacy_query` activation runs. The next WFT delivers `do_update`, emitting Update accept/complete. The query ID and activation order establish this sequence; the `is_replaying` flag differs between SDK representatives.

**Source-derived explanation:** Core suppresses WFT completion during query playback and answers the query separately. The cached run retains the memo command; subsequent Update commands append behind it for the next normal WFT response. See the release-mapped Core code: [`prepare_complete_resp` and the Legacy Query response path](https://github.com/temporalio/sdk-rust/blob/c675c32469af707e1c979a7b808c70b4a0608b82/crates/sdk-core/src/worker/workflow/managed_run.rs), and [`prepare_commands` / `get_commands`](https://github.com/temporalio/sdk-rust/blob/c675c32469af707e1c979a7b808c70b4a0608b82/crates/sdk-core/src/worker/workflow/machines/workflow_machines.rs). This explains the traces/history; **outgoing completion RPCs were not captured**.

**Observed in Server exports:** the signal acknowledgement is event 6; WFT2 is scheduled at 7 and started at 8:

| Event | Memo-first failure | Update-first control |
|---|---|---|
| 9 | WorkflowTaskCompleted | WorkflowTaskCompleted |
| 10 | WorkflowPropertiesModified | WorkflowExecutionUpdateAccepted |
| 11 | WorkflowExecutionUpdateAccepted | WorkflowExecutionUpdateCompleted |
| 12 | WorkflowExecutionUpdateCompleted | WorkflowPropertiesModified |

The memo links to WFT completion **9**; Update acceptance has `acceptedRequestSequencingEventId=7`. Update completion links to acceptance **11** or **10**, respectively. No WFT boundary intervenes. These 12-event exports precede harness termination.

When WFT2 wins the race, `[do_update, resolve_signal_external_workflow]` emits Update accept/complete before the memo, then the query runs. This order replays; the memo-first history fails at event 10.

## Results and controls

Both replay SDKs checked all **42** exported histories, with the same **16 failures / 26 passes** on each (84 recorded outcomes):

| Producer SDK | Memo-first, same WFT: FAIL | Update-first, same WFT: PASS | Memo in earlier WFT: PASS |
|---|---:|---:|---:|
| 1.34.0 | 11 | 16 | 3 |
| 1.36.1 | 5 | 7 | 0 |
| **Total** | **16** | **23** | **3** |

The script labels all 26 passes `update_first`; only 23 are same-WFT controls. `results.json` contains all case IDs, conditions, orders, verdicts and source SHA-256 values.

Four original histories and their traces are included in `evidence/e2e/` after extraction:

| Case ID (history filename without `.history.json`) | Producer SDK | Role / replay on both SDKs |
|---|---|---|
| `S_query-0b25b632` | 1.34.0 | Memo-first / FAIL |
| `S_query-bc995687` | 1.36.1 | Memo-first / FAIL |
| `S_query-bf3f3793` | 1.34.0 | Query-present Update-first control / PASS |
| `S_noquery-d52b552b` | 1.34.0 | No-query control / PASS |

The archive includes all 42 completed run logs, both replay logs, Server logs and provenance. The private archive retains all 42 histories; four are also distributed here. Four-case rechecks on both SDKs matched (`evidence/recheck/`).

Scheduling uses **one Matching read partition and one write partition**, a custom slot supplier withholding sticky polls, and a tracing wrapper holding WFT1 open during query submission. Cache is **1000**, sticky timeout **60 seconds**, poll concurrency **2**. After the acknowledgement, scheduled WFT and Update admission, the harness grants one normal-queue poll, then opens both pollers after either query or Update finishes. Either can win. The worker is instrumented; workflow code is unchanged. Timing delays alone are not evidence.

## Verification and reproduction

From the package directory, run the standard-library verifier with Python 3.9+:

```bash
python3 verify.py
```

Expected: **PASS**, exit 0 (failure: exit 1). This checks checksums, all 42 recorded outcomes per SDK, representative event/WFT links and traces, and saved rechecks. It does **not** run Temporal or Replayer. Add `--archive /path/to/private/archive` to verify all private originals.

Extract into a new directory and check the saved files:

```bash
mkdir unpacked
tar -xzf reproducer.tar.gz -C unpacked
tar -xzf evidence.tar.gz -C unpacked
(cd unpacked/reproducer && sha256sum -c SHA256SUMS)
(cd unpacked/evidence && sha256sum -c SHA256SUMS)
cd unpacked
```

Follow `reproducer/SETUP.md` to install the pinned Python dependencies into `.venv134` and `.venv1361`, then run cold replay without a Server:

```bash
timeout 60 .venv134/bin/python -B -u reproducer/replay_all.py evidence/e2e
timeout 60 .venv1361/bin/python -B -u reproducer/replay_all.py evidence/e2e
```

Expect **4 histories, 2 memo-first TMPRL1100 failures and 2 passes** per SDK. The script exits **0 even with failures** and accepts an empty directory: check its JSON rows/counts. Use an equivalent to GNU `timeout` where unavailable.

`reproducer/SETUP.md` supplies CLI installation, the single-partition Server launch and live commands using unchanged `reproducer/h6_e2e.py`. Use fresh outputs; either ordering can occur. Controls: `--case S_noquery` or `--case S_query --cache 0`. Dependencies and Core mappings are in `evidence/MANIFEST_7B.txt`.

## Limitations

This demonstrates controlled real-Server reproduction, not an uninstrumented production occurrence or its frequency. The original reporter's proprietary history and its attribution to this mechanism remain unverified.

Command buffering is source-derived; definitive root-cause attribution and any fix remain unverified. Other versions/platforms are untested, and the harness uses private Python worker interfaces. Fresh installation and reconstructed live launch commands were not rerun during packaging. Representative replays succeeded in existing environments outside a sandbox that had stalled.
