# Temporal SDK Rust #1766 — minimal executable Stage 7B evidence

This package preserves a **real Temporal Server reproduction under controlled scheduling**. It is not an uninstrumented production occurrence. Original histories and executed Python scripts are byte-for-byte copies. No SDK Core fix or production code change is included.

[Issue #1766](https://github.com/temporalio/sdk-rust/issues/1766) reports cold-replay nondeterminism when a memo upsert precedes accepted/completed Updates in one Workflow Task (WFT). The issue describes a private real-world history and supplies a constructed reduction; neither that proprietary history nor its claimed origin is verified here. Stage 7B supplies independently inspectable Server-exported examples of the same error:

```text
TMPRL1100 Update machine does not handle this event: HistoryEvent(id: 10, WorkflowPropertiesModified)
```

**Correctness invariant:** a history committed by the Server from an unchanged workflow should cold-replay with that workflow on the same SDK. A read-only query should not create a command ordering that breaks this invariant.

## Versions and provenance

Temporal CLI **1.5.1**, embedded Server **1.29.1** (UI **2.42.1**, headless); Python **3.12.3**, SDK **1.34.0** and **1.36.1**, Linux **aarch64**. Original dependency lists, bridge/wheel hashes and release-tag Core mappings are in `evidence/MANIFEST_7B.txt`. The preserved manifest maps the SDKs to Core `e163abd6dc19040064986a63c8b8cf4756ffd320` and `c675c32469af707e1c979a7b808c70b4a0608b82`; these are manifest provenance, not new builds or claims about current Core HEAD. The investigation worktree was recorded at `c4512923c0f593af6b72fca3f0afb8570848b0b0`.

Server startup logs independently name the CLI/Server versions. Producer versions are recorded in each history's first WFT completion metadata and each run log. Packaging rechecks measured the surviving environments and matched both bridge hashes, plus the CLI binary and downloaded release archive hashes. No virtual environments, wheels, CLI binary, or build caches are distributed.

## What the trace and Server history show

The workflow awaits an outgoing signal acknowledgement, upserts memo `progress=1`, then waits indefinitely. Its `add(7)` Update returns `7`; its query only reads `progress`.

For the memo-first representatives, a query is submitted while WFT1 is held open. Once the acknowledgement is available, query playback delivers `resolve_signal_external_workflow` and the workflow emits `modify_workflow_properties`. A separate activation carries query ID `legacy_query`. The next WFT delivers `do_update`, emitting `update_response.accepted` then `update_response.completed`. The Server commits the memo before the Update events. The trace establishes the command order; the exported history establishes the committed order. The SDK 1.34.0 resolve-signal activation has `is_replaying=false`, whereas the SDK 1.36.1 representative has `true`; the explanation does not rely on that flag alone.

In the query-present passing control, WFT2 arrives first with jobs `[do_update, resolve_signal_external_workflow]`. Its completion contains `[update_response.accepted, update_response.completed, modify_workflow_properties]`, and the query follows. This is the alternate outcome of the Query/WFT race.

All four representatives have this actual Server sequence (distinct from the original issue's constructed event numbering):

| Event | Server-committed event |
|---|---|
| 1 | WorkflowExecutionStarted |
| 2, 3, 4 | WFT1 scheduled, started, completed |
| 5 | SignalExternalWorkflowExecutionInitiated, linked to WFT completion 4 |
| 6 | ExternalWorkflowExecutionSignaled, linked to initiation 5 |
| 7, 8, 9 | WFT2 scheduled, started, completed |
| 10, 11, 12 — failure | WorkflowPropertiesModified; UpdateAccepted; UpdateCompleted |
| 10, 11, 12 — control | UpdateAccepted; UpdateCompleted; WorkflowPropertiesModified |

The memo's `workflowTaskCompletedEventId` is **9**. Update acceptance has `acceptedRequestSequencingEventId=7`; completion links back to the acceptance event (11 in failures, 10 in controls). All three events lie after WFT completion 9 with no intervening WFT boundary. Updates do not have a direct WFT-completed field; that association is checked through the batch boundaries and sequencing link. These are 12-event exports taken before harness termination, not complete closed-execution histories.

## Complete matrix and selected originals

Both original independent replay logs contain all **42** exported cases: **16 memo-first failures and 26 passes** on **each** replay SDK, or 84 recorded replay outcomes. The original script calls the passing group `update_first`; strictly, it comprises **23 Update-first same-WFT histories and 3 `N_ungated` histories with the memo in an earlier WFT**. All 16 failures have the signature above. `results.json` retains actual IDs, producer SDK, conditions, order, WFT associations, per-SDK verdicts, and full source SHA-256 values.

| Producer SDK | Memo-first / FAIL | Update-first same WFT / PASS | Memo in earlier WFT / PASS |
|---|---:|---:|---:|
| 1.34.0 | 11 | 16 | 3 |
| 1.36.1 | 5 | 7 | 0 |

The compact package includes these four original histories and their trace JSON files:

| Case ID | Producer | Role | Replay on both SDKs |
|---|---|---|---|
| `S_query-0b25b632` | 1.34.0 | Memo-first | FAIL, TMPRL1100 |
| `S_query-bc995687` | 1.36.1 | Memo-first | FAIL, TMPRL1100 |
| `S_query-bf3f3793` | 1.34.0 | Query-present Update-first control | PASS |
| `S_noquery-d52b552b` | 1.34.0 | No-query control | PASS |

All 42 successful original run logs are retained in the compact evidence archive to corroborate the complete matrix without distributing the other 38 histories. Both original replay logs, all three Server logs, the original manifest, a full 137-file source inventory, and separately labeled packaging rechecks are included.

Three incomplete attempts remain in the private archive and are explicitly excluded from the 42-case matrix: `C1_trailing_query-f6f7f128` stops at `wft1_held_open` without a recorded error/result; `C1_trailing_query-fbbe244a` and `S_query_early-feaac8b7` end with a “no poller seen” RPC error. No exported history or replay verdict is invented for them. Their log hashes and statuses are in `results.json`. The manifest also records earlier Stage 6/7 scratch evidence lost in a prior VM reboot; it has not been reconstructed. Historical intermediate script revisions for incomplete attempts are not present; the final preserved harness is the version covered by the manifest.

## Controlled scheduling and limits

The preserved harness uses a custom workflow slot supplier to withhold sticky polls, a tracing wrapper around the default sandbox runner, and a WFT1 hold while the client issues a query. Sticky timeout is **60 seconds**, cache **1000** for the selected cases, and workflow poll concurrency **2**. S3 uses one Matching read partition and one write partition. The harness waits for the signal acknowledgement and scheduled WFT, admits the Update, grants one normal-queue poll, and opens both pollers after either query or Update finishes. Matching may hand out the legacy query or WFT first; a live run can legitimately produce either order. The original script's 0.5-second pacing is not ordering evidence.

S1 (`17233`) disabled `system.enableStickyQuery`; S2 (`17333`) used defaults; S3 (`17433`) used the single-partition configuration. All selected representatives came from S3. This configuration is recorded in the original manifest; the startup logs do not themselves dump the dynamic configuration. The original exact shell launch transcript is absent. SETUP reconstructs equivalent launch commands from the manifest and checks them against the retained CLI's help. Live commands were not rerun during packaging.

Proven: this controlled real-Server scenario committed the problematic order, and the same original workflow fails cold replay on both tested SDKs. Unverified: frequency or occurrence without the harness, behavior in a production deployment, other Server/SDK versions or platforms, a definitive internal root cause, and any proposed fix. These observations do not establish that all queries or all memo/Update combinations fail.

## Verify the saved evidence (no dependencies or Server)

Run from this package directory with Python 3.9+:

```bash
python3 verify.py
```

Expected: `PASS`, exit 0. Failure: `FAIL`, exit 1. This standard-library verifier reads both tar archives without extraction; checks archive/member SHA-256, manifest and matrix consistency, all 42 original run logs and both replay logs, and the four saved histories' event ordering, WFT links, producer metadata and traces. It also checks the eight saved packaging replay results. **It does not execute Temporal or Python Replayer.** Hashes provide integrity and internal consistency, not a cryptographic attestation of the historical Server run.

To verify all private originals and all 42 histories as well:

```bash
python3 verify.py --archive "$HOME/oss/temporal-review-evidence-archive/1766-20261117-legacy-query-e2e"
```

List/extract into a new directory and check the extracted artifacts:

```bash
tar -tzf reproducer.tar.gz
tar -tzf evidence.tar.gz
mkdir unpacked
tar -xzf reproducer.tar.gz -C unpacked
tar -xzf evidence.tar.gz -C unpacked
(cd unpacked/reproducer && sha256sum -c SHA256SUMS)
(cd unpacked/evidence && sha256sum -c SHA256SUMS)
```

## Cold replay and live reproduction

Read `unpacked/reproducer/SETUP.md` for exact environment installation and live Server commands. From `unpacked`, after creating the two documented environments:

```bash
timeout 60 .venv134/bin/python -B -u reproducer/replay_all.py evidence/e2e
timeout 60 .venv1361/bin/python -B -u reproducer/replay_all.py evidence/e2e
```

Each should report 4 histories, 2 memo-first TMPRL1100 failures, and 2 control passes. The original replay script exits **0 even when replay failures are recorded**, and accepts an empty directory; always inspect its JSON summary. Do not treat its exit status alone as success.

Packaging actually reran those four histories on both surviving original environments with the unmodified archived script; all eight outcomes matched. Logs are in `evidence/recheck/`. Initial sandboxed attempts stalled without results and were stopped after more than five minutes; the bounded replays completed outside the sandbox. No live Server scenario, dependency reinstall, or portability to a different platform was tested during packaging. No modified portable harness is needed or included; the original uses private Python worker interfaces and is intended for the two pinned SDK versions.

The five top-level files follow the requested minimal executable evidence layout: this README, `results.json`, `verify.py`, `reproducer.tar.gz`, and `evidence.tar.gz`. The private archive retains all original artifacts unchanged. Packaging audit additions are separate from those originals. A text/payload scan and inspection found no credentials, tokens, proprietary workflow payloads, or unintended personal data in the public package. Synthetic IDs, localhost addresses, timestamps, PIDs and the generic `research-sandbox-work` worker identity are intentionally preserved as provenance; histories were not redacted or rewritten.
