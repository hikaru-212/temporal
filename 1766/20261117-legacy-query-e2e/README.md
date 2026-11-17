# Temporal SDK Core #1766 — Memo/Update Replay Nondeterminism

## Summary

A real Temporal Server committed a memo upsert before Update acceptance/completion in the same Workflow Task (WFT). Cold replay of the unmodified Server-exported history fails with:

```text
TMPRL1100 Update machine does not handle this event:
HistoryEvent(id: 10, WorkflowPropertiesModified)
```

This reproduces the failure signature reported in [sdk-rust #1766](https://github.com/temporalio/sdk-rust/issues/1766).

**Tested environment:** Temporal Server 1.29.1, CLI 1.5.1, Python SDK 1.34.0 / 1.36.1, Linux aarch64.

## Reproduction and findings

The workflow awaits an external-signal acknowledgment, upserts a memo, and accepts an Update.

Under controlled scheduling, a non-sticky legacy query processes trailing history containing the acknowledgment. The workflow emits a memo command during query processing, before the next real WFT delivers the Update.

The Server-exported history contains:

```text
9   WorkflowTaskCompleted
10  WorkflowPropertiesModified
11  WorkflowExecutionUpdateAccepted
12  WorkflowExecutionUpdateCompleted
```

The memo and Update events belong to the same committed WFT.

The source-derived replay explanation is that `DoUpdate` and signal resolution occur in one activation, producing Update-first commands instead. The saved replay logs confirm the nondeterminism error, but do not directly record those replay activation jobs.

**Likely mechanism:** Core retains a memo command generated during legacy-query playback and later includes it in a real WFT completion. This explanation is supported by Core source and execution traces; the outgoing completion RPC was not captured directly.

### Results

Both SDK versions replayed all 42 exported histories with identical outcomes:

| Server-exported history | Count | Cold replay |
| ----------------------- | ----- | ----------- |
| Memo-first, same WFT | 16 | FAIL — TMPRL1100 |
| Update-first, same WFT | 23 | PASS |
| Memo in an earlier WFT | 3 | PASS |

The package contains four representative histories and their traces, including failures produced by both SDK versions and two passing controls. `results.json` preserves the complete case matrix and checksums.

The live reproduction uses a cached workflow run, withheld sticky polling, one Matching partition, and controlled Query/WFT scheduling. The workflow implementation is unchanged.

## Verification

From this directory, verify the saved evidence using Python 3.9+:

```bash
python3 verify.py
```

This checks the recorded results, history event relationships, traces, and checksums without running Temporal.

To inspect and replay the representative histories:

```bash
mkdir -p unpacked
tar -xzf reproducer.tar.gz -C unpacked
tar -xzf evidence.tar.gz -C unpacked
cd unpacked
```

Follow `reproducer/SETUP.md` to prepare the pinned Python environments, then run:

```bash
timeout 60 .venv134/bin/python -B -u reproducer/replay_all.py evidence/e2e
timeout 60 .venv1361/bin/python -B -u reproducer/replay_all.py evidence/e2e
```

**Expected per SDK:** two `TMPRL1100` failures and two passing controls.

The replay script can exit with code 0 even when expected replay failures occur; inspect its JSON results.

`reproducer/SETUP.md` also documents how to rerun the controlled live-Server scenario.

## Limitations

This establishes a real-Server failure path under controlled scheduling, not its frequency in an uninstrumented production environment.

The original reporter's private history may have a different cause. Command buffering is supported by source analysis but not direct RPC capture. Other Server versions and platforms remain untested, and no fix has been verified.
