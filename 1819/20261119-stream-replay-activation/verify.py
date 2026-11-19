#!/usr/bin/env python3
"""Verifies the saved evidence for temporalio/sdk-rust PR #1819 without building or running Temporal.

Standard library only. Archives are read in memory; nothing is extracted to disk.
Exit status 0 means every check passed.
"""
import hashlib
import json
import re
import sys
import tarfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
SDK_SHA = "f858a1b0901f41255e3d00ca4ce81b86d05044d9"
PATCH_NAME = "sdk-rust-1819-stream-replay-activation.patch"
PATCHED_PATHS = {
    "crates/sdk-core/src/core_tests/streams.rs",
    "crates/sdk-core/src/core_tests/streams/investigation_1819_real.rs",
}
CLI_VERSION = "1.8.3-server-1.32.0-162.0"

failures = 0


def check(name, ok, detail=""):
    global failures
    print(f"{'PASS' if ok else 'FAIL'}  {name}{f'  ({detail})' if detail else ''}")
    if not ok:
        failures += 1
    return ok


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --- archives --------------------------------------------------------------------------------

def read_archive(path: Path, top: str) -> dict:
    """Returns {name: bytes} for an archive of regular files under one safe top directory."""
    files = {}
    with tarfile.open(path, "r:gz") as tar:
        for m in tar.getmembers():
            parts = m.name.split("/")
            safe = (m.isfile() and len(parts) == 2 and parts[0] == top and parts[1]
                    and parts[1] not in (".", "..") and not m.name.startswith("/"))
            if not check(f"{path.name}: safe member {m.name!r}", safe):
                continue
            files[parts[1]] = tar.extractfile(m).read()
    manifest = files.pop("SHA256SUMS", b"").decode()
    listed = {}
    for line in manifest.splitlines():
        digest, name = line.split("  ", 1)
        listed[name] = digest
    check(f"{path.name}: SHA256SUMS lists exactly its files", set(listed) == set(files),
          f"listed={sorted(listed)}")
    for name, data in sorted(files.items()):
        check(f"{path.name}: {name} matches SHA256SUMS", listed.get(name) == sha256(data))
    return files


# --- protobuf wire format (field numbers from temporal/api/history/v1/message.proto) ----------

def varint(buf, i):
    shift = result = 0
    while True:
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, i
        shift += 7


def fields(buf):
    i, out = 0, []
    while i < len(buf):
        key, i = varint(buf, i)
        num, wt = key >> 3, key & 7
        if wt == 0:
            val, i = varint(buf, i)
        elif wt == 2:
            n, i = varint(buf, i)
            val, i = buf[i:i + n], i + n
        elif wt == 1:
            val, i = buf[i:i + 8], i + 8
        elif wt == 5:
            val, i = buf[i:i + 4], i + 4
        else:
            raise ValueError(f"unsupported wire type {wt}")
        out.append((num, wt, val))
    return out


def first(fs, num, default=None):
    return next((v for n, _, v in fs if n == num), default)


def every(fs, num):
    return [v for n, _, v in fs if n == num]


def u32s(fs, num):
    vals = []
    for n, wt, v in fs:
        if n != num:
            continue
        if wt == 0:
            vals.append(v)
        else:
            j = 0
            while j < len(v):
                x, j = varint(v, j)
                vals.append(x)
    return vals


EVENT = {1: "WorkflowExecutionStarted", 2: "WorkflowExecutionCompleted", 5: "WorkflowTaskScheduled",
         6: "WorkflowTaskStarted", 7: "WorkflowTaskCompleted", 17: "TimerStarted",
         25: "MarkerRecorded", 41: "UpdateAccepted", 42: "UpdateRejected", 43: "UpdateCompleted",
         47: "UpdateAdmitted"}


def stream_marker(data):
    """Decodes coresdk.external_data.StreamMarkerData (reads only)."""
    reads = []
    for r in every(fields(data), 1):
        rf = fields(r)
        records = []
        for rec in every(rf, 8):
            body = first(fields(rec), 2, b"")
            records.append(first(fields(body), 2, b"").decode())
        reads.append({"seq": first(rf, 1, 0), "count": first(rf, 6, 0), "records": records,
                      "page_counts": u32s(rf, 9), "page_activations": u32s(rf, 10),
                      "end": first(rf, 11) is not None})
    return reads


def parse_history(raw):
    events = []
    for ev in every(fields(raw), 1):
        ef = fields(ev)
        e = {"id": first(ef, 1), "type": EVENT.get(first(ef, 3), str(first(ef, 3)))}
        if e["type"] == "WorkflowTaskStarted":
            a = fields(first(ef, 11, b""))
            e.update(scheduled=first(a, 1), request_id=first(a, 3, b"").decode())
        elif e["type"] == "WorkflowTaskCompleted":
            a = fields(first(ef, 12, b""))
            e.update(scheduled=first(a, 1), started=first(a, 2))
        elif e["type"] == "TimerStarted":
            a = fields(first(ef, 20, b""))
            e.update(timer_id=first(a, 1, b"").decode(), wft_completed=first(a, 3))
        elif e["type"] == "MarkerRecorded":
            a = fields(first(ef, 25, b""))
            e.update(marker=first(a, 1, b"").decode(), wft_completed=first(a, 3))
            if e["marker"] == "core_stream":
                for entry in every(a, 2):
                    kv = fields(entry)
                    if first(kv, 1, b"").decode() == "data":
                        payload = first(fields(first(kv, 2, b"")), 1, b"")
                        e["reads"] = stream_marker(first(fields(payload), 2, b""))
        events.append(e)
    return events


# --- test trace --------------------------------------------------------------------------------

READ_JOB = re.compile(r'ResolveStreamRead\(seq=(\d+),records=\[(.*?)\],end=(true|false)\)')
TIMER = re.compile(r"StartTimer\(StartTimer \{ seq: (\d+)")
EVICT = re.compile(r'RemoveFromCache\((\w+): (.*)\)"\]')


def trace_lines(trace, kind, case):
    out = []
    for line in trace.splitlines():
        if not line.startswith(f"{kind} case={case} "):
            continue
        head, _, rest = line.partition(" jobs=")
        jobs, _, commands = rest.partition(" commands=")
        attrs = dict(kv.split("=", 1) for kv in head.split()[1:] if "=" in kv)
        out.append({
            **attrs,
            "reads": [re.findall(r'\\"([^\\"]*)\\"', m.group(2)) for m in READ_JOB.finditer(jobs)],
            "timers": [int(s) for s in TIMER.findall(commands)],
            "do_update": "DoUpdate(" in jobs,
            "eviction": EVICT.search(jobs).groups() if EVICT.search(jobs) else None,
        })
    return out


# --- the case checks --------------------------------------------------------------------------

def obs_abs(results, case):
    return results["observed"][case]["w2_marker_absolute_activations"]


def verify_case(case, files, trace, results):
    timers_case = case == "timer_per_page"
    print(f"\n== {case}")
    ev = parse_history(files[f"{case}-server-history.bin"])
    ids = [e["id"] for e in ev]
    check(f"{case}: History event ids are 1..{len(ids)} in order", ids == list(range(1, len(ids) + 1)))
    check(f"{case}: no Update event in History",
          not any(e["type"].startswith("Update") for e in ev))
    check(f"{case}: starts with WorkflowExecutionStarted and ends with WorkflowExecutionCompleted",
          ev[0]["type"] == "WorkflowExecutionStarted" and ev[-1]["type"] == "WorkflowExecutionCompleted")
    started = [e for e in ev if e["type"] == "WorkflowTaskStarted"]
    completed = {e["started"]: e for e in ev if e["type"] == "WorkflowTaskCompleted"}
    check(f"{case}: three Workflow Tasks, each started and completed",
          len(started) == 3 and all(s["id"] in completed for s in started))
    w2s = started[1]["id"]
    w2c = completed[w2s]["id"]
    check(f"{case}: W2 was returned inline by the heartbeat completion of W1",
          started[1]["request_id"] == "request-from-RespondWorkflowTaskCompleted")
    w2_markers = [e for e in ev if e["type"] == "MarkerRecorded" and e.get("marker") == "core_stream"
                  and e["wft_completed"] == w2c]
    if check(f"{case}: one core_stream marker owned by W2 (WorkflowTaskCompleted {w2c})",
             len(w2_markers) == 1 and len(w2_markers[0]["reads"]) == 1):
        r = w2_markers[0]["reads"][0]
        check(f"{case}: W2 marker read seq=1 count=2", (r["seq"], r["count"]) == (1, 2))
        check(f"{case}: W2 marker page_counts=[1,1]", r["page_counts"] == [1, 1])
        check(f"{case}: W2 marker page_activations=[1,1]", r["page_activations"] == [1, 1])
        absolute = [sum(r["page_activations"][:i + 1]) for i in range(len(r["page_activations"]))]
        check(f"{case}: pages recorded at separate activations {absolute}", absolute == [1, 2])
        check(f"{case}: W2 marker records [p1,p2], no end", r["records"] == ["p1", "p2"] and not r["end"])
        expect = results["observed"][case]["w2_marker"]
        check(f"{case}: results.json W2 marker matches History",
              expect == {**{k: r[k] for k in ("seq", "count", "page_counts", "page_activations", "records")},
                         "end": None}
              and obs_abs(results, case) == absolute)
    w2_timers = [(e["id"], e["timer_id"]) for e in ev
                 if e["type"] == "TimerStarted" and e["wft_completed"] == w2c]
    all_timers = [e for e in ev if e["type"] == "TimerStarted"]

    live = [l for l in trace_lines(trace, "LIVE", case) if l["wft"] == "W2"]
    check(f"{case}: live W2 activations carry W2's started id {w2s}",
          len(live) == 3 and all(int(l["history_length"]) == w2s for l in live))
    check(f"{case}: live W2 act0 is the Update only", bool(live) and live[0]["do_update"] and not live[0]["reads"])
    live_reads = [l["reads"] for l in live]
    check(f"{case}: live W2 pages are separate activations [[p1]] [[p2]]",
          live_reads == [[], [["p1"]], [["p2"]]], str(live_reads))
    live_timers = [l["timers"] for l in live]

    replay = trace_lines(trace, "REPLAY", case)
    rw2 = [l for l in replay if int(l["history_length"]) == w2s and not l["eviction"]]
    check(f"{case}: cold replay has one W2 activation", len(rw2) == 1, f"{len(rw2)}")
    check(f"{case}: cold replay W2 is ONE read job with records [p1,p2]",
          len(rw2) == 1 and rw2[0]["reads"] == [["p1", "p2"]], str(rw2[0]["reads"] if rw2 else None))
    evictions = [l["eviction"] for l in replay if l["eviction"]]
    check(f"{case}: exactly one replay eviction", len(evictions) == 1, str(evictions))

    obs = results["observed"][case]
    check(f"{case}: results.json live/replay grouping matches the trace",
          obs["live_w2_reads_by_activation"] == live_reads
          and rw2 and obs["replay_w2_reads_by_activation"] == [rw2[0]["reads"]])
    if timers_case:
        check(f"{case}: live W2 timers per activation [[],[2],[3]]", live_timers == [[], [2], [3]], str(live_timers))
        check(f"{case}: replay W2 timers [2]", bool(rw2) and rw2[0]["timers"] == [2])
        check(f"{case}: History W2 TimerStarted events 10 (timer 2) and 11 (timer 3)",
              w2_timers == [(10, "2"), (11, "3")], str(w2_timers))
        if evictions:
            reason, message = evictions[0]
            m = re.search(r"\[TMPRL1100\] Nondeterminism error: No command scheduled for event "
                          r"HistoryEvent\(id: (\d+), (\w+)\)", message)
            check(f"{case}: replay eviction reason is Nondeterminism", reason == "Nondeterminism")
            if check(f"{case}: eviction is TMPRL1100 for an unmatched History event", bool(m)):
                eid, etype = int(m.group(1)), m.group(2)
                target = next((e for e in ev if e["id"] == eid), {})
                check(f"{case}: TMPRL1100 event {eid} is a TimerStarted in History",
                      etype == "TimerStarted" and target.get("type") == "TimerStarted")
                check(f"{case}: event {eid} is timer {target.get('timer_id')} owned by W2",
                      target.get("wft_completed") == w2c)
                seq = int(target.get("timer_id", "-1"))
                check(f"{case}: timer {seq} was sent by live W2 act2 (page p2) and not by the replay",
                      len(live_timers) == 3 and seq in live_timers[2]
                      and bool(rw2) and seq not in rw2[0]["timers"])
                check(f"{case}: replay stopped right before event {eid}",
                      [int(l["history_length"]) for l in replay if l["eviction"]] == [eid - 1])
        check(f"{case}: results.json timers and error match",
              obs["live_w2_timers_by_activation"] == live_timers and obs["replay_w2_timers"] == [2]
              and obs["replay_eviction"]["reason"] == "Nondeterminism"
              and obs["replay_eviction"]["event_id"] == 11)
    else:
        check(f"{case}: no timers anywhere (History, live, replay)",
              not all_timers and live_timers == [[], [], []] and bool(rw2) and rw2[0]["timers"] == [])
        check(f"{case}: replay eviction is the normal LangRequested one",
              bool(evictions) and evictions[0][0] == "LangRequested", str(evictions))
        check(f"{case}: results.json control result matches",
              obs["replay_eviction"]["reason"] == "LangRequested")
    check(f"{case}: test asserted the case", f"ASSERTED case={case} w2_started={w2s} " in trace)

    ex = files[f"{case}-wft-exchanges.redacted.txt"].decode()
    seq_ids = [int(x) for x in re.findall(r"sequencing_id: Some\(\s*EventId\(\s*(\d+)", ex)]
    check(f"{case}: real Update request sequenced at W2's scheduled event {started[1]['scheduled']}",
          seq_ids == [started[1]["scheduled"]], str(seq_ids))
    check(f"{case}: rejection sent with the W2 completion",
          "type.googleapis.com/temporal.api.update.v1.Rejection" in ex)
    leftover = re.sub(r"task_token: (\[REDACTED\]|TaskToken\(REDACTED\))", "", ex)
    check(f"{case}: no task token left in the exchange dump", "task_token" not in leftover)


def main():
    print(f"Verifying {HERE.name}")
    required = ["README.md", "results.json", "verify.py", "reproduce.sh", "reproducer.tar.gz", "evidence.tar.gz"]
    for name in required:
        check(f"file {name} present", (HERE / name).is_file())
    if failures:
        return 1
    results = json.loads((HERE / "results.json").read_text())
    for name in ("reproducer.tar.gz", "evidence.tar.gz"):
        check(f"{name} SHA-256 matches results.json",
              sha256((HERE / name).read_bytes()) == results["package"]["archives"][name])

    repro = read_archive(HERE / "reproducer.tar.gz", "reproducer")
    pinned = json.loads(repro.get("pinned.json", b"{}"))
    patch = repro.get(PATCH_NAME, b"")
    script = (HERE / "reproduce.sh").read_text()
    pin = lambda key: (re.search(rf'^readonly {key}="([^"]+)"', script, re.M) or [None, None])[1]
    check("patch SHA-256 agrees in pinned.json, reproduce.sh and results.json",
          sha256(patch) == pinned["test_patch"]["sha256"] == pin("PATCH_SHA256")
          == results["sdk_rust"]["test_patch_sha256"])
    check("SDK SHA agrees in pinned.json, reproduce.sh and results.json",
          SDK_SHA == pinned["sdk_rust"]["sha"] == pin("SDK_SHA") == results["sdk_rust"]["sha"])
    check("CLI version agrees in pinned.json, reproduce.sh and results.json",
          CLI_VERSION == pinned["temporal_cli"]["version"] == pin("CLI_VERSION")
          == results["temporal_cli"]["version"])
    touched = set(re.findall(r"^diff --git a/(\S+) b/", patch.decode(), re.M))
    check("patch only touches the two test-only paths", touched == PATCHED_PATHS, str(sorted(touched)))
    check("patch only adds lines", not re.search(r"^-(?!--)", patch.decode(), re.M))

    evidence = read_archive(HERE / "evidence.tar.gz", "evidence")
    prov = json.loads(evidence.get("provenance.json", b"{}"))
    check("evidence provenance pins the same SDK SHA and patch",
          prov.get("sdk_rust_sha") == SDK_SHA and prov.get("test_patch_sha256") == sha256(patch))
    trace = evidence.get("test-trace.txt", b"").decode()
    check("evidence test run passed exactly one test", "test result: ok. 1 passed;" in trace)
    for case in ("timer_per_page", "plain"):
        needed = [f"{case}-server-history.bin", f"{case}-wft-exchanges.redacted.txt"]
        if all(check(f"evidence has {n}", n in evidence) for n in needed):
            try:
                verify_case(case, evidence, trace, results)
            except Exception as error:  # malformed evidence must fail, not pass silently
                check(f"{case}: evidence could be parsed", False, repr(error))

    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
