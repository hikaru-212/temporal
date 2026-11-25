#!/usr/bin/env python3
"""Offline consistency check of the #1835 eviction-performance evidence; never builds or runs anything.

Checks checksums and archive safety, the original investigation manifest, JSONL schemas and sample
counts, source/build identity, correctness classifications, and recomputes every statistic in
results.json and every table row in README.md from the raw measurements.

This validates the integrity and internal consistency of the supplied evidence. It does not prove
that the measurements happened, and it does not reproduce runtime behaviour; see reproducer/SETUP.md.
Standard library only; Python >= 3.9.
"""
import argparse
import hashlib
import io
import json
import math
import random
import re
import statistics as st
import sys
import tarfile
from collections import Counter, defaultdict
from pathlib import Path, PurePosixPath

COMMITS = {
    "U": "6eaaeebaa5d90e573e7bed5152c9b5dfca75dc08",
    "A": "31a1a905c26034957f3968015de45177849a2b3f",
    "B": "9a59ba3c33ac0d64035a197dc1ae103aa670a8a5",
}
PROD_DIFF = {
    "U": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "A": "3a96b62d173cbcd4529c3fbc575805ddcb986c7238dfd55927771ffa3e2e76be",
    "B": "35f5f0c7ef1d86e16d6693be53a1381d545029970fc36d7c4b930b5b6666cf25",
}
BENCH_SHA = "b610cefa509996aa7752ee2059d26b17d06c74fd3c6f4ded027e5fb6c60d8055"
TOP_FILES = ["README.md", "evidence.tar.gz", "reproducer.tar.gz", "results.json", "verify.py"]
ROW_KEYS = {
    "label", "scenario", "cycles", "round", "i", "warmup", "verify", "hist_events", "hist_bytes",
    "wft_count", "partial_events", "lat_ns", "cpu_ns", "rss0_kb", "hwm_kb", "q_lat_ns", "e2e_ns",
    "win_acts", "win_replay_acts", "win_evicts", "win_fetches", "win_fetch_bytes", "total_acts",
    "total_evicts", "total_fetches", "completions", "failures", "query_responses", "expected_queries",
    "progress_commits", "first_cmds_ok", "real_cmds", "replay", "replay_acts", "la_executions",
    "win_la_executions",
}
S4_KEYS = {"markers", "la_delay_ms"}
# (scenario, la_delay_ms) -> cycles, rounds, samples per round, warm-up rows per process
DESIGN = {
    ("s2", 0): ([0, 18, 198, 998, 1998], 10, 10, 3),
    ("s0", 0): ([18, 198, 998, 1998], 6, 10, 3),
    ("s1", 0): ([18, 198, 998, 1998], 6, 10, 3),
    ("s3sig", 0): ([18, 198, 998], 6, 10, 3),
    ("s3upd", 0): ([18, 198, 998], 6, 10, 3),
    ("s4", 0): ([0], 10, 10, 3),
    ("s4", 20): ([0], 6, 5, 2),
}
EXPECTED_EVICTIONS = {
    "A": {"s0": 0, "s1": 0, "s2": 0, "s3sig": 1, "s3upd": 1, "s4": 1},
    "B": {"s0": 0, "s1": 0, "s2": 1, "s3sig": 1, "s3upd": 1, "s4": 1},
    "U": {"s0": 0, "s1": 0, "s2": 0, "s3sig": 0, "s3upd": 0, "s4": 0},
}
INVALID = {("U", "s3sig"), ("U", "s3upd"), ("U", "s4")}
REL_TOL = 1e-9


def check(cond, msg):
    if not cond:
        raise ValueError(msg)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def close(a, b, what):
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        check(math.isclose(a, b, rel_tol=REL_TOL, abs_tol=1e-9), f"{what}: {a} != {b}")
    elif isinstance(a, dict) and isinstance(b, dict):
        check(a.keys() == b.keys(), f"{what}: keys {sorted(a)} != {sorted(b)}")
        for k in a:
            close(a[k], b[k], f"{what}.{k}")
    elif isinstance(a, list) and isinstance(b, list):
        check(len(a) == len(b), f"{what}: length {len(a)} != {len(b)}")
        for i, (x, y) in enumerate(zip(a, b)):
            close(x, y, f"{what}[{i}]")
    else:
        check(a == b, f"{what}: {a!r} != {b!r}")


# --- statistics: same definitions as reproducer/scripts/analyze.py and summarize.py -------------

def pct(xs, p):
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100
    f, c = math.floor(k), math.ceil(k)
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def mwu_p(a, b):
    pooled = sorted([(v, 0) for v in a] + [(v, 1) for v in b])
    rank_sum, ties, i = 0.0, 0.0, 0
    while i < len(pooled):
        j = i
        while j + 1 < len(pooled) and pooled[j + 1][0] == pooled[i][0]:
            j += 1
        rank_sum += sum((i + j) / 2 + 1 for k in range(i, j + 1) if pooled[k][1] == 0)
        ties += (j - i + 1) ** 3 - (j - i + 1)
        i = j + 1
    n1, n2 = len(a), len(b)
    n = n1 + n2
    sigma = math.sqrt(n1 * n2 / 12 * ((n + 1) - ties / (n * (n - 1))))
    if sigma == 0:
        return 1.0
    return math.erfc(abs((rank_sum - n1 * (n1 + 1) / 2 - n1 * n2 / 2) / sigma) / math.sqrt(2))


def boot_ci(a, b):
    rnd = random.Random(1766)
    diffs = []
    for _ in range(2000):
        ra = [a[rnd.randrange(len(a))] for _ in a]
        rb = [b[rnd.randrange(len(b))] for _ in b]
        diffs.append(st.median(rb) - st.median(ra))
    return [pct(diffs, 2.5), pct(diffs, 97.5)]


def dist(rows, f):
    return {str(k): v for k, v in sorted(Counter(r[f] for r in rows).items(), key=lambda kv: str(kv[0]))}


def span(xs):
    return {"min": min(xs), "median": st.median(xs), "max": max(xs)}


def ols(xs, ys):
    mx, my = st.mean(xs), st.mean(ys)
    s = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sum((x - mx) ** 2 for x in xs)
    return {"slope_ns_per_wft": s, "intercept_ns": my - s * mx}


# --- archive handling ------------------------------------------------------------------------

def read_archive(path, top):
    members = {}
    with tarfile.open(path, "r:gz") as tar:
        for m in tar.getmembers():
            p = PurePosixPath(m.name)
            check(m.isfile() and not p.is_absolute() and ".." not in p.parts and p.parts[0] == top,
                  f"unsafe or unexpected tar member {m.name!r} in {path.name}")
            check(m.name not in members, f"duplicate tar member {m.name}")
            members[m.name] = tar.extractfile(m).read()
    sums = members[f"{top}/SHA256SUMS"].decode().splitlines()
    listed = {}
    for line in sums:
        h, name = line.split("  ", 1)
        listed[f"{top}/{name}"] = h
    check(set(listed) == set(members) - {f"{top}/SHA256SUMS"}, f"{top}/SHA256SUMS does not list every member")
    for name, h in listed.items():
        check(sha(members[name]) == h, f"{name} differs from {top}/SHA256SUMS")
    return members


def jsonl(data):
    return [json.loads(line) for line in data.decode().splitlines() if line.strip()]


def key(r):
    return (r["scenario"], r["cycles"], r.get("la_delay_ms", 0))


# --- README rows: each row printed in README.md is regenerated here and must appear verbatim ----

def fmt_ms(ns):
    ms = ns / 1e6
    return f"{ms:.2f}" if abs(ms) < 1 else f"{ms:.1f}" if abs(ms) < 100 else f"{ms:.0f}"


def fmt_us(ns, sign=False):
    return f"{ns / 1e3:+.1f}" if sign else f"{ns / 1e3:.1f}"


def fmt_p(p):
    return "<1e-30" if p < 1e-30 else f"{p:.2g}"


def one(d):
    return next(iter(d)) if len(d) == 1 else ", ".join(f"{k}×{v}" for k, v in d.items())


def readme_rows(res):
    m = res["measurements"]
    cell = {(c["scenario"], c["cycles"], c["la_delay_ms"], c["label"]): c for c in m["timed_cells"]}
    comp = {(c["scenario"], c["cycles"], c["la_delay_ms"], c["label"]): c for c in m["comparisons"]}
    rows = []
    for p in m["s2_scaling"]["points"]:
        k = ("s2", p["cycles"], 0)
        a, b, c = cell[k + ("A",)], cell[k + ("B",)], comp[k + ("B",)]
        lo, hi = c["median_delta_ci95_ns"]
        rows.append(
            f"| {p['hist_events']:,} | {p['wft_count']:,} | {fmt_ms(a['latency_ns']['median'])} ms "
            f"| {fmt_ms(b['latency_ns']['median'])} ms | **{fmt_ms(c['median_delta_ns'])} ms** "
            f"| {fmt_ms(lo)} – {fmt_ms(hi)} ms | {fmt_ms(c['cpu_median_delta_ns'])} ms "
            f"| {one(b['window']['replay_activations'])} | {one(b['window']['history_fetches'])} "
            f"| {b['window']['history_fetch_bytes']['median']:,.0f} |")
    for sc in ["s0", "s1"]:
        for cyc in [18, 198, 998, 1998]:
            k = (sc, cyc, 0)
            a, b, c = cell[k + ("A",)], cell[k + ("B",)], comp[k + ("B",)]
            lo, hi = c["median_delta_ci95_ns"]
            rows.append(
                f"| {sc.upper()} | {a['hist_events'][0]:,} | {fmt_us(a['latency_ns']['median'])} "
                f"| {fmt_us(b['latency_ns']['median'])} | {fmt_us(c['median_delta_ns'], True)} "
                f"| {fmt_us(lo, True)} … {fmt_us(hi, True)} | {fmt_p(c['mwu_p'])} | {a['n']}/{b['n']} |")
    for sc, cyc, lad in [("s3sig", 18, 0), ("s3sig", 198, 0), ("s3sig", 998, 0), ("s3upd", 18, 0),
                         ("s3upd", 198, 0), ("s3upd", 998, 0), ("s4", 0, 0), ("s4", 0, 20)]:
        k = (sc, cyc, lad)
        a, b, c, u = cell[k + ("A",)], cell[k + ("B",)], comp[k + ("B",)], cell[k + ("U",)]
        lo, hi = c["median_delta_ci95_ns"]
        name = sc.upper().replace("SIG", "-signal").replace("UPD", "-update") + (f" (LA {lad} ms)" if sc == "s4" else "")
        rows.append(
            f"| {name} | {a['hist_events'][0]:,} | {fmt_ms(a['latency_ns']['median'])} "
            f"| {fmt_ms(b['latency_ns']['median'])} | {fmt_ms(c['median_delta_ns'])} "
            f"| {fmt_ms(lo)} – {fmt_ms(hi)} | {fmt_p(c['mwu_p'])} "
            f"| {one(a['window']['evictions'])}/{one(b['window']['evictions'])} "
            f"| {one(a['la_executions_total'])}/{one(b['la_executions_total'])} "
            f"| {a['n']}/{b['n']} | {fmt_ms(u['latency_ns']['median'])} ms, {u['correctness']} |")
    s = m["s2_scaling"]
    rows.append(f"Δlatency ≈ {s['latency_fit']['slope_ns_per_wft'] / 1e3:.1f} µs × WFTs + "
                f"{s['latency_fit']['intercept_ns'] / 1e3:.0f} µs")
    rows.append(f"ΔCPU ≈ {s['cpu_fit']['slope_ns_per_wft'] / 1e3:.1f} µs × WFTs + "
                f"{s['cpu_fit']['intercept_ns'] / 1e3:.0f} µs")
    rows.append(f"log-log slope {s['loglog_slope_wft_ge_20']:.2f}")
    rows.append("marginal cost " + ", ".join(
        f"{x['from_wft']:,}→{x['to_wft']:,}: {x['ns_per_wft'] / 1e3:.0f}" for x in s["marginal_latency_ns_per_wft"])
        + " µs/WFT")
    rows.append(f"{m['timed_rows']:,} timed rows ({m['timed_rows_excluding_warmup']:,} measurements, "
                f"{m['timed_rows'] - m['timed_rows_excluding_warmup']:,} warm-up) and {m['verify_rows']} "
                f"correctness rows")
    return rows


# --- main ------------------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dir", type=Path, default=Path(__file__).resolve().parent, help="package directory")
    ap.add_argument("--print-readme-rows", action="store_true", help=argparse.SUPPRESS)
    args = ap.parse_args()
    d = args.dir
    results = json.loads((d / "results.json").read_text())
    if args.print_readme_rows:
        print("\n".join(readme_rows(results)))
        return

    # 1. Top-level checksums.
    listed = dict(reversed(line.split("  ", 1)) for line in (d / "SHA256SUMS").read_text().splitlines())
    check(sorted(listed) == sorted(TOP_FILES), "SHA256SUMS must list exactly the package files")
    for name in TOP_FILES:
        check(sha((d / name).read_bytes()) == listed[name], f"{name} differs from SHA256SUMS")

    # 2. Archives: safety, internal SHA256SUMS, and agreement with results.json.
    ev = read_archive(d / "evidence.tar.gz", "evidence")
    rp = read_archive(d / "reproducer.tar.gz", "reproducer")
    for name, members in [("evidence.tar.gz", ev), ("reproducer.tar.gz", rp)]:
        rec = results["archives"][name]
        check(sha((d / name).read_bytes()) == rec["sha256"], f"{name} differs from results.json")
        check(rec["members_sha256"] == {n: sha(x) for n, x in members.items()}, f"{name} members differ from results.json")
    required = ["evidence/raw/timed.jsonl", "evidence/raw/verify.jsonl", "evidence/raw/run_all.log",
                "evidence/provenance/MANIFEST.original.txt", "evidence/identity/environment.txt",
                "reproducer/SETUP.md", "reproducer/bench/perf_bench_1766.rs", "reproducer/scripts/summarize.py",
                "reproducer/scripts/run_matrix.sh", "reproducer/scripts/build.sh", "reproducer/scripts/check_identity.sh"]
    required += [f"evidence/raw/build_{x}.{e}" for x in "UAB" for e in ("log", "json")]
    required += [f"evidence/identity/identity_{x}.txt" for x in "UAB"]
    for name in required:
        check(name in ev or name in rp, f"missing {name}")
    for name, data in rp.items():
        if name.endswith((".sh", ".py", ".md")):
            check(b"/home/" not in data, f"{name} contains a machine-specific absolute path")

    # 3. Original investigation manifest: every distributed file still matches it.
    mapping = [("raw/", "evidence/raw/"), ("evidence/", "evidence/identity/"),
               ("scripts/", "evidence/provenance/scripts_as_executed/"), ("bench/", "reproducer/bench/"),
               ("REPORT_performance.md", "evidence/provenance/REPORT_performance.zh-TW.md")]
    builds = results["build"]["per_version"]
    matched = 0
    for line in ev["evidence/provenance/MANIFEST.original.txt"].decode().splitlines()[2:]:
        h, path = line.split("  ", 1)
        if path.startswith("bins/"):
            check(builds[path.split("/")[1]]["binary_sha256"] == h, f"binary hash mismatch for {path}")
            continue
        target = next(dst + path[len(src):] for src, dst in mapping if path.startswith(src))
        check(sha({**ev, **rp}[target]) == h, f"{target} differs from the original manifest")
        matched += 1

    # 4. Source, harness and build identity.
    src = results["sources"]
    check(src["commits"] == COMMITS, "commit identities")
    check(results["harness"]["sha256"] == BENCH_SHA == sha(rp["reproducer/bench/perf_bench_1766.rs"]), "harness hash")
    for label in "UAB":
        text = ev[f"evidence/identity/identity_{label}.txt"].decode()
        ident = src["tree_identity"][label]
        check(f"commit {COMMITS[label]}" in text and ident["commit"] == COMMITS[label], f"{label} identity commit")
        check(f"{PROD_DIFF[label]}  -" in text and ident["production_diff_sha256"] == PROD_DIFF[label], f"{label} production diff")
        check(f"{BENCH_SHA}  crates/sdk-core/src/core_tests/perf_bench_1766.rs" in text and ident["bench_module_sha256"] == BENCH_SHA, f"{label} harness")
        check(ident["files_differing_from_commit"] == ["crates/sdk-core/src/core_tests/mod.rs"], f"{label} changed files")
        extra = {"crates/sdk-core/src/core_tests/perf_bench_1766.rs"}
        if label == "U":
            extra |= {"crates/sdk-core/src/core_tests/legacy_query_replay.rs", "changelog/fixed/eavesdropping-crystal-ball.md"}
        check(set(ident["files_not_in_commit"]) == extra, f"{label} extra files")
        build = builds[label]
        art = [json.loads(x) for x in ev[f"evidence/raw/build_{label}.json"].decode().splitlines()]
        art = [x for x in art if x.get("reason") == "compiler-artifact" and x["target"]["name"] == "temporalio_sdk_core" and x["profile"]["test"]]
        check(len(art) == 1 and art[0]["profile"] == build["profile"] and sorted(art[0]["features"]) == build["features"], f"{label} build profile")
        log = ev[f"evidence/raw/build_{label}.log"].decode()
        check("Finished `release` profile" in log and "cargo rc=0" in log and build["binary_sha256"] in log, f"{label} build log")
    check(builds["U"]["profile"] == builds["A"]["profile"] == builds["B"]["profile"], "profiles differ between versions")
    check(builds["U"]["features"] == builds["A"]["features"] == builds["B"]["features"], "features differ between versions")
    check(builds["B"]["profile"] == {"opt_level": "3", "debuginfo": 0, "debug_assertions": False, "overflow_checks": False, "test": True}, "release profile")
    check("disable_workflow_task_chunking" not in builds["B"]["features"], "chunking feature")

    # 5. Raw rows: schema, separation of correctness and timing, sample counts.
    timed = jsonl(ev["evidence/raw/timed.jsonl"])
    verify = jsonl(ev["evidence/raw/verify.jsonl"])
    for name, rows in [("timed.jsonl", timed), ("verify.jsonl", verify)]:
        rec = results["raw_files"][name]
        check(rec["rows"] == len(rows) and rec["sha256"] == sha(ev["evidence/raw/" + name]), f"{name} row count/hash")
        for r in rows:
            want = ROW_KEYS | (S4_KEYS if r["scenario"] == "s4" else set())
            check(set(r) == want, f"{name}: unexpected fields {set(r) ^ want}")
            check(r["label"] in COMMITS and isinstance(r["lat_ns"], int) and r["lat_ns"] > 0, f"{name}: bad row")
    check(all(not r["verify"] and r["replay"] == "skip" for r in timed), "timed rows must not include replay verification")
    check(all(r["verify"] and not r["warmup"] and r["replay"] in ("ok", "nde") for r in verify), "verify rows")
    counts = Counter((key(r), r["label"], r["warmup"]) for r in timed)
    expected = Counter()
    for (sc, lad), (cycles, rounds, samples, warm) in DESIGN.items():
        for cyc in cycles:
            for label in "UAB":
                expected[((sc, cyc, lad), label, False)] = rounds * samples
                expected[((sc, cyc, lad), label, True)] = rounds * warm
    check(counts == expected, "timed sample counts differ from the documented design")
    vcounts = Counter((key(r), r["label"]) for r in verify)
    check(vcounts == Counter({(k[0], k[1]): 3 for k in expected}), "verify sample counts")

    # 6. Correctness: counts per row and the U-only invalid classification.
    for r in timed + verify:
        exp = EXPECTED_EVICTIONS[r["label"]][r["scenario"]]
        check(r["win_evicts"] == exp, f"unexpected eviction count {r}")
        check(r["failures"] == 0 and r["query_responses"] == r["expected_queries"] and r["first_cmds_ok"], f"live run failed {r}")
        check(r["completions"] == (3 if r["scenario"] == "s4" else 2), f"completion count {r}")
        if r["scenario"] != "s4":
            check(r["progress_commits"] == 1, f"progress committed {r['progress_commits']} times")
            check(r["win_fetches"] == r["win_evicts"], "history fetches must equal evictions in s0-s3")
            check(r["win_replay_acts"] == (r["wft_count"] - 1 if r["win_evicts"] else 0), "replay activation count")
            check(r["win_acts"] == (r["wft_count"] + 1 if r["win_evicts"] else (0 if r["scenario"] == "s2" else 1)), "activation count")
        else:
            check(r["la_executions"] == (1 if r["label"] == "U" else 2) and r["win_la_executions"] == (0 if r["label"] == "U" else 1), "LA executions")
            check(r["markers"] == 1 and r["win_fetches"] == 0, "s4 marker/fetch")
    for r in verify:
        check((r["replay"] == "nde") == ((r["label"], r["scenario"]) in INVALID), f"cold replay verdict {r['label']} {r['scenario']} {r['replay']}")
    check({(x["label"], x["scenario"]) for x in results["invalid_baselines"]} == INVALID, "invalid baselines")

    # 7. Recompute every statistic in results.json.
    m = results["measurements"]
    check(m["timed_rows"] == len(timed) and m["verify_rows"] == len(verify), "row totals")
    check(m["timed_rows_excluding_warmup"] == sum(not r["warmup"] for r in timed), "measurement rows")
    groups = defaultdict(lambda: defaultdict(list))
    for r in timed:
        if not r["warmup"]:
            groups[key(r)][r["label"]].append(r)
    cells = {(c["scenario"], c["cycles"], c["la_delay_ms"], c["label"]): c for c in m["timed_cells"]}
    check(len(cells) == sum(len(v) for v in groups.values()), "cell count")
    comps = {(c["scenario"], c["cycles"], c["la_delay_ms"], c["label"]): c for c in m["comparisons"]}
    for k, by_label in groups.items():
        for label, rs in by_label.items():
            c = cells[k + (label,)]
            lat, cpu = [r["lat_ns"] for r in rs], [r["cpu_ns"] for r in rs]
            ql = [r["q_lat_ns"] for r in rs if r["q_lat_ns"] >= 0]
            want = {
                "n": len(rs), "hist_events": sorted({r["hist_events"] for r in rs}),
                "hist_bytes": span([r["hist_bytes"] for r in rs]), "wft_count": sorted({r["wft_count"] for r in rs}),
                "latency_ns": {"median": st.median(lat), "mean": st.mean(lat), "p95": pct(lat, 95), "p99": pct(lat, 99), "min": min(lat), "max": max(lat)},
                "cpu_ns": {"median": st.median(cpu), "p95": pct(cpu, 95)},
                "query_latency_ns_median": st.median(ql) if ql else None,
                "e2e_ns_median": st.median([r["e2e_ns"] for r in rs]),
                "window": {"evictions": dist(rs, "win_evicts"), "activations": dist(rs, "win_acts"),
                           "replay_activations": dist(rs, "win_replay_acts"), "history_fetches": dist(rs, "win_fetches"),
                           "history_fetch_bytes": span([r["win_fetch_bytes"] for r in rs]), "la_executions": dist(rs, "win_la_executions")},
                "la_executions_total": dist(rs, "la_executions"),
                "wft_failures_total": 0,
                "correctness": "correctness_invalid" if (label, k[0]) in INVALID else "valid",
            }
            for f, v in want.items():
                close(c[f], v, f"{k}/{label}/{f}")
        a = by_label["A"]
        for label in ("B", "U"):
            b = by_label[label]
            c = comps[k + (label,)]
            la, lb = [r["lat_ns"] for r in a], [r["lat_ns"] for r in b]
            close(c["median_delta_ns"], st.median(lb) - st.median(la), f"{k}/{label} delta")
            close(c["median_ratio"], st.median(lb) / st.median(la), f"{k}/{label} ratio")
            close(c["median_delta_ci95_ns"], boot_ci(la, lb), f"{k}/{label} CI")
            close(c["mwu_p"], mwu_p(la, lb), f"{k}/{label} MWU")
            close(c["cpu_median_delta_ns"], st.median([r["cpu_ns"] for r in b]) - st.median([r["cpu_ns"] for r in a]), f"{k}/{label} cpu")
            check(c["comparison_valid"] == ((label, k[0]) not in INVALID), f"{k}/{label} validity")

    # 8. S2 incremental cost and scaling fit.
    s = m["s2_scaling"]
    pts = []
    for cyc in DESIGN[("s2", 0)][0]:
        a, b = groups[("s2", cyc, 0)]["A"], groups[("s2", cyc, 0)]["B"]
        pts.append((a[0]["wft_count"], st.median([r["lat_ns"] for r in b]) - st.median([r["lat_ns"] for r in a]),
                    st.median([r["cpu_ns"] for r in b]) - st.median([r["cpu_ns"] for r in a])))
    close([[p["wft_count"], p["latency_delta_ns"], p["cpu_delta_ns"]] for p in s["points"]], [list(p) for p in pts], "S2 points")
    close(s["latency_fit"], ols([p[0] for p in pts], [p[1] for p in pts]), "latency fit")
    close(s["cpu_fit"], ols([p[0] for p in pts], [p[2] for p in pts]), "cpu fit")
    big = [p for p in pts if p[0] >= 20]
    close(s["loglog_slope_wft_ge_20"], ols([math.log(p[0]) for p in big], [math.log(p[1]) for p in big])["slope_ns_per_wft"], "log-log slope")

    # 9. README numbers.
    readme = (d / "README.md").read_text()
    rows = readme_rows(results)
    for row in rows:
        check(row in readme, f"README.md lacks or misstates: {row}")
    check(COMMITS["B"] in readme and PROD_DIFF["B"][:16] in readme and BENCH_SHA in readme, "README identities")

    print(f"PASS: SHA256SUMS and both archives ({len(ev)} evidence and {len(rp)} reproducer members, "
          f"no unsafe paths); {matched} files match the original investigation manifest; three binary hashes "
          "match build logs.")
    print("PASS: commits, production-diff hashes, identical harness, identical release profile and features for U/A/B.")
    print(f"PASS: {len(timed)} timed rows ({sum(not r['warmup'] for r in timed)} measurements) and {len(verify)} "
          "correctness rows match the documented design; timing and replay verification are separate.")
    print("PASS: per-row eviction, activation, replay, fetch and LA counts; U invalid exactly in s3sig/s3upd/s4.")
    print(f"PASS: recomputed {len(cells)} cells, {len(comps)} comparisons (medians, percentiles, bootstrap CIs, "
          f"MWU), the S2 scaling fit, and {len(rows)} README rows.")
    print("Offline verification only: this checks integrity and internal consistency of the saved evidence. "
          "It did not build the SDK or rerun the benchmark.")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # noqa: BLE001
        print(f"FAIL: {error}", file=sys.stderr)
        raise SystemExit(1)
