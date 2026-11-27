#!/usr/bin/env python3
"""Validate the public CSVs and reproduce the report using Python's standard library."""
import argparse
from collections import defaultdict
import csv
import hashlib
import itertools
import math
from pathlib import Path
import random
import re
import statistics as st
import sys

ROOT = Path(__file__).resolve().parent
SOURCES = {
    'V2': '06cb4a97863c0aacda79555fbaabab87a4013cbf',
    'V42': '7f53cc61b066b7320cb384246ddfa7561139affb',
}
SCENARIOS = ['normal', 'non_effectful', 'trailing', 'cancel', 'held_la']
CYCLES = [18, 198, 998, 1998]
NAMES = dict(zip(SCENARIOS, ['Normal WFT', 'Non-effectful legacy query',
                           'Trailing inputs', 'Interleaved cancellation', 'Held LA result']))
METRICS = ['query_ms', 'next_wft_ms', 'combined_ms']
COUNTS = ['evictions', 'fetches', 'la_executions', 'la_reexecutions']
BOOLS = {'live_decision', 'verify', 'warmup', 'state_equivalent'}
INTS = {'cycles', 'round', 'sample', 'hist_events', 'hist_bytes', 'wft_count',
        'rebuild_replay_activations', 'cold_replay_activations', *COUNTS}


def read_csv(path):
    with path.open(newline='') as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        for key, value in row.items():
            if key in BOOLS:
                assert value in ('', 'True', 'False'), (key, value)
                row[key] = None if value == '' else value == 'True'
            elif key in INTS:
                row[key] = int(value)
            elif key.endswith('_ms'):
                row[key] = float(value) if value else None
    return rows


def schedule(mode):
    rng = random.Random(183542)
    result = []
    for rnd in range(6 if mode == 'timing' else 1):
        cells = list(itertools.product(SCENARIOS, CYCLES))
        rng.shuffle(cells)
        for sc, cycles in cells:
            labels = list(SOURCES)
            if (rnd + SCENARIOS.index(sc) + CYCLES.index(cycles)) % 2:
                labels.reverse()
            result.extend((label, sc, cycles, rnd) for label in labels)
    return result


def validate(timing, correctness):
    for mode, rows in [('timing', timing), ('correctness', correctness)]:
        samples = 25 if mode == 'timing' else 1
        expected = [(label, sc, cycles, rnd, i)
                    for label, sc, cycles, rnd in schedule(mode) for i in range(samples)]
        actual = [(r['label'], r['scenario'], r['cycles'], r['round'], r['sample']) for r in rows]
        assert actual == expected, f'{mode}: missing, duplicate, reordered, or extra rows'
        for r in rows:
            sc, label = r['scenario'], r['label']
            assert r['verify'] == (mode == 'correctness')
            assert r['warmup'] == (mode == 'timing' and r['sample'] < 5)
            evict = int(sc == 'cancel' or (label == 'V2' and sc in ('trailing', 'held_la')))
            assert r['evictions'] == r['fetches'] == evict
            assert r['la_executions'] == (1 + evict if sc == 'held_la' else 0)
            assert r['la_reexecutions'] == (evict if sc == 'held_la' else 0)
            assert r['hist_events'] == 5 * r['cycles'] + (14 if sc == 'cancel' else 13 if sc == 'held_la' else 8)
            assert r['wft_count'] == r['cycles'] + (3 if sc in ('cancel', 'held_la') else 2)
            answer = {'normal': '', 'cancel': 'acked=false cancelled=true',
                      'held_la': f"acked={'true' if label == 'V2' else 'false'} la_done=false",
                      'trailing': f"acked={'true' if label == 'V2' else 'false'} cancelled=false",
                      'non_effectful': 'acked=false cancelled=false'}[sc]
            assert r['query_answer'] == answer
            for metric in [*METRICS, 'next_poll_ms']:
                value = r[metric]
                if metric == 'query_ms' and sc == 'normal':
                    assert value is None
                else:
                    assert value is not None and math.isfinite(value) and value >= 0
            assert math.isclose(r['combined_ms'], (r['query_ms'] or 0) + r['next_wft_ms'], rel_tol=1e-10)
            if sc == 'held_la':
                assert isinstance(r['live_decision'], bool)
                if label == 'V42':
                    assert r['live_decision'] is True
                expected_commands = ('RECORD_MARKER+START_TIMER' if r['live_decision']
                                     else 'MODIFY_WORKFLOW_PROPERTIES+RECORD_MARKER')
            else:
                assert r['live_decision'] is None
                expected_commands = 'START_TIMER' if sc == 'cancel' else 'MODIFY_WORKFLOW_PROPERTIES'
            assert r['live_commands'] == expected_commands
            if mode == 'correctness':
                assert r['cold_replay'] == 'ok' and r['state_equivalent'] is True
    assert len(timing) == 6000 and sum(not r['warmup'] for r in timing) == 4800
    assert len(correctness) == 40


def p95(values):
    return sorted(values)[math.ceil(.95 * len(values)) - 1]


def analyze(timing):
    groups = defaultdict(list)
    for r in timing:
        if not r['warmup']:
            groups[r['scenario'], r['cycles'], r['label']].append(r)
    comparisons = {}
    for sc, cycles in itertools.product(SCENARIOS, CYCLES):
        for metric in METRICS:
            if sc == 'normal' and metric == 'query_ms':
                continue
            rounds = {label: [[r[metric] for r in groups[sc, cycles, label] if r['round'] == rnd]
                              for rnd in range(6)] for label in SOURCES}
            values = [[v for block in rounds[label] for v in block] for label in SOURCES]
            medians = list(map(st.median, values))
            # Whole paired rounds retain within-process dependence and version pairing.
            rng = random.Random(183542)
            ratios, deltas = [], []
            for _ in range(10000):
                indexes = [rng.randrange(6) for _ in range(6)]
                a, b = [st.median([v for j in indexes for v in rounds[label][j]]) for label in SOURCES]
                ratios.append(a / b)
                deltas.append(b - a)
            ratios.sort()
            deltas.sort()
            comparisons[sc, cycles, metric] = dict(
                a=medians[0], b=medians[1], ap=p95(values[0]), bp=p95(values[1]),
                speedup=medians[0] / medians[1], lo=ratios[250], hi=ratios[9749],
                delta=medians[1] - medians[0], dlo=deltas[250], dhi=deltas[9749])
    return groups, comparisons


def readme_block(groups, comparisons):
    lines = ['| Scenario | Events | V2 median / P95 (ms) | V4.2 median / P95 (ms) | Speedup (95% CI) | Evictions V2 / V4.2 |',
             '|---|---:|---:|---:|---:|---:|']
    for sc in SCENARIOS:
        r = comparisons[sc, 1998, 'combined_ms']
        a, b = [groups[sc, 1998, label][0] for label in SOURCES]
        name = NAMES[sc] + (' †' if sc in ('trailing', 'held_la') else '')
        lines.append(f"| {name} | {a['hist_events']:,} | {r['a']:.3f} / {r['ap']:.3f} | {r['b']:.3f} / {r['bp']:.3f} | {r['speedup']:.2f}× ({r['lo']:.4g}–{r['hi']:.4g}) | {a['evictions']} / {b['evictions']} |")
    t, h = [comparisons[sc, 1998, 'combined_ms'] for sc in ('trailing', 'held_la')]
    q, n = [comparisons['held_la', 1998, metric]['b'] for metric in ('query_ms', 'next_wft_ms')]
    lines += ['', f"**Trailing inputs:** {t['a']:.3f} ms → {t['b']:.3f} ms, **{t['speedup']:.2f}×**. **Held LA:** {h['a']:.3f} ms → {h['b']:.3f} ms, **{1 / h['speedup']:.2f}× slower**.", '',
              f"Held-LA query median is **{q:.3f} ms** in V4.2; its next-WFT median is only **{n:.3f} ms**. The combined interval includes the query wait."]
    return '\n'.join(lines)


def render_results(groups, comparisons, correctness):
    lines = ['# Full performance results', '',
             'All latencies are milliseconds. Each version/scenario/size has 120 measured samples across six process rounds; the 30 warm-ups remain in [timing.csv](timing.csv) but are excluded from statistics. No outliers or decision branches are removed.', '',
             'Speedup = V2 median / V4.2 median; below 1 means V4.2 is slower. Delta = V4.2 median − V2 median. Both intervals are paired-round 95% bootstrap intervals. Median/P95 values are pooled across rounds, not averages of round medians. Query and next-WFT medians need not sum to the directly measured combined median.', '',
             'Query: mock poll yields query → query response. Next WFT: query response → next WFT completion (including handoff). Combined: query yield → WFT completion. Normal WFT has no query (N/A), and combined equals next-WFT latency measured from its poll. Held LA ends at the completion carrying RecordMarker. The additional raw next_poll_ms field excludes the query-to-next-poll handoff.', '',
             '## History sizes', '',
             '| Timer cycles | Normal / non-effectful / trailing | Cancellation | Held LA |',
             '|---:|---:|---:|---:|']
    for c in CYCLES:
        lines.append(f"| {c:,} | {groups['normal', c, 'V2'][0]['hist_events']:,} | {groups['cancel', c, 'V2'][0]['hist_events']:,} | {groups['held_la', c, 'V2'][0]['hist_events']:,} |")
    lines += ['', 'Each timer cycle contributes five events and one WFT. Tails add 8 events / 2 WFTs for the first three scenarios, 14 / 3 for cancellation, and 13 / 3 for held LA. The full history populates the cache before timing; the next task is delivered as sticky incremental history, requiring a mock full-history fetch on eviction.', '']
    for sc in SCENARIOS:
        lines += [f'## {NAMES[sc]}', '',
                  '| Events | Metric | V2 median | V2 P95 | V4.2 median | V4.2 P95 | Speedup | 95% CI | Delta | Delta 95% CI |',
                  '|---:|---|---:|---:|---:|---:|---:|---|---:|---|']
        for c, m in itertools.product(CYCLES, METRICS):
            if (sc, c, m) not in comparisons:
                continue
            r = comparisons[sc, c, m]
            events = groups[sc, c, 'V2'][0]['hist_events']
            lines.append(f"| {events:,} | {m} | {r['a']:.4f} | {r['ap']:.4f} | {r['b']:.4f} | {r['bp']:.4f} | {r['speedup']:.4f}× | {r['lo']:.4f}–{r['hi']:.4f}× | {r['delta']:+.4f} | {r['dlo']:+.4f}–{r['dhi']:+.4f} |")
        lines += ['']
    lines += ['## Operation counts', '',
              'Totals across the 120 measured samples per version/cell. LA executions include the initial setup execution; re-executions count only extra executions after eviction. Shutdown evictions and warm-ups are excluded.', '',
              '| Scenario | Events | Evictions V2 / V4.2 | History fetches V2 / V4.2 | LA executions V2 / V4.2 | LA re-executions V2 / V4.2 |',
              '|---|---:|---:|---:|---:|---:|']
    for sc, c in itertools.product(SCENARIOS, CYCLES):
        g = [groups[sc, c, label] for label in SOURCES]
        counts = [' / '.join(str(sum(r[k] for r in rows)) for rows in g) for k in COUNTS]
        lines.append(f"| {NAMES[sc]} | {g[0][0]['hist_events']:,} | " + ' | '.join(counts) + ' |')
    lines += ['', '## Round medians', '',
              'Combined milliseconds, V2 / V4.2 in each cell. Rounds are zero-indexed in the CSV. For per-round median/P95 of every timing metric, including next_poll_ms, run `python3 analyze.py --rounds` (CSV on stdout).', '',
              '| Scenario | Events | Round 0 | Round 1 | Round 2 | Round 3 | Round 4 | Round 5 |',
              '|---|---:|---:|---:|---:|---:|---:|---:|']
    for sc, c in itertools.product(SCENARIOS, CYCLES):
        cells = [' / '.join(f"{st.median(r['combined_ms'] for r in groups[sc, c, label] if r['round'] == rnd):.4f}" for label in SOURCES) for rnd in range(6)]
        lines.append(f"| {NAMES[sc]} | {groups[sc, c, 'V2'][0]['hist_events']:,} | " + ' | '.join(cells) + ' |')
    lines += ['', '## Independent correctness and Held-LA decisions', '',
              f"[correctness.csv](correctness.csv) contains {len(correctness)}/{len(correctness)} passing independent version/scenario/size cases: each live completion is committed to history and replayed on a fresh worker, with nondeterminism and final state/decision checks. Timing samples do not run cold replay. Performance is not the correctness oracle.", '',
              '| Held-LA events | Measured V2 true / false | Measured V4.2 true / false | Independent V2 decision | Independent V4.2 decision |',
              '|---:|---:|---:|---|---|']
    for c in CYCLES:
        counts = [' / '.join(str(sum(r['live_decision'] is value for r in groups['held_la', c, label])) for value in (True, False)) for label in SOURCES]
        decisions = [str(next(r['live_decision'] for r in correctness if (r['scenario'], r['cycles'], r['label']) == ('held_la', c, label))).lower() for label in SOURCES]
        lines.append(f"| {groups['held_la', c, 'V2'][0]['hist_events']:,} | " + ' | '.join(counts + decisions) + ' |')
    lines += ['', 'True means the LA result was visible when the acknowledgement decision ran (timer branch); false selects the memo branch. V2 re-execution introduces a new timing choice, so either branch may be committed. Each independent case matches its own cold replay; cross-version decision equality is not claimed. All timing branches are retained.', '',
              '## Statistical and semantic limits', '',
              'P95 is nearest rank: sorted[ceil(0.95 × n) − 1]. Bootstrap resampling draws six whole round indices with replacement, uses those same indices for both versions, and recomputes pooled medians. There are 10,000 resamples, random.Random(183542) reset for each cell/metric; the sorted endpoints are indices 250 and 9749 (zero-based), matching the original analysis. No multiple-comparison adjustment is applied. Six independent process rounds limit precision; only about six observations per cell lie above P95.', '',
              'Trailing inputs and held LA have different query visibility: V2 observes the trailing acknowledgement; V4.2 defers it to the next WFT. Cancellation exposes its resolution and forces eviction in both versions. Cache retention is not a general latency improvement: the held-LA query waits for the heartbeat despite avoiding a rebuild.', '',
              'This mock server adds no network, storage, serialization, or server-queue delay to history fetches. The synthetic Rust language driver is not a production language SDK. Initial cache replay, fixture construction, shutdown, compilation and correctness replay are outside timing. LA bodies return immediately, so real LA service time and side effects are not represented. These intervals describe this shared-VM campaign, not production-wide latency or throughput.', '']
    return '\n'.join(lines)


def verify_package():
    entries = {}
    for line in (ROOT / 'SHA256SUMS').read_text().splitlines():
        digest, name = line.split('  ', 1)
        assert Path(name).name == name and name not in entries
        assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest, name
        entries[name] = digest
    assert set(entries) == {'README.md', 'RESULTS.md', 'timing.csv', 'correctness.csv',
                            'final_benchmark.rs', 'analyze.py', 'reproduce.py'}
    assert {p.name for p in ROOT.iterdir()} == set(entries) | {'SHA256SUMS'}
    for doc in ('README.md', 'RESULTS.md'):
        for target in re.findall(r'\]\(([^)]+)\)', (ROOT / doc).read_text()):
            if '://' in target:
                continue
            assert '#' not in target and (ROOT / target).is_file(), (doc, target)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', type=Path, default=ROOT, help='Directory containing both CSVs')
    parser.add_argument('--output', type=Path, help='Write recalculated RESULTS to this path; must not exist')
    parser.add_argument('--check', action='store_true', help='Verify packaged checksums, links, CSVs and report statistics')
    parser.add_argument('--rounds', action='store_true', help='Print per-round metric summaries as CSV')
    args = parser.parse_args()
    assert sum([args.check, args.rounds, args.output is not None]) <= 1, 'Choose one output mode'
    if args.check:
        assert args.data_dir.resolve() == ROOT, '--check validates only this package'
        verify_package()
    timing = read_csv(args.data_dir / 'timing.csv')
    correctness = read_csv(args.data_dir / 'correctness.csv')
    validate(timing, correctness)
    if args.rounds:
        writer = csv.writer(sys.stdout)
        writer.writerow(['scenario', 'cycles', 'hist_events', 'label', 'round', 'metric', 'n', 'median', 'p95'])
        for sc, c, label, rnd, metric in itertools.product(SCENARIOS, CYCLES, SOURCES, range(6), [*METRICS, 'next_poll_ms']):
            g = [r for r in timing if not r['warmup'] and (r['scenario'], r['cycles'], r['label'], r['round']) == (sc, c, label, rnd)]
            values = [r[metric] for r in g if r[metric] is not None]
            writer.writerow([sc, c, g[0]['hist_events'], label, rnd, metric, len(values), st.median(values) if values else '', p95(values) if values else ''])
        return
    groups, comparisons = analyze(timing)
    report = render_results(groups, comparisons, correctness)
    if args.check:
        assert (ROOT / 'RESULTS.md').read_text() == report, 'RESULTS.md differs from CSV calculations'
        readme = (ROOT / 'README.md').read_text()
        actual = readme.split('<!-- BEGIN CSV RESULTS -->\n', 1)[1].split('\n<!-- END CSV RESULTS -->', 1)[0]
        assert actual == readme_block(groups, comparisons), 'README numeric results differ from CSV calculations'
        assert all(sha in readme for sha in SOURCES.values())
        assert {r['live_decision'] for r in correctness if r['label'] == 'V2' and r['scenario'] == 'held_la'} == {True, False}
        print('PASS: seven file checksums, all relative links, 6000 timing rows (4800 measured + 1200 warm-ups), 240 scheduled processes, 40 independent replay cases, README statistics and every RESULTS table including paired bootstrap intervals.')
        print('Integrity and internal consistency only; this does not independently prove execution or validate the preliminary timeout observation.')
    elif args.output:
        with args.output.open('x') as f:
            f.write(report)
        print(args.output)
    else:
        print(report, end='')


if __name__ == '__main__':
    main()
