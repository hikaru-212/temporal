#!/usr/bin/env python3
"""Build both pinned revisions and run a fresh campaign in a NEW directory (Linux)."""
import argparse
import csv
import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import time

sys.dont_write_bytecode = True
from analyze import SOURCES, SCENARIOS, CYCLES, schedule, read_csv, validate

ROOT = Path(__file__).resolve().parent
PUBLIC_REPO = 'https://github.com/hikaru-212/temporalio-sdk-rust.git'
TEST = 'core_tests::final_benchmark::final_benchmark'
BUILD = ['cargo', 'test', '-p', 'temporalio-sdk-core', '--lib', '--release',
         '--locked', '--offline', '--no-run', '-j', '3', '--message-format=json-render-diagnostics']
DECL = b'\nmod final_benchmark;\n'
MOD = 'crates/sdk-core/src/core_tests/mod.rs'
FEATURES = ['default', 'envconfig', 'ephemeral-server', 'otel', 'prometheus', 'tls-ring']


def run(cmd, **kwargs):
    print('+ ' + ' '.join(map(str, cmd)), flush=True)
    return subprocess.run(cmd, check=True, **kwargs)


def prepare(repo, sha, tree):
    archive = subprocess.check_output(['git', '-C', str(repo), 'archive', sha])
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(tree, filter='data')
    harness = tree / 'crates/sdk-core/src/core_tests/final_benchmark.rs'
    shutil.copyfile(ROOT / 'final_benchmark.rs', harness)
    with (tree / MOD).open('ab') as f:
        f.write(DECL)
    records = subprocess.check_output(['git', '-C', str(repo), 'ls-tree', '-rz', sha]).split(b'\0')
    for record in filter(None, records):
        meta, name = record.split(b'\t', 1)
        mode, kind, expected = meta.decode().split()
        assert kind == 'blob', 'Unexpected submodule; inspect source before proceeding'
        name = os.fsdecode(name)
        path = tree / name
        data = os.readlink(path).encode() if mode == '120000' else path.read_bytes()
        if name == MOD:
            assert data.endswith(DECL)
            data = data[:-len(DECL)]
        actual = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        assert actual == expected, f'Source identity mismatch: {name}'
    return hashlib.sha256(harness.read_bytes()).hexdigest()


def build(root, label, env):
    assert shutil.disk_usage(root).free >= 10 * 1024**3, 'Need 10 GiB free before building; no cleanup is performed'
    messages = root / 'logs' / f'build-{label}.jsonl'
    with messages.open('x') as output, (root / 'logs' / f'build-{label}.log').open('x') as log:
        print('+ ' + ' '.join(BUILD), flush=True)
        proc = subprocess.Popen(BUILD, cwd=root / 'tree', env=env, stdout=output, stderr=log)
        while proc.poll() is None:
            if shutil.disk_usage(root).free < 8 * 1024**3:
                proc.terminate()
                proc.wait()
                raise RuntimeError('Disk guard: below 8 GiB free; stopped without deleting files')
            time.sleep(2)
        assert proc.returncode == 0, f'{label} build failed; see {log.name}'
    artifacts = [r for r in map(json.loads, messages.read_text().splitlines())
                 if r.get('executable') and r.get('target', {}).get('name') == 'temporalio_sdk_core']
    assert len(artifacts) == 1, 'Expected one Core lib-test executable'
    artifact = artifacts[0]
    assert artifact['profile']['opt_level'] == '3' and not artifact['profile']['debug_assertions']
    assert sorted(artifact['features']) == FEATURES, 'Unexpected feature unification'
    dest = root / 'bins' / label
    shutil.copy2(artifact['executable'], dest)
    return dict(profile=artifact['profile'], features=artifact['features'],
                binary_sha256=hashlib.sha256(dest.read_bytes()).hexdigest())


def campaign(root, mode):
    rows = root / (mode + '.jsonl')
    assert not rows.exists(), 'Refusing to overwrite or append an existing campaign'
    plan = schedule(mode)
    (root / (mode + '-schedule.json')).write_text(json.dumps(plan, indent=2) + '\n')
    for i, (label, sc, cycles, rnd) in enumerate(plan):
        env = os.environ.copy()
        # Old benchmark variables must not alter timing or dump fixtures during measurement.
        for key in list(env):
            if key.startswith('B1766_'):
                del env[key]
        env.update(B1766_LABEL=label, B1766_SCENARIO=sc, B1766_CYCLES=str(cycles),
                   B1766_ROUND=str(rnd), B1766_SAMPLES='20' if mode == 'timing' else '1',
                   B1766_WARMUP='5' if mode == 'timing' else '0',
                   B1766_VERIFY='0' if mode == 'timing' else '1', B1766_OUT=str(rows))
        if mode == 'correctness':
            fixtures = root / 'fixtures' / label
            fixtures.mkdir(parents=True, exist_ok=True)
            env['B1766_FIXTURES'] = str(fixtures)
        cmd = ['taskset', '-c', '1-4', str(root / 'bins' / label), TEST,
               '--exact', '--nocapture', '--test-threads=1']
        start = datetime.datetime.now(datetime.timezone.utc).isoformat()
        print(f'{mode} {i + 1}/{len(plan)} {start} {label} {sc} {cycles} round={rnd}', flush=True)
        with (root / 'logs' / f'{mode}-{rnd}-{sc}-{cycles}-{label}.log').open('x') as log:
            log.write(f'{start}\nCOMMAND: {cmd!r}\nENV: ' + json.dumps({k: v for k, v in env.items() if k.startswith('B1766_')}) + '\n')
            log.flush()
            run(cmd, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=180)
    data = [json.loads(line) for line in rows.read_text().splitlines()]
    with rows.with_suffix('.csv').open('x', newline='') as f:
        writer = csv.DictWriter(f, sorted(set().union(*(r.keys() for r in data))))
        writer.writeheader()
        writer.writerows(data)
    print(f'COMPLETE {mode}: {len(data)} rows', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('work_dir', type=Path, help='New, non-existing directory; all generated artifacts stay here')
    parser.add_argument('--repo', type=Path, help='Optional existing local repository containing both SHAs (read-only)')
    parser.add_argument('--plan', action='store_true', help='Show the plan without creating files, building, or running tests')
    args = parser.parse_args()
    root = args.work_dir.expanduser().resolve()
    if args.plan:
        print(json.dumps(dict(work_dir=str(root), sources=SOURCES, build=BUILD,
                             cpu_affinity='1-4', correctness_processes=len(schedule('correctness')),
                             timing_processes=len(schedule('timing')), samples_per_process=20,
                             warmups_per_process=5), indent=2))
        return
    assert not root.exists(), 'Use a new directory; existing output is never overwritten'
    assert sys.version_info >= (3, 12), 'Use Python 3.12 or later'
    assert sys.platform == 'linux' and {1, 2, 3, 4} <= os.sched_getaffinity(0), 'CPUs 1–4 must be available'
    overrides = [k for k, v in os.environ.items() if v and (k.startswith('CARGO_PROFILE_') or
                 k in ('RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'CARGO_BUILD_RUSTFLAGS', 'CARGO_BUILD_TARGET', 'RUSTUP_TOOLCHAIN'))]
    assert not overrides, f'Remove build overrides first: {overrides}'
    assert shutil.disk_usage(root.parent).free >= 10 * 1024**3, 'Need 10 GiB free; no cleanup is performed'
    root.mkdir()
    for name in ('tree', 'bins', 'logs'):
        (root / name).mkdir()
    if args.repo:
        repo = args.repo.expanduser().resolve()
    else:
        repo = root / 'repository'
        run(['git', 'init', str(repo)])
        run(['git', '-C', str(repo), 'remote', 'add', 'origin', PUBLIC_REPO])
        for sha in SOURCES.values():
            run(['git', '-C', str(repo), 'fetch', '--depth=1', 'origin', sha])
    env = os.environ.copy()
    env['CARGO_TARGET_DIR'] = str(root / 'build-cache')
    identities = {}
    for label, sha in SOURCES.items():
        actual = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', f'{sha}^{{commit}}'], text=True).strip()
        assert actual == sha
        harness_hash = prepare(repo, sha, root / 'tree')
        run(['cargo', 'fetch', '--locked'], cwd=root / 'tree', env=env)
        identities[label] = dict(sha=sha, harness_sha256=harness_hash, **build(root, label, env))
    assert identities['V2']['profile'] == identities['V42']['profile']
    assert identities['V2']['features'] == identities['V42']['features']
    (root / 'identities.json').write_text(json.dumps(identities, indent=2) + '\n')
    with (root / 'logs' / 'environment.log').open('x') as f:
        for cmd in (['uname', '-a'], ['lscpu'], ['free', '-h'], ['df', '-h', str(root)],
                    ['rustc', '-Vv'], ['cargo', '-V']):
            run(cmd, cwd=root / 'tree', stdout=f, stderr=subprocess.STDOUT)
    campaign(root, 'correctness')
    expected = {f'{sc}-{c}.{suffix}' for sc in SCENARIOS for c in CYCLES for suffix in ('history.bin', 'tasks.json')}
    for label in SOURCES:
        assert {p.name for p in (root / 'fixtures' / label).iterdir()} == expected
    for name in expected:
        assert (root / 'fixtures' / 'V2' / name).read_bytes() == (root / 'fixtures' / 'V42' / name).read_bytes(), name
    campaign(root, 'timing')
    validate(read_csv(root / 'timing.csv'), read_csv(root / 'correctness.csv'))
    print(f'Complete. Analyze with: python3 {ROOT / "analyze.py"} --data-dir {root} --output {root / "RESULTS.md"}')


if __name__ == '__main__':
    main()
