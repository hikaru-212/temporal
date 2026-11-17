#!/usr/bin/env python3
"""Check saved evidence using only the standard library; never runs Temporal."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile


FAILURE = 'TMPRL1100 Update machine does not handle this event: HistoryEvent(id: 10, WorkflowPropertiesModified)'


def check(condition, message):
    if not condition:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def replay_log(data):
    records = [json.loads(s) for s in data.decode().splitlines() if s.startswith('{')]
    rows = [r for r in records if 'file' in r]
    summaries = [r for r in records if 'replayer_temporalio' in r]
    check(len(summaries) == 1, 'missing or duplicate replay summary')
    check(len({r['file'] for r in rows}) == len(rows), 'duplicate replay case')
    return {r['file']: r for r in rows}, summaries[0]


def run_log(data):
    lines = data.decode().splitlines()
    trace = [json.loads(s[5:]) for s in lines if s.startswith('[h6] {')]
    results = [json.loads(s[12:]) for s in lines if s.startswith('[h6] RESULT ')]
    check(len(results) == 1, 'missing or duplicate E2E result')
    return {'trace': trace, 'result': results[0]}


def history_facts(data):
    ev = json.loads(data)['events']
    ids = [int(e['eventId']) for e in ev]
    check(ids == list(range(1, len(ev) + 1)), 'event IDs must be contiguous')
    by_id = dict(zip(ids, ev))
    names = {i: e['eventType'].removeprefix('EVENT_TYPE_') for i, e in by_id.items()}
    groups, current = {}, None
    versions = set()
    for i, e in by_id.items():
        name = names[i]
        if name == 'WORKFLOW_TASK_SCHEDULED':
            current = None
        elif name == 'WORKFLOW_TASK_STARTED':
            a = e['workflowTaskStartedEventAttributes']
            check(names[int(a['scheduledEventId'])] == 'WORKFLOW_TASK_SCHEDULED', 'WFT start association')
        elif name == 'WORKFLOW_TASK_COMPLETED':
            a = e['workflowTaskCompletedEventAttributes']
            sid, tid = int(a['scheduledEventId']), int(a['startedEventId'])
            check(sid < tid < i and names[sid] == 'WORKFLOW_TASK_SCHEDULED' and names[tid] == 'WORKFLOW_TASK_STARTED', 'WFT completion association')
            check(int(by_id[tid]['workflowTaskStartedEventAttributes']['scheduledEventId']) == sid, 'WFT schedule/start mismatch')
            version = a.get('sdkMetadata', {}).get('sdkVersion')
            if version:
                versions.add(version)
            current = i
            groups[i] = []
        elif current is not None:
            groups[current].append(i)
    memo = [i for i in ids if names[i] == 'WORKFLOW_PROPERTIES_MODIFIED']
    accepted = [i for i in ids if names[i] == 'WORKFLOW_EXECUTION_UPDATE_ACCEPTED']
    completed = [i for i in ids if names[i] == 'WORKFLOW_EXECUTION_UPDATE_COMPLETED']
    check(len(memo) == len(accepted) == len(completed) == 1, 'expected one memo and one Update')
    m, a, c = memo[0], accepted[0], completed[0]
    mw = int(by_id[m]['workflowPropertiesModifiedEventAttributes']['workflowTaskCompletedEventId'])
    uw = next(w for w, group in groups.items() if a in group)
    check(m in groups[mw] and c in groups[uw], 'memo/Update WFT association')
    aa = by_id[a]['workflowExecutionUpdateAcceptedEventAttributes']
    ca = by_id[c]['workflowExecutionUpdateCompletedEventAttributes']
    seq = int(aa['acceptedRequestSequencingEventId'])
    check(seq == int(by_id[uw]['workflowTaskCompletedEventAttributes']['scheduledEventId']), 'Update sequencing event is not its scheduled WFT')
    check(int(ca['acceptedEventId']) == a and a < c, 'Update completion acceptance link')
    check(ca['meta']['updateId'] == aa['acceptedRequest']['meta']['updateId'] == aa['protocolInstanceId'], 'Update identity mismatch')
    signal = by_id[5]['signalExternalWorkflowExecutionInitiatedEventAttributes']
    ack = by_id[6]['externalWorkflowExecutionSignaledEventAttributes']
    check(signal['workflowTaskCompletedEventId'] == '4' and ack['initiatedEventId'] == '5', 'external signal linkage')
    check(signal['workflowExecution'] == ack['workflowExecution'], 'external signal target mismatch')
    detail = []
    for i in groups[uw]:
        name = names[i]
        if i == m:
            name += f'(wftCompleted={mw})'
        if i == a:
            name += f'(seq={seq})'
        detail.append(f'{i}:{name}')
    order = 'memo-first-same-wft' if mw == uw and m < a else ('update-first-same-wft' if mw == uw else 'memo-in-earlier-wft')
    check(len(versions) == 1, 'producer SDK metadata missing or inconsistent')
    return {'event_count': len(ev), 'producer_sdk': versions.pop(),
            'workflow_id': ev[0]['workflowExecutionStartedEventAttributes']['workflowId'],
            'update_wft_completed': uw, 'memo_wft_completed': mw,
            'accepted_request_sequencing_event_id': seq,
            'recorded_order': order, 'committed': detail,
            'event_sequence': [[i, names[i]] for i in ids]}


def verify_history(data, case, trace):
    facts = history_facts(data)
    for key, value in facts.items():
        if key != 'event_sequence':
            check(value == case[key], f"{case['case_id']}: {key} differs from history")
    final = next(t for t in trace['trace'] if t['kind'] == 'final_history')
    check(final['events'] == facts['event_sequence'], 'trace final_history differs from history')
    return facts


def verify_trace(trace, case):
    records, result = trace['trace'], trace['result']
    check([t['seq'] for t in records] == list(range(len(records))), 'trace sequence gap')
    start = records[0]
    for key, value in case['conditions'].items():
        check(start[key] == value, f"{case['case_id']}: condition {key}")
    check(start['main_id'] == result['main_id'] == case['workflow_id'], 'workflow ID mismatch')
    check(start['temporalio'] == case['producer_sdk'], 'producer SDK mismatch')
    failure = result['replay_failure']
    expected_failure = case['recorded_order'] == 'memo-first-same-wft'
    check(bool(failure) == expected_failure, 'E2E recorded replay verdict mismatch')
    check(not failure or all(s in failure for s in ['TMPRL1100', 'Update machine does not handle this event', 'HistoryEvent(id: 10, WorkflowPropertiesModified)']), 'E2E failure signature mismatch')
    check(next(t['failure'] for t in records if t['kind'] == 'cold_replay') == failure, 'cold replay trace/result mismatch')
    check(result['update_result'] == 7, 'live Update did not return 7')
    check(result['case'] == case['conditions']['case'], 'case label mismatch')
    check(next(t['commits'] for t in records if t['kind'] == 'committed_wfts') == result['commits'], 'commit trace/result mismatch')
    final = next(t['events'] for t in records if t['kind'] == 'final_history')
    check(len(final) == case['event_count'], 'run-log event count mismatch')
    memo_commit = next(c['wft_completed'] for c in result['commits'] if any(e[1] == 'WORKFLOW_PROPERTIES_MODIFIED' for e in c['events']))
    check(memo_commit == case['memo_wft_completed'], 'run-log memo WFT mismatch')
    commit = next(c for c in result['commits'] if c['wft_completed'] == case['update_wft_completed'])
    detail = []
    for i, name, link in commit['events']:
        if name == 'WORKFLOW_PROPERTIES_MODIFIED':
            name += f'(wftCompleted={link})'
        if name == 'WORKFLOW_EXECUTION_UPDATE_ACCEPTED':
            name += f'({link})'
        detail.append(f'{i}:{name}')
    check(detail == case['committed'], 'E2E commit/order mismatch')
    if expected_failure:
        commands = [t for t in records if t['kind'] == 'completion']
        memo = next(t['seq'] for t in commands if t['commands'] == ['modify_workflow_properties'])
        query = next(t['seq'] for t in records if t['kind'] == 'activation' and t['query_ids'] == ['legacy_query'])
        update = next(t['seq'] for t in commands if t['commands'] == ['update_response.accepted', 'update_response.completed'])
        check(memo < query < update, 'memo/query/Update trace order mismatch')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--archive', type=Path, help='optional private source archive: check all 137 original files and 42 histories')
    args = ap.parse_args()
    root = Path(__file__).resolve().parent
    results = json.loads((root / 'results.json').read_text())
    payload = {}
    for filename, info in results['archives'].items():
        path = root / filename
        check(digest(path.read_bytes()) == info['sha256'], f'{filename}: checksum mismatch')
        actual = {}
        with tarfile.open(path, 'r:gz') as tar:
            for member in tar:
                p = PurePosixPath(member.name)
                check(member.isfile() and not p.is_absolute() and '..' not in p.parts, 'unsafe tar member')
                check(member.name not in payload, 'duplicate tar member')
                data = tar.extractfile(member).read()
                actual[member.name] = digest(data)
                payload[member.name] = data
        check(actual == info['members_sha256'], f'{filename}: member checksum/set mismatch')
    inventory = json.loads(payload['evidence/source-inventory.json'])
    original_manifest = payload['evidence/MANIFEST_7B.txt'].decode()
    original_hashes = dict((p, h) for h, p in re.findall(r'^  ([a-f0-9]{64})  (.+)$', original_manifest, re.M))
    check(len(original_hashes) == 46, 'original manifest entry count')
    for p, h in original_hashes.items():
        check(inventory[p]['sha256'] == h, f'original manifest differs: {p}')
    for name, data in payload.items():
        relative = name.split('/', 1)[1]
        if relative in inventory:
            check(digest(data) == inventory[relative]['sha256'], f'original artifact changed: {relative}')
    check(results['summary'] == {'exported_cases': 42, 'memo_first_failures_per_replayer': 16, 'recorded_update_first_passes_per_replayer': 26, 'strict_update_first_same_wft_passes': 23, 'memo_in_earlier_wft_passes': 3, 'incomplete_attempts_excluded': 3}, 'matrix summary differs')
    check(set(results['replay_logs']) == set(results['packaging_replay_logs']) == {'1.34.0', '1.36.1'}, 'replayer version set differs')
    incomplete = results['incomplete_attempts']
    check(len(incomplete) == 3, 'incomplete attempt count')
    for attempt in incomplete:
        check(attempt['source_sha256'] == inventory[attempt['source_file']]['sha256'], 'incomplete log checksum mismatch')
        check(attempt['status'] == 'INCOMPLETE_NO_EXPORTED_HISTORY' and attempt['replay_verdict'] is None, 'incomplete attempt given a verdict')
    cases = results['cases']
    check(len(cases) == 42 and len({c['case_id'] for c in cases}) == 42, 'case matrix count/duplicates')
    orders = Counter(c['recorded_order'] for c in cases)
    check(orders == {'memo-first-same-wft': 16, 'update-first-same-wft': 23, 'memo-in-earlier-wft': 3}, 'matrix totals')
    replay = {}
    for version, filename in results['replay_logs'].items():
        rows, summary = replay_log(payload['evidence/' + filename])
        check(summary == {'replayer_temporalio': version, 'histories': 42, 'memo_first': 16, 'memo_first_TMPRL1100': 16, 'update_first': 26, 'update_first_PASS': 26}, 'replay summary differs')
        check(set(rows) == {Path(c['history_file']).name for c in cases}, 'replay case set differs')
        replay[version] = rows
    representatives = []
    for case in cases:
        for kind in ['history', 'trace', 'run_log']:
            p, h = case[kind + '_file'], case[kind + '_sha256']
            check(inventory[p]['sha256'] == h, f'{p}: matrix/inventory checksum mismatch')
        check(original_hashes[case['history_file']] == case['history_sha256'], 'history not in original manifest')
        trace = run_log(payload['evidence/' + case['run_log_file']])
        verify_trace(trace, case)
        for version, rows in replay.items():
            r = rows[Path(case['history_file']).name]
            check(r['sha256'] == case['history_sha256'][:16], 'recorded replay hash mismatch')
            check(r['events'] == case['event_count'] and r['update_wft_completed'] == case['update_wft_completed'] and r['committed'] == case['committed'], 'replay event/order mismatch')
            mf = case['recorded_order'] == 'memo-first-same-wft'
            check(r['memo_first'] == mf and r['replay'] == (FAILURE if mf else 'PASS'), 'recorded replay verdict mismatch')
            check(case['replays'][version] == {'verdict': 'FAIL' if mf else 'PASS', 'recorded_result': r['replay']}, 'matrix replay mismatch')
        if case['representative_role']:
            representatives.append(case)
            data = payload['evidence/' + case['history_file']]
            saved_trace = json.loads(payload['evidence/' + case['trace_file']])
            check(saved_trace == trace, 'saved trace differs from original run log')
            verify_history(data, case, trace)
    check(len(representatives) == 4, 'representative count')
    for version, filename in results['packaging_replay_logs'].items():
        rows, summary = replay_log(payload['evidence/recheck/' + filename])
        check(summary == {'replayer_temporalio': version, 'histories': 4, 'memo_first': 2, 'memo_first_TMPRL1100': 2, 'update_first': 2, 'update_first_PASS': 2}, 'packaging replay summary differs')
        check(rows == {Path(c['history_file']).name: replay[version][Path(c['history_file']).name] for c in representatives}, 'packaging replay results differ')
    if args.archive:
        for p, info in inventory.items():
            data = (args.archive / p).read_bytes()
            check(len(data) == info['bytes'] and digest(data) == info['sha256'], f'private original changed: {p}')
        for case in cases:
            trace = json.loads((args.archive / case['trace_file']).read_text())
            check(trace == run_log((args.archive / case['run_log_file']).read_bytes()), 'private trace/log differ')
            verify_trace(trace, case)
            verify_history((args.archive / case['history_file']).read_bytes(), case, trace)
        print('PASS: private archive: 137 original checksums; 42 histories and traces verified.')
    print('PASS: both tar archives and all member checksums; 42-case recorded replay matrix; 4 original representative histories, event/WFT links and traces; 8 saved packaging replay outcomes.')
    print('Offline verification only. This command did not run Temporal Server or Python Replayer.')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'FAIL: {error}', file=sys.stderr)
        raise SystemExit(1)
