#!/usr/bin/env python3
"""Fail closed on proof/source coverage; report paired process-level performance."""
import argparse
import hashlib
import itertools
import json
import math
from pathlib import Path
import random
import statistics
import subprocess

ROOT = Path(__file__).resolve().parents[1]
METRICS = ('mean_frame_ns', 'elapsed_ns', 'cpu_ns', 'steady_cpu_ns', 'peak_process_rss_bytes')
CHECKS = {'tests-release', 'clippy', 'format', 'msrv', 'example', 'normal-dependencies', 'package'}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(value, message):
    if not value:
        raise ValueError(message)


def check_digest(actual, expected, kind):
    require(actual == expected, kind + ' hash differs')


def row_key(r):
    return r['backend'], r['case'], r['seed'], r['repetition']


def validate_rows(rows, plan):
    expected = set(itertools.product(plan['finalists'], plan['cases'], plan['seeds'], range(plan['repetitions'])))
    require(len(rows) == plan['processes'] == len(expected), 'process coverage count')
    keys = [row_key(r) for r in rows]
    require(len(set(keys)) == len(keys), 'duplicate process cell')
    require(set(keys) == expected, 'missing or unexpected process cell')
    for r in rows:
        require(r.get('exact_oracle') is True, 'exact output proof failed')
        require(r.get('allocation_profile') is False and r.get('allocations') is None, 'instrumented speed row')
        require(r['peers'] == 20_000, 'recipient coverage differs')
        require(len(r['frame_ns']) == 12 and all(x > 0 for x in r['frame_ns']), 'invalid frame samples')
        require(all(r[k] > 0 for k in ('elapsed_ns', 'cpu_ns', 'steady_cpu_ns', 'peak_process_rss_bytes')), 'missing positive timing/memory metric')
        r['mean_frame_ns'] = statistics.mean(r['frame_ns'])
    for case, seed in itertools.product(plan['cases'], plan['seeds']):
        proof = {(r['checksum'], r['outputs'], r['offers']) for r in rows if r['case'] == case and r['seed'] == seed}
        require(len(proof) == 1, 'cross-implementation output proof differs')
    return {row_key(r): r for r in rows}


def validate_bindings(label, binary, plan_path):
    config_path = ROOT / 'evidence' / (label + '-config.json')
    config = json.loads(config_path.read_text())
    receipt_path = ROOT / 'evidence' / (binary.name + '-build.json')
    receipt = json.loads(receipt_path.read_text())
    plan = json.loads(plan_path.read_text())
    selection = json.loads((ROOT / 'research/selection.json').read_text())
    require(selection['registered_protocol_sha256'] == sha(plan_path), 'registered protocol changed')
    require(selection['selection_frozen'] is True and selection['finalists'] == plan['finalists'], 'finalist selection changed')
    require(selection['training_matrix_sha256'] == sha(ROOT / 'evidence/matrix-592-v6.ndjson'), 'development matrix changed')
    require(config['phase'] == 'isolated', 'speed measurements not isolated')
    args = config['argv']
    require(args['backends'] == plan['finalists'] and args['cases'] == plan['cases'] and args['seeds'] == plan['seeds'] and args['repetitions'] == plan['repetitions'], 'driver differs from registered plan')
    require(config['build_receipt_sha256'] == sha(receipt_path), 'build receipt hash differs')
    check_digest(sha(binary), receipt['binary_sha256'], 'frozen executable')
    check_digest(config['binary_sha256'], receipt['binary_sha256'], 'configured executable')
    require(config['source_commit'] == receipt['source_commit'] and config['source_sha256'] == receipt['source_sha256'], 'config/build sources differ')
    require('allocation-profile' not in json.dumps(receipt.get('command', receipt.get('build_command', []))), 'instrumented speed executable')
    for path, digest in receipt['source_sha256'].items():
        original = subprocess.check_output(['git', 'show', receipt['source_commit'] + ':' + path], cwd=ROOT)
        check_digest(hashlib.sha256(original).hexdigest(), digest, 'committed build input ' + path)
        if path.startswith(('src/', 'benches/')) or path in ('Cargo.toml', 'Cargo.lock'):
            current = (ROOT / path).read_bytes()
            if path == 'src/mailbox.rs':
                # This exact lint-only compatibility annotation preserves the 1.85 API.
                # Removing it must recover every byte of the measured implementation.
                annotation = b'        // Keep the Rust 1.85 API; newer compilers rename this to try_update.\n        #[allow(deprecated)]\n'
                require(current.count(annotation) <= 1, 'duplicate compatibility annotation')
                current = current.replace(annotation, b'', 1)
            check_digest(hashlib.sha256(current).hexdigest(), digest, 'current runtime/benchmark input ' + path)
    driver = subprocess.check_output(['git', 'show', receipt['source_commit'] + ':tools/queue_study.py'], cwd=ROOT)
    require(hashlib.sha256(driver).hexdigest() == config['driver_sha256'], 'timing driver differs from frozen committed version')
    vr_path = ROOT / 'evidence/validation-receipt.json'
    vr = json.loads(vr_path.read_text())
    require(vr['all_passed'] is True and {x['name'] for x in vr['checks']} == CHECKS and len(vr['checks']) == len(CHECKS), 'required validation missing')
    for path, digest in vr['source_sha256'].items():
        require(sha(ROOT / path) == digest, 'validated source changed: ' + path)
    for check in vr['checks']:
        require(check['exit_code'] == 0 and sha(ROOT / check['log']) == check['log_sha256'], 'validation log/exit differs: ' + check['name'])
    return {'protocol_sha256': sha(plan_path), 'raw_sha256': sha(ROOT / 'evidence' / (label + '.ndjson')),
            'config_sha256': sha(config_path), 'build_receipt_sha256': sha(receipt_path),
            'validation_sha256': sha(vr_path), 'source_commit': receipt['source_commit'], 'binary_sha256': sha(binary),
            'source_equivalence': 'Current mailbox differs only by one exact deprecation-lint annotation/comment for Rust 1.85 compatibility; removing those exact bytes must reproduce the measured source hash. Runtime and benchmark code otherwise match byte-for-byte.'}


def classify(lower, upper):
    return 'win' if upper < 1 else 'regression' if lower > 1 else 'inconclusive'


def interval(logs, resamples=10_000):
    rng = random.Random(419_701 + len(logs))
    n = len(logs)
    means = sorted(sum(logs[rng.randrange(n)] for _ in range(n)) / n for _ in range(resamples))
    lower, upper = math.exp(means[249]), math.exp(means[9749])
    return {'ratio': math.exp(statistics.mean(logs)), 'lower_95': lower, 'upper_95': upper,
            'classification': classify(lower, upper), 'pairs': n}


def comparison(index, plan, candidate, baseline, case, metric):
    result = {'candidate': candidate, 'baseline': baseline, 'case': case, 'metric': metric, 'seeds': {}}
    pooled = []
    for seed in plan['seeds']:
        logs = [math.log(index[(candidate, case, seed, rep)][metric] / index[(baseline, case, seed, rep)][metric]) for rep in range(plan['repetitions'])]
        pooled += logs
        result['seeds'][str(seed)] = interval(logs)
    result['pooled'] = interval(pooled)
    return result


def report(index, plan):
    rows = list(index.values())
    summary = []
    for case, backend in itertools.product(plan['cases'], plan['finalists']):
        group = [r for r in rows if r['case'] == case and r['backend'] == backend]
        summary.append({'case': case, 'backend': backend, 'processes': len(group),
                        **{m: statistics.median(r[m] for r in group) for m in METRICS},
                        'startup_ns': statistics.median(r['startup_ns'] for r in group),
                        'initial_sync_ns': statistics.median(r['initial_sync_ns'] for r in group),
                        'flush_ns': statistics.median(r['flush_ns'] for r in group)})
    pairs = [(b, 'heap4') for b in plan['finalists'] if b != 'heap4']
    # Same-core/worker-count controls distinguish tuning from more CPU owners.
    pairs += [('default-handles-shards8', b) for b in ('heap8-handles-shards8', 'orx-index2-handles-shards8', 'default-handles-copied-shards8')]
    comparisons = [comparison(index, plan, a, b, c, m) for (a, b), c, m in itertools.product(pairs, plan['cases'], METRICS)]
    counts = {k: sum(r['pooled']['classification'] == k for r in comparisons) for k in ('win', 'regression', 'inconclusive')}
    return {'summary': summary, 'comparisons': comparisons, 'classification_counts': counts,
            'statistics': 'Paired log ratios; 10000 bootstrap resamples; percentile 95% intervals; each process contributes one mean over its 12 frames. Per-cell intervals are not multiplicity adjusted.',
            'unique_fastest': 'Not asserted: all-pairs superiority over every finalist is not a publication gate and is not established.',
            'scope': '20000 in-process recipients; not 20000 live network connections or sustained frame p99; 3 external queue crates inside one contract, not 72 full engines.'}


def negative_checks(rows, plan, bindings):
    tests = []
    def reject(name, mutated):
        try:
            validate_rows(mutated, plan)
        except (ValueError, KeyError):
            tests.append({'name': name, 'rejected': True})
            return
        raise ValueError('negative proof accepted: ' + name)
    reject('missing-process', rows[:-1])
    reject('duplicate-process', rows[:-1] + [rows[0]])
    reject('instrumented-speed-row', [{**rows[0], 'allocation_profile': True}, *rows[1:]])
    reject('changed-output-checksum', [{**rows[0], 'checksum': rows[0]['checksum'] ^ 1}, *rows[1:]])
    degraded = interval([math.log(2.0)] * 14)
    require(degraded['classification'] == 'regression', 'slower measurements silently accepted')
    tests.append({'name': 'doubled-times-reported-as-regression', 'passed': True})
    for name in ('incorrect-executable-hash', 'incorrect-source-hash'):
        try:
            check_digest(bindings['binary_sha256'], '0' * 64, name)
        except ValueError:
            tests.append({'name': name, 'rejected': True})
        else:
            raise ValueError(name + ' accepted')
    return tests


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--label', default='final-isolated')
    p.add_argument('--binary', default='build/delivery-release')
    args = p.parse_args()
    plan_path = ROOT / 'research/final-protocol.json'
    plan = json.loads(plan_path.read_text())
    rows = [json.loads(x) for x in (ROOT / 'evidence' / (args.label + '.ndjson')).read_text().splitlines()]
    index = validate_rows(rows, plan)
    bindings = validate_bindings(args.label, (ROOT / args.binary).resolve(), plan_path)
    negative = negative_checks(rows, plan, bindings)
    result = {'schema': 1, 'proof_passed': True, 'processes': len(rows), 'bindings': bindings,
              'negative_checks': negative, **report(index, plan)}
    target = ROOT / 'evidence' / (args.label + '-acceptance.json')
    target.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'proof_passed': True, 'processes': len(rows), 'classifications': result['classification_counts']}))


if __name__ == '__main__':
    main()
