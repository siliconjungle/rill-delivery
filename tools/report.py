#!/usr/bin/env python3
"""Render an honest performance table from accepted paired comparisons."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
result = json.loads((ROOT / 'evidence/final-isolated-acceptance.json').read_text())
assert result['proof_passed']
summary = {(r['case'], r['backend']): r for r in result['summary']}
comparisons = {(r['case'], r['candidate'], r['baseline'], r['metric']): r for r in result['comparisons']}
cases = ['fanout', 'resident', 'sparse', 'slow', 'loss', 'events']
chosen = 'default-handles-shards8'
lines = ['# Final delivery study', '',
    'Twelve finalists, six workloads, two performance seeds unseen during selection, seven repetitions: **1,008 fresh processes**. Every run checked its complete delivery vector against the reference and converged to the newest acknowledged revisions. Timings are uninstrumented. The default and finalists were registered before these runs.', '',
    'The machine is an 8-core Apple Silicon host running macOS with 8 GiB RAM. Other workers were left running. Each process measures twelve fixture frames; the statistical unit is a process mean, not twelve independent frames. These short in-process fixtures do not establish sustained network p99 or 20,000 live connections.', '',
    '## Frame wall time and CPU', '',
    'Values are medians over 14 processes. Ratios and percentile 95% intervals use paired log ratios with 10,000 bootstrap resamples. Smaller is better. Intervals are per comparison and are not multiplicity adjusted. The baseline is the same delivery core using a 4-ary heap, raw keys and one owner. Eight owners use more CPU resources; this is not a queue-only comparison.', '',
    '| Workload | Baseline frame ms | Prepared, 8 owners frame ms | Paired wall ratio [95% interval] | Result | Steady CPU ratio [95% interval] |',
    '| --- | ---: | ---: | --- | --- | --- |']
for c in cases:
    a, b = summary[c, chosen], summary[c, 'heap4']
    w = comparisons[c, chosen, 'heap4', 'mean_frame_ns']['pooled']
    cpu = comparisons[c, chosen, 'heap4', 'steady_cpu_ns']['pooled']
    lines.append(f"| {c} | {b['mean_frame_ns']/1e6:.3f} | {a['mean_frame_ns']/1e6:.3f} | {w['ratio']:.3f} [{w['lower_95']:.3f}, {w['upper_95']:.3f}] | {w['classification']} | {cpu['ratio']:.3f} [{cpu['lower_95']:.3f}, {cpu['upper_95']:.3f}] |")
lines += ['', '## Matched eight-owner controls', '',
    'These controls keep prepared handles and eight owners. The external orx queue is a component adapter inside the same lifecycle, not a comparison against an entire replication engine.', '',
    '| Workload | Control | Frame ratio [95% interval] | Result |', '| --- | --- | --- | --- |']
for c in cases:
    for b in ('heap8-handles-shards8', 'orx-index2-handles-shards8', 'default-handles-copied-shards8'):
        w = comparisons[c, chosen, b, 'mean_frame_ns']['pooled']
        lines.append(f"| {c} | {b} | {w['ratio']:.3f} [{w['lower_95']:.3f}, {w['upper_95']:.3f}] | {w['classification']} |")
lines += ['', '## Lifecycle costs and memory', '',
    'The table includes construction, initial synchronization, measured frames, final flushing, output collection and cleanup. Process CPU excludes the reference run. RSS includes reference, fixtures and proof output history; it is **not library-only resident memory**. Byte counters are declared logical wire costs, not physical payload sizes.', '',
    '| Workload | Configuration | Lifecycle wall ms | Process CPU ms | Startup ms | Initial sync ms | Whole-process peak RSS MiB |',
    '| --- | --- | ---: | ---: | ---: | ---: | ---: |']
for c in cases:
    for b in ('heap4', 'default-handles', chosen):
        r = summary[c,b]
        lines.append(f"| {c} | {b} | {r['elapsed_ns']/1e6:.2f} | {r['cpu_ns']/1e6:.2f} | {r['startup_ns']/1e6:.2f} | {r['initial_sync_ns']/1e6:.2f} | {r['peak_process_rss_bytes']/2**20:.1f} |")
lines += ['', '## Allocation profile', '',
    'A separate instrumented development-seed study counts allocations only during the candidate lifecycle, including its fixture/proof buffers. Counter overhead invalidates these rows for speed acceptance. Requested bytes include repeated allocation and are not a resident-size measure.', '',
    '| Workload | Configuration | Allocation calls | Requested MiB | Extra live high-water MiB |', '| --- | --- | ---: | ---: | ---: |']
for raw in (ROOT/'evidence/allocation-study.ndjson').read_text().splitlines():
    r = json.loads(raw); assert r['allocation_profile'] and r['exact_oracle']
    a = r['allocations']
    lines.append(f"| {r['case']} | {r['backend']} | {a['calls']:,} | {a['requested_bytes']/2**20:.1f} | {a['peak_extra_live_bytes']/2**20:.1f} |")
lines += ['', '## Evidence and limits', '',
    '- [All final process observations](../evidence/final-isolated.ndjson), [configuration](../evidence/final-isolated-config.json), [acceptance with every per-seed and pooled comparison](../evidence/final-isolated-acceptance.json).',
    '- Current source adds only an exact deprecation-lint annotation/comment around the legacy Rust 1.85 atomic API. The verifier strips only those exact bytes and requires the measured source hash; runtime and benchmark code otherwise match byte-for-byte.',
    '- [Registered plan](final-protocol.json), [frozen selection](selection.json), [exact build receipt](../evidence/delivery-release-build.json), [required validation receipt](../evidence/validation-receipt.json).',
    '- Development includes 37 queue structures and a parallel 592-configuration matrix. Screens are selection data, not headline speed evidence.',
    '- The survey covers 72 related Rust crates plus one CPU measurement dependency and seven public Frontier packages. Three external queue crates have executable component adapters; no claim that 72 complete engines were defeated.',
    '- Ratios reflect the declared fixtures, budgets, ordering, ACK and retry rules. No globally fastest queue is established. Consumers can choose explicit queues and worker counts for their workloads.',
    '- Tail delivery age is constrained by exact reference handoff ticks in these fixtures. There is no sustained arrival-rate, socket throughput, congestion, server tick or end-to-end latency claim.',
    '', 'Reproduce with `python3 tools/build_queues.py reproduced --bench delivery`, then `python3 tools/final_study.py --binary build/reproduced --label reproduced`. The checked-in acceptance verifier expects the registered final label/binary by default; use its flags for another run. Frozen builds require committed inputs.', '']
(ROOT / 'research/results.md').write_text('\n'.join(lines))
print('research/results.md')
