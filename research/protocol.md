# Rill Delivery investigation — 2026-10-21

Objective: a standalone, public-ready, generic Rust delivery scheduler that coalesces
replaceable state, shares payloads, budgets recipients, prioritizes without accidental
starvation, tracks recipient progress, handles overload and preserves event semantics.
No Nocturne dependency, ECS requirement, transport, runtime, serializer or schema.

Research must survey primary sources across replication, conflating channels,
traffic scheduling, rate limiting, queue data structures and Frontier. Each source is
classified as a direct comparison, component comparison, design reference or excluded
with a reason. A catalog is not evidence of a performance win. “All libraries on the
internet” is not a verifiable completeness claim; record search coverage and gaps.

## Competing implementations

Build multiple independently selectable scheduling structures, not just tune a single
implementation: linear scan, sorted-vector batches, BTreeSet, lazy binary heap,
indexed binary heap, indexed 4-ary heap, indexed 8-ary heap, and external keyed/priority
queues where their contracts can be normalized. Also compare sparse and dense record
storage, shared and copied payload handling, and several fair scheduling policies.
The production choice must follow measurements; losing designs remain reproducible
in Git history and benchmark source rather than duplicated workspaces.

Parallelize correctness and exploratory runs. Final timing runs use fresh processes,
alternate candidate order, and avoid simultaneous candidate timing. Other active
workers remain running. Record host/runtime/source hashes and interference caveats.

## Correctness

Use independent model/oracle traces, exhaustive small cases and randomized churn.
Check latest-state convergence, strictly ordered events, bounded pending/in-flight
storage, per-recipient and global byte limits, exact ACK identity, stale ACK rejection,
retry after loss, revision monotonicity, removal/reconnect, overflow and failed
admission atomicity. Never coalesce events or treat queued state as delivered.
Only equivalent semantics enter an apples-to-apples speed comparison.

## Performance

Include 20,000 recipients; report relationship count, payload size, overlap, churn,
update rate, budget, slow-recipient proportion and simulated loss separately.
Include sparse idle, dense dirty, repeated overwrites, fanout, mixed priorities,
variable payload sizes, constrained budgets, reconnect churn and loss/retry.
Measure complete lifecycle and stages: admission, selection, transport handoff,
ACK/retry, resident memory/allocation and tail update age. Measure actual delivered
data; check output digests and freshness/fairness against the model. Sending less
work or leaving more stale data is not a speed win.

Record raw observations and uncertainties; select configurations on a development
cohort and validate on a separate seed/workload cohort. No universal superiority
claim if any eligible comparison is unbuilt, untested, incorrect or faster.

## Publication and retention

Retain source, lockfiles, licenses, independent proofs, selected raw measurements,
fetch recipes and source pins. Use one minimal build tree with debug/incremental
disabled for untimed validation. Publish only generic sources and research metadata;
no private game code or unrelated user data. GitHub publication is requested; registry
publication is separate. Disk reserve target is 100,000,000,000 bytes, without stopping
other workers or deleting protected inputs.
