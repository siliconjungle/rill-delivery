# Final delivery study

Twelve finalists, six workloads, two performance seeds unseen during selection, seven repetitions: **1,008 fresh processes**. Every run checked its complete delivery vector against the reference and converged to the newest acknowledged revisions. Timings are uninstrumented. The default and finalists were registered before these runs.

The machine is an 8-core Apple Silicon host running macOS with 8 GiB RAM. Other workers were left running. Each process measures twelve fixture frames; the statistical unit is a process mean, not twelve independent frames. These short in-process fixtures do not establish sustained network p99 or 20,000 live connections.

## Frame wall time and CPU

Values are medians over 14 processes. Ratios and percentile 95% intervals use paired log ratios with 10,000 bootstrap resamples. Smaller is better. Intervals are per comparison and are not multiplicity adjusted. The baseline is the same delivery core using a 4-ary heap, raw keys and one owner. Eight owners use more CPU resources; this is not a queue-only comparison.

| Workload | Baseline frame ms | Prepared, 8 owners frame ms | Paired wall ratio [95% interval] | Result | Steady CPU ratio [95% interval] |
| --- | ---: | ---: | --- | --- | --- |
| fanout | 6.840 | 2.824 | 0.407 [0.374, 0.435] | win | 2.271 [2.092, 2.432] |
| resident | 22.822 | 4.393 | 0.186 [0.180, 0.192] | win | 1.031 [0.983, 1.081] |
| sparse | 0.097 | 0.144 | 1.413 [0.998, 2.109] | inconclusive | 3.184 [2.811, 3.579] |
| slow | 16.962 | 3.787 | 0.226 [0.219, 0.234] | win | 1.262 [1.224, 1.296] |
| loss | 23.100 | 6.327 | 0.272 [0.263, 0.284] | win | 1.578 [1.524, 1.641] |
| events | 13.010 | 3.662 | 0.295 [0.281, 0.312] | win | 1.657 [1.584, 1.751] |

## Matched eight-owner controls

These controls keep prepared handles and eight owners. The external orx queue is a component adapter inside the same lifecycle, not a comparison against an entire replication engine.

| Workload | Control | Frame ratio [95% interval] | Result |
| --- | --- | --- | --- |
| fanout | heap8-handles-shards8 | 1.046 [1.007, 1.083] | regression |
| fanout | orx-index2-handles-shards8 | 1.063 [1.017, 1.108] | regression |
| fanout | default-handles-copied-shards8 | 0.937 [0.888, 0.992] | win |
| resident | heap8-handles-shards8 | 0.986 [0.932, 1.045] | inconclusive |
| resident | orx-index2-handles-shards8 | 0.993 [0.939, 1.047] | inconclusive |
| resident | default-handles-copied-shards8 | 0.828 [0.795, 0.863] | win |
| sparse | heap8-handles-shards8 | 1.578 [0.980, 2.465] | inconclusive |
| sparse | orx-index2-handles-shards8 | 1.438 [0.967, 2.073] | inconclusive |
| sparse | default-handles-copied-shards8 | 1.419 [0.965, 2.129] | inconclusive |
| slow | heap8-handles-shards8 | 1.036 [0.998, 1.073] | inconclusive |
| slow | orx-index2-handles-shards8 | 0.995 [0.953, 1.038] | inconclusive |
| slow | default-handles-copied-shards8 | 0.803 [0.780, 0.833] | win |
| loss | heap8-handles-shards8 | 1.005 [0.973, 1.045] | inconclusive |
| loss | orx-index2-handles-shards8 | 0.981 [0.942, 1.022] | inconclusive |
| loss | default-handles-copied-shards8 | 0.885 [0.848, 0.933] | win |
| events | heap8-handles-shards8 | 1.102 [1.027, 1.189] | regression |
| events | orx-index2-handles-shards8 | 1.054 [0.981, 1.141] | inconclusive |
| events | default-handles-copied-shards8 | 0.946 [0.884, 1.018] | inconclusive |

## Lifecycle costs and memory

The table includes construction, initial synchronization, measured frames, final flushing, output collection and cleanup. For sharded runs, startup/initial-sync/flush stage values are maximum worker phase durations; total wall time also includes spawning and aggregation. Process CPU excludes the reference run. RSS includes reference, fixtures and proof output history; it is **not library-only resident memory**. Byte counters are declared logical wire costs, not physical payload sizes.

| Workload | Configuration | Lifecycle wall ms | Process CPU ms | Startup ms | Initial sync ms | Whole-process peak RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| fanout | heap4 | 92.40 | 92.32 | 5.59 | 2.79 | 70.3 |
| fanout | default-handles | 74.45 | 74.53 | 5.90 | 2.77 | 70.6 |
| fanout | default-handles-shards8 | 43.19 | 242.33 | 5.69 | 0.81 | 95.2 |
| resident | heap4 | 592.65 | 592.25 | 143.02 | 157.99 | 511.5 |
| resident | default-handles | 540.21 | 540.16 | 167.62 | 171.12 | 527.7 |
| resident | default-handles-shards8 | 179.21 | 1048.10 | 71.97 | 47.83 | 652.1 |
| sparse | heap4 | 318.72 | 318.60 | 142.40 | 156.66 | 446.6 |
| sparse | default-handles | 353.46 | 353.70 | 169.35 | 165.03 | 462.8 |
| sparse | default-handles-shards8 | 119.69 | 743.48 | 72.21 | 47.17 | 533.7 |
| slow | heap4 | 343.59 | 343.43 | 61.73 | 55.71 | 296.6 |
| slow | default-handles | 314.44 | 314.52 | 73.58 | 56.58 | 306.1 |
| slow | default-handles-shards8 | 111.34 | 617.36 | 32.90 | 18.60 | 362.8 |
| loss | heap4 | 414.86 | 414.49 | 61.16 | 42.89 | 336.2 |
| loss | default-handles | 386.54 | 386.56 | 73.77 | 44.61 | 339.4 |
| loss | default-handles-shards8 | 145.15 | 810.79 | 34.35 | 16.36 | 461.7 |
| events | heap4 | 207.99 | 207.86 | 28.35 | 14.67 | 182.0 |
| events | default-handles | 183.50 | 183.60 | 30.78 | 15.62 | 188.7 |
| events | default-handles-shards8 | 76.88 | 425.94 | 17.61 | 8.47 | 259.5 |

## Allocation profile

A separate instrumented development-seed study counts allocations only during the candidate lifecycle, including its fixture/proof buffers. Counter overhead invalidates these rows for speed acceptance. Requested bytes include repeated allocation and are not a resident-size measure.

| Workload | Configuration | Allocation calls | Requested MiB | Extra live high-water MiB |
| --- | --- | ---: | ---: | ---: |
| resident | heap4 | 420,196 | 582.9 | 300.4 |
| resident | default-handles-shards8 | 441,493 | 712.7 | 316.8 |
| resident | default-handles | 440,197 | 600.5 | 316.8 |
| resident | default-handles-copied-shards8 | 2,681,493 | 794.5 | 319.5 |
| fanout | heap4 | 80,176 | 76.3 | 46.5 |
| fanout | default-handles-shards8 | 101,333 | 105.7 | 48.7 |
| fanout | default-handles | 100,177 | 79.7 | 48.7 |
| fanout | default-handles-copied-shards8 | 2,061,333 | 173.0 | 50.0 |
| sparse | heap4 | 420,195 | 470.9 | 244.4 |
| sparse | default-handles-shards8 | 441,485 | 553.0 | 260.8 |
| sparse | default-handles | 440,196 | 488.5 | 260.8 |
| sparse | default-handles-copied-shards8 | 1,728,781 | 599.6 | 262.8 |

## Evidence and limits

- [All final process observations](../evidence/final-isolated.ndjson), [configuration](../evidence/final-isolated-config.json), [acceptance with every per-seed and pooled comparison](../evidence/final-isolated-acceptance.json).
- Current source adds only an exact deprecation-lint annotation/comment around the legacy Rust 1.85 atomic API. The verifier strips only those exact bytes and requires the measured source hash; runtime and benchmark code otherwise match byte-for-byte.
- [Registered plan](final-protocol.json), [frozen selection](selection.json), [exact build receipt](../evidence/delivery-release-build.json), [required validation receipt](../evidence/validation-receipt.json).
- Development includes 37 queue structures and a parallel 592-configuration matrix. Screens are selection data, not headline speed evidence.
- The survey covers 72 related Rust crates plus one CPU measurement dependency and seven public Frontier packages. Three external queue crates have executable component adapters; no claim that 72 complete engines were defeated.
- Ratios reflect the declared fixtures, budgets, ordering, ACK and retry rules. No globally fastest queue is established. Consumers can choose explicit queues and worker counts for their workloads.
- Tail delivery age is constrained by exact reference handoff ticks in these fixtures. There is no sustained arrival-rate, socket throughput, congestion, server tick or end-to-end latency claim.

Reproduce with `python3 tools/build_queues.py reproduced --bench delivery`, then `python3 tools/final_study.py --binary build/reproduced --label reproduced`. The checked-in acceptance verifier expects the registered final label/binary by default; use its flags for another run. Frozen builds require committed inputs.
