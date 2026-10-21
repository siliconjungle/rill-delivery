# Primary-source findings

Surveyed on 2026-10-21 (Australia/Sydney). The versioned metadata, licenses,
checksums, repository URLs and seven Frontier source pins are in
[inventory.json](inventory.json). Twelve close source families were fetched as
checksum-verified excerpts; [source-pass.json](source-pass.json) preserves their
exact file hashes. `tools/research.py` and `tools/source_pass.py` reproduce discovery.

This is a broad survey of 72 Rust crates, seven Frontier packages and the additional
references below. It does not establish that every relevant library has been found,
and a catalog entry is not a benchmark result. Dependencies and building blocks are
distinguished from full engines. Current executable comparisons exercise three
external queue crates inside the same delivery state machine; they do **not** prove
superiority over entire replication, messaging or durable scheduling systems.

## Closest scheduling and coalescing sources

| Source | What was inspected | Decision |
| --- | --- | --- |
| [Rifts](https://docs.rs/rifts/0.3.10/rifts/flow/backpressure/enum.BackpressureStrategy.html) | State-key coalescing, downgrade, pause, disconnect and snapshot fallback strategies | Reference: make overload visible; leave policy decisions to callers rather than silently dropping events |
| [Firq](https://docs.rs/firq-core/0.1.3/firq_core/) | Generic tenant scheduler, task costs, per-tenant quantum, deadline expiry, sharding and locks | Reference: cost-aware fairness; concurrency and expiry contracts differ from this synchronous non-expiring delivery core |
| [NexusNet scheduler](https://docs.rs/nexusnet-scheduler/0.1.0/nexusnet_scheduler/) | `src/priority.rs`: bounded classes, weights, enqueue/dequeue and replenishment | Reference: its priority queue spends credit in **items**; our dispatcher spends credit in bytes. Variable-size traffic is not an equivalent fairness comparison |
| [coalescing_buffer](https://docs.rs/coalescing_buffer/0.1.0/coalescing_buffer/) | SPSC keyed ring offer/poll and unread replacement | Reference: useful conflation contract; it does not implement per-recipient ACK state, byte budgets or scheduling |
| [Tokio watch](https://docs.rs/tokio/latest/tokio/sync/watch/) and [n0-watcher](https://docs.rs/n0-watcher/latest/n0_watcher/) | Latest-value observation rather than retained event history | Reference: replacement state and events require different contracts; async notification is deliberately outside the core |
| [Zenoh QoS](https://spec.zenoh.io/spec/1.0.0/concepts/qos.html) | Priority lanes and congestion-control policies | Reference: transport backpressure remains necessary in addition to an application scheduler |
| [Zenoh RingChannel](https://docs.rs/zenoh/latest/zenoh/handlers/struct.RingChannel.html) | Bounded FIFO with oldest-element dropping | Excluded from equivalent state tests: retaining the last N messages does not retain the latest value of every key |
| [NATS slow consumers](https://github.com/nats-io/nats.docs/blob/master/running-a-nats-service/nats_admin/slow_consumers.md) | Pending message/byte limits, notification and disconnect behavior | Reference: a continuously slow consumer cannot be fixed by merely growing its queue |

Other cataloged candidates include taskmill, hart-link, IPFRS, cohort-queue,
governor, leaky-bucket, channel implementations, MQTT/AMQP/Kafka clients and durable
replication libraries. Their roles and current comparison status are explicit in the
catalog; no speed claim is made about an unadapted library.

## Replication and algorithm references

- [Bevy Replicon](https://docs.rs/bevy_replicon/latest/bevy_replicon/) and
  [Lightyear replication](https://docs.rs/lightyear_replication/latest/lightyear_replication/):
  visibility, mutation priorities, entity replication and transport adapters. These
  are larger ECS replication layers. Our library accepts already-selected recipients
  and opaque payloads; it does not replace their prediction or entity mapping.
- [Naia](https://docs.rs/naia-server/latest/naia_server/), Renet, Laminar and Turbulence
  provide other replication/message transport contexts. Their full protocols are not
  interchangeable ready queues.
- [Unreal Replication Graph](https://dev.epicgames.com/documentation/en-us/unreal-engine/replication-graph-in-unreal-engine):
  retain shared relevance data across frames instead of repeatedly rebuilding every
  recipient's world view. This reinforces the boundary between Rill subscriptions
  and delivery; the initial delivery API does not claim cohort-compressed bookkeeping.
- [Gaffer: State Synchronization](https://new.gafferongames.com/post/state_synchronization/):
  accumulate importance, fit updates into a packet budget and retain deferred work.
  Our current policy instead uses a stable age/urgency rank, avoiding a periodic
  priority increment across every pending record. Both policies require quality
  measurements; they are not identical scheduling contracts.
- [Efficient Fair Queuing Using Deficit Round-Robin](https://web.stanford.edu/class/ee384x/EE384X/papers/DRR.pdf)
  and [RFC 8290](https://datatracker.ietf.org/doc/rfc8290/): byte credits and active
  queues inform the dispatcher. We do not implement CoDel packet dropping.
- [ZeroMQ socket options](https://libzmq.readthedocs.io/en/latest/zmq_setsockopt.html):
  conflation is an adjacent semantic, not a substitute for keyed state and event FIFO.
- [Valve GameNetworkingSockets](https://github.com/ValveSoftware/GameNetworkingSockets):
  reliability, fragmentation, encryption and pacing belong below this library.
- [Bytes](https://docs.rs/bytes/latest/bytes/): shared immutable buffers. The core
  uses generic `Arc<T>` and also supports `Arc<[u8]>`; it does not impose a codec.

## Frontier inspection

Exact revisions are in the inventory. The source links below are immutable.

| Package | Finding and boundary |
| --- | --- |
| [Frontier core](https://github.com/siliconjungle/-shapeshift-labs-frontier/tree/2c72dbb4030e2695b682d3e49ec82d8c5c8ffec2) | Diff/apply, dirty paths, revisions and reusable output arrays are complementary to delivery. Computing a patch is a different task from deciding when each recipient can receive it. |
| [Realtime](https://github.com/siliconjungle/-shapeshift-labs-frontier-realtime/tree/e4b7b5c2a2d197c3d6df223b0517f79ada2650cf) | Commands, snapshots, deltas, prediction and reconciliation provide useful protocol boundaries. |
| [Realtime server](https://github.com/siliconjungle/-shapeshift-labs-frontier-realtime-server/blob/7524d51e0cd2bebb6a0fafa43eab69bd52013506/src/room.ts) | Room command validation and snapshot history; queued commands are sorted. This is not a sender-side byte scheduler. |
| [Realtime WebSocket](https://github.com/siliconjungle/-shapeshift-labs-frontier-realtime-websocket/blob/71cbfc2244c476043abd7abd7ddcb835c34438a7/src/server.ts) | Per-client snapshot/delta creation and an outgoing buffered-amount cutoff. It tracks last snapshots sent, which should not be silently equated with application ACKs on an unreliable transport. |
| [Scheduler](https://github.com/siliconjungle/-shapeshift-labs-frontier-scheduler/blob/4a86212cec6b3c899f840a31267e9ed8d97e44bd/src/index.ts) | Lanes, units, priorities and backpressure. `coalesce-key` retains an existing task; `replace-key` replaces it. Our state mode explicitly keeps the newest revision and preserves pending age. |
| [Queue](https://github.com/siliconjungle/-shapeshift-labs-frontier-queue/blob/b4cd3672ae430b7530688ae1dcc377b8cd0c2991/src/index.ts) | Durable jobs, leases, retries, dedupe and replayable state are useful design references, but durable execution is outside this in-memory library. |
| [Event log](https://github.com/siliconjungle/-shapeshift-labs-frontier-event-log/tree/e3e73e9b33475b059ba8ec74a4dd1ff14859f025) | Replay/cursor history belongs above delivery. A pending sender queue is not durable history. |

The proposed integration is change feed -> subscriptions -> delivery -> caller-owned
encoding/transport -> actual ACK. No Frontier or Nocturne source is copied into the
Rust library. A chain of Frontier patches cannot be coalesced by simply retaining its
last patch: materialize a replacement snapshot, or encode against the recipient's
confirmed baseline at handoff. CRDT operations likewise require their own compaction
semantics or event delivery.

## Implemented experiments

Reference -> measured -> accepted status is determined by retained evidence:

- 37 queue variants across packed scans, cached minima, sorted runs, trees, bounded
  lazy deletion, indexed d-ary heaps, tournament trees, adaptive runs and hysteresis.
- External adapters use priority-queue 2.7.0, keyed_priority_queue 0.4.2 and
  orx-priority-queue 1.9.0. The same delivery state machine wraps every adapter.
- Independent ordered-queue oracle, declarative delivery oracle, capacity/reconnect
  tests and explicit 20,000-recipient convergence test.
- Full lifecycle fixtures retain state until ACK and compare complete delivery
  vectors, including content, recipient, revision, event identity and queued age.
- Fused admission lookup, no-op rank suppression and conservative cost-floor
  experiments are separately preserved in Git and frozen build receipts.

The preregistered isolated comparison completed all 1,008 fresh processes. The
[results](results.md) and machine-readable acceptance evidence report every retained
comparison, including regressions and inconclusive intervals. The default remains
the development-selected switching queue. Whole-engine universal superiority is unproven.
