# Rill Delivery

A bounded, coalescing delivery scheduler in Rust. It accepts arbitrary record keys,
immutable payloads and recipient handles. The core has no dependencies, game rules,
ECS, transport, database, serializer or async runtime. Library source forbids unsafe
code. MIT OR Apache-2.0.

**Experimental:** final performance selection is in progress. The retained
comparisons do not establish universal superiority over replication engines.

## What it does

- Keeps the newest pending replacement state for each `(recipient, key)`.
- Shares payload storage through `Arc<T>`, including `Arc<[u8]>` and `Arc<str>`.
- Maintains separate FIFO events, which are never coalesced or silently discarded.
- Applies global/per-recipient dispatch budgets and deterministic token buckets.
- Rotates active recipients with byte-credit fairness and configurable quanta.
- Preserves pending age while replacing state, so new urgent work cannot reset the
  age of every deferred update indefinitely.
- Tracks exact in-flight attempts and acknowledged state revisions. Retry invalidates
  the old receipt; a newer pending state can supersede a failed older attempt.
- Bounds known state keys, retained events and logical buffered bytes. In-flight
  messages remain charged until ACK, retry supersession or explicit removal.
- Recycles recipient and state slots without allowing old ACKs to acknowledge new
  attempts. The active-recipient list has no disconnected-recipient tombstones.

## Example

```rust
use rill_delivery::{DispatchBudget, Dispatcher, Message, PeerConfig};
use std::sync::Arc;

let mut sender = Dispatcher::<String, [u8]>::new(20_000, 64 * 1024 * 1024)?;
let peer = sender.add_peer(PeerConfig::default())?;

// Encode a replacement snapshot once, then clone its shared handle for recipients.
let payload: Arc<[u8]> = Arc::from(&b"encoded replacement snapshot"[..]);
let wire_upper_bound = payload.len() as u32 + 64;
let message = Message::new(payload, wire_upper_bound)?;
sender.offer_state(peer, "record:42".into(), 1, message, 0, 0)?;

let mut outgoing = Vec::new();
sender.dispatch_into(1, DispatchBudget {
    bytes: 256 * 1024,
    per_peer_bytes: 16 * 1024,
    deliveries: 4096,
    visits: 20_000,
}, &mut outgoing)?;

for envelope in outgoing {
    let receipt = envelope.delivery.receipt;
    let _payload = envelope.delivery.message.payload;
    // Hand off through your transport. Only after its application ACK arrives:
    sender.ack(envelope.peer, receipt);
    // On a failed attempt, use sender.retry(envelope.peer, receipt) instead.
}
# Ok::<(), rill_delivery::Error>(())
```

`Mailbox<K, T>` provides the same state/event lifecycle for a single recipient.
`Dispatcher<K, T>` adds recipient management, active polling, budgets and fair service.
`Progress` exposes the latest, pending, in-flight and acknowledged revisions.
`Receipt::parts/from_parts` support application-owned ACK envelopes without imposing
a serializer. A `Receipt` is process-local bookkeeping, not a durable session token.

## Contracts that matter

**Replacement state:** each value must be independently applicable. A chain of JSON
patches or CRDT operations cannot be coalesced by simply keeping its final operation.
Materialize the latest snapshot, or encode a delta against a confirmed baseline at
handoff. Frontier remains useful for this encoding layer; see the
[Frontier inspection](research/findings.md#frontier-inspection).

**Events:** one event attempt per recipient is in flight at a time. Its successor is
released after ACK; unrelated state can still proceed. Retries preserve the event's
sequence, enabling receiver deduplication. Event sequences increase but can have gaps.
Exactly-once receiver side effects and durable recovery are application concerns.

**Time and priorities:** supply monotonically increasing `u64` ticks in your chosen
unit. A priority delay is an urgency/age offset, not a future release time. All queued
messages are immediately eligible. Priority delay zero is most urgent. A nonzero
quantum and a sufficiently large eventual budget are necessary for progress.

**Budgets:** `Message::bytes` is a positive conservative wire-cost estimate including
your framing. Dispatch caps charge attempts, including retries. Buffer accounting
charges pending and in-flight references; it is not an introspective physical heap
meter for arbitrary `T`. The caller owns payload allocation size and outgoing buffers.
No fragmentation, socket pacing or congestion controller is supplied. A message must
fit the configured burst and a later dispatch allowance to make progress.

**Capacity:** failed admission returns an error without advancing the record's
revision or replacing its pending payload. Retained acknowledged-key metadata counts
toward the state-key limit until `forget_state`. Callers choose whether to defer,
resynchronize or disconnect an overloaded recipient. `forget_state` cancels local
tracking; encode deletions explicitly if the remote must observe them. Use a fresh
record incarnation or monotonic revision domain when reusing application keys.

**Concurrency:** use a single owner per dispatcher or shard independent dispatchers
across workers. Payloads can be shared between shards. No global mutex or runtime
waker is required by the core. This is not a durable queue or network server.

## Performance research and reproduction

The [protocol](research/protocol.md), [source catalog](research/catalog.md) and
[findings](research/findings.md) distinguish discovery, source inspection, component
timing and full delivery timing. The catalog covers 72 Rust crates and seven Frontier
packages. The implementation search includes 37 queue variants. Three external queue
crates are adapted to the same delivery contract; unadapted engines are not claimed
as defeated.

Queue fixtures cover batch, interleaved, tiny, large, sparse, churn and overwrite
patterns. Delivery fixtures use 20,000 in-process recipients, up to 640,000 known
recipient/key relationships, shared encoded payloads, variable byte costs, sparse
activity, slow recipients, FIFO events and correlated retry bursts. Each candidate
must match the reference's **complete ordered delivery vector** and end with all
latest revisions acknowledged. A separate declarative model checks the state machine.

Parallel screens are exploratory. Final comparisons use fresh processes in
interleaved order. Frozen build receipts bind executable hashes to committed source,
compiler, lockfile and flags. Raw observations live in `evidence/`; peak process RSS
includes fixture/reference/output storage and is not presented as library-only memory.
Short fixture frame samples are not sustained network p99 measurements. Other host
workers remain running. Twenty thousand recipients is not proof of 20,000 live
connections or a complete game's tick rate.

```sh
cargo test --locked --all-features --release
cargo clippy --locked --all-features --all-targets -- -D warnings
python3 tools/build_queues.py local-delivery --bench delivery
python3 tools/queue_study.py --binary build/local-delivery --bench delivery \
  --phase isolated --label local-delivery --repetitions 3 \
  --cases fanout resident sparse slow loss events
```

Building a frozen benchmark requires committed source and refuses to overwrite an
existing frozen executable. `comparisons` enables the optional external queue
dependencies; normal library users do not need that feature. `ReadyQueue` and explicit
queue types permit workload-specific tuning without changing delivery semantics.
