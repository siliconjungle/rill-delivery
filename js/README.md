# Rill Delivery for JavaScript

Generic, synchronous, dependency-free ECMAScript modules for Node.js 22+ and modern
browsers. MIT OR Apache-2.0. This is a JavaScript implementation, not a native Rust
binding; it requires no WASM, GPU, transport, database, codec or async runtime.

```sh
# Source distribution is on GitHub; this package is not published to npm.
cd js
npm test
```

```js
import {Dispatcher, Message} from './index.mjs';
const sender = new Dispatcher(20_000, 64 * 1024 * 1024);
const peer = sender.addPeer();
const snapshot = new Message(new Uint8Array([1, 2, 3]), 67);
sender.offerState(peer, 'record:42', 1, snapshot, 0, 0);
const out = [];
sender.dispatchInto(1, {
  bytes: 256 * 1024, perPeerBytes: 16 * 1024, deliveries: 4096, visits: 20_000,
}, out);
// Send each envelope through your transport. Only on an application ACK:
for (const {peer, delivery} of out) sender.ack(peer, delivery.receipt);
```

`Mailbox` is the single-recipient state machine. `Dispatcher` adds recipient
incarnations, active-only polling, byte-credit fairness and token buckets. Both
provide `offerState`, prepared `stream`/`offerStream`, `offerEvent`, `ack`, `retry`,
`forgetState`, `progress` and `bufferedBytes`. `Mailbox.drainInto(now, bytes,
maxItems, out)` appends attempts; `Dispatcher.dispatchInto` adds global/per-peer
caps. Default peer options are `limits`, `burst`, `bytesPerTick`, and `quantum`.
Limits use `states`, `events`, `bytes`, `maxMessageBytes`, and optional `strategy`.
Four ready queues (`heap4`, `heap8`, `scan`, `cached`) support workload experiments.

The contracts match the Rust core: newest *replacement* state, separate FIFO
events, one state attempt per key and one event attempt per recipient in flight,
exact retry receipts, stale-ACK rejection, age retention, explicit byte/count
bounds and admission atomicity. Prepared handles become stale after removal, even
when slots are reused. Keys use JavaScript Map equality (SameValueZero), including
object identity. Payloads are shared references: callers must keep them immutable;
the library freezes message wrappers, not arbitrary objects or typed-array bytes.
Returned arrays and retained acknowledged-key metadata are caller-managed.

Numbers are checked nonnegative safe integers, at most `Number.MAX_SAFE_INTEGER`;
this port does not silently truncate Rust u64 values. Priorities are delays 0–65535;
message costs are positive u32 values. Supply monotonic ticks. Byte counts are
conservative wire costs, including in-flight attempts, not physical heap usage.
Receipts are process-local identifiers, not authentication or durable sessions.
Receivers reject old revisions and deduplicate event sequences before application
ACK. A socket accepting bytes does not constitute such an ACK.

Never coalesce a chain of dependent patches or event/CRDT operations by retaining
only its last element. Use replacement snapshots or materialize against a confirmed
baseline. Reconnection, persistence, congestion control and fragmentation are owned
by the application.

## Replacement-bank fast path

`SnapshotOrder` selects a bounded bank fairly across keys while retaining payload
references. It does not ACK messages or reorder a reliable patch stream.

```js
import {SnapshotOrder} from './snapshots.mjs';
const order = new SnapshotOrder({strategy: 'runs', key: row => row[0]});
const selected = [];
order.fill([['a', 'a1'], ['a', 'a2'], ['b', 'b1']], selected, 3);
// selected: [['a','a1'], ['b','b1'], ['a','a2']]
```

Variants are `reference`, `pooled`, `linked`, and `runs`. `runs` specializes contiguous
key groups and falls back to linked indexing for interleaved keys. One synchronous
owner can reuse the workspace across many recipients. Output arrays belong to the
caller and must differ from input arrays. `fill` validates arguments and cleans up
after key-reader exceptions; it rejects reentrancy. `fillInto` is a prepared fast path
for trusted internal producers: distinct arrays, a prevalidated nonnegative integer
limit, a pure nonthrowing key reader and no reentrancy are required. It releases
scratch key/row references after a successful call. Scratch numeric storage can
retain the high-water input length; bound inputs separately when needed. No expiry,
transport credit, rotation cursor or delivery acknowledgement is inferred by ordering.
Those policies remain with the caller. Empty output does not mean delivered state.

## Validation

Tests include independent queue ordering, event/retry/byte-bound invariants,
20,000-recipient convergence, heterogeneous key grouping and exact Rust trace parity.
The Rust oracle deliberately exercises failed admissions, forgotten keys, prepared
streams, ACKs, retries, budgets and ordered events. To run parity rather than skip it:

```sh
# From the repository root
mkdir -p build
cargo run --locked --release --example js_oracle > build/js-oracle.ndjson
RILL_RUST_ORACLE="$PWD/build/js-oracle.ndjson" node --test js/test/*.test.mjs
```

Performance is runtime- and workload-dependent. Rust results do not transfer to
JavaScript, and 20,000 in-process recipients do not prove 20,000 live connections.
