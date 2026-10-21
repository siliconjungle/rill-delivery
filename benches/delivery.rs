use rill_delivery::queue::{external::*, *};
use rill_delivery::*;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::Instant;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Stamp {
    peer: u32,
    kind: u8,
    key: u64,
    revision: u64,
    age: u64,
    bytes: u32,
    content: u64,
    sent_at: u64,
}
struct Metrics {
    total: u128,
    startup: u128,
    initial_sync: u128,
    frames: Vec<u128>,
    flush: u128,
    checksum: u64,
    offers: u64,
    peak_bytes: u64,
}
#[derive(Clone, Copy, Default)]
struct Mode {
    prepared: bool,
    copied: bool,
}
struct Worker {
    mode: Mode,
    offset: usize,
    peers: usize,
    barrier: Option<Arc<Barrier>>,
    shards: usize,
}
fn recipient_message(message: &Message<Vec<u8>>, copied: bool) -> Message<Vec<u8>> {
    if copied {
        Message::new(Arc::new(message.payload.as_ref().clone()), message.bytes()).unwrap()
    } else {
        message.clone()
    }
}
fn payload(key: u64, revision: u64, seed: u64) -> Message<Vec<u8>> {
    let len = 8 + ((key ^ seed) % 8) * 8;
    let mut bytes = vec![0u8; len as usize];
    let word = key.wrapping_mul(31) ^ revision.wrapping_mul(81239) ^ seed;
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = (word.rotate_left(i as u32) as u8).wrapping_add(i as u8);
    }
    Message::new(Arc::new(bytes), len as u32 + 16).unwrap()
}
// Eight independent fixture observations, including the actual logical handoff tick.
#[allow(clippy::too_many_arguments)]
fn consume<Q: ReadyQueue>(
    d: &mut Dispatcher<u64, Vec<u8>, Q>,
    out: &mut Vec<Envelope<u64, Vec<u8>>>,
    ids: &HashMap<Peer, u32>,
    stamps: &mut Vec<Stamp>,
    held: &mut Vec<(Peer, Receipt)>,
    loss: bool,
    checksum: &mut u64,
    now: u64,
) {
    for e in out.drain(..) {
        let peer = ids[&e.peer];
        let item = e.delivery;
        let mut content = 0u64;
        for &byte in black_box(item.message.payload.as_slice()) {
            content = content.wrapping_mul(31).wrapping_add(u64::from(byte));
        }
        let (kind, key, revision) = match item.kind {
            Kind::State { key, revision } => (0, key, revision),
            Kind::Event { sequence } => (1, sequence, 0),
        };
        *checksum = checksum
            .wrapping_mul(37)
            .wrapping_add(content ^ u64::from(peer) ^ key ^ revision);
        stamps.push(Stamp {
            peer,
            kind,
            key,
            revision,
            age: item.queued_at,
            bytes: item.message.bytes(),
            content,
            sent_at: now,
        });
        if loss && content % 7 == 0 {
            held.push((e.peer, item.receipt));
        } else {
            assert!(d.ack(e.peer, item.receipt));
        }
    }
}
fn run<Q: ReadyQueue>(case: &str, seed: u64, worker: Worker) -> (Metrics, Vec<Stamp>) {
    let Worker {
        mode,
        offset: global_offset,
        peers: n,
        barrier,
        shards,
    } = worker;
    let width = match case {
        "fanout" => 1,
        "resident" | "sparse" => 32,
        "slow" | "loss" => 16,
        "events" => 8,
        _ => panic!("unknown case"),
    };
    let started = Instant::now();
    let mut d = Dispatcher::<u64, Vec<u8>, Q>::new(n, (1 << 30) / shards as u64).unwrap();
    let base = PeerConfig {
        limits: Limits {
            states: width as usize,
            events: 16,
            bytes: 16384,
            max_message_bytes: 128,
        },
        burst: 512,
        bytes_per_tick: 256,
        quantum: 256,
    };
    let mut peers = Vec::new();
    let mut ids = HashMap::new();
    for i in 0..n {
        let mut cfg = base;
        if case == "slow" && (i + global_offset) % 5 == 0 {
            cfg.bytes_per_tick = 16;
            cfg.burst = 256;
        }
        let peer = d.add_peer(cfg).unwrap();
        peers.push(peer);
        ids.insert(peer, (i + global_offset) as u32);
    }
    let mut offers = 0;
    let mut streams: Vec<Vec<Stream>> = if mode.prepared {
        (0..n).map(|_| Vec::with_capacity(width as usize)).collect()
    } else {
        Vec::new()
    };
    for key in 0..width {
        let message = payload(key, 0, seed);
        for (i, &peer) in peers.iter().enumerate() {
            d.offer_state(
                peer,
                key,
                0,
                recipient_message(&message, mode.copied),
                (key % 16) as u16,
                0,
            )
            .unwrap();
            offers += 1;
            if mode.prepared {
                streams[i].push(d.stream(peer, &key).unwrap());
            }
        }
    }
    let startup = started.elapsed().as_nanos();
    let budget = DispatchBudget {
        bytes: (n * 512) as u64,
        per_peer_bytes: 512,
        deliveries: n * 16,
        visits: n,
    };
    let mut stamps = Vec::new();
    let mut out = Vec::new();
    let mut held = Vec::new();
    let mut checksum = 0u64;
    let mut now = 0;
    let initial = Instant::now();
    while d.ready_peers() > 0 {
        d.dispatch_into(now, budget, &mut out).unwrap();
        consume(
            &mut d,
            &mut out,
            &ids,
            &mut stamps,
            &mut held,
            false,
            &mut checksum,
            now,
        );
        now += 1;
        assert!(now < 512, "initial sync did not converge");
    }
    assert_eq!(d.buffered_bytes(), 0);
    let initial_sync = initial.elapsed().as_nanos();
    let mut frames = Vec::new();
    let mut peak_bytes = 0;
    for frame in 1..=12 {
        if let Some(b) = &barrier {
            b.wait();
        }
        let begin = Instant::now();
        now += 1;
        let versions = if case == "fanout" { 4 } else { 1 };
        for subversion in 0..versions {
            let revision = frame * 4 + subversion;
            for offset in 0..width.min(2) {
                let key = (frame * 7 + offset * 11) % width;
                let message = payload(key, revision, seed);
                for (i, &peer) in peers.iter().enumerate() {
                    if case == "sparse" && ((i + global_offset) as u64 + seed) % 131 != 0 {
                        continue;
                    }
                    let message = recipient_message(&message, mode.copied);
                    if mode.prepared {
                        d.offer_stream(
                            peer,
                            streams[i][key as usize],
                            revision,
                            message,
                            ((key + seed) % 16) as u16,
                            now,
                        )
                        .unwrap();
                    } else {
                        d.offer_state(
                            peer,
                            key,
                            revision,
                            message,
                            ((key + seed) % 16) as u16,
                            now,
                        )
                        .unwrap();
                    }
                    offers += 1;
                }
            }
        }
        if case == "events" {
            let event = payload(1000, frame, seed);
            for (i, &peer) in peers.iter().enumerate() {
                if ((i + global_offset) as u64 + frame) % 10 == 0 {
                    d.offer_event(peer, recipient_message(&event, mode.copied), 0, now)
                        .unwrap();
                    offers += 1;
                }
            }
        }
        for (peer, receipt) in held.drain(..) {
            assert!(d.retry(peer, receipt));
            assert!(!d.ack(peer, receipt));
        }
        peak_bytes = peak_bytes.max(d.buffered_bytes());
        d.dispatch_into(now, budget, &mut out).unwrap();
        consume(
            &mut d,
            &mut out,
            &ids,
            &mut stamps,
            &mut held,
            case == "loss",
            &mut checksum,
            now,
        );
        if let Some(b) = &barrier {
            b.wait();
        }
        frames.push(begin.elapsed().as_nanos());
    }
    let flushing = Instant::now();
    for (peer, receipt) in held.drain(..) {
        assert!(d.retry(peer, receipt));
        assert!(!d.ack(peer, receipt));
    }
    let mut flushes = 0;
    while d.ready_peers() > 0 {
        now += 128;
        d.dispatch_into(now, budget, &mut out).unwrap();
        consume(
            &mut d,
            &mut out,
            &ids,
            &mut stamps,
            &mut held,
            false,
            &mut checksum,
            now,
        );
        flushes += 1;
        assert!(flushes < 128, "flush did not converge");
    }
    assert_eq!(d.buffered_bytes(), 0);
    for &peer in &peers {
        for key in 0..width {
            let p = d.progress(peer, &key).unwrap();
            assert_eq!(p.acknowledged, Some(p.latest));
            assert_eq!(p.pending, None);
            assert_eq!(p.in_flight, None);
        }
    }
    let flush = flushing.elapsed().as_nanos();
    drop(d);
    black_box(checksum);
    let total = started.elapsed().as_nanos();
    (
        Metrics {
            total,
            startup,
            initial_sync,
            frames,
            flush,
            checksum,
            offers,
            peak_bytes,
        },
        stamps,
    )
}
fn checked<Q: ReadyQueue>(case: &str, seed: u64, expected: &[Stamp]) -> Metrics {
    let name = std::env::args().nth(1).unwrap();
    let mode = Mode {
        prepared: name.contains("-handles"),
        copied: name.contains("-copied"),
    };
    let shards = name
        .split("-shards")
        .nth(1)
        .map_or(1, |n| n.parse::<usize>().unwrap());
    assert!([1, 2, 4, 8].contains(&shards));
    let (mut metrics, mut actual) = if shards == 1 {
        run::<Q>(
            case,
            seed,
            Worker {
                mode,
                offset: 0,
                peers: 20000,
                barrier: None,
                shards: 1,
            },
        )
    } else {
        let barrier = Arc::new(Barrier::new(shards));
        let wall = Instant::now();
        let results = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for index in 0..shards {
                let worker = Worker {
                    mode,
                    offset: index * (20000 / shards),
                    peers: 20000 / shards,
                    barrier: Some(barrier.clone()),
                    shards,
                };
                handles.push(scope.spawn(move || {
                    // A failed proof must fail the process rather than strand its
                    // sibling benchmark threads at a synchronization barrier.
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        run::<Q>(case, seed, worker)
                    })) {
                        Ok(result) => result,
                        Err(_) => std::process::exit(101),
                    }
                }));
            }
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        let total = wall.elapsed().as_nanos();
        let mut aggregate = Metrics {
            total,
            startup: 0,
            initial_sync: 0,
            frames: vec![0; 12],
            flush: 0,
            checksum: 0,
            offers: 0,
            peak_bytes: 0,
        };
        let mut output = Vec::new();
        for (m, mut values) in results {
            aggregate.startup = aggregate.startup.max(m.startup);
            aggregate.initial_sync = aggregate.initial_sync.max(m.initial_sync);
            aggregate.flush = aggregate.flush.max(m.flush);
            aggregate.offers += m.offers;
            aggregate.peak_bytes += m.peak_bytes;
            for (i, n) in m.frames.into_iter().enumerate() {
                aggregate.frames[i] = aggregate.frames[i].max(n);
            }
            output.append(&mut values);
        }
        (aggregate, output)
    };
    // Independent recipients have no cross-recipient ordering contract. Compare
    // exact handoff ticks and complete order within each recipient/tick instead.
    actual.sort_by_key(|s| (s.sent_at, s.peer));
    assert_eq!(
        actual, expected,
        "full delivery vector differs from ScanQueue reference state machine"
    );
    metrics.checksum = actual.iter().fold(0u64, |h, s| {
        h.wrapping_mul(37)
            .wrapping_add(s.content ^ u64::from(s.peer) ^ s.key ^ s.revision ^ s.sent_at)
    });
    metrics
}
fn main() {
    let a: Vec<_> = std::env::args().collect();
    let backend = &a[1];
    let case = &a[2];
    let seed = a[3].parse::<u64>().unwrap();
    // Reference run is outside the measured candidate interval. The state machine
    // is independently model-tested; this comparison proves exact queue substitution.
    let (_, mut expected) = run::<ScanQueue>(
        case,
        seed,
        Worker {
            mode: Mode::default(),
            offset: 0,
            peers: 20000,
            barrier: None,
            shards: 1,
        },
    );
    expected.sort_by_key(|s| (s.sent_at, s.peer));
    let stripped = backend.replace("-handles", "").replace("-copied", "");
    let base = stripped.split("-shards").next().unwrap();
    let m = match base {
        "scan" => checked::<ScanQueue>(case, seed, &expected),
        "sorted" => checked::<SortedQueue>(case, seed, &expected),
        "tree" => checked::<TreeQueue>(case, seed, &expected),
        "lazy" => checked::<LazyHeap>(case, seed, &expected),
        "heap2" => checked::<IndexedHeap<2>>(case, seed, &expected),
        "heap4" => checked::<IndexedHeap<4>>(case, seed, &expected),
        "heap8" => checked::<IndexedHeap<8>>(case, seed, &expected),
        "heap16" => checked::<IndexedHeap<16>>(case, seed, &expected),
        "hybrid4" => checked::<Hybrid<4>>(case, seed, &expected),
        "hybrid8" => checked::<Hybrid<8>>(case, seed, &expected),
        "hybrid16" => checked::<Hybrid<16>>(case, seed, &expected),
        "hybrid32" => checked::<Hybrid<32>>(case, seed, &expected),
        "hybrid64" => checked::<Hybrid<64>>(case, seed, &expected),
        "priority" => checked::<Priority>(case, seed, &expected),
        "keyed" => checked::<Keyed>(case, seed, &expected),
        "orx-map2" => checked::<OrxMap<2>>(case, seed, &expected),
        "orx-map4" => checked::<OrxMap<4>>(case, seed, &expected),
        "orx-map8" => checked::<OrxMap<8>>(case, seed, &expected),
        "orx-index2" => checked::<OrxIndex<2>>(case, seed, &expected),
        "orx-index4" => checked::<OrxIndex<4>>(case, seed, &expected),
        "orx-index8" => checked::<OrxIndex<8>>(case, seed, &expected),
        "cached-scan" => checked::<CachedScan>(case, seed, &expected),
        "tournament" => checked::<Tournament>(case, seed, &expected),
        "cached4" => checked::<CachedHybrid<4>>(case, seed, &expected),
        "cached8" => checked::<CachedHybrid<8>>(case, seed, &expected),
        "cached16" => checked::<CachedHybrid<16>>(case, seed, &expected),
        "cached32" => checked::<CachedHybrid<32>>(case, seed, &expected),
        "cached64" => checked::<CachedHybrid<64>>(case, seed, &expected),
        "adaptive4" => checked::<AdaptiveQueue<4>>(case, seed, &expected),
        "adaptive8" => checked::<AdaptiveQueue<8>>(case, seed, &expected),
        "switch0-8" => checked::<SwitchingQueue<0, 8>>(case, seed, &expected),
        "switch4-16" => checked::<SwitchingQueue<4, 16>>(case, seed, &expected),
        "switch8-32" => checked::<SwitchingQueue<8, 32>>(case, seed, &expected),
        "switch16-64" => checked::<SwitchingQueue<16, 64>>(case, seed, &expected),
        "switch4-32" => checked::<SwitchingQueue<4, 32>>(case, seed, &expected),
        "adaptive16" => checked::<AdaptiveQueue<16>>(case, seed, &expected),
        "adaptive32" => checked::<AdaptiveQueue<32>>(case, seed, &expected),
        _ => panic!("unknown backend"),
    };
    println!("{{\"backend\":\"{backend}\",\"case\":\"{case}\",\"seed\":{seed},\"peers\":20000,\"elapsed_ns\":{},\"startup_ns\":{},\"initial_sync_ns\":{},\"frame_ns\":{:?},\"flush_ns\":{},\"checksum\":{},\"outputs\":{},\"offers\":{},\"peak_buffered_bytes\":{},\"exact_oracle\":true}}",m.total,m.startup,m.initial_sync,m.frames,m.flush,m.checksum,expected.len(),m.offers,m.peak_bytes);
}
