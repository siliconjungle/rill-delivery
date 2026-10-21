use rill_delivery::queue::{external::*, *};
use rill_delivery::*;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
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
fn payload(key: u64, revision: u64, seed: u64) -> Message<Vec<u8>> {
    let len = 8 + ((key ^ seed) % 8) * 8;
    let mut bytes = vec![0u8; len as usize];
    let word = key.wrapping_mul(31) ^ revision.wrapping_mul(81239) ^ seed;
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = (word.rotate_left(i as u32) as u8).wrapping_add(i as u8);
    }
    Message::new(Arc::new(bytes), len as u32 + 16).unwrap()
}
fn consume<Q: ReadyQueue>(
    d: &mut Dispatcher<u64, Vec<u8>, Q>,
    out: &mut Vec<Envelope<u64, Vec<u8>>>,
    ids: &HashMap<Peer, u32>,
    stamps: &mut Vec<Stamp>,
    held: &mut Vec<(Peer, Receipt)>,
    loss: bool,
    checksum: &mut u64,
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
        });
        if loss && content % 7 == 0 {
            held.push((e.peer, item.receipt));
        } else {
            assert!(d.ack(e.peer, item.receipt));
        }
    }
}
fn run<Q: ReadyQueue>(case: &str, seed: u64) -> (Metrics, Vec<Stamp>) {
    let n = 20000;
    let width = match case {
        "fanout" => 1,
        "resident" | "sparse" => 32,
        "slow" | "loss" => 16,
        "events" => 8,
        _ => panic!("unknown case"),
    };
    let started = Instant::now();
    let mut d = Dispatcher::<u64, Vec<u8>, Q>::new(n, 1 << 30).unwrap();
    let base = PeerConfig {
        limits: Limits {
            states: 64,
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
        if case == "slow" && i % 5 == 0 {
            cfg.bytes_per_tick = 16;
            cfg.burst = 256;
        }
        let peer = d.add_peer(cfg).unwrap();
        peers.push(peer);
        ids.insert(peer, i as u32);
    }
    let mut offers = 0;
    for key in 0..width {
        let message = payload(key, 0, seed);
        for &peer in &peers {
            d.offer_state(peer, key, 0, message.clone(), (key % 16) as u16, 0)
                .unwrap();
            offers += 1;
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
        );
        now += 1;
        assert!(now < 512, "initial sync did not converge");
    }
    assert_eq!(d.buffered_bytes(), 0);
    let initial_sync = initial.elapsed().as_nanos();
    let mut frames = Vec::new();
    let mut peak_bytes = 0;
    for frame in 1..=12 {
        let begin = Instant::now();
        now += 1;
        let versions = if case == "fanout" { 4 } else { 1 };
        for subversion in 0..versions {
            let revision = frame * 4 + subversion;
            for offset in 0..width.min(2) {
                let key = (frame * 7 + offset * 11) % width;
                let message = payload(key, revision, seed);
                for (i, &peer) in peers.iter().enumerate() {
                    if case == "sparse" && (i as u64 + seed) % 131 != 0 {
                        continue;
                    }
                    d.offer_state(
                        peer,
                        key,
                        revision,
                        message.clone(),
                        ((key + seed) % 16) as u16,
                        now,
                    )
                    .unwrap();
                    offers += 1;
                }
            }
        }
        if case == "events" {
            let event = payload(1000, frame, seed);
            for (i, &peer) in peers.iter().enumerate() {
                if (i as u64 + frame) % 10 == 0 {
                    d.offer_event(peer, event.clone(), 0, now).unwrap();
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
        );
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
    let (metrics, actual) = run::<Q>(case, seed);
    assert_eq!(
        actual, expected,
        "full delivery vector differs from ScanQueue reference state machine"
    );
    metrics
}
fn main() {
    let a: Vec<_> = std::env::args().collect();
    let backend = &a[1];
    let case = &a[2];
    let seed = a[3].parse::<u64>().unwrap();
    // Reference run is outside the measured candidate interval. The state machine
    // is independently model-tested; this comparison proves exact queue substitution.
    let (_, expected) = run::<ScanQueue>(case, seed);
    let m = match backend.as_str() {
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
