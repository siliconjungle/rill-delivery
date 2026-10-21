use rill_delivery::queue::{*, external::*};
use std::collections::{BTreeMap,BTreeSet};
use std::hint::black_box;
use std::time::Instant;

#[derive(Clone, Copy)]
enum Op { Put(usize,Entry), Remove(usize,usize), Pop(usize) }
struct Trace { peers: usize, width: usize, ops: Vec<Op> }

fn fixture(case: &str, mut rng: u64) -> Trace {
    let (peers,width,rounds) = match case {
        "tiny" => (20000,4,4), "small" => (20000,16,3),
        "coalesce" => (20000,16,3), "sparse" => (20000,64,4),
        "dense" => (20000,64,2), "large" => (1,20000,4),
        "churn" => (20000,16,3), _ => panic!("unknown case"),
    };
    let mut next = || { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; rng };
    let mut ops = Vec::new(); let mut order = 0;
    for round in 0..rounds {
        for peer in 0..peers {
            if case == "sparse" && peer % 97 != 0 { continue; }
            for id in 0..width {
                order += 1;
                let e = Entry { id, rank: Rank { due: next() % 256 + round * 100, order } };
                ops.push(Op::Put(peer,e));
                if case == "coalesce" {
                    for _ in 0..2 { order += 1; ops.push(Op::Put(peer,Entry { rank:Rank {due:next()%256,order},..e})); }
                }
                if case == "churn" && id % 3 == 0 { ops.push(Op::Remove(peer,id)); }
            }
            for _ in 0..width / 2 { ops.push(Op::Pop(peer)); }
        }
    }
    for peer in 0..peers { for _ in 0..width { ops.push(Op::Pop(peer)); } }
    Trace { peers,width,ops }
}

fn oracle(trace: &Trace) -> Vec<(usize,Option<Entry>)> {
    let mut maps = vec![BTreeMap::<usize,Entry>::new();trace.peers];
    let mut orders = vec![BTreeSet::<Entry>::new();trace.peers];
    let mut out = Vec::new();
    for &op in &trace.ops {
        match op {
            Op::Put(p,e) => {
                if let Some(old) = maps[p].insert(e.id,e) { orders[p].remove(&old); }
                orders[p].insert(e);
            }
            Op::Remove(p,id) => { if let Some(old) = maps[p].remove(&id) { orders[p].remove(&old); } }
            Op::Pop(p) => {
                let e = orders[p].pop_first();
                if let Some(e) = e { maps[p].remove(&e.id); }
                out.push((p,e));
            }
        }
    }
    out
}

fn run<Q: ReadyQueue>(trace: &Trace, expected: &[(usize,Option<Entry>)]) -> (u128,u64,usize) {
    // Includes all queue allocation, promotions, compaction and final draining.
    // The input trace and independent expected vector are prepared outside timing.
    let start = Instant::now();
    let mut queues: Vec<Q> = (0..trace.peers).map(|_| Q::with_bound(trace.width)).collect();
    let mut actual = Vec::with_capacity(expected.len());
    let mut checksum = 0u64;
    for &op in black_box(&trace.ops) {
        match op {
            Op::Put(p,e) => queues[p].upsert(black_box(e)),
            Op::Remove(p,id) => queues[p].remove(black_box(id)),
            Op::Pop(p) => {
                let e = queues[p].pop();
                if let Some(e) = e { checksum = checksum.wrapping_mul(31).wrapping_add(e.rank.due ^ e.rank.order ^ e.id as u64 ^ p as u64); }
                actual.push((p,e));
            }
        }
    }
    assert!(queues.iter().all(ReadyQueue::is_empty));
    drop(queues);
    black_box(checksum);
    let elapsed = start.elapsed().as_nanos();
    assert_eq!(actual,expected,"complete ordered output differs from independent BTreeMap oracle");
    (elapsed,checksum,actual.len())
}

fn main() {
    let a: Vec<_> = std::env::args().collect();
    if a.len() < 4 { panic!("usage: queues BACKEND CASE SEED"); }
    let backend = &a[1]; let case = &a[2]; let seed = a[3].parse::<u64>().unwrap();
    let trace = fixture(case,seed);
    let expected = oracle(&trace);
    let (ns,checksum,outputs) = match backend.as_str() {
        "scan"=>run::<ScanQueue>(&trace,&expected), "sorted"=>run::<SortedQueue>(&trace,&expected),
        "tree"=>run::<TreeQueue>(&trace,&expected), "lazy"=>run::<LazyHeap>(&trace,&expected),
        "heap2"=>run::<IndexedHeap<2>>(&trace,&expected), "heap4"=>run::<IndexedHeap<4>>(&trace,&expected),
        "heap8"=>run::<IndexedHeap<8>>(&trace,&expected), "heap16"=>run::<IndexedHeap<16>>(&trace,&expected),
        "hybrid4"=>run::<Hybrid<4>>(&trace,&expected), "hybrid8"=>run::<Hybrid<8>>(&trace,&expected),
        "hybrid16"=>run::<Hybrid<16>>(&trace,&expected), "hybrid32"=>run::<Hybrid<32>>(&trace,&expected),
        "hybrid64"=>run::<Hybrid<64>>(&trace,&expected), "priority"=>run::<Priority>(&trace,&expected),
        "keyed"=>run::<Keyed>(&trace,&expected), "orx-map2"=>run::<OrxMap<2>>(&trace,&expected),
        "orx-map4"=>run::<OrxMap<4>>(&trace,&expected), "orx-map8"=>run::<OrxMap<8>>(&trace,&expected),
        "orx-index2"=>run::<OrxIndex<2>>(&trace,&expected), "orx-index4"=>run::<OrxIndex<4>>(&trace,&expected),
        "orx-index8"=>run::<OrxIndex<8>>(&trace,&expected), _=>panic!("unknown backend")
    };
    println!("{{\"backend\":\"{backend}\",\"case\":\"{case}\",\"seed\":{seed},\"peers\":{},\"width\":{},\"operations\":{},\"elapsed_ns\":{ns},\"checksum\":{checksum},\"outputs\":{outputs},\"exact_oracle\":true}}",trace.peers,trace.width,trace.ops.len());
}
