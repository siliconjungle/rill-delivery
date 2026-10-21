use rill_delivery::queue::*;
use std::collections::BTreeMap;

fn check<Q: ReadyQueue>() {
    for seed in 1..=24 {
        let mut random = seed * 81239;
        let mut next = || {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random
        };
        let mut q = Q::default();
        let mut model = BTreeMap::new();
        for _ in 0..12000 {
            let n = next();
            let id = (n as usize >> 3) % 257;
            match n % 5 {
                0 => {
                    q.remove(id);
                    model.remove(&id);
                }
                1 => {
                    let expected = model.values().min().copied();
                    assert_eq!(q.pop(), expected);
                    if let Some(e) = expected {
                        model.remove(&e.id);
                    }
                }
                _ => {
                    // Intentionally tie both rank fields; id is the final tie break.
                    let e = Entry {
                        id,
                        rank: Rank {
                            due: next() % 41,
                            order: next() % 17,
                        },
                    };
                    q.upsert(e);
                    model.insert(id, e);
                }
            }
            assert_eq!(q.peek(), model.values().min().copied());
            assert_eq!(q.len(), model.len());
        }
        while let Some(e) = q.pop() {
            assert_eq!(Some(e), model.values().min().copied());
            model.remove(&e.id);
        }
        assert!(model.is_empty());
    }
}
macro_rules! prove {
    ($name:ident,$q:ty) => {
        #[test]
        fn $name() {
            check::<$q>();
        }
    };
}
prove!(scan, ScanQueue);
prove!(cached_scan, CachedScan);
prove!(tournament, Tournament);
prove!(adaptive4, AdaptiveQueue<4>);
prove!(adaptive8, AdaptiveQueue<8>);
prove!(adaptive16, AdaptiveQueue<16>);
prove!(adaptive32, AdaptiveQueue<32>);
prove!(cached_hybrid4, CachedHybrid<4>);
prove!(cached_hybrid8, CachedHybrid<8>);
prove!(cached_hybrid16, CachedHybrid<16>);
prove!(cached_hybrid32, CachedHybrid<32>);
prove!(cached_hybrid64, CachedHybrid<64>);
prove!(sorted, SortedQueue);
prove!(tree, TreeQueue);
prove!(lazy, LazyHeap);
prove!(indexed2, IndexedHeap<2>);
prove!(indexed4, IndexedHeap<4>);
prove!(indexed8, IndexedHeap<8>);
prove!(indexed16, IndexedHeap<16>);
prove!(hybrid4, Hybrid<4>);
prove!(hybrid8, Hybrid<8>);
prove!(hybrid16, Hybrid<16>);
prove!(hybrid32, Hybrid<32>);
prove!(hybrid64, Hybrid<64>);
#[cfg(feature = "comparisons")]
mod comparisons {
    use super::*;
    use rill_delivery::queue::external::*;
    prove!(priority, Priority);
    prove!(keyed, Keyed);
    prove!(orx_map2, OrxMap<2>);
    prove!(orx_map4, OrxMap<4>);
    prove!(orx_map8, OrxMap<8>);
    prove!(orx_index2, OrxIndex<2>);
    prove!(orx_index4, OrxIndex<4>);
    prove!(orx_index8, OrxIndex<8>);
}
