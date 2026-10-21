//! Interchangeable exact ready queues. Smaller ranks run first.
//!
//! This module deliberately exposes the scheduling structure so applications can
//! reproduce comparisons without changing coalescing or acknowledgement semantics.
use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};

/// Stable total ordering. `due` is an urgency rank, not a release time.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Rank {
    pub due: u64,
    pub order: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Entry {
    pub rank: Rank,
    pub id: usize,
}

/// Keys are internal dense slot indices, never application record identifiers.
/// An upsert replaces an existing key; remove is idempotent. Implementations must
/// contain no more than one *logical* entry per key and return the exact minimum.
pub trait ReadyQueue: Default {
    fn with_bound(_bound: usize) -> Self { Self::default() }
    fn upsert(&mut self, entry: Entry);
    fn remove(&mut self, id: usize);
    fn peek(&mut self) -> Option<Entry>;
    fn pop(&mut self) -> Option<Entry>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool { self.len() == 0 }
}

/// Packed indexed d-ary heap: bounded by the high-water slot count, no tombstones.
pub struct IndexedHeap<const D: usize> {
    heap: Vec<Entry>,
    positions: Vec<usize>,
}
impl<const D: usize> Default for IndexedHeap<D> {
    fn default() -> Self {
        assert!(D >= 2);
        Self { heap: Vec::new(), positions: Vec::new() }
    }
}
impl<const D: usize> IndexedHeap<D> {
    fn swap(&mut self, a: usize, b: usize) {
        self.heap.swap(a, b);
        self.positions[self.heap[a].id] = a;
        self.positions[self.heap[b].id] = b;
    }
    fn up(&mut self, mut at: usize) -> usize {
        while at > 0 {
            let parent = (at - 1) / D;
            if self.heap[parent] <= self.heap[at] { break; }
            self.swap(parent, at);
            at = parent;
        }
        at
    }
    fn down(&mut self, mut at: usize) {
        loop {
            let first = at.saturating_mul(D).saturating_add(1);
            if first >= self.heap.len() { break; }
            let end = first.saturating_add(D).min(self.heap.len());
            let mut best = first;
            for child in first + 1..end {
                if self.heap[child] < self.heap[best] { best = child; }
            }
            if self.heap[at] <= self.heap[best] { break; }
            self.swap(at, best);
            at = best;
        }
    }
    fn remove_at(&mut self, at: usize) -> Entry {
        let result = self.heap.swap_remove(at);
        self.positions[result.id] = usize::MAX;
        if at < self.heap.len() {
            self.positions[self.heap[at].id] = at;
            let next = self.up(at);
            self.down(next);
        }
        result
    }
}
impl<const D: usize> ReadyQueue for IndexedHeap<D> {
    fn upsert(&mut self, entry: Entry) {
        if self.positions.len() <= entry.id { self.positions.resize(entry.id + 1, usize::MAX); }
        let at = self.positions[entry.id];
        if at == usize::MAX {
            let at = self.heap.len();
            self.heap.push(entry);
            self.positions[entry.id] = at;
            self.up(at);
        } else if self.heap[at] != entry {
            self.heap[at] = entry;
            let next = self.up(at);
            self.down(next);
        }
    }
    fn remove(&mut self, id: usize) {
        if let Some(&at) = self.positions.get(id) {
            if at != usize::MAX { self.remove_at(at); }
        }
    }
    fn peek(&mut self) -> Option<Entry> { self.heap.first().copied() }
    fn pop(&mut self) -> Option<Entry> { (!self.heap.is_empty()).then(|| self.remove_at(0)) }
    fn len(&self) -> usize { self.heap.len() }
}

/// Unsorted packed vector. Cheap updates and linear minimum selection.
#[derive(Default)]
pub struct ScanQueue { entries: Vec<Entry>, positions: Vec<usize> }
impl ReadyQueue for ScanQueue {
    fn upsert(&mut self, entry: Entry) {
        if self.positions.len() <= entry.id { self.positions.resize(entry.id + 1, usize::MAX); }
        let at = self.positions[entry.id];
        if at == usize::MAX {
            self.positions[entry.id] = self.entries.len();
            self.entries.push(entry);
        } else { self.entries[at] = entry; }
    }
    fn remove(&mut self, id: usize) {
        if let Some(&at) = self.positions.get(id) {
            if at != usize::MAX {
                self.entries.swap_remove(at);
                self.positions[id] = usize::MAX;
                if at < self.entries.len() { self.positions[self.entries[at].id] = at; }
            }
        }
    }
    fn peek(&mut self) -> Option<Entry> { self.entries.iter().min().copied() }
    fn pop(&mut self) -> Option<Entry> { let e = self.peek()?; self.remove(e.id); Some(e) }
    fn len(&self) -> usize { self.entries.len() }
}

/// Sorted-on-demand batches; mutations invalidate the sorted run.
#[derive(Default)]
pub struct SortedQueue { inner: ScanQueue, sorted: Vec<Entry>, dirty: bool }
impl ReadyQueue for SortedQueue {
    fn upsert(&mut self, e: Entry) { self.inner.upsert(e); self.dirty = true; }
    fn remove(&mut self, id: usize) { self.inner.remove(id); self.dirty = true; }
    fn peek(&mut self) -> Option<Entry> {
        if self.dirty {
            self.sorted.clear(); self.sorted.extend_from_slice(&self.inner.entries);
            self.sorted.sort_unstable_by(|a,b| b.cmp(a)); self.dirty = false;
        }
        self.sorted.last().copied()
    }
    fn pop(&mut self) -> Option<Entry> {
        let e = self.peek()?; self.sorted.pop(); self.inner.remove(e.id); Some(e)
    }
    fn len(&self) -> usize { self.inner.len() }
}

#[derive(Default)]
pub struct TreeQueue { tree: BTreeSet<Entry>, slots: Vec<Option<Entry>> }
impl ReadyQueue for TreeQueue {
    fn upsert(&mut self, e: Entry) {
        if self.slots.len() <= e.id { self.slots.resize(e.id + 1, None); }
        if let Some(old) = self.slots[e.id].replace(e) { self.tree.remove(&old); }
        self.tree.insert(e);
    }
    fn remove(&mut self, id: usize) {
        if let Some(slot) = self.slots.get_mut(id) { if let Some(old) = slot.take() { self.tree.remove(&old); } }
    }
    fn peek(&mut self) -> Option<Entry> { self.tree.first().copied() }
    fn pop(&mut self) -> Option<Entry> {
        let e = self.tree.pop_first()?; self.slots[e.id] = None; Some(e)
    }
    fn len(&self) -> usize { self.tree.len() }
}

/// Standard-library heap with bounded lazy deletion. Rebuild at 2*live+32 entries.
/// Compaction is included in every measured lifecycle.
#[derive(Default)]
pub struct LazyHeap { heap: BinaryHeap<Reverse<Entry>>, slots: Vec<Option<Entry>>, live: usize }
impl LazyHeap {
    fn compact(&mut self) {
        if self.heap.len() > self.live.saturating_mul(2).saturating_add(32) {
            self.heap = self.slots.iter().flatten().copied().map(Reverse).collect();
        }
    }
}

/// Scan small queues, promote once to an indexed heap when the threshold is
/// exceeded. Promotion cost is included; no per-pop conversion or hidden warmup.
pub struct Hybrid<const N: usize> { scan: ScanQueue, heap: Option<IndexedHeap<4>> }
impl<const N: usize> Default for Hybrid<N> {
    fn default() -> Self { Self { scan: ScanQueue::default(), heap: None } }
}
impl<const N: usize> ReadyQueue for Hybrid<N> {
    fn upsert(&mut self, e: Entry) {
        if let Some(h) = &mut self.heap { h.upsert(e); return; }
        self.scan.upsert(e);
        if self.scan.len() > N {
            let mut h = IndexedHeap::default();
            for &entry in &self.scan.entries { h.upsert(entry); }
            self.scan = ScanQueue::default();
            self.heap = Some(h);
        }
    }
    fn remove(&mut self, id: usize) { if let Some(h) = &mut self.heap { h.remove(id) } else { self.scan.remove(id) } }
    fn peek(&mut self) -> Option<Entry> { if let Some(h) = &mut self.heap { h.peek() } else { self.scan.peek() } }
    fn pop(&mut self) -> Option<Entry> { if let Some(h) = &mut self.heap { h.pop() } else { self.scan.pop() } }
    fn len(&self) -> usize { if let Some(h) = &self.heap { h.len() } else { self.scan.len() } }
}
impl ReadyQueue for LazyHeap {
    fn upsert(&mut self, e: Entry) {
        if self.slots.len() <= e.id { self.slots.resize(e.id + 1, None); }
        if self.slots[e.id] == Some(e) { return; }
        if self.slots[e.id].replace(e).is_none() { self.live += 1; }
        self.heap.push(Reverse(e)); self.compact();
    }
    fn remove(&mut self, id: usize) {
        if self.slots.get_mut(id).and_then(Option::take).is_some() { self.live -= 1; }
        self.compact();
    }
    fn peek(&mut self) -> Option<Entry> {
        while let Some(&Reverse(e)) = self.heap.peek() {
            if self.slots[e.id] == Some(e) { return Some(e); }
            self.heap.pop();
        }
        None
    }
    fn pop(&mut self) -> Option<Entry> {
        let e = self.peek()?; self.heap.pop(); self.slots[e.id] = None;
        self.live -= 1; self.compact(); Some(e)
    }
    fn len(&self) -> usize { self.live }
}

#[cfg(feature = "comparisons")]
pub mod external {
    use super::*;
    #[derive(Default)]
    pub struct Priority(pub priority_queue::PriorityQueue<usize, Reverse<Entry>>);
    impl ReadyQueue for Priority {
        fn upsert(&mut self, e: Entry) { self.0.push(e.id, Reverse(e)); }
        fn remove(&mut self, id: usize) { self.0.remove(&id); }
        fn peek(&mut self) -> Option<Entry> { self.0.peek().map(|(_,&Reverse(e))| e) }
        fn pop(&mut self) -> Option<Entry> { self.0.pop().map(|(_,Reverse(e))| e) }
        fn len(&self) -> usize { self.0.len() }
    }
    #[derive(Default)]
    pub struct Keyed(pub keyed_priority_queue::KeyedPriorityQueue<usize, Reverse<Entry>>);
    impl ReadyQueue for Keyed {
        fn upsert(&mut self, e: Entry) { self.0.push(e.id, Reverse(e)); }
        fn remove(&mut self, id: usize) { self.0.remove(&id); }
        fn peek(&mut self) -> Option<Entry> { self.0.peek().map(|(_,&Reverse(e))| e) }
        fn pop(&mut self) -> Option<Entry> { self.0.pop().map(|(_,Reverse(e))| e) }
        fn len(&self) -> usize { self.0.len() }
    }

    use orx_priority_queue::{PriorityQueue, PriorityQueueDecKey};
    pub struct OrxMap<const D: usize>(pub orx_priority_queue::DaryHeapWithMap<usize,Entry,D>);
    impl<const D: usize> Default for OrxMap<D> { fn default() -> Self { Self(Default::default()) } }
    impl<const D: usize> ReadyQueue for OrxMap<D> {
        fn upsert(&mut self, e: Entry) { self.0.update_key_or_push(&e.id, e); }
        fn remove(&mut self, id: usize) { if self.0.contains(&id) { self.0.remove(&id); } }
        fn peek(&mut self) -> Option<Entry> { self.0.peek().map(|(_,e)| *e) }
        fn pop(&mut self) -> Option<Entry> { self.0.pop().map(|(_,e)| e) }
        fn len(&self) -> usize { self.0.len() }
    }
    pub struct OrxIndex<const D: usize>(pub orx_priority_queue::DaryHeapOfIndices<usize,Entry,D>);
    impl<const D: usize> Default for OrxIndex<D> {
        fn default() -> Self { Self(orx_priority_queue::DaryHeapOfIndices::with_index_bound(1)) }
    }
    impl<const D: usize> ReadyQueue for OrxIndex<D> {
        fn with_bound(bound: usize) -> Self { Self(orx_priority_queue::DaryHeapOfIndices::with_index_bound(bound.max(1))) }
        fn upsert(&mut self, e: Entry) {
            if e.id >= self.0.index_bound() {
                let bound = (e.id + 1).next_power_of_two();
                let mut next = orx_priority_queue::DaryHeapOfIndices::with_index_bound(bound);
                while let Some((id,rank)) = self.0.pop() { next.push(id,rank); }
                self.0 = next;
            }
            self.0.update_key_or_push(&e.id, e);
        }
        fn remove(&mut self, id: usize) {
            if id < self.0.index_bound() && self.0.contains(&id) { self.0.remove(&id); }
        }
        fn peek(&mut self) -> Option<Entry> { self.0.peek().map(|(_,e)| *e) }
        fn pop(&mut self) -> Option<Entry> { self.0.pop().map(|(_,e)| e) }
        fn len(&self) -> usize { self.0.len() }
    }
}
