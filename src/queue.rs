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
    fn with_bound(_bound: usize) -> Self {
        Self::default()
    }
    fn upsert(&mut self, entry: Entry);
    fn remove(&mut self, id: usize);
    fn peek(&mut self) -> Option<Entry>;
    fn pop(&mut self) -> Option<Entry>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Packed indexed d-ary heap: bounded by the high-water slot count, no tombstones.
pub struct IndexedHeap<const D: usize> {
    heap: Vec<Entry>,
    positions: Vec<usize>,
}
impl<const D: usize> Default for IndexedHeap<D> {
    fn default() -> Self {
        assert!(D >= 2);
        Self {
            heap: Vec::new(),
            positions: Vec::new(),
        }
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
            if self.heap[parent] <= self.heap[at] {
                break;
            }
            self.swap(parent, at);
            at = parent;
        }
        at
    }
    fn down(&mut self, mut at: usize) {
        loop {
            let first = at.saturating_mul(D).saturating_add(1);
            if first >= self.heap.len() {
                break;
            }
            let end = first.saturating_add(D).min(self.heap.len());
            let mut best = first;
            for child in first + 1..end {
                if self.heap[child] < self.heap[best] {
                    best = child;
                }
            }
            if self.heap[at] <= self.heap[best] {
                break;
            }
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
        if self.positions.len() <= entry.id {
            self.positions.resize(entry.id + 1, usize::MAX);
        }
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
            if at != usize::MAX {
                self.remove_at(at);
            }
        }
    }
    fn peek(&mut self) -> Option<Entry> {
        self.heap.first().copied()
    }
    fn pop(&mut self) -> Option<Entry> {
        (!self.heap.is_empty()).then(|| self.remove_at(0))
    }
    fn len(&self) -> usize {
        self.heap.len()
    }
}

/// Unsorted packed vector. Cheap updates and linear minimum selection.
#[derive(Default)]
pub struct ScanQueue {
    entries: Vec<Entry>,
    positions: Vec<usize>,
}
impl ReadyQueue for ScanQueue {
    fn upsert(&mut self, entry: Entry) {
        if self.positions.len() <= entry.id {
            self.positions.resize(entry.id + 1, usize::MAX);
        }
        let at = self.positions[entry.id];
        if at == usize::MAX {
            self.positions[entry.id] = self.entries.len();
            self.entries.push(entry);
        } else {
            self.entries[at] = entry;
        }
    }
    fn remove(&mut self, id: usize) {
        if let Some(&at) = self.positions.get(id) {
            if at != usize::MAX {
                self.entries.swap_remove(at);
                self.positions[id] = usize::MAX;
                if at < self.entries.len() {
                    self.positions[self.entries[at].id] = at;
                }
            }
        }
    }
    fn peek(&mut self) -> Option<Entry> {
        self.entries.iter().min().copied()
    }
    fn pop(&mut self) -> Option<Entry> {
        let e = self.peek()?;
        self.remove(e.id);
        Some(e)
    }
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Packed scan with a cached minimum. Non-minimum updates keep the cache valid;
/// a peek followed by a pop performs only one minimum search.
#[derive(Default)]
pub struct CachedScan {
    inner: ScanQueue,
    minimum: Option<Entry>,
    valid: bool,
}
impl ReadyQueue for CachedScan {
    fn upsert(&mut self, e: Entry) {
        let old = self
            .inner
            .positions
            .get(e.id)
            .and_then(|&p| self.inner.entries.get(p))
            .copied();
        self.inner.upsert(e);
        if self.valid {
            if self.minimum.is_none_or(|m| e < m) {
                self.minimum = Some(e);
            } else if old == self.minimum && old != Some(e) {
                self.valid = false;
            }
        }
    }
    fn remove(&mut self, id: usize) {
        self.inner.remove(id);
        if self.minimum.is_some_and(|e| e.id == id) {
            self.valid = false;
        }
    }
    fn peek(&mut self) -> Option<Entry> {
        if !self.valid {
            self.minimum = self.inner.peek();
            self.valid = true;
        }
        self.minimum
    }
    fn pop(&mut self) -> Option<Entry> {
        let e = self.peek()?;
        self.remove(e.id);
        Some(e)
    }
    fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Binary tournament tree over slot indices. Updating an entry stops at the first
/// unchanged subtree minimum. Capacity grows geometrically with actual slot use.
pub struct Tournament {
    tree: Vec<Option<Entry>>,
    bound: usize,
    live: usize,
}
impl Default for Tournament {
    fn default() -> Self {
        Self {
            tree: vec![None; 2],
            bound: 1,
            live: 0,
        }
    }
}
fn minimum(a: Option<Entry>, b: Option<Entry>) -> Option<Entry> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, None) => a,
        (None, b) => b,
    }
}
impl Tournament {
    fn set(&mut self, id: usize, value: Option<Entry>) {
        if id >= self.bound {
            return;
        }
        let mut at = self.bound + id;
        self.tree[at] = value;
        while at > 1 {
            at /= 2;
            let next = minimum(self.tree[at * 2], self.tree[at * 2 + 1]);
            if next == self.tree[at] {
                break;
            }
            self.tree[at] = next;
        }
    }
}
impl ReadyQueue for Tournament {
    fn upsert(&mut self, e: Entry) {
        if e.id >= self.bound {
            let bound = (e.id + 1).next_power_of_two();
            let mut next = vec![None; bound * 2];
            next[bound..bound + self.bound].copy_from_slice(&self.tree[self.bound..self.bound * 2]);
            for at in (1..bound).rev() {
                next[at] = minimum(next[at * 2], next[at * 2 + 1]);
            }
            self.tree = next;
            self.bound = bound;
        }
        if self.tree[self.bound + e.id].is_none() {
            self.live += 1;
        }
        self.set(e.id, Some(e));
    }
    fn remove(&mut self, id: usize) {
        if id < self.bound && self.tree[self.bound + id].is_some() {
            self.live -= 1;
            self.set(id, None);
        }
    }
    fn peek(&mut self) -> Option<Entry> {
        self.tree[1]
    }
    fn pop(&mut self) -> Option<Entry> {
        let e = self.tree[1]?;
        self.remove(e.id);
        Some(e)
    }
    fn len(&self) -> usize {
        self.live
    }
}

/// Cached scans for small queues, with one-way promotion to a 4-ary heap.
pub struct CachedHybrid<const N: usize> {
    scan: CachedScan,
    heap: Option<IndexedHeap<4>>,
}
impl<const N: usize> Default for CachedHybrid<N> {
    fn default() -> Self {
        Self {
            scan: CachedScan::default(),
            heap: None,
        }
    }
}
impl<const N: usize> ReadyQueue for CachedHybrid<N> {
    fn upsert(&mut self, e: Entry) {
        if let Some(h) = &mut self.heap {
            h.upsert(e);
            return;
        }
        self.scan.upsert(e);
        if self.scan.len() > N {
            let mut heap = IndexedHeap::default();
            for &entry in &self.scan.inner.entries {
                heap.upsert(entry);
            }
            self.scan = CachedScan::default();
            self.heap = Some(heap);
        }
    }
    fn remove(&mut self, id: usize) {
        if let Some(h) = &mut self.heap {
            h.remove(id)
        } else {
            self.scan.remove(id)
        }
    }
    fn peek(&mut self) -> Option<Entry> {
        if let Some(h) = &mut self.heap {
            h.peek()
        } else {
            self.scan.peek()
        }
    }
    fn pop(&mut self) -> Option<Entry> {
        if let Some(h) = &mut self.heap {
            h.pop()
        } else {
            self.scan.pop()
        }
    }
    fn len(&self) -> usize {
        if let Some(h) = &self.heap {
            h.len()
        } else {
            self.scan.len()
        }
    }
}

/// Keep batch-friendly sorted runs until the observed mutation pattern becomes
/// interleaved. Small sets use a cached scan. A small mutation batch after an
/// existing sorted run promotes once to a heap; promotion never changes ordering.
/// This policy uses observed queue operations, not application/workload names.
pub struct AdaptiveQueue<const N: usize = 8> {
    scan: CachedScan,
    sorted: Vec<Entry>,
    valid: bool,
    used_sorted: bool,
    mutations: usize,
    heap: Option<IndexedHeap<4>>,
}
impl<const N: usize> Default for AdaptiveQueue<N> {
    fn default() -> Self {
        Self {
            scan: CachedScan::default(),
            sorted: Vec::new(),
            valid: false,
            used_sorted: false,
            mutations: 0,
            heap: None,
        }
    }
}
impl<const N: usize> ReadyQueue for AdaptiveQueue<N> {
    fn upsert(&mut self, e: Entry) {
        if let Some(h) = &mut self.heap {
            h.upsert(e);
            return;
        }
        if self
            .scan
            .inner
            .positions
            .get(e.id)
            .and_then(|&p| self.scan.inner.entries.get(p))
            == Some(&e)
        {
            return;
        }
        self.scan.upsert(e);
        self.mutations = self.mutations.saturating_add(1);
        self.valid = false;
    }
    fn remove(&mut self, id: usize) {
        if let Some(h) = &mut self.heap {
            h.remove(id);
            return;
        }
        let before = self.scan.len();
        self.scan.remove(id);
        if self.scan.len() != before {
            self.mutations = self.mutations.saturating_add(1);
            self.valid = false;
        }
    }
    fn peek(&mut self) -> Option<Entry> {
        if let Some(h) = &mut self.heap {
            return h.peek();
        }
        if self.scan.len() <= N {
            return self.scan.peek();
        }
        if !self.valid {
            if self.used_sorted && self.mutations > 0 && self.mutations < self.scan.len() / 4 {
                let mut h = IndexedHeap::default();
                for &entry in &self.scan.inner.entries {
                    h.upsert(entry);
                }
                self.scan = CachedScan::default();
                self.sorted = Vec::new();
                self.heap = Some(h);
                return self.heap.as_mut().unwrap().peek();
            }
            self.sorted.clear();
            self.sorted.extend_from_slice(&self.scan.inner.entries);
            self.sorted.sort_unstable_by(|a, b| b.cmp(a));
            self.valid = true;
            self.used_sorted = true;
            self.mutations = 0;
        }
        self.sorted.last().copied()
    }
    fn pop(&mut self) -> Option<Entry> {
        if let Some(h) = &mut self.heap {
            return h.pop();
        }
        if self.scan.len() <= N {
            let e = self.scan.pop();
            if e.is_some() {
                self.valid = false;
            }
            return e;
        }
        self.peek()?;
        if let Some(h) = &mut self.heap {
            return h.pop();
        }
        let e = self.sorted.pop()?;
        self.scan.remove(e.id);
        Some(e)
    }
    fn len(&self) -> usize {
        if let Some(h) = &self.heap {
            h.len()
        } else {
            self.scan.len()
        }
    }
}

/// One packed allocation, with scan/heap hysteresis. A drained large queue returns
/// to the small-queue path without copying entries or discarding reserved storage.
/// LOW < HIGH avoids repeated heapification around a single size boundary.
pub struct SwitchingQueue<const LOW: usize = 8, const HIGH: usize = 32> {
    inner: IndexedHeap<4>,
    heap: bool,
    minimum: Option<Entry>,
    valid: bool,
}
impl<const LOW: usize, const HIGH: usize> Default for SwitchingQueue<LOW, HIGH> {
    fn default() -> Self {
        assert!(LOW < HIGH && HIGH >= 2);
        Self {
            inner: IndexedHeap::default(),
            heap: false,
            minimum: None,
            valid: true,
        }
    }
}
impl<const LOW: usize, const HIGH: usize> SwitchingQueue<LOW, HIGH> {
    fn demote(&mut self) {
        if self.inner.len() <= LOW {
            self.heap = false;
            self.minimum = self.inner.heap.first().copied();
            self.valid = true;
        }
    }
    fn scan_remove(&mut self, id: usize) {
        let Some(&at) = self.inner.positions.get(id) else {
            return;
        };
        if at == usize::MAX {
            return;
        }
        self.inner.heap.swap_remove(at);
        self.inner.positions[id] = usize::MAX;
        if at < self.inner.heap.len() {
            self.inner.positions[self.inner.heap[at].id] = at;
        }
        if self.minimum.is_some_and(|e| e.id == id) {
            self.valid = false;
        }
    }
}
impl<const LOW: usize, const HIGH: usize> ReadyQueue for SwitchingQueue<LOW, HIGH> {
    fn upsert(&mut self, e: Entry) {
        if self.heap {
            self.inner.upsert(e);
            return;
        }
        if self.inner.positions.len() <= e.id {
            self.inner.positions.resize(e.id + 1, usize::MAX);
        }
        let at = self.inner.positions[e.id];
        let old = if at == usize::MAX {
            self.inner.positions[e.id] = self.inner.heap.len();
            self.inner.heap.push(e);
            None
        } else {
            Some(std::mem::replace(&mut self.inner.heap[at], e))
        };
        if self.valid {
            if self.minimum.is_none_or(|m| e < m) {
                self.minimum = Some(e);
            } else if old == self.minimum && old != Some(e) {
                self.valid = false;
            }
        }
        if self.inner.len() >= HIGH {
            for at in (0..=(self.inner.len() - 2) / 4).rev() {
                self.inner.down(at);
            }
            self.heap = true;
        }
    }
    fn remove(&mut self, id: usize) {
        if self.heap {
            self.inner.remove(id);
            self.demote();
        } else {
            self.scan_remove(id);
        }
    }
    fn peek(&mut self) -> Option<Entry> {
        if self.heap {
            return self.inner.peek();
        }
        if !self.valid {
            self.minimum = self.inner.heap.iter().min().copied();
            self.valid = true;
        }
        self.minimum
    }
    fn pop(&mut self) -> Option<Entry> {
        if self.heap {
            let e = self.inner.pop();
            self.demote();
            e
        } else {
            let e = self.peek()?;
            self.scan_remove(e.id);
            Some(e)
        }
    }
    fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Sorted-on-demand batches; mutations invalidate the sorted run.
#[derive(Default)]
pub struct SortedQueue {
    inner: ScanQueue,
    sorted: Vec<Entry>,
    dirty: bool,
}
impl ReadyQueue for SortedQueue {
    fn upsert(&mut self, e: Entry) {
        self.inner.upsert(e);
        self.dirty = true;
    }
    fn remove(&mut self, id: usize) {
        self.inner.remove(id);
        self.dirty = true;
    }
    fn peek(&mut self) -> Option<Entry> {
        if self.dirty {
            self.sorted.clear();
            self.sorted.extend_from_slice(&self.inner.entries);
            self.sorted.sort_unstable_by(|a, b| b.cmp(a));
            self.dirty = false;
        }
        self.sorted.last().copied()
    }
    fn pop(&mut self) -> Option<Entry> {
        let e = self.peek()?;
        self.sorted.pop();
        self.inner.remove(e.id);
        Some(e)
    }
    fn len(&self) -> usize {
        self.inner.len()
    }
}

#[derive(Default)]
pub struct TreeQueue {
    tree: BTreeSet<Entry>,
    slots: Vec<Option<Entry>>,
}
impl ReadyQueue for TreeQueue {
    fn upsert(&mut self, e: Entry) {
        if self.slots.len() <= e.id {
            self.slots.resize(e.id + 1, None);
        }
        if let Some(old) = self.slots[e.id].replace(e) {
            self.tree.remove(&old);
        }
        self.tree.insert(e);
    }
    fn remove(&mut self, id: usize) {
        if let Some(slot) = self.slots.get_mut(id) {
            if let Some(old) = slot.take() {
                self.tree.remove(&old);
            }
        }
    }
    fn peek(&mut self) -> Option<Entry> {
        self.tree.first().copied()
    }
    fn pop(&mut self) -> Option<Entry> {
        let e = self.tree.pop_first()?;
        self.slots[e.id] = None;
        Some(e)
    }
    fn len(&self) -> usize {
        self.tree.len()
    }
}

/// Standard-library heap with bounded lazy deletion. Rebuild at 2*live+32 entries.
/// Compaction is included in every measured lifecycle.
#[derive(Default)]
pub struct LazyHeap {
    heap: BinaryHeap<Reverse<Entry>>,
    slots: Vec<Option<Entry>>,
    live: usize,
}
impl LazyHeap {
    fn compact(&mut self) {
        if self.heap.len() > self.live.saturating_mul(2).saturating_add(32) {
            self.heap = self.slots.iter().flatten().copied().map(Reverse).collect();
        }
    }
}

/// Scan small queues, promote once to an indexed heap when the threshold is
/// exceeded. Promotion cost is included; no per-pop conversion or hidden warmup.
pub struct Hybrid<const N: usize> {
    scan: ScanQueue,
    heap: Option<IndexedHeap<4>>,
}
impl<const N: usize> Default for Hybrid<N> {
    fn default() -> Self {
        Self {
            scan: ScanQueue::default(),
            heap: None,
        }
    }
}
impl<const N: usize> ReadyQueue for Hybrid<N> {
    fn upsert(&mut self, e: Entry) {
        if let Some(h) = &mut self.heap {
            h.upsert(e);
            return;
        }
        self.scan.upsert(e);
        if self.scan.len() > N {
            let mut h = IndexedHeap::default();
            for &entry in &self.scan.entries {
                h.upsert(entry);
            }
            self.scan = ScanQueue::default();
            self.heap = Some(h);
        }
    }
    fn remove(&mut self, id: usize) {
        if let Some(h) = &mut self.heap {
            h.remove(id)
        } else {
            self.scan.remove(id)
        }
    }
    fn peek(&mut self) -> Option<Entry> {
        if let Some(h) = &mut self.heap {
            h.peek()
        } else {
            self.scan.peek()
        }
    }
    fn pop(&mut self) -> Option<Entry> {
        if let Some(h) = &mut self.heap {
            h.pop()
        } else {
            self.scan.pop()
        }
    }
    fn len(&self) -> usize {
        if let Some(h) = &self.heap {
            h.len()
        } else {
            self.scan.len()
        }
    }
}
impl ReadyQueue for LazyHeap {
    fn upsert(&mut self, e: Entry) {
        if self.slots.len() <= e.id {
            self.slots.resize(e.id + 1, None);
        }
        if self.slots[e.id] == Some(e) {
            return;
        }
        if self.slots[e.id].replace(e).is_none() {
            self.live += 1;
        }
        self.heap.push(Reverse(e));
        self.compact();
    }
    fn remove(&mut self, id: usize) {
        if self.slots.get_mut(id).and_then(Option::take).is_some() {
            self.live -= 1;
        }
        self.compact();
    }
    fn peek(&mut self) -> Option<Entry> {
        while let Some(&Reverse(e)) = self.heap.peek() {
            if self.slots[e.id] == Some(e) {
                return Some(e);
            }
            self.heap.pop();
        }
        None
    }
    fn pop(&mut self) -> Option<Entry> {
        let e = self.peek()?;
        self.heap.pop();
        self.slots[e.id] = None;
        self.live -= 1;
        self.compact();
        Some(e)
    }
    fn len(&self) -> usize {
        self.live
    }
}

#[cfg(feature = "comparisons")]
pub mod external {
    use super::*;
    #[derive(Default)]
    pub struct Priority(pub priority_queue::PriorityQueue<usize, Reverse<Entry>>);
    impl ReadyQueue for Priority {
        fn upsert(&mut self, e: Entry) {
            self.0.push(e.id, Reverse(e));
        }
        fn remove(&mut self, id: usize) {
            self.0.remove(&id);
        }
        fn peek(&mut self) -> Option<Entry> {
            self.0.peek().map(|(_, &Reverse(e))| e)
        }
        fn pop(&mut self) -> Option<Entry> {
            self.0.pop().map(|(_, Reverse(e))| e)
        }
        fn len(&self) -> usize {
            self.0.len()
        }
    }
    #[derive(Default)]
    pub struct Keyed(pub keyed_priority_queue::KeyedPriorityQueue<usize, Reverse<Entry>>);
    impl ReadyQueue for Keyed {
        fn upsert(&mut self, e: Entry) {
            self.0.push(e.id, Reverse(e));
        }
        fn remove(&mut self, id: usize) {
            self.0.remove(&id);
        }
        fn peek(&mut self) -> Option<Entry> {
            self.0.peek().map(|(_, &Reverse(e))| e)
        }
        fn pop(&mut self) -> Option<Entry> {
            self.0.pop().map(|(_, Reverse(e))| e)
        }
        fn len(&self) -> usize {
            self.0.len()
        }
    }

    use orx_priority_queue::{PriorityQueue, PriorityQueueDecKey};
    pub struct OrxMap<const D: usize>(pub orx_priority_queue::DaryHeapWithMap<usize, Entry, D>);
    impl<const D: usize> Default for OrxMap<D> {
        fn default() -> Self {
            Self(Default::default())
        }
    }
    impl<const D: usize> ReadyQueue for OrxMap<D> {
        fn upsert(&mut self, e: Entry) {
            self.0.update_key_or_push(&e.id, e);
        }
        fn remove(&mut self, id: usize) {
            if self.0.contains(&id) {
                self.0.remove(&id);
            }
        }
        fn peek(&mut self) -> Option<Entry> {
            self.0.peek().map(|(_, e)| *e)
        }
        fn pop(&mut self) -> Option<Entry> {
            self.0.pop().map(|(_, e)| e)
        }
        fn len(&self) -> usize {
            self.0.len()
        }
    }
    pub struct OrxIndex<const D: usize>(pub orx_priority_queue::DaryHeapOfIndices<usize, Entry, D>);
    impl<const D: usize> Default for OrxIndex<D> {
        fn default() -> Self {
            Self(orx_priority_queue::DaryHeapOfIndices::with_index_bound(1))
        }
    }
    impl<const D: usize> ReadyQueue for OrxIndex<D> {
        fn with_bound(bound: usize) -> Self {
            Self(orx_priority_queue::DaryHeapOfIndices::with_index_bound(
                bound.max(1),
            ))
        }
        fn upsert(&mut self, e: Entry) {
            if e.id >= self.0.index_bound() {
                let bound = (e.id + 1).next_power_of_two();
                let mut next = orx_priority_queue::DaryHeapOfIndices::with_index_bound(bound);
                while let Some((id, rank)) = self.0.pop() {
                    next.push(id, rank);
                }
                self.0 = next;
            }
            self.0.update_key_or_push(&e.id, e);
        }
        fn remove(&mut self, id: usize) {
            if id < self.0.index_bound() && self.0.contains(&id) {
                self.0.remove(&id);
            }
        }
        fn peek(&mut self) -> Option<Entry> {
            self.0.peek().map(|(_, e)| *e)
        }
        fn pop(&mut self) -> Option<Entry> {
            self.0.pop().map(|(_, e)| e)
        }
        fn len(&self) -> usize {
            self.0.len()
        }
    }
}
