//! One recipient's bounded delivery state machine.
use crate::queue::{Entry, IndexedHeap, Rank, ReadyQueue};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::{atomic::{AtomicU64, Ordering}, Arc};

static NEXT_MAILBOX: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error { InvalidConfig, Capacity, MessageTooLarge, ClockWentBack, Exhausted }

/// Logical wire-byte bounds, including both pending and unacknowledged messages.
/// Payload allocation size is application-owned; bytes must include framing overhead.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub states: usize,
    pub events: usize,
    pub bytes: u64,
    pub max_message_bytes: u32,
}
impl Default for Limits {
    fn default() -> Self { Self { states: 1024, events: 256, bytes: 1 << 20, max_message_bytes: 64 << 10 } }
}

/// Immutable shared payload plus its conservatively estimated wire cost.
/// Sharing is opt-in: construct once and clone across recipients.
pub struct Message<T> { pub payload: Arc<T>, bytes: u32 }
impl<T> Clone for Message<T> {
    fn clone(&self) -> Self { Self { payload: self.payload.clone(), bytes: self.bytes } }
}
impl<T> Message<T> {
    pub fn new(payload: Arc<T>, bytes: u32) -> Result<Self, Error> {
        if bytes == 0 { return Err(Error::InvalidConfig); }
        Ok(Self { payload, bytes })
    }
    pub fn bytes(&self) -> u32 { self.bytes }
}

/// Opaque exact attempt identity. Tokens from another mailbox, a canceled record,
/// or an earlier retry cannot acknowledge a new attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Receipt { mailbox: u64, serial: u64, slot: usize, event: bool }

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Kind<K> { State { key: K, revision: u64 }, Event { sequence: u64 } }

pub struct Delivery<K, T> {
    pub receipt: Receipt,
    pub kind: Kind<K>,
    pub message: Message<T>,
    pub queued_at: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Offer { Inserted, Replaced, Stale }

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Progress {
    pub latest: u64,
    pub acknowledged: Option<u64>,
    pub pending: Option<u64>,
    pub in_flight: Option<u64>,
}

struct Value<T> { revision: u64, rank: Rank, queued_at: u64, message: Message<T> }
struct Flight<T> { receipt: Receipt, value: Value<T> }
struct State<K,T> {
    key: K, latest: u64, acknowledged: Option<u64>, pending: Option<Value<T>>, flight: Option<Flight<T>>,
}
struct Event<T> { value: Value<T>, flight: Option<Receipt> }

/// Generic keys and payloads; the ready-queue structure is independently selectable.
/// All methods are synchronous. Shard mailboxes across threads rather than locking
/// a global queue in the core. A state key is a stream of *replacement snapshots*,
/// not a chain of patches that depend on skipped revisions.
pub struct Mailbox<K,T,Q: ReadyQueue = IndexedHeap<4>> {
    id: u64,
    limits: Limits,
    keys: HashMap<K,usize>,
    slots: Vec<Option<State<K,T>>>,
    free: Vec<usize>,
    events: VecDeque<Event<T>>,
    ready: Q,
    deferred: Vec<Entry>,
    bytes: u64,
    serial: u64,
    now: u64,
}

impl<K: Eq + Hash + Clone,T,Q: ReadyQueue> Mailbox<K,T,Q> {
    pub fn new(limits: Limits) -> Result<Self, Error> {
        if limits.bytes == 0 || limits.max_message_bytes == 0 || u64::from(limits.max_message_bytes) > limits.bytes {
            return Err(Error::InvalidConfig);
        }
        let id = NEXT_MAILBOX.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| Error::Exhausted)?;
        Ok(Self { id, limits, keys: HashMap::new(), slots: Vec::new(), free: Vec::new(),
                  events: VecDeque::new(), ready: Q::with_bound(limits.states), deferred: Vec::new(), bytes: 0, serial: 0, now: 0 })
    }
    fn check(&self, now: u64, message: &Message<T>) -> Result<(), Error> {
        if now < self.now { return Err(Error::ClockWentBack); }
        if message.bytes > self.limits.max_message_bytes { return Err(Error::MessageTooLarge); }
        Ok(())
    }
    fn next(&self) -> Result<u64, Error> { self.serial.checked_add(1).ok_or(Error::Exhausted) }
    fn rank(&self, now: u64, priority_delay: u16, order: u64) -> Result<Rank, Error> {
        Ok(Rank { due: now.checked_add(u64::from(priority_delay)).ok_or(Error::Exhausted)?, order })
    }

    /// Retain only the latest unsent version; an older in-flight version remains
    /// until exact ACK or retry. Replacements preserve age and can promote urgency.
    /// Admission failure leaves existing queued messages and revision watermarks intact.
    pub fn offer_state(&mut self, key: K, revision: u64, message: Message<T>, priority_delay: u16, now: u64) -> Result<Offer, Error> {
        self.check(now, &message)?;
        let index = self.keys.get(&key).copied();
        if let Some(i) = index {
            if revision <= self.slots[i].as_ref().unwrap().latest { return Ok(Offer::Stale); }
        } else if self.keys.len() >= self.limits.states { return Err(Error::Capacity); }
        let old_bytes = index.and_then(|i| self.slots[i].as_ref()?.pending.as_ref()).map_or(0, |p| u64::from(p.message.bytes));
        let total = (self.bytes - old_bytes).checked_add(u64::from(message.bytes)).ok_or(Error::Capacity)?;
        if total > self.limits.bytes { return Err(Error::Capacity); }
        let serial = self.next()?;
        let rank = self.rank(now, priority_delay, serial)?;
        let mut value = Value { revision, rank, queued_at: now, message };
        let outcome;
        let i = if let Some(i) = index {
            let state = self.slots[i].as_mut().unwrap();
            if let Some(old) = &state.pending {
                value.rank = old.rank.min(rank);
                value.queued_at = old.queued_at;
                outcome = Offer::Replaced;
            } else { outcome = Offer::Inserted; }
            state.pending = Some(value);
            state.latest = revision;
            i
        } else {
            let i = self.free.pop().unwrap_or(self.slots.len());
            let state = State { key: key.clone(), latest: revision, acknowledged: None, pending: Some(value), flight: None };
            if i == self.slots.len() { self.slots.push(Some(state)); } else { self.slots[i] = Some(state); }
            self.keys.insert(key, i);
            outcome = Offer::Inserted;
            i
        };
        let state = self.slots[i].as_ref().unwrap();
        if state.flight.is_none() { self.ready.upsert(Entry { id: i, rank: state.pending.as_ref().unwrap().rank }); }
        self.bytes = total; self.serial = serial; self.now = now;
        Ok(outcome)
    }

    /// FIFO events are never coalesced or silently dropped. There is one event
    /// attempt in flight at a time; ACK it before handing off its successor.
    pub fn offer_event(&mut self, message: Message<T>, priority_delay: u16, now: u64) -> Result<u64, Error> {
        self.check(now, &message)?;
        if self.events.len() >= self.limits.events { return Err(Error::Capacity); }
        let total = self.bytes.checked_add(u64::from(message.bytes)).ok_or(Error::Capacity)?;
        if total > self.limits.bytes { return Err(Error::Capacity); }
        let serial = self.next()?;
        let rank = self.rank(now, priority_delay, serial)?;
        self.events.push_back(Event { value: Value { revision: serial, rank, queued_at: now, message }, flight: None });
        self.bytes = total; self.serial = serial; self.now = now;
        Ok(serial)
    }

    /// Append attempts fitting an exact byte budget. Oversized candidates remain
    /// pending while smaller state updates may proceed. No delivery is implicitly
    /// acknowledged: call `ack` after confirmed delivery, or `retry` after failure.
    /// All candidates are eligible immediately; rank expresses urgency and age.
    pub fn drain_into(&mut self, now: u64, budget: u64, max_items: usize, out: &mut Vec<Delivery<K,T>>) -> Result<u64, Error> {
        if now < self.now { return Err(Error::ClockWentBack); }
        // Reserve enough serial space up front, including attempts later skipped.
        self.serial.checked_add(u64::try_from(max_items).map_err(|_| Error::Exhausted)?).ok_or(Error::Exhausted)?;
        self.now = now;
        let mut remaining = budget;
        let mut sent = 0;
        let mut event_blocked = false;
        self.deferred.clear();
        while sent < max_items && remaining > 0 {
            let state = self.ready.peek();
            let event_rank = if event_blocked { None } else {
                self.events.front().filter(|e| e.flight.is_none()).map(|e| e.value.rank)
            };
            let choose_event = event_rank.is_some_and(|r| state.is_none_or(|s| r < s.rank));
            if choose_event {
                let event = self.events.front_mut().unwrap();
                if u64::from(event.value.message.bytes) > remaining { event_blocked = true; continue; }
                self.serial += 1;
                let receipt = Receipt { mailbox: self.id, serial: self.serial, slot: 0, event: true };
                event.flight = Some(receipt);
                remaining -= u64::from(event.value.message.bytes);
                out.push(Delivery { receipt, kind: Kind::Event { sequence: event.value.revision },
                                    message: event.value.message.clone(), queued_at: event.value.queued_at });
            } else if let Some(entry) = state {
                self.ready.pop();
                let slot = self.slots[entry.id].as_mut().unwrap();
                if u64::from(slot.pending.as_ref().unwrap().message.bytes) > remaining {
                    self.deferred.push(entry); continue;
                }
                let value = slot.pending.take().unwrap();
                self.serial += 1;
                let receipt = Receipt { mailbox: self.id, serial: self.serial, slot: entry.id, event: false };
                remaining -= u64::from(value.message.bytes);
                out.push(Delivery { receipt, kind: Kind::State { key: slot.key.clone(), revision: value.revision },
                                    message: value.message.clone(), queued_at: value.queued_at });
                slot.flight = Some(Flight { receipt, value });
            } else { break; }
            sent += 1;
        }
        for entry in self.deferred.drain(..) { self.ready.upsert(entry); }
        Ok(budget - remaining)
    }

    /// Exact ACK. Stale, canceled, duplicated or foreign receipts return false.
    pub fn ack(&mut self, receipt: Receipt) -> bool {
        if receipt.mailbox != self.id { return false; }
        if receipt.event {
            if self.events.front().is_some_and(|e| e.flight == Some(receipt)) {
                self.bytes -= u64::from(self.events.pop_front().unwrap().value.message.bytes);
                return true;
            }
        } else if let Some(Some(state)) = self.slots.get_mut(receipt.slot) {
            if state.flight.as_ref().is_some_and(|f| f.receipt == receipt) {
                let f = state.flight.take().unwrap();
                self.bytes -= u64::from(f.value.message.bytes);
                state.acknowledged = Some(f.value.revision);
                if let Some(value) = &state.pending { self.ready.upsert(Entry { id: receipt.slot, rank: value.rank }); }
                return true;
            }
        }
        false
    }

    /// Return an unacknowledged attempt to ready state. A newer state supersedes
    /// its older failed attempt; events retain exact payload and FIFO position.
    /// The old receipt is immediately invalidated, so late ACKs cannot clear retries.
    pub fn retry(&mut self, receipt: Receipt) -> bool {
        if receipt.mailbox != self.id { return false; }
        if receipt.event {
            if let Some(e) = self.events.front_mut() {
                if e.flight == Some(receipt) { e.flight = None; return true; }
            }
        } else if let Some(Some(state)) = self.slots.get_mut(receipt.slot) {
            if state.flight.as_ref().is_some_and(|f| f.receipt == receipt) {
                let flight = state.flight.take().unwrap();
                if let Some(pending) = &mut state.pending {
                    self.bytes -= u64::from(flight.value.message.bytes);
                    pending.rank = pending.rank.min(flight.value.rank);
                    pending.queued_at = pending.queued_at.min(flight.value.queued_at);
                } else { state.pending = Some(flight.value); }
                self.ready.upsert(Entry { id: receipt.slot, rank: state.pending.as_ref().unwrap().rank });
                return true;
            }
        }
        false
    }

    /// Explicitly cancel a state stream and its pending/in-flight payloads.
    /// Retractions/deletions must themselves be encoded as replacement state if
    /// the remote recipient needs to observe them before forgetting the stream.
    pub fn forget_state(&mut self, key: &K) -> bool {
        let Some(i) = self.keys.remove(key) else { return false; };
        let state = self.slots[i].take().unwrap();
        if let Some(p) = state.pending { self.bytes -= u64::from(p.message.bytes); }
        if let Some(f) = state.flight { self.bytes -= u64::from(f.value.message.bytes); }
        self.ready.remove(i); self.free.push(i); true
    }
    pub fn progress(&self, key: &K) -> Option<Progress> {
        let s = self.slots[*self.keys.get(key)?].as_ref()?;
        Some(Progress { latest: s.latest, acknowledged: s.acknowledged,
                        pending: s.pending.as_ref().map(|p| p.revision), in_flight: s.flight.as_ref().map(|f| f.value.revision) })
    }
    pub fn buffered_bytes(&self) -> u64 { self.bytes }
    pub fn state_count(&self) -> usize { self.keys.len() }
    pub fn event_count(&self) -> usize { self.events.len() }
    pub fn has_ready(&self) -> bool {
        !self.ready.is_empty() || self.events.front().is_some_and(|e| e.flight.is_none())
    }
}
