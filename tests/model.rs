//! Independent declarative delivery oracle: no production queues, counters,
//! indexes, slot recycling or Mailbox methods are used to derive expected state.
use rill_delivery::queue::*;
use rill_delivery::*;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Datum {
    revision: u64,
    payload: u64,
    bytes: u32,
    due: u64,
    order: u64,
    age: u64,
}
#[derive(Default)]
struct Record {
    latest: u64,
    acked: Option<u64>,
    pending: Option<Datum>,
    flight: Option<(u64, Datum)>,
}
#[derive(Default)]
struct Model {
    records: BTreeMap<u64, Record>,
    events: VecDeque<(Datum, Option<u64>)>,
    serial: u64,
}
impl Model {
    fn bytes(&self) -> u64 {
        self.records
            .values()
            .map(|r| {
                r.pending.as_ref().map_or(0, |v| u64::from(v.bytes))
                    + r.flight.as_ref().map_or(0, |(_, v)| u64::from(v.bytes))
            })
            .sum::<u64>()
            + self
                .events
                .iter()
                .map(|(v, _)| u64::from(v.bytes))
                .sum::<u64>()
    }
    fn offer(
        &mut self,
        key: u64,
        revision: u64,
        payload: u64,
        bytes: u32,
        delay: u16,
        now: u64,
    ) -> Result<Offer, Error> {
        if self.records.get(&key).is_some_and(|r| revision <= r.latest) {
            return Ok(Offer::Stale);
        }
        if !self.records.contains_key(&key) && self.records.len() == 7 {
            return Err(Error::Capacity);
        }
        let pending = self.records.get(&key).and_then(|r| r.pending.as_ref());
        if self.bytes() - pending.map_or(0, |v| u64::from(v.bytes)) + u64::from(bytes) > 150 {
            return Err(Error::Capacity);
        }
        self.serial += 1;
        let (due, order, age, outcome) = if let Some(old) = pending {
            let rank = (old.due, old.order).min((now + u64::from(delay), self.serial));
            (rank.0, rank.1, old.age, Offer::Replaced)
        } else {
            (now + u64::from(delay), self.serial, now, Offer::Inserted)
        };
        let r = self.records.entry(key).or_default();
        r.latest = revision;
        r.pending = Some(Datum {
            revision,
            payload,
            bytes,
            due,
            order,
            age,
        });
        Ok(outcome)
    }
    fn event(&mut self, payload: u64, bytes: u32, delay: u16, now: u64) -> Result<u64, Error> {
        if self.events.len() == 4 || self.bytes() + u64::from(bytes) > 150 {
            return Err(Error::Capacity);
        }
        self.serial += 1;
        self.events.push_back((
            Datum {
                revision: self.serial,
                payload,
                bytes,
                due: now + u64::from(delay),
                order: self.serial,
                age: now,
            },
            None,
        ));
        Ok(self.serial)
    }
    fn drain(&mut self, mut budget: u64, count: usize) -> Vec<(Kind<u64>, Datum, u64)> {
        let mut out = Vec::new();
        for _ in 0..count {
            // Recompute the set of fitting candidates from the logical contents.
            let state = self
                .records
                .iter()
                .filter(|(_, r)| r.flight.is_none())
                .filter_map(|(&k, r)| {
                    r.pending
                        .as_ref()
                        .filter(|v| u64::from(v.bytes) <= budget)
                        .map(|v| (k, v.clone()))
                })
                .min_by_key(|(_, v)| (v.due, v.order));
            let event = self
                .events
                .front()
                .filter(|(v, f)| f.is_none() && u64::from(v.bytes) <= budget)
                .map(|(v, _)| v.clone());
            let event_first = event.as_ref().is_some_and(|e| {
                state
                    .as_ref()
                    .is_none_or(|(_, s)| (e.due, e.order) < (s.due, s.order))
            });
            let (kind, value) = if event_first {
                let v = event.unwrap();
                (
                    Kind::Event {
                        sequence: v.revision,
                    },
                    v,
                )
            } else if let Some((key, value)) = state {
                (
                    Kind::State {
                        key,
                        revision: value.revision,
                    },
                    value,
                )
            } else {
                break;
            };
            self.serial += 1;
            let token = self.serial;
            match kind {
                Kind::Event { .. } => self.events.front_mut().unwrap().1 = Some(token),
                Kind::State { key, .. } => {
                    let r = self.records.get_mut(&key).unwrap();
                    r.pending = None;
                    r.flight = Some((token, value.clone()));
                }
            }
            budget -= u64::from(value.bytes);
            out.push((kind, value, token));
        }
        out
    }
    fn settle(&mut self, token: u64, retry: bool) -> bool {
        if self.events.front().is_some_and(|(_, f)| *f == Some(token)) {
            if retry {
                self.events.front_mut().unwrap().1 = None;
            } else {
                self.events.pop_front();
            }
            return true;
        }
        for r in self.records.values_mut() {
            if r.flight.as_ref().is_some_and(|(f, _)| *f == token) {
                let (_, old) = r.flight.take().unwrap();
                if retry {
                    if let Some(p) = &mut r.pending {
                        (p.due, p.order) = (p.due, p.order).min((old.due, old.order));
                        p.age = p.age.min(old.age);
                    } else {
                        r.pending = Some(old);
                    }
                } else {
                    r.acked = Some(old.revision);
                }
                return true;
            }
        }
        false
    }
}

fn prove<Q: ReadyQueue>(seed: u64) {
    let limits = Limits {
        states: 7,
        events: 4,
        bytes: 150,
        max_message_bytes: 150,
    };
    let mut actual = Mailbox::<u64, u64, Q>::new(limits).unwrap();
    let mut model = Model::default();
    let mut rng = seed;
    let mut random = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut receipts = Vec::new();
    let mut out = Vec::new();
    for now in 0..30000 {
        let n = random();
        let key = (n >> 8) % 10;
        match n % 9 {
            0..=3 => {
                let rev = random() % (now + 1);
                let cost = (random() % 40 + 1) as u32;
                let delay = (random() % 20) as u16;
                let message = Message::new(Arc::new(n), cost).unwrap();
                assert_eq!(
                    actual.offer_state(key, rev, message, delay, now),
                    model.offer(key, rev, n, cost, delay, now)
                );
            }
            4 => {
                let cost = (random() % 40 + 1) as u32;
                let delay = (random() % 20) as u16;
                assert_eq!(
                    actual.offer_event(Message::new(Arc::new(n), cost).unwrap(), delay, now),
                    model.event(n, cost, delay, now)
                );
            }
            5 => {
                assert_eq!(
                    actual.forget_state(&key),
                    model.records.remove(&key).is_some()
                );
            }
            6 => {
                let budget = random() % 160;
                let count = (random() % 10) as usize;
                let expected = model.drain(budget, count);
                let used = actual.drain_into(now, budget, count, &mut out).unwrap();
                assert_eq!(
                    used,
                    expected
                        .iter()
                        .map(|(_, v, _)| u64::from(v.bytes))
                        .sum::<u64>()
                );
                assert_eq!(out.len(), expected.len());
                for (d, (kind, v, token)) in out.drain(..).zip(expected) {
                    assert_eq!(d.kind, kind);
                    assert_eq!(*d.message.payload, v.payload);
                    assert_eq!(d.message.bytes(), v.bytes);
                    assert_eq!(d.queued_at, v.age);
                    receipts.push((d.receipt, token));
                }
            }
            _ => {
                if !receipts.is_empty() {
                    let i = random() as usize % receipts.len();
                    let (receipt, token) = receipts[i];
                    let retry = n & 16 != 0;
                    assert_eq!(
                        if retry {
                            actual.retry(receipt)
                        } else {
                            actual.ack(receipt)
                        },
                        model.settle(token, retry)
                    );
                    if n & 32 == 0 {
                        receipts.swap_remove(i);
                    }
                }
            }
        }
        assert_eq!(
            actual.buffered_bytes(),
            model.bytes(),
            "seed={seed} step={now}"
        );
        assert_eq!(actual.state_count(), model.records.len());
        assert_eq!(actual.event_count(), model.events.len());
        for k in 0..10 {
            assert_eq!(
                actual.progress(&k),
                model.records.get(&k).map(|r| Progress {
                    latest: r.latest,
                    acknowledged: r.acked,
                    pending: r.pending.as_ref().map(|v| v.revision),
                    in_flight: r.flight.as_ref().map(|(_, v)| v.revision)
                })
            );
        }
    }
}
#[test]
fn independent_model_heap() {
    for seed in [19, 71923, 982451653] {
        prove::<IndexedHeap<4>>(seed);
    }
}
#[test]
fn independent_model_adaptive() {
    for seed in [31, 61937, 961748941] {
        prove::<AdaptiveQueue<8>>(seed);
    }
}
#[test]
fn independent_model_switching() {
    for seed in [41, 71923, 961748941] {
        prove::<SwitchingQueue<8, 32>>(seed);
    }
}
