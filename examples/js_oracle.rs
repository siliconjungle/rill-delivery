//! Deterministic exact Rust/JavaScript behavior oracle; no serialization dependency.
use rill_delivery::{Kind, Limits, Mailbox, Message};
use std::sync::Arc;
fn rng(n: &mut u32) -> u32 {
    *n ^= *n << 13;
    *n ^= *n >> 17;
    *n ^= *n << 5;
    *n
}
fn main() {
    let mut m = Mailbox::<u32, u32>::new(Limits {
        states: 16,
        events: 8,
        bytes: 256,
        max_message_bytes: 64,
    })
    .unwrap();
    let mut random = 71923u32;
    let mut receipts = Vec::new();
    for step in 0..6000u64 {
        let op = rng(&mut random) % 8;
        let key = rng(&mut random) % 24;
        let rev = u64::from(rng(&mut random) % 2000);
        let bytes = rng(&mut random) % 64 + 1;
        let delay = (rng(&mut random) % 12) as u16;
        let message = Message::new(Arc::new(key), bytes).unwrap();
        let (operation, result) = match op {
            0..=2 => {
                let use_stream = op == 2 && m.stream(&key).is_some();
                let r = if use_stream {
                    m.offer_stream(m.stream(&key).unwrap(), rev, message, delay, step)
                } else {
                    m.offer_state(key, rev, message, delay, step)
                };
                (format!("\"op\":\"offer\",\"stream\":{use_stream},\"key\":{key},\"revision\":{rev},\"bytes\":{bytes},\"delay\":{delay}"),match r{Ok(v)=>format!("\"{}\"",format!("{v:?}").to_lowercase()),Err(e)=>format!("\"error:{e:?}\"")})
            }
            3 => {
                let r = m.offer_event(message, delay, step);
                (
                    format!("\"op\":\"event\",\"key\":{key},\"bytes\":{bytes},\"delay\":{delay}"),
                    match r {
                        Ok(n) => n.to_string(),
                        Err(e) => format!("\"error:{e:?}\""),
                    },
                )
            }
            4 => {
                let budget = u64::from(rng(&mut random) % 100);
                let max = (rng(&mut random) % 8) as usize;
                let mut out = Vec::new();
                m.drain_into(step, budget, max, &mut out).unwrap();
                let mut rows = Vec::new();
                for d in out {
                    let (_, serial, slot, _) = d.receipt.parts();
                    receipts.push(d.receipt);
                    let (kind, k, rev) = match d.kind {
                        Kind::State { key, revision } => (0, u64::from(key), revision),
                        Kind::Event { sequence } => (1, sequence, 0),
                    };
                    rows.push(format!(
                        "[{kind},{k},{rev},{},{},{serial},{slot}]",
                        *d.message.payload, d.queued_at
                    ));
                }
                (
                    format!("\"op\":\"drain\",\"budget\":{budget},\"max\":{max}"),
                    format!("[{}]", rows.join(",")),
                )
            }
            5 | 6 if !receipts.is_empty() => {
                let i = (rng(&mut random) as usize) % receipts.len();
                let retry = op == 6;
                let r = if retry {
                    m.retry(receipts[i])
                } else {
                    m.ack(receipts[i])
                };
                (
                    format!(
                        "\"op\":\"{}\",\"index\":{i}",
                        if retry { "retry" } else { "ack" }
                    ),
                    r.to_string(),
                )
            }
            7 => (
                format!("\"op\":\"forget\",\"key\":{key}"),
                m.forget_state(&key).to_string(),
            ),
            _ => ("\"op\":\"noop\"".into(), "null".into()),
        };
        let progress = (0..24)
            .map(|k| match m.progress(&k) {
                None => "null".into(),
                Some(p) => format!(
                    "[{},{},{},{}]",
                    p.latest,
                    p.acknowledged.map_or("null".into(), |v| v.to_string()),
                    p.pending.map_or("null".into(), |v| v.to_string()),
                    p.in_flight.map_or("null".into(), |v| v.to_string())
                ),
            })
            .collect::<Vec<_>>()
            .join(",");
        println!("{{{operation},\"now\":{step},\"result\":{result},\"progress\":[{progress}],\"buffered\":{},\"ready\":{}}}",m.buffered_bytes(),m.has_ready());
    }
}
