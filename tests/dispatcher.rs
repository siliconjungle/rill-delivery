use rill_delivery::*;
use std::sync::Arc;

fn message(v: u64, bytes: u32) -> Message<u64> {
    Message::new(Arc::new(v), bytes).unwrap()
}
fn config() -> PeerConfig {
    PeerConfig {
        limits: Limits {
            states: 64,
            events: 8,
            bytes: 4096,
            max_message_bytes: 256,
        },
        burst: 4096,
        bytes_per_tick: 4096,
        quantum: 4096,
    }
}
fn budget(bytes: u64) -> DispatchBudget {
    DispatchBudget {
        bytes,
        per_peer_bytes: 4096,
        deliveries: 100000,
        visits: 20000,
    }
}

#[test]
fn twenty_thousand_recipients_converge_with_exact_limits() {
    let mut d = Dispatcher::<u64, u64>::new(20000, 1 << 26).unwrap();
    let mut cfg = config();
    cfg.quantum = 8;
    let peers: Vec<_> = (0..20000).map(|_| d.add_peer(cfg).unwrap()).collect();
    let shared = message(123, 16);
    for &peer in &peers {
        d.offer_state(peer, 11, 1, shared.clone(), 0, 0).unwrap();
    }
    assert_eq!(d.buffered_bytes(), 320000);
    let mut out = Vec::new();
    let first = d.dispatch_into(0, budget(320000), &mut out).unwrap();
    assert_eq!(first.bytes, 0);
    assert_eq!(first.visits, 20000);
    let second = d.dispatch_into(1, budget(320000), &mut out).unwrap();
    assert_eq!(second.bytes, 320000);
    assert_eq!(out.len(), 20000);
    assert_eq!(d.ready_peers(), 0);
    // In-flight payloads remain charged until actual ACK.
    assert_eq!(d.buffered_bytes(), 320000);
    for e in out.drain(..) {
        assert!(d.ack(e.peer, e.delivery.receipt));
    }
    assert_eq!(d.buffered_bytes(), 0);
    for peer in peers {
        assert_eq!(d.progress(peer, &11).unwrap().acknowledged, Some(1));
    }
}

#[test]
fn byte_credit_weights_and_rotation_avoid_starvation() {
    let mut d = Dispatcher::<u64, u64>::new(2, 4096).unwrap();
    let mut cfg = config();
    cfg.quantum = 1;
    let a = d.add_peer(cfg).unwrap();
    cfg.quantum = 3;
    let b = d.add_peer(cfg).unwrap();
    for peer in [a, b] {
        for key in 0..64 {
            d.offer_state(peer, key, 1, message(key, 1), 0, 0).unwrap();
        }
    }
    let mut counts = [0, 0];
    let mut out = Vec::new();
    for tick in 0..8 {
        let result = d.dispatch_into(tick, budget(4), &mut out).unwrap();
        assert_eq!(result.bytes, 4);
        for e in out.drain(..) {
            counts[usize::from(e.peer == b)] += 1;
            assert!(d.ack(e.peer, e.delivery.receipt));
        }
    }
    assert_eq!(counts, [8, 24]);
}

#[test]
fn removal_unlinks_active_peers_and_reconnect_rejects_old_handles() {
    let mut d = Dispatcher::<u64, u64>::new(3, 4096).unwrap();
    let a = d.add_peer(config()).unwrap();
    let b = d.add_peer(config()).unwrap();
    let c = d.add_peer(config()).unwrap();
    for peer in [a, b, c] {
        d.offer_state(peer, 1, 1, message(1, 16), 0, 0).unwrap();
    }
    assert!(d.remove_peer(b));
    assert_eq!(d.ready_peers(), 2);
    assert_eq!(d.buffered_bytes(), 32);
    let new = d.add_peer(config()).unwrap();
    assert_ne!(new, b);
    assert_eq!(
        d.offer_state(b, 1, 1, message(1, 1), 0, 0),
        Err(Error::UnknownPeer)
    );
    d.offer_state(new, 1, 1, message(1, 16), 0, 0).unwrap();
    assert!(d.remove_peer(a));
    assert!(d.remove_peer(new));
    let mut out = Vec::new();
    d.dispatch_into(0, budget(4096), &mut out).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].peer, c);
    assert!(!d.ack(new, out[0].delivery.receipt));
    assert!(d.ack(c, out[0].delivery.receipt));
    assert_eq!(d.buffered_bytes(), 0);
}

#[test]
fn global_capacity_is_atomic_and_backwards_time_is_rejected() {
    let mut d = Dispatcher::<u64, u64>::new(2, 100).unwrap();
    let a = d.add_peer(config()).unwrap();
    let b = d.add_peer(config()).unwrap();
    d.offer_state(a, 1, 1, message(1, 60), 0, 0).unwrap();
    assert_eq!(
        d.offer_state(b, 1, 1, message(1, 41), 0, 0),
        Err(Error::Capacity)
    );
    assert_eq!(d.progress(b, &1), None);
    assert_eq!(d.buffered_bytes(), 60);
    d.offer_state(a, 1, 2, message(2, 80), 0, 1).unwrap();
    assert_eq!(d.buffered_bytes(), 80);
    assert_eq!(
        d.offer_state(a, 1, 1, message(1, 100), 0, 1),
        Ok(Offer::Stale)
    );
    assert_eq!(d.offer_event(b, message(1, 21), 0, 1), Err(Error::Capacity));
    assert_eq!(
        d.offer_event(b, message(1, 1), 0, 0),
        Err(Error::ClockWentBack)
    );
    assert_eq!(
        d.dispatch_into(0, budget(100), &mut Vec::new()),
        Err(Error::ClockWentBack)
    );
}

#[test]
fn slow_peer_cannot_consume_fast_peers_budget_and_acks_wake_pending_state() {
    let mut d = Dispatcher::<u64, u64>::new(2, 4096).unwrap();
    let mut cfg = config();
    cfg.limits.max_message_bytes = 16;
    cfg.burst = 16;
    cfg.bytes_per_tick = 0;
    let slow = d.add_peer(cfg).unwrap();
    let fast = d.add_peer(config()).unwrap();
    let mut out = Vec::new();
    for peer in [slow, fast] {
        d.offer_state(peer, 1, 1, message(1, 16), 0, 0).unwrap();
    }
    d.dispatch_into(0, budget(32), &mut out).unwrap();
    for e in out.drain(..) {
        d.offer_state(e.peer, 1, 2, message(2, 16), 0, 1).unwrap();
        assert!(d.ack(e.peer, e.delivery.receipt));
    }
    d.dispatch_into(1, budget(32), &mut out).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].peer, fast);
    assert_eq!(d.progress(slow, &1).unwrap().pending, Some(2));
}

#[test]
fn token_bucket_handles_maximum_integer_refill_without_overflow() {
    let mut b = TokenBucket::new(u64::MAX, u64::MAX, 0).unwrap();
    assert!(b.consume(u64::MAX));
    assert!(!b.consume(1));
    assert_eq!(b.available(u64::MAX).unwrap(), u64::MAX);
    assert_eq!(b.available(0), Err(Error::ClockWentBack));
}

#[test]
fn prepared_global_admission_and_reconnect_are_atomic() {
    let mut d = Dispatcher::<u64, u64>::new(2, 100).unwrap();
    let a = d.add_peer(config()).unwrap();
    let b = d.add_peer(config()).unwrap();
    d.offer_state(a, 1, 1, message(1, 49), 0, 0).unwrap();
    d.offer_state(b, 1, 1, message(1, 49), 0, 0).unwrap();
    let stream = d.stream(a, &1).unwrap();
    assert_eq!(
        d.offer_stream(a, stream, 2, message(2, 52), 0, 1),
        Err(Error::Capacity)
    );
    assert_eq!(d.progress(a, &1).unwrap().latest, 1);
    assert_eq!(d.buffered_bytes(), 98);
    d.offer_stream(a, stream, 2, message(2, 51), 0, 1).unwrap();
    assert_eq!(d.buffered_bytes(), 100);
    assert_eq!(
        d.offer_stream(b, stream, 2, message(2, 1), 0, 1),
        Err(Error::UnknownStream)
    );
    assert!(d.remove_peer(a));
    let fresh = d.add_peer(config()).unwrap();
    assert_eq!(
        d.offer_stream(fresh, stream, 3, message(3, 1), 0, 1),
        Err(Error::UnknownStream)
    );
}
