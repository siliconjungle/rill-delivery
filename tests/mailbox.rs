use rill_delivery::*;
use rill_delivery::queue::*;
use std::sync::Arc;

fn msg(value: u64, bytes: u32) -> Message<u64> { Message::new(Arc::new(value),bytes).unwrap() }
fn limits() -> Limits { Limits { states: 64, events: 16, bytes: 4096, max_message_bytes: 4096 } }

#[test]
fn coalescing_ack_retry_and_slot_reuse() {
    let mut m = Mailbox::<String,u64>::new(limits()).unwrap();
    let key = "arbitrary/application/key".to_owned();
    m.offer_state(key.clone(),1,msg(10,10),20,0).unwrap();
    m.offer_state(key.clone(),2,msg(20,20),30,1).unwrap();
    assert_eq!(m.buffered_bytes(),20);
    let mut out = Vec::new();
    m.drain_into(2,20,1,&mut out).unwrap();
    assert_eq!(out[0].kind,Kind::State { key: key.clone(),revision:2 });
    assert_eq!(m.progress(&key).unwrap().acknowledged,None);
    let old = out.remove(0).receipt;
    m.offer_state(key.clone(),3,msg(30,30),0,3).unwrap();
    assert_eq!(m.buffered_bytes(),50);
    assert!(m.retry(old));
    assert!(!m.ack(old));
    assert_eq!(m.buffered_bytes(),30);
    m.drain_into(4,30,1,&mut out).unwrap();
    assert_eq!(*out[0].message.payload,30);
    assert!(m.ack(out.remove(0).receipt));
    assert_eq!(m.progress(&key).unwrap().acknowledged,Some(3));
    assert_eq!(m.buffered_bytes(),0);
    assert_eq!(m.offer_state(key.clone(),2,msg(20,20),0,5).unwrap(),Offer::Stale);
    m.offer_state(key.clone(),4,msg(40,40),0,6).unwrap();
    m.drain_into(6,40,1,&mut out).unwrap();
    let forgotten = out.remove(0).receipt;
    assert!(m.forget_state(&key));
    m.offer_state(key.clone(),1,msg(1,1),0,7).unwrap();
    m.drain_into(7,40,1,&mut out).unwrap();
    assert!(!m.ack(forgotten));
    assert!(m.ack(out.remove(0).receipt));
    assert_eq!(m.state_count(),1);
}

#[test]
fn exact_budget_skips_large_state_but_events_are_fifo() {
    let mut m = Mailbox::<u64,u64>::new(limits()).unwrap();
    m.offer_state(1,1,msg(1,100),0,0).unwrap();
    m.offer_state(2,1,msg(2,10),10,0).unwrap();
    let first = m.offer_event(msg(3,20),0,0).unwrap();
    let second = m.offer_event(msg(4,1),0,0).unwrap();
    let mut out = Vec::new();
    assert_eq!(m.drain_into(0,11,10,&mut out).unwrap(),10);
    assert_eq!(out.len(),1);
    assert!(matches!(out[0].kind,Kind::State {key:2,..}));
    assert!(m.ack(out.remove(0).receipt));
    m.drain_into(0,20,10,&mut out).unwrap();
    assert_eq!(out[0].kind,Kind::Event { sequence:first });
    let receipt = out.remove(0).receipt;
    assert!(m.retry(receipt));
    assert!(!m.ack(receipt));
    m.drain_into(0,20,10,&mut out).unwrap();
    assert_eq!(out[0].kind,Kind::Event { sequence:first });
    assert!(m.ack(out.remove(0).receipt));
    m.drain_into(0,1,10,&mut out).unwrap();
    assert_eq!(out[0].kind,Kind::Event { sequence:second });
}

#[test]
fn admission_failure_is_atomic_and_counts_inflight() {
    let mut m = Mailbox::<u64,u64>::new(Limits { states:1,events:1,bytes:100,max_message_bytes:100 }).unwrap();
    m.offer_state(1,1,msg(1,70),0,0).unwrap();
    let mut out = Vec::new(); m.drain_into(0,70,1,&mut out).unwrap();
    assert_eq!(m.offer_state(1,2,msg(2,31),0,1),Err(Error::Capacity));
    assert_eq!(m.progress(&1).unwrap().latest,1);
    assert_eq!(m.offer_state(2,1,msg(1,1),0,1),Err(Error::Capacity));
    m.offer_state(1,2,msg(2,30),0,1).unwrap();
    assert_eq!(m.buffered_bytes(),100);
    assert_eq!(m.offer_event(msg(3,1),0,1),Err(Error::Capacity));
    assert!(m.ack(out.remove(0).receipt));
    m.offer_state(1,3,msg(3,100),0,2).unwrap();
    assert_eq!(m.buffered_bytes(),100);
    assert_eq!(m.offer_state(1,4,msg(4,1),0,1),Err(Error::ClockWentBack));
}

#[test]
fn foreign_receipts_and_payload_sharing() {
    let shared = msg(123,20);
    let mut a = Mailbox::<u64,u64>::new(limits()).unwrap();
    let mut b = Mailbox::<u64,u64>::new(limits()).unwrap();
    a.offer_state(1,1,shared.clone(),0,0).unwrap(); b.offer_state(1,1,shared.clone(),0,0).unwrap();
    let mut x = Vec::new(); let mut y = Vec::new();
    a.drain_into(0,20,1,&mut x).unwrap(); b.drain_into(0,20,1,&mut y).unwrap();
    assert!(Arc::ptr_eq(&x[0].message.payload,&y[0].message.payload));
    assert!(!a.ack(y[0].receipt)); assert!(!b.retry(x[0].receipt));
}

#[test]
fn continuous_urgent_updates_do_not_reset_old_age() {
    let mut m = Mailbox::<u64,u64>::new(limits()).unwrap();
    m.offer_state(0,0,msg(0,1),20,0).unwrap();
    let mut out = Vec::new(); let mut observed = false;
    for now in 0..=40 {
        m.offer_state(1,now,msg(now,1),0,now).unwrap();
        m.offer_state(0,now+1,msg(0,1),20,now).unwrap();
        m.drain_into(now,1,1,&mut out).unwrap();
        for d in out.drain(..) {
            if let Kind::State {key:0,..} = d.kind { observed = true; }
            assert!(m.ack(d.receipt));
        }
        if observed { break; }
    }
    assert!(observed);
}

fn randomized_equivalence<Q: ReadyQueue>() {
    // ScanQueue is separately proven against BTreeMap, then used as a delivery
    // differential oracle across arbitrary scheduling structures and lifecycle churn.
    let mut a = Mailbox::<u64,u64,Q>::new(limits()).unwrap();
    let mut b = Mailbox::<u64,u64,ScanQueue>::new(limits()).unwrap();
    let mut random = 412349123u64;
    let mut next = || { random ^= random << 13; random ^= random >> 7; random ^= random << 17; random };
    let mut receipts = Vec::new();
    let mut x = Vec::new(); let mut y = Vec::new();
    for now in 0..20000 {
        let n = next(); let key = (n >> 8) % 80;
        match n % 8 {
            0..=2 => {
                let rev = next() % (now + 1); let bytes = (next() % 300 + 1) as u32; let priority = (next() % 25) as u16;
                assert_eq!(a.offer_state(key,rev,msg(n,bytes),priority,now),b.offer_state(key,rev,msg(n,bytes),priority,now));
            }
            3 => { assert_eq!(a.forget_state(&key),b.forget_state(&key)); }
            4 => {
                let bytes = (next() % 200 + 1) as u32;
                assert_eq!(a.offer_event(msg(n,bytes),0,now),b.offer_event(msg(n,bytes),0,now));
            }
            5 => {
                let budget = next() % 1000; let count = (next() % 20) as usize;
                assert_eq!(a.drain_into(now,budget,count,&mut x),b.drain_into(now,budget,count,&mut y));
                assert_eq!(x.len(),y.len());
                for (dx,dy) in x.drain(..).zip(y.drain(..)) {
                    assert_eq!(dx.kind,dy.kind); assert_eq!(dx.queued_at,dy.queued_at);
                    assert_eq!(*dx.message.payload,*dy.message.payload);
                    receipts.push((dx.receipt,dy.receipt));
                }
            }
            _ => if !receipts.is_empty() {
                let i = next() as usize % receipts.len(); let (ra,rb) = receipts[i];
                if n & 8 == 0 { assert_eq!(a.ack(ra),b.ack(rb)); }
                else { assert_eq!(a.retry(ra),b.retry(rb)); }
                if n & 16 == 0 { receipts.swap_remove(i); }
            }
        }
        assert_eq!(a.buffered_bytes(),b.buffered_bytes()); assert!(a.buffered_bytes() <= limits().bytes);
        assert_eq!(a.event_count(),b.event_count()); assert_eq!(a.state_count(),b.state_count());
        for key in 0..80 { assert_eq!(a.progress(&key),b.progress(&key)); }
    }
}
#[test] fn randomized_indexed() { randomized_equivalence::<IndexedHeap<4>>(); }
#[test] fn randomized_hybrid() { randomized_equivalence::<Hybrid<16>>(); }
#[test] fn randomized_sorted() { randomized_equivalence::<SortedQueue>(); }
#[test] fn randomized_lazy() { randomized_equivalence::<LazyHeap>(); }
#[cfg(feature="comparisons")]
#[test] fn randomized_external() { randomized_equivalence::<external::Priority>(); randomized_equivalence::<external::OrxIndex<4>>(); }
