use crate::{Delivery,Error,Limits,Mailbox,Message,Offer,Progress,Receipt,TokenBucket};
use crate::queue::{IndexedHeap,ReadyQueue};
use std::hash::Hash;

/// A handle to one recipient incarnation. Disconnect/reconnect never reuses its
/// identity, even when a storage slot is recycled or the handle crosses dispatchers.
#[derive(Clone,Copy,Debug,Eq,PartialEq,Hash)]
pub struct Peer { slot: usize, identity: u64 }

#[derive(Clone,Copy,Debug)]
pub struct PeerConfig {
    pub limits: Limits,
    pub burst: u64,
    pub bytes_per_tick: u64,
    /// Bytes of fair-service credit per visit. Different quanta express weights.
    pub quantum: u64,
}
impl Default for PeerConfig {
    fn default() -> Self {
        Self { limits:Limits::default(), burst:1<<20, bytes_per_tick:64<<10, quantum:16<<10 }
    }
}

#[derive(Clone,Copy,Debug)]
pub struct DispatchBudget {
    pub bytes: u64,
    pub per_peer_bytes: u64,
    pub deliveries: usize,
    pub visits: usize,
}
#[derive(Clone,Copy,Debug,Eq,PartialEq)]
pub struct DispatchStats { pub bytes:u64, pub deliveries:usize, pub visits:usize, pub ready_peers:usize }
pub struct Envelope<K,T> { pub peer:Peer, pub delivery:Delivery<K,T> }

struct Recipient<K,T,Q:ReadyQueue> {
    mailbox:Mailbox<K,T,Q>, bucket:TokenBucket, quantum:u64, deficit:u64,
}
struct Slot<K,T,Q:ReadyQueue> {
    value:Option<Recipient<K,T,Q>>, identity:u64,
    // Intrusive active list: disconnect is O(1) and cannot leave stale queue entries.
    prev:Option<usize>, next:Option<usize>, active:bool,
}

/// Bounded multi-recipient scheduler with an intrusive active list and byte-credit
/// fairness. Each call visits a recipient at most once; inactive recipients consume
/// no polling work. Rate-limited active recipients can still consume one visit.
pub struct Dispatcher<K,T,Q:ReadyQueue=IndexedHeap<4>> {
    peers:Vec<Slot<K,T,Q>>, free:Vec<usize>, count:usize,
    head:Option<usize>, tail:Option<usize>, active:usize,
    max_peers:usize, max_bytes:u64, bytes:u64, now:u64,
    scratch:Vec<Delivery<K,T>>,
}
impl<K:Eq+Hash+Clone,T,Q:ReadyQueue> Dispatcher<K,T,Q> {
    pub fn new(max_peers:usize,max_buffered_bytes:u64) -> Result<Self,Error> {
        if max_peers==0 || max_buffered_bytes==0 { return Err(Error::InvalidConfig); }
        Ok(Self { peers:Vec::new(),free:Vec::new(),count:0,head:None,tail:None,active:0,
                  max_peers,max_bytes:max_buffered_bytes,bytes:0,now:0,scratch:Vec::new() })
    }
    fn index(&self, peer:Peer) -> Result<usize,Error> {
        self.peers.get(peer.slot).filter(|s| s.identity==peer.identity && s.value.is_some())
            .map(|_|peer.slot).ok_or(Error::UnknownPeer)
    }
    fn unlink(&mut self,i:usize) {
        if !self.peers[i].active { return; }
        let prev=self.peers[i].prev; let next=self.peers[i].next;
        if let Some(p)=prev { self.peers[p].next=next; } else { self.head=next; }
        if let Some(n)=next { self.peers[n].prev=prev; } else { self.tail=prev; }
        self.peers[i].active=false; self.peers[i].prev=None; self.peers[i].next=None;
        self.active-=1;
    }
    fn refresh(&mut self,i:usize) {
        if self.peers[i].value.as_ref().unwrap().mailbox.has_ready() {
            if !self.peers[i].active {
                let prev=self.tail;
                self.peers[i].prev=prev; self.peers[i].next=None; self.peers[i].active=true;
                if let Some(p)=prev { self.peers[p].next=Some(i); } else { self.head=Some(i); }
                self.tail=Some(i); self.active+=1;
            }
        } else {
            self.unlink(i);
            self.peers[i].value.as_mut().unwrap().deficit=0;
        }
    }
    pub fn add_peer(&mut self, config:PeerConfig) -> Result<Peer,Error> {
        if self.count>=self.max_peers { return Err(Error::Capacity); }
        if config.quantum==0 || config.burst<u64::from(config.limits.max_message_bytes) { return Err(Error::InvalidConfig); }
        let mailbox=Mailbox::new(config.limits)?;
        let identity=mailbox.identity();
        let recipient=Recipient { mailbox,bucket:TokenBucket::new(config.burst,config.bytes_per_tick,self.now)?,quantum:config.quantum,deficit:0 };
        let i=self.free.pop().unwrap_or(self.peers.len());
        let slot=Slot { value:Some(recipient),identity,prev:None,next:None,active:false };
        if i==self.peers.len() { self.peers.push(slot); } else { self.peers[i]=slot; }
        self.count+=1;
        Ok(Peer {slot:i,identity})
    }
    pub fn remove_peer(&mut self, peer:Peer) -> bool {
        let Ok(i)=self.index(peer) else { return false; };
        self.unlink(i);
        self.bytes-=self.peers[i].value.take().unwrap().mailbox.buffered_bytes();
        self.count-=1; self.free.push(i); true
    }
    pub fn offer_state(&mut self,peer:Peer,key:K,revision:u64,message:Message<T>,priority_delay:u16,now:u64) -> Result<Offer,Error> {
        if now<self.now { return Err(Error::ClockWentBack); }
        let i=self.index(peer)?;
        let m=&mut self.peers[i].value.as_mut().unwrap().mailbox;
        let stale=m.progress(&key).is_some_and(|p|revision<=p.latest);
        if !stale {
            let projected=(self.bytes-m.pending_bytes(&key)).checked_add(u64::from(message.bytes())).ok_or(Error::Capacity)?;
            if projected>self.max_bytes { return Err(Error::Capacity); }
        }
        let before=m.buffered_bytes();
        let outcome=m.offer_state(key,revision,message,priority_delay,now)?;
        self.bytes=self.bytes-before+m.buffered_bytes(); self.now=now;
        self.refresh(i); Ok(outcome)
    }
    pub fn offer_event(&mut self,peer:Peer,message:Message<T>,priority_delay:u16,now:u64) -> Result<u64,Error> {
        if now<self.now { return Err(Error::ClockWentBack); }
        let i=self.index(peer)?;
        let projected=self.bytes.checked_add(u64::from(message.bytes())).ok_or(Error::Capacity)?;
        if projected>self.max_bytes { return Err(Error::Capacity); }
        let event=self.peers[i].value.as_mut().unwrap().mailbox.offer_event(message,priority_delay,now)?;
        self.bytes=projected; self.now=now; self.refresh(i); Ok(event)
    }
    pub fn ack(&mut self,peer:Peer,receipt:Receipt) -> bool {
        let Ok(i)=self.index(peer) else { return false; };
        let m=&mut self.peers[i].value.as_mut().unwrap().mailbox;
        let before=m.buffered_bytes(); let result=m.ack(receipt);
        self.bytes-=before-m.buffered_bytes(); self.refresh(i); result
    }
    pub fn retry(&mut self,peer:Peer,receipt:Receipt) -> bool {
        let Ok(i)=self.index(peer) else { return false; };
        let m=&mut self.peers[i].value.as_mut().unwrap().mailbox;
        let before=m.buffered_bytes(); let result=m.retry(receipt);
        self.bytes-=before-m.buffered_bytes(); self.refresh(i); result
    }
    pub fn forget_state(&mut self,peer:Peer,key:&K) -> bool {
        let Ok(i)=self.index(peer) else { return false; };
        let m=&mut self.peers[i].value.as_mut().unwrap().mailbox;
        let before=m.buffered_bytes(); let result=m.forget_state(key);
        self.bytes-=before-m.buffered_bytes(); self.refresh(i); result
    }
    /// Global and per-peer caps apply to this call; token buckets apply across
    /// calls. Fair-service credit carries forward only while the peer is ready.
    /// A packet must fit all caps; callers must allow a large enough budget on a
    /// later poll to deliver a larger message. No automatic fragmentation occurs.
    pub fn dispatch_into(&mut self,now:u64,budget:DispatchBudget,out:&mut Vec<Envelope<K,T>>) -> Result<DispatchStats,Error> {
        if now<self.now { return Err(Error::ClockWentBack); }
        self.now=now;
        let mut remaining=budget.bytes; let mut delivered=0; let mut visited=0;
        let visit_limit=budget.visits.min(self.active);
        while remaining>0 && delivered<budget.deliveries && visited<visit_limit {
            let i=self.head.unwrap(); self.unlink(i); visited+=1;
            let peer=Peer { slot:i,identity:self.peers[i].identity };
            let r=self.peers[i].value.as_mut().unwrap();
            r.deficit=r.deficit.saturating_add(r.quantum).min(r.mailbox.byte_limit());
            let allowance=remaining.min(budget.per_peer_bytes).min(r.deficit).min(r.bucket.available(now)?);
            self.scratch.clear();
            let used=match r.mailbox.drain_into(now,allowance,budget.deliveries-delivered,&mut self.scratch) {
                Ok(used)=>used,
                Err(error)=>{ self.refresh(i); return Err(error); }
            };
            assert!(r.bucket.consume(used)); r.deficit-=used; remaining-=used;
            delivered+=self.scratch.len();
            out.extend(self.scratch.drain(..).map(|delivery|Envelope {peer,delivery}));
            self.refresh(i);
        }
        Ok(DispatchStats {bytes:budget.bytes-remaining,deliveries:delivered,visits:visited,ready_peers:self.active})
    }
    pub fn progress(&self,peer:Peer,key:&K) -> Option<Progress> {
        self.peers.get(self.index(peer).ok()?)?.value.as_ref()?.mailbox.progress(key)
    }
    pub fn buffered_bytes(&self) -> u64 { self.bytes }
    pub fn peer_count(&self) -> usize { self.count }
    pub fn ready_peers(&self) -> usize { self.active }
}
