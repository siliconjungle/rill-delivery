// Generic synchronous JavaScript port of Rill Delivery. Payloads are caller-owned
// immutable references. Counters use checked safe integers, not lossy u64 casts.
const MAX = Number.MAX_SAFE_INTEGER;
let identity = 0;
export class DeliveryError extends Error {
  constructor(code) { super(code); this.name = 'DeliveryError'; this.code = code; }
}
const fail = code => { throw new DeliveryError(code); };
const integer = (n, min = 0, max = MAX) => { if (!Number.isSafeInteger(n) || n < min || n > max) fail('InvalidConfig'); return n; };
const next = n => { if (n === MAX) fail('Exhausted'); return n + 1; };
const add = (a,b) => { if (a > MAX-b) fail('Exhausted'); return a+b; };
const before = (a,b) => a.due < b.due || (a.due === b.due && a.order < b.order);
export class Message {
  constructor(payload, bytes) { this.payload = payload; this.bytes = integer(bytes,1,0xffffffff); Object.freeze(this); }
}
export class ReadyQueue {
  constructor(strategy = 'heap4') {
    if (!['heap4','heap8','scan','cached'].includes(strategy)) fail('InvalidConfig');
    this.strategy = strategy; this.arity = strategy === 'heap8' ? 8 : strategy === 'heap4' ? 4 : 0;
    this.items = []; this.positions = []; this.minimum = null;
  }
  get length() { return this.items.length; }
  swap(a,b) { const v=this.items[a];this.items[a]=this.items[b];this.items[b]=v;this.positions[this.items[a].id]=a;this.positions[v.id]=b; }
  up(i) { while(i>0){const p=Math.floor((i-1)/this.arity);if(!before(this.items[i],this.items[p]))break;this.swap(i,p);i=p;}return i; }
  down(i) { const n=this.items.length;for(;;){let best=i;const first=i*this.arity+1;for(let c=first;c<Math.min(first+this.arity,n);c++)if(before(this.items[c],this.items[best]))best=c;if(best===i)break;this.swap(i,best);i=best;} }
  upsert(entry) {
    const at=this.positions[entry.id];this.minimum=null;
    if(at===undefined||at<0){this.positions[entry.id]=this.items.length;this.items.push(entry);if(this.arity)this.up(this.items.length-1);}
    else {this.items[at]=entry;if(this.arity)this.down(this.up(at));}
  }
  peek() {
    if(!this.items.length)return null;if(this.arity)return this.items[0];
    if(this.strategy==='cached'&&this.minimum!==null)return this.minimum;
    let best=this.items[0];for(let i=1;i<this.items.length;i++)if(before(this.items[i],best))best=this.items[i];
    this.minimum=best;return best;
  }
  remove(id) {
    const at=this.positions[id];if(at===undefined||at<0)return null;
    const old=this.items[at],last=this.items.pop();this.positions[id]=-1;this.minimum=null;
    if(at<this.items.length){this.items[at]=last;this.positions[last.id]=at;if(this.arity)this.down(this.up(at));}return old;
  }
  pop() { const entry=this.peek();return entry ? this.remove(entry.id) : null; }
}
export class TokenBucket {
  constructor(capacity,rate,now=0) { this.capacity=integer(capacity,1);this.rate=integer(rate);this.last=integer(now);this.tokens=capacity; }
  available(now) { integer(now);if(now<this.last)fail('ClockWentBack');const dt=now-this.last;this.tokens=dt>=Math.ceil((this.capacity-this.tokens)/(this.rate||1))&&this.rate>0?this.capacity:Math.min(this.capacity,this.tokens+dt*this.rate);this.last=now;return this.tokens; }
  consume(bytes) { integer(bytes);if(bytes>this.tokens)return false;this.tokens-=bytes;return true; }
}
export class Mailbox {
  constructor({states=1024,events=256,bytes=1<<20,maxMessageBytes=64<<10,strategy='heap4'}={}) {
    this.limits={states:integer(states),events:integer(events),bytes:integer(bytes,1),maxMessageBytes:integer(maxMessageBytes,1,0xffffffff)};
    if(maxMessageBytes>bytes)fail('InvalidConfig');this.id=identity=next(identity);
    this.keys=new Map();this.slots=[];this.free=[];this.events=[];this.eventHead=0;this.ready=new ReadyQueue(strategy);
    this.bytes=0;this.serial=0;this.now=0;this.costFloor=Infinity;this.deferred=[];
  }
  check(now,message,delay=0) { integer(now);integer(delay,0,65535);if(now<this.now)fail('ClockWentBack');if(!(message instanceof Message))fail('InvalidConfig');if(message.bytes>this.limits.maxMessageBytes)fail('MessageTooLarge'); }
  stream(key) { const i=this.keys.get(key);if(i===undefined)return null;return Object.freeze({mailbox:this.id,slot:i,identity:this.slots[i].identity}); }
  offerState(key,revision,message,delay=0,now=this.now,headroom=MAX) { this.check(now,message,delay);return this.offerAt(this.keys.get(key),key,revision,message,delay,now,headroom); }
  offerStream(stream,revision,message,delay=0,now=this.now,headroom=MAX) {
    this.check(now,message,delay);const s=this.slots[stream?.slot];if(stream?.mailbox!==this.id||!s||s.identity!==stream.identity)fail('UnknownStream');
    return this.offerAt(stream.slot,undefined,revision,message,delay,now,headroom);
  }
  offerAt(i,key,revision,message,delay,now,headroom) {
    integer(revision);integer(headroom);let s=i===undefined?null:this.slots[i];if(s&&revision<=s.latest)return 'stale';
    if(!s&&this.keys.size>=this.limits.states)fail('Capacity');const old=s?.pending,oldBytes=old?.message.bytes??0;
    const total=this.bytes-oldBytes+message.bytes;if(!Number.isSafeInteger(total)||total>this.limits.bytes||message.bytes-oldBytes>headroom)fail('Capacity');
    const serial=next(this.serial),due=add(now,delay);
    const value={revision,due:old?Math.min(old.due,due):due,order:old&&old.due<=due?old.order:serial,queuedAt:old?old.queuedAt:now,message};
    if(s){s.latest=revision;s.pending=value;}
    else {i=this.free.length?this.free.pop():this.slots.length;s={key,latest:revision,acknowledged:null,pending:value,flight:null,identity:serial,slot:i};this.slots[i]=s;this.keys.set(key,i);}
    if(!s.flight&&(!old||old.due!==value.due||old.order!==value.order))this.ready.upsert({id:i,due:value.due,order:value.order});
    this.bytes=total;this.serial=serial;this.now=now;this.costFloor=Math.min(this.costFloor,message.bytes);return old?'replaced':'inserted';
  }
  offerEvent(message,delay=0,now=this.now) {
    this.check(now,message,delay);if(this.events.length-this.eventHead>=this.limits.events||this.bytes+message.bytes>this.limits.bytes)fail('Capacity');
    const serial=next(this.serial),due=add(now,delay);this.events.push({value:{revision:serial,due,order:serial,queuedAt:now,message},flight:null});
    this.bytes+=message.bytes;this.serial=serial;this.now=now;this.costFloor=Math.min(this.costFloor,message.bytes);return serial;
  }
  receipt(slot,event) { this.serial=next(this.serial);return Object.freeze({mailbox:this.id,serial:this.serial,slot,event}); }
  drainInto(now,budget,maxItems,out) {
    integer(now);integer(budget);integer(maxItems);if(now<this.now)fail('ClockWentBack');
    if(budget<this.costFloor){this.now=now;return 0;}
    const event=this.events[this.eventHead],attempts=Math.min(maxItems,this.ready.length+(event&&!event.flight?1:0));add(this.serial,attempts);
    this.now=now;
    let remaining=budget,sent=0,eventBlocked=false,min=Infinity,complete=false;this.deferred.length=0;
    while(sent<maxItems&&remaining>0){
      const state=this.ready.peek(),e=this.events[this.eventHead],ev=!eventBlocked&&e&&!e.flight;
      if(ev&&(!state||before(e.value,state))){
        if(e.value.message.bytes>remaining){min=Math.min(min,e.value.message.bytes);eventBlocked=true;continue;}
        const receipt=this.receipt(0,true);e.flight=receipt;remaining-=e.value.message.bytes;sent++;
        out.push({receipt,kind:'event',sequence:e.value.revision,message:e.value.message,queuedAt:e.value.queuedAt});
      }else if(state){
        this.ready.pop();const s=this.slots[state.id],v=s.pending;
        if(v.message.bytes>remaining){min=Math.min(min,v.message.bytes);this.deferred.push(state);continue;}
        const receipt=this.receipt(state.id,false);s.pending=null;s.flight={receipt,value:v};remaining-=v.message.bytes;sent++;
        out.push({receipt,kind:'state',key:s.key,revision:v.revision,message:v.message,queuedAt:v.queuedAt});
      }else{complete=true;break;}
    }
    for(const entry of this.deferred)this.ready.upsert(entry);this.deferred.length=0;
    if(!sent&&complete)this.costFloor=min;return budget-remaining;
  }
  matching(a,b) { return !!a&&!!b&&a.mailbox===b.mailbox&&a.serial===b.serial&&a.slot===b.slot&&a.event===b.event; }
  activate(s) { if(s.pending){const v=s.pending;this.ready.upsert({id:s.slot,due:v.due,order:v.order});this.costFloor=Math.min(this.costFloor,v.message.bytes);} }
  ack(receipt) {
    if(receipt?.mailbox!==this.id)return false;
    if(receipt.event){const e=this.events[this.eventHead];if(!e||!this.matching(e.flight,receipt))return false;this.bytes-=e.value.message.bytes;this.events[this.eventHead++]=null;
      if(this.eventHead===this.events.length){this.events.length=0;this.eventHead=0;}else if(this.eventHead>128&&this.eventHead*2>this.events.length){this.events=this.events.slice(this.eventHead);this.eventHead=0;}
      const following=this.events[this.eventHead];if(following)this.costFloor=Math.min(this.costFloor,following.value.message.bytes);return true;}
    const s=this.slots[receipt.slot];if(!s||!this.matching(s.flight?.receipt,receipt))return false;
    this.bytes-=s.flight.value.message.bytes;s.acknowledged=s.flight.value.revision;s.flight=null;this.activate(s);return true;
  }
  retry(receipt) {
    if(receipt?.mailbox!==this.id)return false;
    if(receipt.event){const e=this.events[this.eventHead];if(!e||!this.matching(e.flight,receipt))return false;e.flight=null;this.costFloor=Math.min(this.costFloor,e.value.message.bytes);return true;}
    const s=this.slots[receipt.slot];if(!s||!this.matching(s.flight?.receipt,receipt))return false;const old=s.flight.value;s.flight=null;
    if(s.pending){this.bytes-=old.message.bytes;if(before(old,s.pending)){s.pending.due=old.due;s.pending.order=old.order;}s.pending.queuedAt=Math.min(s.pending.queuedAt,old.queuedAt);}else s.pending=old;this.activate(s);return true;
  }
  forgetState(key) { const i=this.keys.get(key);if(i===undefined)return false;const s=this.slots[i];this.bytes-=(s.pending?.message.bytes??0)+(s.flight?.value.message.bytes??0);this.ready.remove(i);this.keys.delete(key);this.slots[i]=null;this.free.push(i);return true; }
  progress(key) { const s=this.slots[this.keys.get(key)];return s?{latest:s.latest,acknowledged:s.acknowledged,pending:s.pending?.revision??null,inFlight:s.flight?.value.revision??null}:null; }
  get hasReady() { const e=this.events[this.eventHead];return !!this.ready.length||!!(e&&!e.flight); }
  get bufferedBytes() { return this.bytes; }
}
export class Dispatcher {
  constructor(maxPeers,maxBufferedBytes) { this.maxPeers=integer(maxPeers,1);this.maxBytes=integer(maxBufferedBytes,1);this.peers=[];this.free=[];this.count=0;this.head=null;this.tail=null;this.active=0;this.bytes=0;this.now=0;this.scratch=[]; }
  index(peer) { const s=this.peers[peer?.slot];if(!s||s.identity!==peer.identity||!s.value)fail('UnknownPeer');return peer.slot; }
  unlink(i) { const s=this.peers[i];if(!s.active)return;if(s.prev!==null)this.peers[s.prev].next=s.next;else this.head=s.next;if(s.next!==null)this.peers[s.next].prev=s.prev;else this.tail=s.prev;s.prev=null;s.next=null;s.active=false;this.active--; }
  refresh(i) { const s=this.peers[i];if(s.value.mailbox.hasReady){if(!s.active){s.prev=this.tail;s.next=null;s.active=true;if(this.tail!==null)this.peers[this.tail].next=i;else this.head=i;this.tail=i;this.active++;}}else{this.unlink(i);s.value.deficit=0;} }
  addPeer({limits={},burst=1<<20,bytesPerTick=64<<10,quantum=16<<10}={}) {
    if(this.count>=this.maxPeers)fail('Capacity');integer(quantum,1);const m=new Mailbox(limits);if(burst<m.limits.maxMessageBytes)fail('InvalidConfig');
    const value={mailbox:m,bucket:new TokenBucket(burst,bytesPerTick,this.now),quantum,deficit:0},i=this.free.length?this.free.pop():this.peers.length;
    this.peers[i]={value,identity:m.id,prev:null,next:null,active:false};this.count++;return Object.freeze({slot:i,identity:m.id});
  }
  removePeer(peer) { let i;try{i=this.index(peer);}catch{return false;}this.unlink(i);this.bytes-=this.peers[i].value.mailbox.bytes;this.peers[i].value=null;this.free.push(i);this.count--;return true; }
  offerState(peer,key,revision,message,delay=0,now=this.now) { integer(now);if(now<this.now)fail('ClockWentBack');const i=this.index(peer),m=this.peers[i].value.mailbox,old=m.bytes;const r=m.offerState(key,revision,message,delay,now,this.maxBytes-this.bytes);this.bytes+=m.bytes-old;this.now=now;this.refresh(i);return r; }
  stream(peer,key) { let i;try{i=this.index(peer);}catch{return null;}return this.peers[i].value.mailbox.stream(key); }
  offerStream(peer,stream,revision,message,delay=0,now=this.now) { integer(now);if(now<this.now)fail('ClockWentBack');const i=this.index(peer),m=this.peers[i].value.mailbox,old=m.bytes;const r=m.offerStream(stream,revision,message,delay,now,this.maxBytes-this.bytes);this.bytes+=m.bytes-old;this.now=now;this.refresh(i);return r; }
  offerEvent(peer,message,delay=0,now=this.now) { integer(now);if(now<this.now)fail('ClockWentBack');const i=this.index(peer),m=this.peers[i].value.mailbox;if(this.bytes+message.bytes>this.maxBytes)fail('Capacity');const r=m.offerEvent(message,delay,now);this.bytes+=message.bytes;this.now=now;this.refresh(i);return r; }
  finish(peer,receipt,method) { let i;try{i=this.index(peer);}catch{return false;}const m=this.peers[i].value.mailbox,old=m.bytes,r=m[method](receipt);this.bytes+=m.bytes-old;this.refresh(i);return r; }
  ack(peer,receipt) { return this.finish(peer,receipt,'ack'); }
  retry(peer,receipt) { return this.finish(peer,receipt,'retry'); }
  forgetState(peer,key) { let i;try{i=this.index(peer);}catch{return false;}const m=this.peers[i].value.mailbox,old=m.bytes,r=m.forgetState(key);this.bytes+=m.bytes-old;this.refresh(i);return r; }
  progress(peer,key) { let i;try{i=this.index(peer);}catch{return null;}return this.peers[i].value.mailbox.progress(key); }
  dispatchInto(now,{bytes,perPeerBytes,deliveries,visits},out) {
    integer(now);integer(bytes);integer(perPeerBytes);integer(deliveries);integer(visits);if(now<this.now)fail('ClockWentBack');this.now=now;
    let remaining=bytes,sent=0,visited=0;const limit=Math.min(visits,this.active);
    while(remaining>0&&sent<deliveries&&visited<limit){const i=this.head;this.unlink(i);visited++;const s=this.peers[i],r=s.value;
      r.deficit=Math.min(r.mailbox.limits.bytes,r.deficit+Math.min(r.quantum,MAX-r.deficit));const allowance=Math.min(remaining,perPeerBytes,r.deficit,r.bucket.available(now));
      this.scratch.length=0;let used;try{used=r.mailbox.drainInto(now,allowance,deliveries-sent,this.scratch);}catch(error){this.refresh(i);throw error;}
      r.bucket.consume(used);r.deficit-=used;remaining-=used;sent+=this.scratch.length;
      for(const delivery of this.scratch)out.push({peer:Object.freeze({slot:i,identity:s.identity}),delivery});this.refresh(i);
    }
    this.scratch.length=0;return {bytes:bytes-remaining,deliveries:sent,visits:visited,readyPeers:this.active};
  }
  get bufferedBytes(){return this.bytes;}
  get peerCount(){return this.count;}
  get readyPeers(){return this.active;}
}
export {SnapshotOrder} from './snapshots.mjs';
