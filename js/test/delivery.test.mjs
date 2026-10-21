import test from 'node:test';
import assert from 'node:assert/strict';
import {Mailbox,Message,Dispatcher,TokenBucket,ReadyQueue} from '../index.mjs';
const msg=(v,n=4)=>new Message(v,n),drain=(m,now=0,bytes=1000,max=100)=>{const out=[];m.drainInto(now,bytes,max,out);return out;};
for(const strategy of ['heap4','heap8','scan','cached']) {
 test(`${strategy}: queue agrees with a sorted independent map under mutation`,()=>{
  const q=new ReadyQueue(strategy),model=new Map();let seed=71923;const rand=()=>seed=(Math.imul(seed,1664525)+1013904223)>>>0;
  for(let n=0;n<20000;n++){const id=rand()%128;if(rand()%3){const e={id,due:rand()%8,order:n};q.upsert(e);model.set(id,e);}else{q.remove(id);model.delete(id);}const best=[...model.values()].sort((a,b)=>a.due-b.due||a.order-b.order)[0]??null;assert.deepEqual(q.peek(),best);assert.equal(q.length,model.size);if(n%11===0){assert.deepEqual(q.pop(),best);if(best)model.delete(best.id);}}
 });
 test(`${strategy}: state coalesces but an exact attempt survives until ACK or retry`,()=>{
  const m=new Mailbox({strategy});m.offerState('a',1,msg(1));const handle=m.stream('a'),[old]=drain(m);m.offerStream(handle,2,msg(2));m.offerStream(handle,3,msg(3));assert.deepEqual(drain(m),[]);assert.equal(m.bufferedBytes,8);assert.equal(m.retry(old.receipt),true);assert.equal(m.ack(old.receipt),false);const [latest]=drain(m);assert.equal(latest.revision,3);assert.equal(latest.message.payload,3);assert.equal(m.ack(latest.receipt),true);assert.equal(m.bufferedBytes,0);assert.deepEqual(m.progress('a'),{latest:3,pending:null,inFlight:null,acknowledged:3});
  m.forgetState('a');m.offerState('b',1,msg(4));assert.throws(()=>m.offerStream(handle,99,msg(99)),/UnknownStream/);assert.equal(m.ack(latest.receipt),false);
 });
 test(`${strategy}: events stay ordered through retry and cannot coalesce`,()=>{
  const m=new Mailbox({strategy});const one=m.offerEvent(msg('one')),two=m.offerEvent(msg('two'));const [first]=drain(m);assert.equal(first.sequence,one);assert.equal(m.hasReady,false);m.retry(first.receipt);const [again]=drain(m);assert.equal(again.sequence,one);assert.equal(m.ack(first.receipt),false);m.ack(again.receipt);const [last]=drain(m);assert.equal(last.sequence,two);m.ack(last.receipt);assert.equal(m.bufferedBytes,0);
 });
 test(`${strategy}: budget scans skip a large row and floor decreases when work reactivates`,()=>{
  const m=new Mailbox({strategy});m.offerState('large',1,msg('large',50));assert.deepEqual(drain(m,0,10),[]);m.offerState('small',1,msg('small',2));const [small]=drain(m,0,10);assert.equal(small.key,'small');m.offerState('small',2,msg('next',1));assert.deepEqual(drain(m,0,10),[]);m.ack(small.receipt);const [next]=drain(m,0,1);assert.equal(next.revision,2);m.ack(next.receipt);assert.equal(m.bufferedBytes,50);
 });
}
test('admission is atomic, priority promotion retains age, counters reject lossy integers',()=>{
 const m=new Mailbox({states:1,events:1,bytes:8,maxMessageBytes:8});m.offerState('a',1,msg(1,4),9,1);assert.throws(()=>m.offerState('b',1,msg(2),0,2),/Capacity/);assert.throws(()=>m.offerState('a',2,msg(2,9),0,2),/MessageTooLarge/);assert.equal(m.progress('a').latest,1);m.offerState('a',2,msg(2,5),0,2);const [v]=drain(m,2);assert.equal(v.queuedAt,1);assert.throws(()=>m.offerState('a',3,msg(3),0,1),/ClockWentBack/);assert.throws(()=>m.offerState('a',Number.MAX_SAFE_INTEGER+1,msg(3),0,2),/InvalidConfig/);assert.equal(m.ack({...v.receipt,serial:v.receipt.serial+1}),false);m.ack(v.receipt);assert.equal(m.offerState('a',1,msg(1),0,2),'stale');
});
test('global retained-byte bounds include flights and protect peer incarnations',()=>{
 const d=new Dispatcher(2,8),p=d.addPeer({limits:{bytes:8,maxMessageBytes:8}}),q=d.addPeer({limits:{bytes:8,maxMessageBytes:8}});
 d.offerState(p,'a',1,msg(1));d.offerState(q,'b',1,msg(2));const out=[];d.dispatchInto(0,{bytes:8,perPeerBytes:4,deliveries:2,visits:2},out);assert.equal(out.length,2);assert.equal(d.bufferedBytes,8);assert.throws(()=>d.offerState(p,'a',2,msg(3)),/Capacity/);for(const r of out)d.ack(r.peer,r.delivery.receipt);assert.equal(d.bufferedBytes,0);d.removePeer(p);const replacement=d.addPeer({limits:{bytes:8,maxMessageBytes:8}});assert.equal(d.ack(p,out[0].delivery.receipt),false);assert.throws(()=>d.offerState(p,'x',1,msg(0)),/UnknownPeer/);assert.notEqual(p.identity,replacement.identity);
});
test('integer token bucket saturates without overflowing and rejects reversed time',()=>{
 const b=new TokenBucket(Number.MAX_SAFE_INTEGER,Number.MAX_SAFE_INTEGER);b.consume(Number.MAX_SAFE_INTEGER);assert.equal(b.available(Number.MAX_SAFE_INTEGER),Number.MAX_SAFE_INTEGER);assert.throws(()=>b.available(0),/ClockWentBack/);const stopped=new TokenBucket(100,0);stopped.consume(100);assert.equal(stopped.available(Number.MAX_SAFE_INTEGER),0);
});
test('20,000 peers all converge with bounded service and idle recipients stop polling',()=>{
 const d=new Dispatcher(20000,1000000),peers=[];for(let n=0;n<20000;n++){const p=d.addPeer({limits:{states:2,bytes:64,maxMessageBytes:16},quantum:8,burst:32,bytesPerTick:8});peers.push(p);d.offerState(p,0,1,msg(n));d.offerState(p,0,2,msg(n));}
 for(let at=0;at<50&&d.readyPeers;at++){const out=[];d.dispatchInto(at,{bytes:16000,perPeerBytes:8,deliveries:4000,visits:20000},out);for(const e of out){assert.equal(e.delivery.revision,2);d.ack(e.peer,e.delivery.receipt);}}
 for(const p of peers)assert.equal(d.progress(p,0).acknowledged,2);assert.equal(d.bufferedBytes,0);assert.equal(d.readyPeers,0);
});
test('retrying a superseded flight preserves its older age and urgency',()=>{
 const m=new Mailbox();m.offerState('x',1,msg(1),0,0);const [first]=drain(m);m.offerState('x',2,msg(2),8,10);m.retry(first.receipt);const [latest]=drain(m,10);assert.equal(latest.revision,2);assert.equal(latest.queuedAt,0);assert.equal(m.ack(first.receipt),false);
});
test('failed admission does not mutate progress and a budget-zero drain cannot exhaust serial space',()=>{
 const m=new Mailbox({bytes:4,maxMessageBytes:4});m.offerState('x',1,msg(1));const [first]=drain(m);assert.throws(()=>m.offerState('x',2,msg(2)),/Capacity/);assert.deepEqual(m.progress('x'),{latest:1,acknowledged:null,pending:null,inFlight:1});m.ack(first.receipt);m.serial=Number.MAX_SAFE_INTEGER;assert.equal(m.drainInto(0,0,0,[]),0);assert.throws(()=>m.offerState('x',2,msg(2)),/Exhausted/);assert.equal(m.bufferedBytes,0);
});
