import test from 'node:test';import assert from 'node:assert/strict';import {readFileSync} from 'node:fs';import {Mailbox,Message} from '../index.mjs';
const file=process.env.RILL_RUST_ORACLE;
test('complete deterministic mutation traces match Rust for every JS ready queue',{skip:!file},()=>{
 const rows=readFileSync(file,'utf8').trim().split('\n').map(JSON.parse);
 for(const strategy of ['heap4','heap8','scan','cached']){
  const m=new Mailbox({states:16,events:8,bytes:256,maxMessageBytes:64,strategy}),receipts=[];
  for(const r of rows){let actual;
   try{switch(r.op){
    case 'offer':{const message=new Message(r.key,r.bytes);actual=r.stream?m.offerStream(m.stream(r.key),r.revision,message,r.delay,r.now):m.offerState(r.key,r.revision,message,r.delay,r.now);break;}
    case 'event':actual=m.offerEvent(new Message(r.key,r.bytes),r.delay,r.now);break;
    case 'drain':{const out=[];m.drainInto(r.now,r.budget,r.max,out);actual=out.map(d=>{receipts.push(d.receipt);return[d.kind==='event'?1:0,d.kind==='event'?d.sequence:d.key,d.revision??0,d.message.payload,d.queuedAt,d.receipt.serial,d.receipt.slot];});break;}
    case 'ack':actual=m.ack(receipts[r.index]);break;
    case 'retry':actual=m.retry(receipts[r.index]);break;
    case 'forget':actual=m.forgetState(r.key);break;
    case 'noop':actual=null;break;
   }}catch(error){actual='error:'+error.code;}
   assert.deepEqual(actual,r.result,`${strategy} step ${r.now} ${r.op}`);assert.equal(m.bufferedBytes,r.buffered,`${strategy} bytes at ${r.now}`);assert.equal(m.hasReady,r.ready);
   const progress=Array.from({length:24},(_,k)=>{const p=m.progress(k);return p?[p.latest,p.acknowledged,p.pending,p.inFlight]:null;});assert.deepEqual(progress,r.progress,`${strategy} progress at ${r.now}`);
  }
 }
});
