import test from 'node:test';import assert from 'node:assert/strict';import {SnapshotOrder} from '../snapshots.mjs';
function oracle(rows,limit){const groups=new Map();for(const row of rows){const key=row[0];if(!groups.has(key))groups.set(key,[]);if(groups.get(key).length<limit)groups.get(key).push(row);}const out=[];for(let i=0;out.length<limit;i++){let found=false;for(const list of groups.values())if(list[i]){out.push(list[i]);found=true;if(out.length===limit)break;}if(!found)break;}return out;}
for(const strategy of ['reference','pooled','linked','runs'])test(`${strategy}: exact fair order, payload identity and bounded output across repeated heterogeneous banks`,()=>{
 const order=new SnapshotOrder({strategy}),out=[];let seed=61937;const rng=()=>seed=(Math.imul(seed,1664525)+1013904223)>>>0;
 const keys=['a','b',undefined,NaN,{},Symbol('x'),0];
 for(let n=0;n<2000;n++){const rows=Array.from({length:rng()%150},(_,i)=>[keys[rng()%keys.length],{n,i}]),limit=rng()%40,expected=oracle(rows,limit);const result=order.fill(rows,out,limit);assert.equal(result,out);assert.deepEqual(result,expected);for(let i=0;i<expected.length;i++)assert.equal(result[i],expected[i]);order.fillInto(rows,out,limit);assert.deepEqual(out,expected);assert.equal(order.map.size,0);assert.equal(order.keys.length,0);for(const group of order.groups)assert.equal(group.length,0);}
 assert.throws(()=>order.fill(out,out,2));assert.throws(()=>order.fill([],out,-1));
});
test('reentrant key callbacks fail explicitly and temporary references are released on exceptions',()=>{
 let order;order=new SnapshotOrder({strategy:'pooled',key:row=>{if(row==='reenter')order.fill([],[],1);return row;}});assert.throws(()=>order.fill(['a','reenter'],[],3),/not reentrant/);assert.equal(order.busy,false);assert.equal(order.map.size,0);assert.equal(order.groups[0].length,0);
});
