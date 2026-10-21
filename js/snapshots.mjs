// Reusable fair selection of a replacement snapshot bank. This is a synchronous
// batch fast path: it does not infer ACKs, retain event history, or mutate payloads.
const keyOf = row => row[0];
const equal = (a,b) => a===b || (a!==a && b!==b);
export class SnapshotOrder {
  constructor({strategy='linked',key=keyOf}={}) {
    if(!['reference','pooled','linked','runs'].includes(strategy)||typeof key!=='function')throw new TypeError('Invalid snapshot ordering configuration');
    this.strategy=strategy;this.key=key;this.map=new Map();this.groups=[];this.keys=[];this.heads=[];this.tails=[];this.next=[];this.ends=[];this.used=0;this.busy=false;
  }
  fill(input,out,limit) {
    if(!Array.isArray(input)||!Array.isArray(out)||input===out||!Number.isSafeInteger(limit)||limit<0)throw new TypeError('Distinct arrays and a nonnegative safe item limit are required');
    if(this.busy)throw new Error('SnapshotOrder is not reentrant');this.busy=true;
    try {out.length=0;if(!limit||!input.length)return out;
      if(this.strategy==='reference')return this.reference(input,out,limit);
      if(this.strategy==='pooled')return this.pooled(input,out,limit);
      if(this.strategy==='runs')return this.runs(input,out,limit);
      return this.linked(input,out,limit);
    } finally {this.map.clear();this.keys.length=0;this.heads.length=0;this.tails.length=0;this.ends.length=0;for(let i=0;i<this.used;i++)this.groups[i].length=0;this.used=0;this.busy=false;}
  }
  // Prepared hot path: distinct arrays, prevalidated integer limit, and a pure,
  // nonthrowing key reader on one synchronous owner. Use fill for external inputs.
  fillInto(input,out,limit) {
    out.length=0;if(!limit||!input.length)return out;
    if(this.strategy==='reference')return this.reference(input,out,limit);
    if(this.strategy==='pooled'){this.pooled(input,out,limit);this.map.clear();for(let i=0;i<this.used;i++)this.groups[i].length=0;this.used=0;return out;}
    if(this.strategy==='runs')this.runs(input,out,limit);else this.linked(input,out,limit);
    this.keys.length=0;this.heads.length=0;this.tails.length=0;this.ends.length=0;return out;
  }
  reference(input,out,limit) {
    const groups=new Map();for(const row of input){let list=groups.get(this.key(row));if(!list){list=[];groups.set(this.key(row),list);}if(list.length<limit)list.push(row);}
    for(let i=0;out.length<limit;i++){let found=false;for(const list of groups.values())if(i<list.length){out.push(list[i]);found=true;if(out.length===limit)break;}if(!found)break;}return out;
  }
  pooled(input,out,limit) {
    for(const row of input){const key=this.key(row);let index=this.map.get(key);if(index===undefined){if(this.used===limit)continue;index=this.used++;this.map.set(key,index);if(!this.groups[index])this.groups[index]=[];}const list=this.groups[index];if(list.length<limit)list.push(row);}
    for(let i=0;out.length<limit;i++){let found=false;for(let g=0;g<this.used;g++){const list=this.groups[g];if(i<list.length){out.push(list[i]);found=true;if(out.length===limit)break;}}if(!found)break;}return out;
  }
  linked(input,out,limit) {
    const keys=this.keys,heads=this.heads,tails=this.tails,next=this.next;
    for(let i=0;i<input.length;i++){
      const key=this.key(input[i]);let group=0;while(group<keys.length&&!equal(keys[group],key))group++;
      if(group===keys.length){if(group===limit)continue;keys.push(key);heads.push(i);tails.push(i);}else{next[tails[group]]=i;tails[group]=i;}next[i]=-1;
    }
    while(out.length<limit){let found=false;for(let g=0;g<heads.length;g++){const i=heads[g];if(i>=0){out.push(input[i]);heads[g]=next[i];found=true;if(out.length===limit)break;}}if(!found)break;}return out;
  }
  runs(input,out,limit) {
    const keys=this.keys,heads=this.heads,ends=this.ends;let previous=this.key(input[0]),start=0;
    for(let i=1;i<input.length;i++){
      const key=this.key(input[i]);if(equal(key,previous))continue;
      keys.push(previous);heads.push(start);ends.push(i);
      for(let g=0;g<keys.length;g++)if(equal(keys[g],key)){keys.length=0;heads.length=0;ends.length=0;return this.linked(input,out,limit);}
      previous=key;start=i;
    }
    if(!keys.length){for(let i=0;i<Math.min(input.length,limit);i++)out.push(input[i]);return out;}
    keys.push(previous);heads.push(start);ends.push(input.length);
    while(out.length<limit){let found=false;for(let g=0;g<heads.length;g++)if(heads[g]<ends[g]){out.push(input[heads[g]++]);found=true;if(out.length===limit)break;}if(!found)break;}return out;
  }
}
