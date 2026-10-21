#!/usr/bin/env python3
"""Fetch checksum-verified source excerpts for the closest source families."""
import concurrent.futures as cf
import hashlib
import io
import json
from pathlib import Path,PurePosixPath
import tarfile
from research import ROOT,get

NAMES={'rifts','nexusnet-scheduler','firq-core','coalescing_buffer','cohort-queue',
       'orx-priority-queue','priority-queue','keyed_priority_queue','indexed_priority_queue',
       'dary_heap','binary-heap-plus','radix-heap'}

def fetch(row):
    name=row['name'];version=row['version']
    url=f'https://static.crates.io/crates/{name}/{name}-{version}.crate'
    raw=get(url)
    digest=hashlib.sha256(raw).hexdigest()
    assert digest==row['checksum'],f'checksum mismatch: {name}'
    output=ROOT/'research/cache'/f'{name}-{version}'
    files=[]
    with tarfile.open(fileobj=io.BytesIO(raw),mode='r:gz') as archive:
        for member in archive.getmembers():
            if not member.isfile():continue
            path=PurePosixPath(member.name)
            assert path.parts[0]==f'{name}-{version}' and '..' not in path.parts
            relative=Path(*path.parts[1:])
            if relative.name.lower().startswith(('license','readme')) or relative.as_posix()=='Cargo.toml' or (
                relative.as_posix().startswith('src/') and any(s in relative.as_posix() for s in ('lib.rs','queue','priority','backpressure','sched','budget','coales','heap','bucket'))):
                data=archive.extractfile(member).read()
                destination=output/relative;destination.parent.mkdir(parents=True,exist_ok=True);destination.write_bytes(data)
                files.append({'path':relative.as_posix(),'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)})
    return {'name':name,'version':version,'archive_url':url,'checksum_verified':digest,'files':files}

if __name__=='__main__':
    inventory=json.loads((ROOT/'research/inventory.json').read_text())
    with cf.ThreadPoolExecutor(max_workers=3) as pool:
        rows=list(pool.map(fetch,[c for c in inventory['crates'] if c['name'] in NAMES]))
    (ROOT/'research/source-pass.json').write_text(json.dumps(rows,indent=2)+'\n')
    print(json.dumps({'sources':len(rows),'files':sum(len(r['files']) for r in rows),'bytes':sum(f['bytes'] for r in rows for f in r['files'])}))
