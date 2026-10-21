#!/usr/bin/env python3
"""Run required local checks and bind their exit codes/logs to current source."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
CARGO=str(Path.home()/'.cargo/bin/cargo')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
inputs=[ROOT/'Cargo.toml',ROOT/'Cargo.lock',ROOT/'README.md',*sorted((ROOT/'src').glob('*.rs')),*sorted((ROOT/'tests').glob('*.rs')),*sorted((ROOT/'examples').glob('*.rs'))]
before={str(p.relative_to(ROOT)):sha(p) for p in inputs}
tasks=[
    ('tests-release',[CARGO,'test','--locked','--all-features','--release'],{}),
    ('clippy',[CARGO,'clippy','--locked','--all-targets','--all-features','--','-D','warnings'],{}),
    ('format',[CARGO,'fmt','--all','--','--check'],{}),
    ('msrv',[CARGO,'+1.85.0','test','--locked','--no-default-features'],{'CARGO_TARGET_DIR':str(ROOT/'build/msrv-target'),'CARGO_INCREMENTAL':'0','CARGO_PROFILE_DEV_DEBUG':'0'}),
    ('example',[CARGO,'run','--locked','--release','--example','shards'],{}),
    ('normal-dependencies',[CARGO,'tree','--locked','--no-default-features','--edges','normal'],{}),
    ('package',[CARGO,'package','--locked','--allow-dirty'],{}),
]
rows=[]
for name,command,extra in tasks:
    path=ROOT/'evidence'/('final-'+name+'.log')
    with path.open('w') as log:
        result=subprocess.run(command,cwd=ROOT,env={**os.environ,**extra},stdout=log,stderr=subprocess.STDOUT)
    rows.append({'name':name,'command':command,'exit_code':result.returncode,'log':str(path.relative_to(ROOT)),'log_sha256':sha(path)})
    print(json.dumps({'check':name,'exit_code':result.returncode}),flush=True)
assert before=={str(p.relative_to(ROOT)):sha(p) for p in inputs},'validation inputs changed during checks'
receipt={'schema':1,'source_sha256':before,'checks':rows,'all_passed':all(r['exit_code']==0 for r in rows)}
(ROOT/'evidence/validation-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
sys.exit(0 if receipt['all_passed'] else 1)
