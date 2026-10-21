#!/usr/bin/env python3
"""Bind a frozen executable to an exact committed source tree and compiler."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import argparse

ROOT=Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser();p.add_argument('label');p.add_argument('--bench',default='queues')
args=p.parse_args();label=args.label;bench=args.bench
if not label.replace('-','').isalnum(): raise SystemExit('invalid label')
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def command(*args): return subprocess.check_output(args,cwd=ROOT,text=True).strip()
source_paths=[ROOT/'Cargo.toml',ROOT/'Cargo.lock',*sorted((ROOT/'src').glob('*.rs')),ROOT/f'benches/{bench}.rs']
subprocess.run(['git','diff','--exit-code','HEAD','--',*[str(p.relative_to(ROOT)) for p in source_paths]],cwd=ROOT,check=True)
revision=command('git','rev-parse','HEAD')
hashes={str(p.relative_to(ROOT)):sha(p) for p in source_paths}
entry=Path.home()/'.cargo/bin/cargo'
build=[str(entry) if entry.exists() else 'cargo','bench','--locked','--all-features','--bench',bench,'--no-run','--message-format=json']
result=subprocess.run(build,cwd=ROOT,text=True,capture_output=True)
(ROOT/'evidence'/f'{label}-build.log').write_text(result.stderr)
result.check_returncode()
artifacts=[json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
exe=next(Path(a['executable']) for a in artifacts if a.get('reason')=='compiler-artifact' and a['target']['name']==bench and 'bench' in a['target']['kind'] and a.get('executable'))
destination=ROOT/'build'/label
if destination.exists(): raise SystemExit('refusing to overwrite a frozen executable')
shutil.copy2(exe,destination)
assert all(sha(ROOT/p)==h for p,h in hashes.items()),'source changed while building'
receipt={'schema':1,'source_commit':revision,'source_sha256':hashes,'binary_sha256':sha(destination),
         'compiler':command('rustc','-Vv'),'cargo':command('cargo','-V'),'build_command':build,
         'executable':str(destination.relative_to(ROOT)),'source_clean_before_build':True}
(ROOT/'evidence'/f'{label}-build.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt,indent=2))
