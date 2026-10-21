#!/usr/bin/env python3
"""Explore every queue x binding x payload x worker-count configuration."""
import argparse
import itertools
import json
from pathlib import Path
import sys
from queue_study import BACKENDS,ROOT,main

p=argparse.ArgumentParser()
p.add_argument('--binary',required=True)
p.add_argument('--label',required=True)
p.add_argument('--cases',nargs='+',default=['resident'])
p.add_argument('--seeds',nargs='+',default=['71923'])
args=p.parse_args()
variants=[]
for backend,prepared,copied,shards in itertools.product(BACKENDS,[False,True],[False,True],[1,2,4,8]):
    variants.append(backend+('-handles' if prepared else '')+('-copied' if copied else '')+(f'-shards{shards}' if shards>1 else ''))
assert len(variants)==len(set(variants))==592
plan={'schema':1,'status':'exploratory-parallel-not-acceptance','axes':{'queue':BACKENDS,'prepared':[False,True],'copied':[False,True],'workers':[1,2,4,8]},
      'configurations':variants,'cases':args.cases,'seeds':args.seeds,
      'selection_rule':'Select Pareto finalists on development data; validate unseen performance seeds in isolated fresh processes. No universal claim from a single workload.'}
path=ROOT/'evidence'/(args.label+'-plan.json')
if path.exists():raise SystemExit('Refusing to overwrite a registered search plan')
path.write_text(json.dumps(plan,indent=2)+'\n')
sys.argv=['queue_study.py','--binary',args.binary,'--bench','delivery','--phase','parallel-screen','--label',args.label,
          '--repetitions','1','--cases',*args.cases,'--seeds',*args.seeds,'--backends',*variants]
main()
