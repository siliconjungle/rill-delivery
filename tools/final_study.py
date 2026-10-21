#!/usr/bin/env python3
"""Execute exactly the preregistered isolated comparison."""
import argparse
import json
import sys
from queue_study import ROOT,main

p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--label',default='final-isolated')
args=p.parse_args();plan=json.loads((ROOT/'research/final-protocol.json').read_text())
assert len(plan['finalists'])*len(plan['cases'])*len(plan['seeds'])*plan['repetitions']==plan['processes']
sys.argv=['queue_study.py','--binary',args.binary,'--bench','delivery','--phase','isolated','--label',args.label,
          '--repetitions',str(plan['repetitions']),'--seeds',*map(str,plan['seeds']),'--cases',*plan['cases'],'--backends',*plan['finalists']]
main()
