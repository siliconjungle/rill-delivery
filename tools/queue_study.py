#!/usr/bin/env python3
"""Parallel exploratory comparisons followed by interleaved isolated timing."""
import argparse
import concurrent.futures as cf
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
BACKENDS = "scan sorted tree lazy heap2 heap4 heap8 heap16 hybrid4 hybrid8 hybrid16 hybrid32 hybrid64 priority keyed orx-map2 orx-map4 orx-map8 orx-index2 orx-index4 orx-index8 cached-scan tournament cached4 cached8 cached16 cached32 cached64 adaptive4 adaptive8 adaptive16 adaptive32".split()
CASES = "tiny small coalesce sparse dense large churn interleave deep-interleave".split()

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--binary", required=True)
    p.add_argument("--phase", choices=["parallel-screen", "isolated"], required=True)
    p.add_argument("--repetitions", type=int, default=3)
    p.add_argument("--cases", nargs="+", default=CASES)
    p.add_argument("--backends", nargs="+", default=BACKENDS)
    p.add_argument("--label",default=None)
    args = p.parse_args()
    binary = Path(args.binary).resolve()
    label=args.label or args.phase
    output = ROOT / "evidence" / (label + ".ndjson")
    if output.exists(): raise SystemExit("Refusing to overwrite existing measurements")
    config = {"schema":1,"phase":args.phase,"binary_sha256":sha(binary),"host":platform.platform(),
              "machine":platform.machine(),"python":platform.python_version(),"argv":vars(args),
              "source_sha256":{str(f.relative_to(ROOT)):sha(f) for f in sorted(ROOT.glob("src/*.rs"))},
              "bench_sha256":sha(ROOT/"benches/queues.rs"),"driver_sha256":sha(Path(__file__)),
              "clock":"Instant wall time; output construction and queue destruction included; output vector verification/destruction outside timing",
              "other_workers":"left running; parallel screen timings cannot support acceptance claims",
              "seeds":[71923,61937] if args.phase=="parallel-screen" else [982451653,961748941]}
    (ROOT/"evidence"/(label+"-config.json")).write_text(json.dumps(config,indent=2)+"\n")
    tasks=[]
    for rep in range(args.repetitions):
        for seed in config["seeds"]:
            for case in args.cases:
                backends=args.backends.copy()
                random.Random(seed+rep).shuffle(backends)
                tasks += [(b,case,seed,rep) for b in backends]
    def run(task):
        b,c,s,rep=task
        started=time.monotonic()
        proc=subprocess.run([str(binary),b,c,str(s)],capture_output=True,text=True)
        if proc.returncode: raise RuntimeError((task,proc.returncode,proc.stderr))
        result=json.loads(proc.stdout)
        return {**result,"repetition":rep,"process_seconds":time.monotonic()-started}
    with output.open("w",buffering=1) as f:
        if args.phase=="parallel-screen":
            with cf.ThreadPoolExecutor(max_workers=2) as pool:
                for i,row in enumerate(pool.map(run,tasks)):
                    f.write(json.dumps(row)+"\n")
                    if (i+1)%21==0: print(f"completed {i+1}/{len(tasks)}",flush=True)
        else:
            for i,task in enumerate(tasks):
                f.write(json.dumps(run(task))+"\n")
                if (i+1)%21==0: print(f"completed {i+1}/{len(tasks)}",flush=True)
    rows=[json.loads(x) for x in output.read_text().splitlines()]
    by={}
    for row in rows: by.setdefault((row["case"],row["backend"]),[]).append(row["elapsed_ns"])
    summary={c: sorted([{"backend":b,"median_ms":statistics.median(v)/1e6,"runs":len(v)} for (case,b),v in by.items() if case==c],key=lambda x:x["median_ms"]) for c in args.cases}
    (ROOT/"evidence"/(label+"-summary.json")).write_text(json.dumps(summary,indent=2)+"\n")
    print(json.dumps({c:rows[:3] for c,rows in summary.items()},indent=2))

if __name__=="__main__": main()
