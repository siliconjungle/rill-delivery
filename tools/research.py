#!/usr/bin/env python3
"""Reproducible primary-source discovery, without cloning entire repositories."""
import concurrent.futures as cf
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
SOURCES = {
    "replication": "bevy_replicon lightyear lightyear_replication renet naia-server naia-shared turbulence laminar",
    "delivery-scheduling": "rifts nexusnet-scheduler firq-core ipfrs-network hart-link taskmill",
    "transport-reference": "quinn-proto wtransport iroh libp2p yamux s2n-quic quiche",
    "coalescing-and-channels": "tokio n0-watcher async-broadcast async-watch bus thingbuf flume kanal crossbeam-channel postage coalescing_buffer",
    "queue-components": "priority-queue keyed_priority_queue indexed_priority_queue orx-priority-queue dary_heap binary-heap-plus radix-heap priq priority-channel cohort-queue",
    "storage-components": "bytes slab slotmap generational-arena fixedbitset bitvec roaring hashbrown ahash",
    "rate-limit-components": "governor ratelimit leaky-bucket leaky-bucket-lite direct",
    "broker-and-flow-control-reference": "zenoh async-nats nats rumqttc paho-mqtt redis lapin rdkafka rskafka h2 tonic tarpc",
    "durable-replication-reference": "openraft raft spacetimedb spacetimedb-client-api",
}


def get(url):
    # System curl uses the platform trust store; retain certificate verification.
    return subprocess.check_output(["curl", "--fail", "--silent", "--show-error", "--location",
                                   "--max-time", "30", "--user-agent", "rill-delivery-research/0.1", url])


def crate(item):
    category, name = item
    url = f"https://crates.io/api/v1/crates/{name}"
    row = {"name": name, "category": category, "metadata_url": url,
           "classification": "design-reference" if category in {"replication", "transport-reference"} else "candidate-component",
           "benchmark_status": "not-yet-adapted"}
    try:
        raw = get(url)
        data = json.loads(raw)
        c = data["crate"]
        version = c.get("max_stable_version") or c["max_version"]
        v = next(v for v in data["versions"] if v["num"] == version)
        row.update(version=version, description=c.get("description"), repository=c.get("repository"),
                   documentation=c.get("documentation") or f"https://docs.rs/{name}/{version}/",
                   license=v.get("license"), checksum=v.get("checksum"), updated_at=c.get("updated_at"),
                   metadata_sha256=hashlib.sha256(raw).hexdigest(), fetched=True)
    except Exception as e:
        row.update(fetched=False, error=str(e), benchmark_status="discovery-failed")
    return row


def frontier(repo):
    sha = subprocess.check_output(["gh", "api", f"repos/siliconjungle/{repo}/commits/HEAD", "--jq", ".sha"], text=True).strip()
    tree = json.loads(subprocess.check_output(["gh", "api", f"repos/siliconjungle/{repo}/git/trees/{sha}?recursive=1"], text=True))
    # Read published APIs and scheduling-related sources; no private code.
    paths = [r["path"] for r in tree["tree"] if r["type"] == "blob" and (
        r["path"] in {"README.md", "package.json", "src/index.ts", "src/runtime.ts"} or
        (r["path"].startswith("src/") and any(t in r["path"] for t in ("queue", "backpressure", "scheduler", "server", "room", "flow"))))]
    out = ROOT / "research/cache" / repo
    out.mkdir(parents=True, exist_ok=True)
    files = []
    for path in paths:
        url = f"https://raw.githubusercontent.com/siliconjungle/{repo}/{sha}/{path}"
        raw = get(url)
        destination = out / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(raw)
        files.append({"path": path, "url": url, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)})
    return {"repository": f"https://github.com/siliconjungle/{repo}", "commit": sha, "files": files}


if __name__ == "__main__":
    items = [(category, name) for category, names in SOURCES.items() for name in names.split()]
    with cf.ThreadPoolExecutor(max_workers=4) as pool:
        rows = list(pool.map(crate, items))
    frontiers = [frontier(repo) for repo in ("-shapeshift-labs-frontier", "-shapeshift-labs-frontier-realtime", "-shapeshift-labs-frontier-realtime-server", "-shapeshift-labs-frontier-queue", "-shapeshift-labs-frontier-scheduler", "-shapeshift-labs-frontier-event-log", "-shapeshift-labs-frontier-realtime-websocket")]
    result = {"schema": 1, "fetched_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "coverage": "Primary crates.io metadata across seven source families; a discovery catalog, not a claim of universal performance superiority.",
              "crates": rows, "frontier": frontiers}
    (ROOT / "research/inventory.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"candidates": len(rows), "fetched": sum(r["fetched"] for r in rows),
                      "failed": [r["name"] for r in rows if not r["fetched"]],
                      "frontier_files": sum(len(r["files"]) for r in frontiers)}))
