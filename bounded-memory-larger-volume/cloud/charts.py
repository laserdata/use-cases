"""Builds the working results page for the Large-cluster runs from the results files.

Every number on the page comes from results/<label>/rep-<n>-<test>/ (ramp.json, events.jsonl,
samples.csv, env.json); nothing is typed in by hand.

Usage: python3 cloud/charts.py results/cloud-large-3node out.html
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

PARTITIONS = [2, 10, 30, 40, 50]
COLORS = {2: "--c1", 10: "--c2", 30: "--c3", 40: "--c4", 50: "--c5"}


def events(d: Path) -> list[dict]:
    f = d / "events.jsonl"
    return [json.loads(x) for x in f.read_text().splitlines() if x.strip()] if f.exists() else []


def series(d: Path, role: str, phase_prefix: str | None = None, every: int = 5) -> list[list[float]]:
    """(seconds from first matching sample, RSS MiB) for one role, thinned to one point per `every` s."""
    rows = []
    with open(d / "samples.csv") as f:
        for r in csv.DictReader(f):
            if r["target"] != role or not r["rss_mib"]:
                continue
            if phase_prefix and not r["phase"].startswith(phase_prefix):
                continue
            rows.append((float(r["t"]), float(r["rss_mib"])))
    if not rows:
        return []
    t0 = rows[0][0]
    out, last = [], None
    for i, (t, v) in enumerate(rows):
        s = round(t - t0)
        if last is None or s - last >= every or i == len(rows) - 1:
            out.append([s, v])
            last = s
    return out


def ramp(root: Path, rep: int = 1) -> dict | None:
    d = root / f"rep-{rep}-ramp"
    if not (d / "ramp.json").exists():
        return None
    steps = json.loads((d / "ramp.json").read_text())["steps"]
    mem = series(d, "producer", every=5)
    # peak producer memory per step from the samples
    peaks = {}
    with open(d / "samples.csv") as f:
        for r in csv.DictReader(f):
            if r["target"] == "producer" and r["rss_mib"] and r["phase"].startswith("rate-"):
                k = int(r["phase"][5:])
                peaks[k] = max(peaks.get(k, 0), float(r["rss_mib"]))
    for s in steps:
        s["mem"] = peaks.get(s["target"])
    return {"steps": steps, "mem": mem, "env": json.loads((d / "env.json").read_text())}


def backlog(root: Path, p: int, rep: int = 1) -> dict | None:
    d = root / f"rep-{rep}-backlog-{p}"
    if not d.exists():
        return None
    ev = events(d)
    pre = next((e for e in ev if e.get("event") == "preloaded"), None)
    con = next((e for e in ev if e.get("event") == "consumed"), None)
    mem = series(d, "consumer", every=5)
    log = (d / "run.log").read_text() if (d / "run.log").exists() else ""
    ok = bool(con) and con.get("missing") == 0 and "status 0" in log
    return {"p": p, "pre": pre, "con": con, "mem": mem, "ok": ok,
            "peak": max((v for _, v in mem), default=None),
            "end": mem[-1][1] if mem else None,
            "server": server(d, pre, con)}


def server(d: Path, pre: dict | None, con: dict | None) -> dict | None:
    """Per-node server metrics recorded from the LaserData console (server-nodes.csv)."""
    f = d / "server-nodes.csv"
    if not f.exists() or not pre or not con:
        return None
    rows = list(csv.DictReader(open(f)))
    w0, w1, r1 = pre["t"] - pre["seconds"], pre["t"], con["t"]
    nodes = {}
    for r in rows:
        nodes.setdefault(int(r["node"]), []).append(r)
    out = {"t0": w0, "write_end": w1 - w0, "read_end": r1 - w0, "nodes": []}
    for n, rs in sorted(nodes.items()):
        rs.sort(key=lambda r: float(r["t"]))
        num = lambda r, k: float(r[k])
        during = [r for r in rs if w0 - 30 <= num(r, "t") <= r1 + 2]
        write = [r for r in rs if w0 <= num(r, "t") <= w1]
        read = [r for r in rs if w1 + 15 < num(r, "t") <= r1]
        after = [r for r in rs if num(r, "t") > r1 + 4]
        avg = lambda a, k: round(sum(num(r, k) for r in a) / len(a), 1) if a else None
        out["nodes"].append({
            "node": n, "role": "leader" if rs[-1]["role"] == "l" else "follower",
            "series": [[round(num(r, "t") - w0), num(r, "iggy_mem_mib")] for r in rs],
            "mem_start": num(during[0], "iggy_mem_mib") if during else None,
            "mem_write_peak": max((num(r, "iggy_mem_mib") for r in write), default=None),
            "mem_read_peak": max((num(r, "iggy_mem_mib") for r in read), default=None),
            "mem_after": num(after[0], "iggy_mem_mib") if after else None,
            "cpu_write": avg(write, "iggy_cpu"), "cpu_read": avg(read, "iggy_cpu"),
            "disk_start": num(during[0], "disk_used_mib") if during else None,
            "disk_peak": max((num(r, "disk_used_mib") for r in rs), default=None),
            "disk_after": num(after[0], "disk_used_mib") if after else None,
            "under_repl": max(int(r["under_repl"] or 0) for r in rs),
            "below_quorum": max(int(r["below_quorum"] or 0) for r in rs)})
    return out


def producer(root: Path, rep: int = 1) -> dict | None:
    d = root / f"rep-{rep}-producer"
    if not (d / "producer_outage-producer.json").exists():
        return None
    prod = json.loads((d / "producer_outage-producer.json").read_text())
    ver = json.loads((d / "producer_outage-verify.json").read_text())
    ev = events(d)
    cut = next(e["t"] for e in ev if e.get("event") == "network_cut")
    back = next(e["t"] for e in ev if e.get("event") == "network_restored")
    rows = [r for r in csv.DictReader(open(d / "samples.csv")) if r["target"] == "producer" and r["rss_mib"]]
    t0 = float(rows[0]["t"])
    phases: dict[str, list[float]] = {}
    for r in rows:
        phases.setdefault(r["phase"], []).append(float(r["rss_mib"]))
    srv = None
    f = d / "server-nodes.csv"
    if f.exists():
        nodes: dict[int, list] = {}
        for r in csv.DictReader(open(f)):
            nodes.setdefault(int(r["node"]), []).append(r)
        srv = [{"node": n, "role": "leader" if rs[-1]["role"] == "l" else "follower",
                "series": [[round(float(r["t"]) - t0), float(r["iggy_mem_mib"])] for r in rs]}
               for n, rs in sorted(nodes.items())]
    consumers = {}
    for i in range(ver["workers"]):
        consumers[i] = json.loads((d / f"producer_outage-consumer-{i}.json").read_text())
    return {"mem": series(d, "producer", every=5), "cut": round(cut - t0), "back": round(back - t0),
            "phase_mem": {k: [min(v), max(v), v[-1]] for k, v in phases.items()},
            "prod": prod, "verify": ver, "server": srv,
            "consumer_dups": sum(c["duplicates"] for c in consumers.values())}


def main() -> int:
    root, out = Path(sys.argv[1]), Path(sys.argv[2])
    data = {"ramp": ramp(root), "backlog": [b for p in PARTITIONS if (b := backlog(root, p))],
            "producer": producer(root)}
    tpl = (Path(__file__).parent / "charts_template.html").read_text()
    out.write_text(tpl.replace("/*DATA*/null", json.dumps(data)))
    print(f"wrote {out}: ramp={'yes' if data['ramp'] else 'no'}, backlog={[b['p'] for b in data['backlog']]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
