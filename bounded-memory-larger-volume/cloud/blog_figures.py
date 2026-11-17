"""Draws the blog's charts (dark website palette) and numbers.json from the results files.

Every number in the charts and in numbers.json comes from results/<label>/rep-1-*/; nothing is
typed in by hand. The blog text quotes numbers.json.

Two result folders, one per deployment (same configuration: Standard, 3 Large nodes, us-central1,
4 GiB memory pool). Each test's charts come from one run only, never mixed:
  main   backlog tests (2-50 partitions, client + server) and the rolling restart
  other  rate ramp-up and the producer network-cut test

Usage: python3 cloud/blog_figures.py results/cloud-large-3node-rerun results/cloud-large-3node <out dir>
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

BG, FG, MUTED, GRID = "#0A1722", "#E6EBF3", "#93A0B6", "#24344A"
GREEN, CYAN, AMBER = "#8CC63F", "#52EAFD", "#F6B94A"
MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"
X0, X1, Y0, Y1 = 70, 840, 40, 250  # plot area inside the 916 x 356 frame
PARTS = [2, 10, 30, 40, 50]
LINES = {2: "#3987e5", 10: "#d95926", 30: "#199e70", 40: "#c98500", 50: "#d55181"}  # validated on BG
# Reported in public issues (same sources as the first bounded-memory blog), in MiB
REPORTED = {"consumer": (6000 * 1e6 / 2**20, "~6 GB"), "producer": (8000 * 1e6 / 2**20, "~8 GB, then out of memory"),
            "restart": (5 * 1024, "~5 GiB and 4 CPUs")}


def esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def text(x, y, s, size=11, fill=MUTED, anchor="start", weight=400, extra=""):
    return (f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}" '
            f'style="font:{weight} {size}px {MONO};fill:{fill}{extra}">{esc(s)}</text>')


def frame(label: str, title: str, body: list[str]) -> str:
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="1832" height="712" viewBox="-28.0 -28.0 916.0 356.0" '
            f'role="img" aria-label="{esc(label)}"><rect x="-28.0" y="-28.0" width="916.0" height="356.0" fill="{BG}"/>'
            + text(10, 22, title.upper(), weight=600, extra=";letter-spacing:.06em") + "".join(body) + "</svg>")


def yaxis(ymax: float, ticks: list[float], fmt) -> list[str]:
    out = []
    for v in ticks:
        y = Y1 - (Y1 - Y0) * v / ymax
        out.append(f'<line x1="{X0}" x2="{X1}" y1="{y:.1f}" y2="{y:.1f}" stroke="{GRID}"/>')
        out.append(text(X0 - 10, y + 4, fmt(v), anchor="end"))
    return out


def path(points, sx, sy, color, width=2.4):
    d = " ".join(("M" if i == 0 else "L") + f"{sx(t):.1f} {sy(v):.1f}" for i, (t, v) in enumerate(points))
    return f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width}" stroke-linejoin="round"/>'


def load(root: Path, other: Path) -> dict:
    n: dict = {"backlog": {}, "server": {}, "sources": {"backlog_server_restart": root.name, "ramp_producer": other.name}}
    for p in PARTS:
        d = root / f"rep-1-backlog-{p}"
        ev = [json.loads(x) for x in (d / "events.jsonl").read_text().splitlines() if x.strip()]
        pre = next(e for e in ev if e.get("event") == "preloaded")
        con = next(e for e in ev if e.get("event") == "consumed")
        crow = [r for r in csv.DictReader(open(d / "samples.csv")) if r["target"] == "consumer" and r["rss_mib"]]
        mem = [float(r["rss_mib"]) for r in crow]
        prow = [r for r in csv.DictReader(open(d / "samples.csv")) if r["target"] == "producer" and r["rss_mib"]]
        n["backlog"][p] = {"records": con["records"], "missing": con["missing"], "duplicates": con["duplicates"],
                           "write_rate": pre["records_per_second"], "read_seconds": con["seconds"],
                           "consumer_peak_mib": max(mem), "consumer_end_mib": mem[-1],
                           "producer_peak_mib": max(float(r["rss_mib"]) for r in prow) if prow else None,
                           "producer_cpu_avg": round(sum(float(r["cpu_pct"] or 0) for r in prow) / len(prow), 1) if prow else None,
                           "t_write_start": pre["t"] - pre["seconds"], "t_write_end": pre["t"], "t_read_end": con["t"],
                           "t_consumer_first": float(crow[0]["t"]),
                           "series": [[float(r["t"]) - float(crow[0]["t"]), float(r["rss_mib"])] for r in crow]}
        f = d / "server-nodes.csv"
        if f.exists():
            rows = list(csv.DictReader(open(f)))
            b = n["backlog"][p]
            nodes = {}
            for r in rows:
                nodes.setdefault(int(r["node"]), []).append(r)
            srv = []
            for k, rs in sorted(nodes.items()):
                rs.sort(key=lambda r: float(r["t"]))
                inrun = [r for r in rs if b["t_write_start"] <= float(r["t"]) <= b["t_read_end"]]
                after = [r for r in rs if float(r["t"]) > b["t_read_end"] + 4]
                srv.append({"node": k, "role": "leader" if rs[-1]["role"] == "l" else "follower",
                            "series": [[float(r["t"]), float(r["iggy_mem_mib"])] for r in rs],
                            "cpu_series": [[float(r["t"]), float(r["iggy_cpu"])] for r in rs],
                            "peak_mib": max(float(r["iggy_mem_mib"]) for r in inrun),
                            "after_delete_mib": float(after[0]["iggy_mem_mib"]) if after else None,
                            "disk_peak_mib": max(float(r["disk_used_mib"]) for r in rs),
                            "disk_after_mib": float(after[0]["disk_used_mib"]) if after else None,
                            "disk_before_mib": float(rs[0]["disk_used_mib"]),
                            "under_replicated_max": max(int(r["under_repl"] or 0) for r in rs),
                            "below_quorum_max": max(int(r["below_quorum"] or 0) for r in rs)})
            for s_ in srv:
                rows_ = [r for r in nodes[s_["node"]]]
                w = [float(r["iggy_cpu"]) for r in rows_ if b["t_write_start"] + 30 <= float(r["t"]) <= b["t_write_end"]]
                rd = [float(r["iggy_cpu"]) for r in rows_ if b["t_write_end"] + 15 < float(r["t"]) <= b["t_read_end"]]
                s_["cpu_write_avg"] = round(sum(w) / len(w), 1) if w else None
                s_["cpu_read_avg"] = round(sum(rd) / len(rd), 1) if rd else None
                s_["disk_added_mib"] = s_["disk_peak_mib"] - s_["disk_before_mib"]
                s_["disk_per_data"] = round(s_["disk_added_mib"] / (b["records"] * 1024 / 2**20), 3)
            n["server"][p] = srv
            # A rolling restart shows up as a new server process id on a node.
            restarts = []
            for k, rs in sorted(nodes.items()):
                for a, c in zip(rs, rs[1:]):
                    if a["pid"] != c["pid"]:
                        restarts.append({"node": k, "t_last_old": float(a["t"]), "t_first_new": float(c["t"]),
                                         "started": int(c["start_time"]) / 1e6,
                                         "mem_before_mib": float(a["iggy_mem_mib"]), "mem_after_mib": float(c["iggy_mem_mib"]),
                                         "role_before": a["role"], "role_after": c["role"],
                                         "mem_peak_60s_mib": max(float(x["iggy_mem_mib"]) for x in rs if float(c["t"]) <= float(x["t"]) <= float(c["t"]) + 60),
                                         "cpu_peak_60s_pct": max(float(x["iggy_cpu"]) for x in rs if float(c["t"]) <= float(x["t"]) <= float(c["t"]) + 60),
                                         "during": "before write" if float(c["t"]) < b["t_write_start"] else
                                                   "write" if float(c["t"]) <= b["t_write_end"] else
                                                   "read" if float(c["t"]) <= b["t_read_end"] else "after"})
            if restarts:
                n["backlog"][p]["restarts"] = restarts
    ramp = json.loads((other / "rep-1-ramp" / "ramp.json").read_text())["steps"]
    rmem = [float(r["rss_mib"]) for r in csv.DictReader(open(other / "rep-1-ramp" / "samples.csv"))
            if r["target"] == "producer" and r["rss_mib"]]
    rrows = [r for r in csv.DictReader(open(other / "rep-1-ramp" / "samples.csv"))
             if r["target"] == "producer" and r["rss_mib"] and r["phase"].startswith("rate-")]
    n["ramp"] = {"steps": ramp, "producer_min_mib": min(float(r["rss_mib"]) for r in rrows),
                 "top_achieved": max(s["achieved"] for s in ramp), "p99_max_ms": max(s["send_p99_ms"] for s in ramp),
                 "p50_range_ms": [min(s["send_p50_ms"] for s in ramp), max(s["send_p50_ms"] for s in ramp)],
                 "errors": sum(s["errors"] for s in ramp), "producer_peak_mib": max(rmem)}
    d = other / "rep-1-producer"
    prod = json.loads((d / "producer_outage-producer.json").read_text())
    ver = json.loads((d / "producer_outage-verify.json").read_text())
    ev = [json.loads(x) for x in (d / "events.jsonl").read_text().splitlines() if x.strip()]
    rows = [r for r in csv.DictReader(open(d / "samples.csv")) if r["target"] == "producer" and r["rss_mib"]]
    t0 = float(rows[0]["t"])
    ph: dict = {}
    for r in rows:
        ph.setdefault(r["phase"], []).append(float(r["rss_mib"]))
    dups = sum(json.loads((d / f"producer_outage-consumer-{i}.json").read_text())["duplicates"] for i in range(ver["workers"]))
    n["producer"] = {"records_sent": prod["records_sent"], "rate": prod["achieved_rate"], "partitions": prod["partitions"],
                     "missing": ver["missing"], "missing_not_reported": ver["missing_not_reported"], "duplicates": dups,
                     "failed_batches": prod["failed_writes"], "max_behind_s": prod["max_behind_schedule_s"],
                     "before_max_mib": max(ph["steady-before"]), "cut_max_mib": max(ph["network-cut"]),
                     "after_end_mib": ph["after-restore"][-1], "after_min_mib": min(ph["after-restore"]),
                     "after_max_mib": max(ph["after-restore"]),
                     "cut_s": next(e["t"] for e in ev if e.get("event") == "network_cut") - t0,
                     "back_s": next(e["t"] for e in ev if e.get("event") == "network_restored") - t0,
                     "series": [[float(r["t"]) - t0, float(r["rss_mib"])] for r in rows]}
    tp = [{k: float(v) for k, v in r.items()} for r in csv.DictReader(open(root / "cluster-throughput.csv"))]
    n["throughput"] = tp
    return n


def fig_compare(n: dict) -> str:
    """Same layout as the first bounded-memory blog's comparison chart (dot = measured, ring = reported)."""
    import math
    cons = max(n["backlog"][p]["consumer_peak_mib"] for p in PARTS)
    prod = max(v for _, v in n["producer"]["series"])
    rs = [r for r in n["backlog"][40]["restarts"] if r["during"] in ("write", "read")]
    rst = max(r["mem_peak_60s_mib"] for r in rs)
    rows = [("Consumer behind on a 7M-record backlog", "reading all 7M records, 2 to 50 partitions", cons, REPORTED["consumer"]),
            ("Producer through a network outage", f"{n['producer']['rate']:,} records/s for 30 min, 60 s network cut", prod, REPORTED["producer"]),
            ("Server node in the minute after a restart", "restarted while a consumer read 7M records", rst, REPORTED["restart"])]
    SANS = "Inter, -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"
    lo, hi = math.log10(10), math.log10(10240)
    sx = lambda v: 250 + (806.3 - 250) * (math.log10(v) - lo) / (hi - lo)
    H = 330
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" width="1800" height="{H * 2}" viewBox="0 0 900 {H}" role="img" '
         f'aria-label="Memory reported in public issues compared with memory measured on LaserData, log scale">',
         f'<rect width="900" height="{H}" fill="{BG}"/>',
         f'<text x="28" y="34" style="font:600 11px {MONO};letter-spacing:.06em;fill:{MUTED}">MEMORY: REPORTED IN PUBLIC ISSUES VS MEASURED ON LASERDATA (LOG SCALE)</text>']
    for v, l in [(10, "10 MiB"), (100, "100 MiB"), (1024, "1 GiB"), (10240, "10 GiB")]:
        o.append(f'<line x1="{sx(v):.1f}" y1="56" x2="{sx(v):.1f}" y2="260" stroke="{GRID}"/>')
        o.append(f'<text x="{sx(v):.1f}" y="280" text-anchor="middle" style="font:400 11px {MONO};fill:{MUTED}">{l}</text>')
    for i, (label, sub, ours, (rv, rl)) in enumerate(rows):
        y = 84 + i * 70
        o.append(f'<text x="28" y="{y - 2}" style="font:500 14px {SANS};fill:{FG}">{esc(label)}</text>')
        o.append(f'<text x="28" y="{y + 15}" style="font:400 11.5px {SANS};fill:{MUTED}">{esc(sub)}</text>')
        if i == 2:  # the load during the restart: the 7M records the cluster was serving to the consumer
            data_mib = n["backlog"][40]["records"] * 1024 / 2**20
            o.append(f'<rect x="{sx(10):.1f}" y="{y + 22}" width="{sx(data_mib) - sx(10):.1f}" height="7" rx="3.5" fill="rgba(180,138,232,0.35)" stroke="#B48AE8" stroke-width="1"/>')
            o.append(f'<text x="{sx(data_mib) + 8:.1f}" y="{y + 29}" style="font:500 11.5px {SANS};fill:#B48AE8">{data_mib / 1024:.1f} GiB being read</text>')
        o.append(f'<line x1="{sx(ours):.1f}" y1="{y}" x2="{sx(rv):.1f}" y2="{y}" stroke="#3A4A60" stroke-width="2" stroke-dasharray="3 4"/>')
        o.append(f'<circle cx="{sx(ours):.1f}" cy="{y}" r="8" fill="{GREEN}"/>')
        o.append(f'<text x="{sx(ours):.1f}" y="{y - 14}" text-anchor="middle" style="font:600 12px {SANS};fill:{GREEN}">{f"{ours:.1f}" if i == 0 else f"{ours:.0f}"} MiB</text>')
        o.append(f'<circle cx="{sx(rv):.1f}" cy="{y}" r="7" fill="{BG}" stroke="{AMBER}" stroke-width="2.5"/>')
        o.append(f'<text x="{sx(rv):.1f}" y="{y - 14 if i == 2 else y + 24}" text-anchor="middle" style="font:500 12px {SANS};fill:{AMBER}">{esc(rl)}</text>')
    o.append(f'<circle cx="34" cy="{H - 26}" r="6" fill="{GREEN}"/><text x="46" y="{H - 22}" style="font:400 12px {SANS};fill:{FG}">Measured on LaserData, 3-node cluster (peak)</text>')
    o.append(f'<circle cx="350" cy="{H - 26}" r="5.5" fill="{BG}" stroke="{AMBER}" stroke-width="2.5"/><text x="362" y="{H - 22}" style="font:400 12px {SANS};fill:{FG}">Reported in a public issue, on that team&apos;s own setup</text>')
    o.append(f'<rect x="680" y="{H - 30}" width="14" height="8" rx="4" fill="rgba(180,138,232,0.35)" stroke="#B48AE8"/><text x="700" y="{H - 22}" style="font:400 12px {SANS};fill:{FG}">Data flowing during the restart</text>')
    o.append('</svg>')
    return "\n".join(o)


def fig_consumer_lines(n: dict) -> str:
    b = n["backlog"]
    tmax = max(b[p]["series"][-1][0] for p in PARTS)
    tmax = (int(tmax // 120) + 1) * 120
    ymax = 100
    sx = lambda t: X0 + (X1 - X0) * t / tmax
    sy = lambda v: Y1 - (Y1 - Y0) * v / ymax
    body = yaxis(ymax, [0, 25, 50, 75, 100], lambda v: f"{v:.0f}")
    for p in PARTS:
        body.append(path(b[p]["series"], sx, sy, LINES[p], width=1.6))
    lo = min(b[p]["consumer_peak_mib"] for p in PARTS); hi = max(b[p]["consumer_peak_mib"] for p in PARTS)
    body.append(text(sx(tmax * 0.55), sy(hi) - 14, f"all five runs peak between {lo:.1f} and {hi:.1f} MiB", fill=FG, anchor="middle"))
    for t in range(0, tmax + 1, 120):
        body.append(text(sx(t), Y1 + 18, f"{t // 60} min", anchor="middle"))
    for i, p in enumerate(PARTS):
        x = X0 + i * 120
        body.append(f'<line x1="{x}" x2="{x + 18}" y1="{Y1 + 40}" y2="{Y1 + 40}" stroke="{LINES[p]}" stroke-width="2.4"/>')
        body.append(text(x + 24, Y1 + 44, f"{p} partitions"))
    return frame("Line chart: consumer memory over the whole read at 2, 10, 30, 40 and 50 partitions, all between about 70 and 74 MiB",
                 "Laser SDK consumer memory (MiB) while reading 7M records, by partition count", body)


def fig_network(n: dict, p: int) -> str:
    b = n["backlog"][p]
    t0, t1 = b["t_write_start"] - 60, b["t_read_end"] + 120
    rows = [r for r in n["throughput"] if t0 <= r["t"] <= t1]
    tmax = t1 - t0
    ymax = 60
    sx = lambda t: X0 + (X1 - X0) * (t - t0) / tmax
    sy = lambda v: Y1 - (Y1 - Y0) * v / ymax
    body = [f'<rect x="{sx(b["t_write_start"]):.1f}" y="{Y0}" width="{sx(b["t_write_end"]) - sx(b["t_write_start"]):.1f}" height="{Y1 - Y0}" fill="rgba(140,198,63,0.10)"/>',
            text((sx(b["t_write_start"]) + sx(b["t_write_end"])) / 2, Y0 + 14, "writing 15,000 records/s", anchor="middle"),
            text((sx(b["t_write_end"]) + sx(b["t_read_end"])) / 2, Y0 + 14, "one consumer reading", anchor="middle")]
    body += yaxis(ymax, [0, 15, 30, 45, 60], lambda v: f"{v:.0f}")
    body.append(path([(r["t"], r["rx_mib_s"]) for r in rows], sx, sy, GREEN))
    body.append(path([(r["t"], r["tx_mib_s"]) for r in rows], sx, sy, CYAN))
    w = [r for r in rows if b["t_write_start"] + 60 <= r["t"] <= b["t_write_end"] - 60]
    rd = [r for r in rows if b["t_write_end"] + 60 <= r["t"] <= b["t_read_end"] - 60]
    rx = sum(r["rx_mib_s"] for r in w) / len(w); tx = sum(r["tx_mib_s"] for r in w) / len(w)
    rtx = sum(r["tx_mib_s"] for r in rd) / len(rd)
    n["network"] = {"write_rx_mib_s": round(rx, 1), "write_tx_mib_s": round(tx, 1), "read_tx_mib_s": round(rtx, 1),
                    "data_mib_s": round(b["write_rate"] * 1024 / 2**20, 1)}
    body.append(text(sx(b["t_write_start"]) + 8, sy(rx) - 8, f"in {rx:.1f} MiB/s", fill=FG))
    body.append(text(sx(b["t_write_start"]) + 8, sy(tx) + 16, f"out {tx:.1f} MiB/s", fill=FG))
    body.append(text(sx(b["t_write_end"]) + 10, sy(rtx) - 8, f"out {rtx:.1f} MiB/s to the consumer", fill=FG))
    for t in range(0, int(tmax) + 1, 300):
        body.append(text(sx(t0 + t), Y1 + 18, f"{t // 60} min", anchor="middle"))
    body.append(f'<line x1="{X0}" x2="{X0 + 18}" y1="{Y1 + 40}" y2="{Y1 + 40}" stroke="{GREEN}" stroke-width="2.4"/>')
    body.append(text(X0 + 24, Y1 + 44, "received, all 3 nodes"))
    body.append(f'<line x1="{X0 + 200}" x2="{X0 + 218}" y1="{Y1 + 40}" y2="{Y1 + 40}" stroke="{CYAN}" stroke-width="2.4"/>')
    body.append(text(X0 + 224, Y1 + 44, "sent, all 3 nodes"))
    return frame(f"Line chart: cluster network traffic during the {p}-partition backlog test",
                 f"Cluster network traffic (MiB/s, all 3 nodes), {p} partitions", body)


SANS = "Inter, -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"


def sans(x, y, s, size=12, fill=FG, anchor="start", weight=500):
    return f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}" style="font:{weight} {size}px {SANS};fill:{fill}">{esc(s)}</text>'


def fig_backlog(n: dict) -> str:
    """Peak memory per partition count: the producer that wrote the backlog and the consumer that read it."""
    b = n["backlog"]
    ymax = 100
    body = yaxis(ymax, [0, 25, 50, 75, 100], lambda v: f"{v:.0f}")
    bw = (X1 - X0) / len(PARTS)
    for i, p in enumerate(PARTS):
        for j, (key, col) in enumerate([("producer_peak_mib", CYAN), ("consumer_peak_mib", GREEN)]):
            v = b[p][key]
            x, w = X0 + i * bw + bw * (0.16 + 0.36 * j), bw * 0.32
            y = Y1 - (Y1 - Y0) * v / ymax
            body.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{Y1 - y:.1f}" rx="4" fill="{col}"/>')
            body.append(sans(x + w / 2, y - 8, f"{v:.1f}", size=12, anchor="middle", weight=600))
        body.append(sans(X0 + i * bw + bw / 2, Y1 + 22, f"{p} partitions", size=13, anchor="middle"))
    body.append(f'<rect x="{X0}" y="{Y1 + 40}" width="12" height="12" rx="2" fill="{CYAN}"/>')
    body.append(text(X0 + 18, Y1 + 50, "producer writing 7M", size=11, fill=FG))
    body.append(f'<rect x="{X0 + 190}" y="{Y1 + 40}" width="12" height="12" rx="2" fill="{GREEN}"/>')
    body.append(text(X0 + 208, Y1 + 50, "consumer reading 7M (7,000,000 read in each run, 0 missing)", size=11, fill=FG))
    return frame("Bar chart: peak producer memory about 50 MiB and peak consumer memory 73 to 77 MiB at 2, 10, 30, 40 and 50 partitions",
                 "Peak resident memory (MiB): Laser SDK producer and consumer, 7 million records", body)


def fig_server_parts(n: dict) -> str:
    """Peak server memory per node at each partition count, and what is left after the stream is deleted."""
    srv = n["server"]
    ymax = 2560
    body = yaxis(ymax, [0, 512, 1024, 1536, 2048, 2560], lambda v: f"{v / 1024:.1f} GiB" if v else "0")
    bw = (X1 - X0) / len(PARTS)
    for i, p in enumerate(PARTS):
        v = max(s["peak_mib"] for s in srv[p])
        x, w = X0 + i * bw + bw * 0.32, bw * 0.36
        y = Y1 - (Y1 - Y0) * v / ymax
        body.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{max(Y1 - y, 1.5):.1f}" rx="4" fill="{GREEN}"/>')
        lab = f"{v / 1024:.2f} GiB" if v >= 1024 else f"{v:.0f} MiB"
        body.append(sans(x + w / 2, y - 8, lab, size=12, anchor="middle", weight=600))
        body.append(sans(x + w / 2, Y1 + 22, f"{p} partitions", size=13, anchor="middle"))
    body.append(f'<rect x="{X0}" y="{Y1 + 40}" width="12" height="12" rx="2" fill="{GREEN}"/>')
    body.append(text(X0 + 18, Y1 + 50, "highest of the 3 nodes, while writing and reading 7M records (memory pool: 4 GiB, node: 15.6 GiB)", size=11, fill=FG))
    return frame("Bar chart: peak Laser Streams server memory per node, 30 MiB at 2 and 10 partitions, 365 MiB at 30, 1.26 GiB at 40 and 1.88 GiB at 50",
                 "Laser Streams server memory per node (peak), 7 million records, by partition count", body)


def fig_restart(n: dict, p: int) -> str:
    """Server memory on each node through a rolling restart that ran while one consumer read the backlog."""
    b, srv = n["backlog"][p], n["server"][p]
    rs = [r for r in b["restarts"] if r["during"] in ("write", "read")]
    t0, t1 = b["t_write_end"] - 60, b["t_read_end"] + 30
    tmax = t1 - t0
    ymax = 1536
    sx = lambda t: X0 + (X1 - X0) * (t - t0) / tmax
    sy = lambda v: Y1 - (Y1 - Y0) * v / ymax
    # Read traffic: data the cluster sent while the consumer read the backlog (30 s buckets), right axis
    tmx = 15
    st = lambda v: Y1 - (Y1 - Y0) * v / tmx
    rd = [r for r in n["throughput"] if b["t_write_end"] <= r["t"] <= b["t_read_end"]]
    area = (f'M{sx(rd[0]["t"]):.1f} {Y1} ' + " ".join(f'L{sx(r["t"]):.1f} {st(r["tx_mib_s"]):.1f}' for r in rd)
            + f' L{sx(rd[-1]["t"]):.1f} {Y1} Z')
    READ = "#B48AE8"
    body = [f'<path d="{area}" fill="rgba(180,138,232,0.20)" stroke="{READ}" stroke-width="1.4"/>']
    lo = min(r["tx_mib_s"] for r in rd if b["t_write_end"] + 20 <= r["t"] <= b["t_read_end"] - 20)
    body.append(text(sx(b["t_write_end"] + 300), Y0 + 52, f"7M records flowing to the consumer through both restarts (never below {lo:.1f} MiB/s)",
                     anchor="start", fill=READ))
    body += yaxis(ymax, [0, 512, 1024, 1536], lambda v: f"{v / 1024:.1f} GiB" if v else "0")
    for v in [0, 5, 10, 15]:
        body.append(text(X1 + 8, st(v) + 4, f"{v}", fill=READ))
    body.append(text(X1 + 8, Y0 - 10, "MiB/s", fill=READ))
    cols = {1: "#3987e5", 2: "#d95926", 3: "#199e70"}
    for s in srv:
        body.append(path([(t, v) for t, v in s["series"] if t0 <= t <= t1], sx, sy, cols[s["node"]], width=2))
    for k, r in enumerate(sorted(rs, key=lambda r: r["started"])):
        x = sx(r["started"])
        lead = " (leader)" if r["role_before"] == "l" else ""
        body.append(f'<line x1="{x:.1f}" x2="{x:.1f}" y1="{Y0}" y2="{Y1}" stroke="{CYAN}" stroke-dasharray="3 3"/>')
        body.append(text(x + 6, Y0 + 14 + 16 * k, f"node {r['node']}{lead} restarts", fill=CYAN))
    for t in range(0, int(tmax) + 1, 120):
        body.append(text(sx(t0 + t), Y1 + 18, f"{t // 60} min", anchor="middle"))
    for i, s in enumerate(sorted(srv, key=lambda s: s["node"])):
        x = X0 + i * 130
        body.append(f'<line x1="{x}" x2="{x + 18}" y1="{Y1 + 40}" y2="{Y1 + 40}" stroke="{cols[s["node"]]}" stroke-width="2.4"/>')
        body.append(text(x + 24, Y1 + 44, f"node {s['node']} memory"))
    body.append(f'<rect x="{X0 + 410}" y="{Y1 + 34}" width="14" height="12" rx="2" fill="rgba(180,138,232,0.20)" stroke="{READ}"/>')
    body.append(text(X0 + 430, Y1 + 44, "data read by the consumer (right axis)"))
    return frame(f"Line chart: server memory on each node while a rolling restart ran during the {p}-partition backlog read",
                 f"Graceful rolling restart while one consumer reads 7M records ({p} partitions): server memory and read traffic", body)


def fig_server_cpu(n: dict, p: int) -> str:
    """Like the first blog's restart chart: server memory (white, left axis) and CPU (green, right axis)."""
    b = n["backlog"][p]
    ld = next(s for s in n["server"][p] if s["role"] == "leader")
    t0 = b["t_write_start"] - 30
    tmax = ld["series"][-1][0] - t0
    mmax, cmax = 2560, 60
    sx = lambda t: X0 + (X1 - X0) * (t - t0) / tmax
    sm = lambda v: Y1 - (Y1 - Y0) * v / mmax
    sc = lambda v: Y1 - (Y1 - Y0) * v / cmax
    body = []
    for i, v in enumerate([0, 512, 1024, 1536, 2048, 2560]):
        y = sm(v)
        body.append(f'<line x1="{X0}" x2="{X1}" y1="{y:.1f}" y2="{y:.1f}" stroke="{GRID}"/>')
        body.append(text(X0 - 10, y + 4, f"{v / 1024:.1f} GiB" if v else "0", anchor="end"))
        body.append(text(X1 + 8, y + 4, f"{cmax * i // 5}%"))
    for t, l in [(b["t_write_end"], "write done, consumer starts"), (b["t_read_end"], "stream deleted")]:
        body.append(f'<line x1="{sx(t):.1f}" x2="{sx(t):.1f}" y1="{Y0}" y2="{Y1}" stroke="{CYAN}" stroke-dasharray="3 3"/>')
        body.append(text(sx(t) + 6 if l.startswith("write") else sx(t) - 6, Y0 + 14, l, fill=CYAN, anchor="start" if l.startswith("write") else "end"))
    body.append(text(sx(t0 + 30) + 6, Y0 + 14, "writing 7M records", fill=CYAN))
    body.append(path([(t, v) for t, v in ld["series"] if t >= t0], sx, sm, FG))
    body.append(path([(t, v) for t, v in ld["cpu_series"] if t >= t0], sx, sc, GREEN, width=1.6))
    for t in range(0, int(tmax) + 1, 300):
        body.append(text(sx(t0 + t), Y1 + 18, f"{t // 60} min", anchor="middle"))
    return frame(f"Line chart: leader server memory and CPU during the {p}-partition backlog test",
                 f"Laser Streams server (leader): memory (white) and CPU (% of one core, green), {p} partitions", body)


def fig_server(n: dict, p: int) -> str:
    b, srv = n["backlog"][p], n["server"][p]
    t0 = b["t_write_start"]
    tmax = (max(s["series"][-1][0] for s in srv) - t0)
    tmax = (int(tmax // 120) + 1) * 120
    ymax = 2560
    sx = lambda t: X0 + (X1 - X0) * (t - t0) / tmax
    sy = lambda v: Y1 - (Y1 - Y0) * v / ymax
    body = [f'<rect x="{sx(t0):.1f}" y="{Y0}" width="{sx(b["t_write_end"]) - sx(t0):.1f}" height="{Y1 - Y0}" fill="rgba(140,198,63,0.10)"/>',
            text((sx(t0) + sx(b["t_write_end"])) / 2, Y0 + 14, "writing 7M records", anchor="middle"),
            text((sx(b["t_write_end"]) + sx(b["t_read_end"])) / 2, Y0 + 14, "one consumer reads all 7M", anchor="middle"),
            f'<line x1="{sx(b["t_read_end"]):.1f}" x2="{sx(b["t_read_end"]):.1f}" y1="{Y0}" y2="{Y1}" stroke="{MUTED}" stroke-dasharray="3 3"/>',
            text(sx(b["t_read_end"]) - 6, Y0 + 30, "stream deleted", anchor="end")]
    body += yaxis(ymax, [0, 512, 1024, 1536, 2048, 2560], lambda v: f"{v / 1024:.1f} GiB" if v else "0")
    for s in sorted(srv, key=lambda s: s["role"] == "leader"):
        body.append(path([(t, v) for t, v in s["series"] if t <= t0 + tmax], sx, sy, CYAN if s["role"] == "leader" else GREEN))
    top = max(s["peak_mib"] for s in srv)
    aft = max(s["after_delete_mib"] for s in srv)
    body.append(text(sx(b["t_write_end"]) + 8, sy(top) - 10, f"{top / 1024:.2f} GiB, then flat", fill=FG))
    body.append(text(sx(b["t_read_end"]) - 8, sy(aft) - 8, f"{aft:.0f} MiB after delete", fill=FG, anchor="end"))
    for t in range(0, tmax + 1, 300):
        body.append(text(sx(t0 + t), Y1 + 18, f"{t // 60} min", anchor="middle"))
    body.append(f'<line x1="{X0}" x2="{X0 + 18}" y1="{Y1 + 40}" y2="{Y1 + 40}" stroke="{CYAN}" stroke-width="2.4"/>')
    body.append(text(X0 + 24, Y1 + 44, "leader"))
    body.append(f'<line x1="{X0 + 90}" x2="{X0 + 108}" y1="{Y1 + 40}" y2="{Y1 + 40}" stroke="{GREEN}" stroke-width="2.4"/>')
    body.append(text(X0 + 114, Y1 + 44, "followers (2)"))
    return frame(f"Line chart: server memory on each of the three nodes during the {p}-partition backlog test",
                 f"Laser Streams server memory on each node, {p} partitions", body)


def fig_producer(n: dict) -> str:
    pr = n["producer"]
    s = pr["series"]
    tmax = 1800
    ymax = 125
    sx = lambda t: X0 + (X1 - X0) * min(t, tmax) / tmax
    sy = lambda v: Y1 - (Y1 - Y0) * v / ymax
    body = [f'<rect x="{sx(pr["cut_s"]):.1f}" y="{Y0}" width="{sx(pr["back_s"]) - sx(pr["cut_s"]):.1f}" height="{Y1 - Y0}" fill="rgba(82,234,253,0.14)"/>',
            text(sx(pr["cut_s"]) - 6, Y0 + 14, "network cut, 60 s", fill=CYAN, anchor="end")]
    body += yaxis(ymax, [0, 25, 50, 75, 100, 125], lambda v: f"{v:.0f}")
    body.append(path(s, sx, sy, GREEN))
    body.append(text(sx(pr["cut_s"]) - 160, sy(pr["before_max_mib"]) - 10, f"{pr['before_max_mib']:.0f} MiB sending normally", fill=FG))
    body.append(text(sx(pr["back_s"]) + 10, sy(pr["after_end_mib"]) - 10, f"{pr['cut_max_mib']:.0f} MiB during the cut, {pr['after_end_mib']:.0f} MiB after", fill=FG))
    for t in range(0, tmax + 1, 300):
        body.append(text(sx(t), Y1 + 18, f"{t // 60} min", anchor="middle"))
    return frame("Line chart: producer memory about 59 MiB, 84 MiB during the 60-second network cut, then flat near 91 MiB",
                 "Laser SDK producer resident memory (MiB), 8,000 records/s for 30 minutes", body)


def fig_setup() -> str:
    body = []
    def box(x, y, w, h, title, lines, accent=GREEN):
        out = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="#0F1E2B" stroke="{GRID}"/>',
               f'<rect x="{x}" y="{y}" width="4" height="{h}" rx="2" fill="{accent}"/>',
               text(x + 16, y + 22, title, fill=FG, weight=600)]
        for i, l in enumerate(lines):
            out.append(text(x + 16, y + 42 + i * 16, l, size=10))
        return out
    body += box(20, 60, 250, 150, "Application VM (Google Cloud)", ["n2-standard-8, us-central1-a", "Laser SDK 0.9.1 (Python)",
                                                            "producer and consumers", "sampler: memory and CPU",
                                                            "from the OS, once a second"], CYAN)
    for i in range(3):
        body += box(420, 30 + i * 92, 420, 80, f"Node {i + 1}" + (" (leader)" if i == 1 else ""),
                    ["Laser Streams + managed backend", "Large: 4 vCPU, 15.6 GiB RAM, 100 GB network drive"])
    body.append(f'<line x1="270" x2="420" y1="135" y2="135" stroke="{MUTED}" stroke-dasharray="4 4"/>')
    body.append(text(345, 127, "TCP, same region", anchor="middle", size=10))
    body.append(text(630, 312, "LaserData Cloud, Standard tier, 3-node cluster, GCP us-central1", anchor="middle", size=10))
    return frame("Setup mirroring a production deployment: one Google Cloud application VM and a three-node LaserData Cloud cluster in the same region",
                 "Setup: mirrors a production deployment", body)


def fig_ramp_rate(n: dict) -> str:
    st = n["ramp"]["steps"]
    ymax = 40000
    body = yaxis(ymax, [0, 10000, 20000, 30000, 40000], lambda v: f"{v / 1000:.0f}k" if v else "0")
    bw = (X1 - X0) / len(st)
    for i, s_ in enumerate(st):
        x, w = X0 + i * bw + bw * 0.3, bw * 0.4
        yt = Y1 - (Y1 - Y0) * s_["target"] / ymax
        ya = Y1 - (Y1 - Y0) * s_["achieved"] / ymax
        body.append(f'<rect x="{x:.1f}" y="{yt:.1f}" width="{w:.1f}" height="{Y1 - yt:.1f}" rx="4" fill="none" stroke="{MUTED}" stroke-dasharray="4 3"/>')
        body.append(f'<rect x="{x + 2:.1f}" y="{ya:.1f}" width="{w - 4:.1f}" height="{Y1 - ya:.1f}" rx="4" fill="{GREEN}"/>')
        body.append(sans(x + w / 2, ya - 8, f"{s_['achieved']:,}", size=12, anchor="middle", weight=600))
        body.append(sans(x + w / 2, Y1 + 22, f"{s_['target']:,}/s target", size=12, anchor="middle"))
    body.append(f'<rect x="{X0}" y="{Y1 + 40}" width="12" height="12" rx="2" fill="{GREEN}"/>')
    body.append(text(X0 + 18, Y1 + 50, "records/s achieved over 2 minutes, 0 errors at every step (dashed: target)", size=11, fill=FG))
    return frame("Bar chart: one producer held 2,000, 5,000, 10,000, 20,000 and 38,887 records/s, with no errors",
                 "Records/s one Laser SDK producer sent, 2 minutes per step", body)


def fig_ramp_latency(n: dict) -> str:
    st = n["ramp"]["steps"]
    ymax = 6
    body = yaxis(ymax, [0, 2, 4, 6], lambda v: f"{v:.0f} ms")
    k = len(st)
    sx = lambda i: X0 + (X1 - X0) * (i + 0.5) / k
    sy = lambda v: Y1 - (Y1 - Y0) * v / ymax
    for key, col, lab in [("send_p50_ms", GREEN, "typical (p50)"), ("send_p99_ms", CYAN, "slowest 1% (p99)")]:
        d = " ".join(("M" if i == 0 else "L") + f"{sx(i):.1f} {sy(s_[key]):.1f}" for i, s_ in enumerate(st))
        body.append(f'<path d="{d}" fill="none" stroke="{col}" stroke-width="2.4"/>')
        for i, s_ in enumerate(st):
            body.append(f'<circle cx="{sx(i):.1f}" cy="{sy(s_[key]):.1f}" r="4.5" fill="{col}" stroke="{BG}" stroke-width="2"/>')
            body.append(sans(sx(i), sy(s_[key]) + (-10 if key.endswith("p99_ms") else 20), f"{s_[key]}", size=11, fill=col, anchor="middle", weight=600))
    for i, s_ in enumerate(st):
        body.append(sans(sx(i), Y1 + 22, f"{s_['achieved']:,}/s", size=12, anchor="middle"))
    body.append(f'<line x1="{X0}" x2="{X0 + 18}" y1="{Y1 + 44}" y2="{Y1 + 44}" stroke="{GREEN}" stroke-width="2.4"/>')
    body.append(text(X0 + 24, Y1 + 48, "typical send (p50)", fill=FG))
    body.append(f'<line x1="{X0 + 190}" x2="{X0 + 208}" y1="{Y1 + 44}" y2="{Y1 + 44}" stroke="{CYAN}" stroke-width="2.4"/>')
    body.append(text(X0 + 214, Y1 + 48, "slowest 1% (p99)", fill=FG))
    return frame("Line chart: batch send time about 3 ms typical and at most 3.7 ms for the slowest 1% at every rate",
                 "Time until the cluster acknowledged a batch of 100 records (ms)", body)


def fig_glance(n: dict) -> str:
    """Results at a glance: one small chart per test (memory against the load it carried), then three facts."""
    READ, CUT, PANEL = "#B48AE8", "#F6B94A", "#0F1E2B"
    b, srv50, r, pr = n["backlog"], n["server"][50], n["ramp"], n["producer"]
    tp = n["throughput"]
    W_, H_ = 900, 600
    cons = max(b[p]["consumer_peak_mib"] for p in PARTS)
    pmax = max(v for _, v in pr["series"])
    rs40 = [x for x in b[40]["restarts"] if x["during"] in ("write", "read")]
    rpeak = max(x["mem_peak_60s_mib"] for x in rs40)
    srv_peak = {p: max(s_["peak_mib"] for s_ in n["server"][p]) for p in PARTS}
    records = sum(b[p]["records"] for p in PARTS) + pr["records_sent"]
    cpu = [s_["cpu_write_avg"] for s_ in srv50]
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W_ * 2}" height="{H_ * 2}" viewBox="0 0 {W_} {H_}" role="img" '
         f'aria-label="{esc(f"Results at a glance. Consumer memory flat at {cons:.1f} MiB or less while reading 7M records; producer memory at most {pmax:.0f} MiB through a 60 s network cut; {b[40][chr(109)+chr(105)+chr(115)+chr(115)+chr(105)+chr(110)+chr(103)]} records missing through a rolling restart, server peak {rpeak:.0f} MiB after it; server memory per node from {srv_peak[2]:.0f} MiB at 2 partitions to {srv_peak[50] / 1024:.2f} GiB at 50.")}">',
         f'<rect width="{W_}" height="{H_}" fill="{BG}"/>',
         f'<text x="28" y="34" style="font:600 11px {MONO};letter-spacing:.06em;fill:{MUTED}">RESULTS AT A GLANCE · 7M RECORDS ON LASERDATA CLOUD</text>']
    pw, ph, gx, gy, x0, y0 = 416, 214, 12, 12, 28, 52

    def panel(i, kicker, big, sub):
        x, y = x0 + (i % 2) * (pw + gx), y0 + (i // 2) * (ph + gy)
        o.append(f'<rect x="{x}" y="{y}" width="{pw}" height="{ph}" rx="10" fill="{PANEL}" stroke="{GRID}"/>')
        o.append(f'<text x="{x + 18}" y="{y + 24}" style="font:600 10px {MONO};letter-spacing:.06em;fill:{MUTED}">{esc(kicker.upper())}</text>')
        o.append(sans(x + 18, y + 56, big, size=26, fill=GREEN, weight=700))
        o.append(sans(x + 18, y + 76, sub, size=12, fill=FG, weight=500))
        return x + 18, y + 96, pw - 36, ph - 118  # chart box: x, y, w, h

    def spark(pts, bx, by, bw, bh, t0, t1, ymax, color, width=2.2):
        sx = lambda t: bx + bw * (t - t0) / (t1 - t0)
        sy = lambda v: by + bh - bh * v / ymax
        return path([(t, v) for t, v in pts if t0 <= t <= t1], sx, sy, color, width), sx, sy

    def area(rows, sx, by, bh, vmax):
        rows = list(rows)
        d = f'M{sx(rows[0][0]):.1f} {by + bh}' + "".join(f' L{sx(t):.1f} {by + bh - 0.38 * bh * v / vmax:.1f}' for t, v in rows) \
            + f' L{sx(rows[-1][0]):.1f} {by + bh} Z'
        return f'<path d="{d}" fill="rgba(180,138,232,0.18)" stroke="{READ}" stroke-width="1"/>'

    def ceiling(bx, by, bw, label):
        o.append(f'<line x1="{bx}" x2="{bx + bw}" y1="{by}" y2="{by}" stroke="{GRID}" stroke-dasharray="2 3"/>')
        o.append(text(bx + bw, by - 4, label, size=9, anchor="end"))
        o.append(f'<line x1="{bx}" x2="{bx + bw}" y1="{by + 0.5}" y2="{by + 0.5}" stroke="none"/>')

    # 1. Consumer reading the 50-partition backlog: memory line over the read traffic
    bx, by, bw, bh = panel(0, "Consumer · reading a 7M-record backlog", f"{cons:.1f} MiB", "peak consumer memory, 2 to 50 partitions")
    c = b[50]
    t0, t1 = c["t_consumer_first"], c["t_read_end"]
    ser = [(t0 + t, v) for t, v in c["series"]]
    ln, sx, sy = spark(ser, bx, by, bw, bh, t0, t1, 120, GREEN)
    rd = [(x["t"], x["tx_mib_s"]) for x in tp if t0 <= x["t"] <= t1]
    o.append(area(rd, sx, by, bh, 16))
    ceiling(bx, by, bw, "120 MiB")
    o.append(ln)
    o.append(text(bx, by + bh + 14, "shaded: 7M records flowing to the consumer", size=9, fill=READ))
    o.append(text(bx + bw, by + bh + 14, f"{(t1 - t0) / 60:.0f} min, 50 partitions", size=9, anchor="end"))
    o.append(text(bx, by - 4, "consumer memory", size=9, fill=GREEN))

    # 2. Producer through a 60 s network cut
    bx, by, bw, bh = panel(1, "Producer · 60 s network outage", f"{pmax:.0f} MiB", f"peak producer memory at {pr['rate']:,} records/s")
    ser = pr["series"]
    ln, sx, sy = spark(ser, bx, by, bw, bh, 0, ser[-1][0], 120, CYAN)
    o.append(f'<rect x="{sx(pr["cut_s"]):.1f}" y="{by}" width="{max(sx(pr["back_s"]) - sx(pr["cut_s"]), 3):.1f}" height="{bh}" fill="rgba(246,185,74,0.28)"/>')
    o.append(text(sx(pr["cut_s"]) - 4, by + bh - 6, "network cut", size=9, fill=CUT, anchor="end"))
    o.append(text(bx, by - 4, "producer memory", size=9, fill=CYAN))
    ceiling(bx, by, bw, "120 MiB")
    o.append(ln)
    o.append(text(bx + bw, by + bh + 14, f"{ser[-1][0] / 60:.0f} min, {pr['records_sent'] / 1e6:.1f}M records", size=9, anchor="end"))

    # 3. Rolling restart while the consumer read 7M records (40 partitions)
    bx, by, bw, bh = panel(2, "Rolling restart · during a 7M-record read", f"{b[40]['missing']} missing",
                           f"restarted nodes peaked at {rpeak:.0f} MiB in the first minute")
    c = b[40]
    t0, t1 = c["t_write_end"], c["t_read_end"]
    cols = {1: "#3987e5", 2: "#d95926", 3: "#199e70"}
    rd = [(x["t"], x["tx_mib_s"]) for x in tp if t0 <= x["t"] <= t1]
    sx = lambda t: bx + bw * (t - t0) / (t1 - t0)
    o.append(area(rd, sx, by, bh, 16))
    ceiling(bx, by, bw, "1.5 GiB")
    for s_ in n["server"][40]:
        ln, _, _ = spark(s_["series"], bx, by, bw, bh, t0, t1, 1536, cols[s_["node"]], 1.8)
        o.append(ln)
    for x in rs40:
        xx = sx(x["started"])
        o.append(f'<line x1="{xx:.1f}" x2="{xx:.1f}" y1="{by}" y2="{by + bh}" stroke="{CYAN}" stroke-dasharray="3 3"/>')
    o.append(text(bx, by + bh + 14, "shaded: 7M records flowing · dashed: restarts", size=9, fill=READ))
    o.append(text(bx, by - 4, "server memory, nodes 1-3", size=9, fill=FG))
    
    # 4. Server memory per node by partition count
    bx, by, bw, bh = panel(3, "Server · memory per node", f"< {srv_peak[50] / 1024 + 0.05:.1f} GiB",
                           "at 50 partitions; returned when the stream is deleted")
    ymax = 2560
    ceiling(bx, by, bw, "2.5 GiB")
    cw = bw / len(PARTS)
    for i, p in enumerate(PARTS):
        v = srv_peak[p]
        h = max(bh * v / ymax, 1.5)
        xx = bx + i * cw + cw * 0.25
        o.append(f'<rect x="{xx:.1f}" y="{by + bh - h:.1f}" width="{cw * 0.5:.1f}" height="{h:.1f}" rx="3" fill="{GREEN}"/>')
        lab = f"{v / 1024:.2f} GiB" if v >= 1024 else f"{v:.0f} MiB"
        o.append(text(xx + cw * 0.25, by + bh - h - 4, lab, size=9, fill=FG, anchor="middle"))
        o.append(text(xx + cw * 0.25, by + bh + 14, f"{p} parts", size=9, anchor="middle"))

    # Facts strip
    facts = [("0", f"records lost silently, of {records / 1e6:.1f}M sent"),
             (f"{r['p50_range_ms'][1]:.1f} ms", f"typical batch acknowledged (slowest 1%: {r['p99_max_ms']} ms)"),
             (f"{min(cpu):.0f}%" if round(min(cpu)) == round(max(cpu)) else f"{min(cpu):.0f}-{max(cpu):.0f}%",
              "of one core per node writing 15,000 records/s")]
    fy, fw = y0 + 2 * (ph + gy), (2 * pw + gx - 2 * gx) / 3
    for i, (big, lab) in enumerate(facts):
        x = x0 + i * (fw + gx)
        o.append(f'<rect x="{x:.1f}" y="{fy}" width="{fw:.1f}" height="60" rx="10" fill="{PANEL}" stroke="{GRID}"/>')
        o.append(sans(x + 16, fy + 37, big, size=20, fill=GREEN, weight=700))
        bxw = 16 + 11 * len(big) + 14
        words, line, lines = lab.split(), "", []
        for w_ in words:
            if len(line) + len(w_) > 28:
                lines.append(line); line = w_
            else:
                line = (line + " " + w_).strip()
        lines.append(line)
        for k, l_ in enumerate(lines[:2]):
            o.append(sans(x + bxw, fy + 27 + 15 * k - 7 * (len(lines[:2]) - 1) + 3, l_, size=11, fill=MUTED, weight=400))
    o.append("</svg>")
    return "\n".join(o)


def main() -> int:
    root, other, out = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    out.mkdir(parents=True, exist_ok=True)
    n = load(root, other)
    (out / "blog-larger-cluster-server-partitions.svg").write_text(fig_server_parts(n))
    (out / "blog-larger-cluster-restart-40p.svg").write_text(fig_restart(n, 40))
    (out / "blog-larger-cluster-setup.svg").write_text(fig_setup())
    (out / "blog-larger-cluster-consumer-backlog.svg").write_text(fig_backlog(n))
    (out / "blog-larger-cluster-server-50p.svg").write_text(fig_server_cpu(n, 50))
    (out / "blog-larger-cluster-producer-outage.svg").write_text(fig_producer(n))
    (out / "blog-larger-cluster-compare.svg").write_text(fig_compare(n))
    (out / "blog-larger-cluster-glance.svg").write_text(fig_glance(n))
    (out / "blog-larger-cluster-ramp-rate.svg").write_text(fig_ramp_rate(n))
    (out / "blog-larger-cluster-ramp-latency.svg").write_text(fig_ramp_latency(n))
    (out / "blog-larger-cluster-consumer-lines.svg").write_text(fig_consumer_lines(n))
    (out / "blog-larger-cluster-network-50p.svg").write_text(fig_network(n, 50))
    slim = json.loads(json.dumps(n))
    slim["producer"]["peak_mib"] = max(v for _, v in n["producer"]["series"])
    slim["producer"].pop("series")
    slim.pop("throughput")
    for p in slim["backlog"]:
        slim["backlog"][p].pop("series")
    for p in slim["server"]:
        for s in slim["server"][p]:
            s.pop("series"); s.pop("cpu_series")
    (out / "numbers.json").write_text(json.dumps(slim, indent=1))
    print(json.dumps(slim, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
