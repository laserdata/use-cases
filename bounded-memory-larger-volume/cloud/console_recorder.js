// Records LaserData Cloud per-node server metrics for a whole test.
//
// The console keeps only about 30 s of per-node history (10 samples, 3 s apart), so this polls
// every 8 s and keeps every sample it has not seen yet. Polling runs in a Web Worker: Chrome slows
// timers in background tabs to about once a minute, which would lose samples; workers are not slowed.
//
// Run it in a logged-in console tab (browser devtools console, or a browser automation tool), on any
// page of the deployment, and leave that tab open and on the same page for the whole test:
//
//   window.LD_DEPLOYMENT = '<deployment id>';                       // from the console URL
//   window.LD_NODES = {1: '<node id>', 2: '<node id>', 3: '<node id>'}; // from the Monitor page's
//                                                                    // nodes/<id>/metrics requests
//   <paste this file>
//
//   __ldrec.count()            rows recorded so far
//   __ldrec.latest()           newest row per node (check every node is current)
//   __ldrec.csv(from, to)      CSV text for a time window (unix seconds, optional)
//   __ldrec.download(name, from, to)   saves the CSV to the browser's download folder
//   __ldrec.stop()             stops polling
//
// Columns: t (unix s), node (1-3), server CPU %, server memory MiB, host CPU %, host memory used MiB,
// disk used MiB, server bytes read/written MiB (cumulative), role (l/f), under-replicated and
// below-write-quorum partitions, partitions, clients, messages, server process id and start time
// (a restart shows up as a new pid and start time on that node).
(() => {
  const NODES = window.LD_NODES;
  if (!window.LD_DEPLOYMENT || !NODES) throw new Error('set window.LD_DEPLOYMENT and window.LD_NODES first');
  const BASE = `https://supervisor-gcp-us.laserdata.cloud/deployments/${window.LD_DEPLOYMENT}`;
  const COLS = ['t', 'node', 'iggy_cpu', 'iggy_mem_mib', 'host_cpu', 'host_mem_used_mib', 'disk_used_mib',
    'read_mib', 'written_mib', 'role', 'under_repl', 'below_quorum', 'partitions', 'clients', 'messages',
    'pid', 'start_time'];
  const worker = `
const NODES = ${JSON.stringify(NODES)}; const BASE = ${JSON.stringify(BASE)}; const MiB = 1048576;
const num = (x, d = 1) => (x == null ? '' : +(+x).toFixed(d));
async function tick() {
  const out = [];
  for (const [n, id] of Object.entries(NODES)) {
    try {
      const j = await (await fetch(BASE + '/nodes/' + id + '/metrics', { credentials: 'include' })).json();
      const hosts = j.host || [];
      for (const g of j.iggy || []) {
        const t = Math.round(g.timestamp / 1e3) / 1e3;
        let h = {}, best = Infinity;
        for (const x of hosts) { const d = Math.abs(x.timestamp - g.timestamp); if (d < best) { best = d; h = x; } }
        const hostUsed = h.total_memory != null && h.available_memory != null ? (h.total_memory - h.available_memory) / MiB : '';
        const disk = g.total_disk_space_bytes != null ? (g.total_disk_space_bytes - g.free_disk_space_bytes) / MiB : '';
        out.push([t, +n, num(g.cpu_usage), num(g.memory_usage / MiB), num(h.total_cpu_usage ?? h.cpu_usage),
          num(hostUsed, 0), num(disk, 0), num(g.read_bytes / MiB, 0), num(g.written_bytes / MiB, 0),
          (g.role || '').slice(0, 1), g.partitions_under_replicated ?? '', g.partitions_below_write_quorum ?? '',
          g.partitions_count, g.clients_count, g.messages_count, g.process_id, g.start_time]);
      }
    } catch (e) { postMessage({ err: [Date.now() / 1e3, n, String(e).slice(0, 80)] }); }
  }
  postMessage({ rows: out });
}
setInterval(tick, 8000); tick();`;
  window.__ldrec?.stop?.();
  const rows = window.__ldrec?.rows || [];
  const errors = window.__ldrec?.errors || [];
  const seen = new Set(rows.map(r => r[1] + ':' + r[0]));
  const w = new Worker(URL.createObjectURL(new Blob([worker], { type: 'text/javascript' })));
  w.onmessage = e => {
    if (e.data.err) errors.push(e.data.err);
    for (const r of e.data.rows || []) {
      const k = r[1] + ':' + r[0];
      if (!seen.has(k)) { seen.add(k); rows.push(r); }
    }
  };
  const sorted = () => [...rows].sort((a, b) => a[1] - b[1] || a[0] - b[0]);
  window.__ldrec = {
    rows, errors,
    stop: () => w.terminate(),
    count: () => rows.length,
    latest: () => Object.keys(NODES).map(n => rows.filter(r => r[1] == n).sort((a, b) => b[0] - a[0])[0]),
    csv: (from = 0, to = Infinity) =>
      [COLS.join(',')].concat(sorted().filter(r => r[0] >= from && r[0] <= to).map(r => r.join(','))).join('\n'),
    download(name, from, to) {
      const a = document.createElement('a');
      a.href = URL.createObjectURL(new Blob([this.csv(from, to) + '\n'], { type: 'text/csv' }));
      a.download = name;
      a.click();
      return name;
    },
  };
  return 'recording ' + Object.keys(NODES).length + ' nodes every 8 s';
})();
