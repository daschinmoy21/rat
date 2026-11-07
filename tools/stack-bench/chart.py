#!/usr/bin/env python3
"""Create a self-contained interactive comparison from measured cgroup data."""
import argparse
import json
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("data_dir", type=Path)
args = p.parse_args()
data = []
for title, filename in (("JVM stack", "legacy-loaded.json"), ("Rust + Iggy", "rust-final-loaded.json")):
    d = json.loads((args.data_dir / filename).read_text())["summary"]["TOTAL"]
    data.append({"name": title, "working": d["mean_working_set_mib"],
                 "combined": d["mean_working_set_mib"] + d["mean_swap_mib"],
                 "cpu": d["cpu_percent_one_core"]})
html = '''<!doctype html><html><head><meta charset="utf-8"><style>
body{margin:0;font-family:var(--font-sans);color:var(--foreground)}
.controls{display:flex;align-items:center;gap:12px;flex-wrap:wrap;font-size:14px}
select{font:inherit;color:var(--foreground);background:var(--secondary);border:1px solid var(--border);border-radius:var(--radius);padding:6px}
svg{display:block;width:100%;height:230px;margin-top:8px}svg text{font-family:var(--font-sans);fill:var(--foreground)}
.note{font-size:12px;line-height:1.5;color:var(--muted-foreground);margin:6px 0}
.result{font-size:16px;font-weight:600;margin:6px 0}
</style></head><body>
<div class="controls"><label for="metric">Measure</label><select id="metric">
<option value="combined">Working memory + swap</option><option value="working">Resident working memory</option><option value="cpu">Average CPU (one core = 100%)</option></select></div>
<svg id="chart" viewBox="0 0 700 230" role="img" aria-label="Measured resource use, legacy JVM stack compared with Rust and Iggy"></svg>
<p id="result" class="result"></p>
<p class="note">Matched 90-second windows, 10,000 events; identical 7,500 correlations and 2,500 raw rows. Rust uses persisted broker topics and ext4 storage.</p>
<p class="note">Service cgroups only; Python producers and dashboard excluded on both paths. Shared host; these observations are not a maximum-throughput benchmark.</p>
<script>const data=__DATA__;
function draw(){const key=document.getElementById('metric').value;
const values=data.map(x=>x[key]), max=Math.max(...values)*1.16;
const unit=key==='cpu'?'% of one core':'MiB';const fmt=x=>x.toLocaleString(undefined,{maximumFractionDigits:1});
let svg='';for(let i=0;i<=4;i++){const x=122+i*112;svg+=`<line x1="${x}" y1="24" x2="${x}" y2="177" stroke="var(--border)"/><text x="${x}" y="202" font-size="12" text-anchor="middle">${fmt(max*i/4)}</text>`;}
data.forEach((row,i)=>{const y=48+i*76,w=values[i]/max*448;svg+=`<text x="106" y="${y+20}" font-size="14" text-anchor="end">${row.name}</text><rect x="122" y="${y}" width="${w}" height="32" rx="5" fill="var(--chart-${i+1})"/><text x="${130+w}" y="${y+21}" font-size="14">${fmt(values[i])}</text>`;});
svg+=`<text x="690" y="225" font-size="12" text-anchor="end">${unit}</text>`;
document.getElementById('chart').innerHTML=svg;document.getElementById('result').textContent=`${fmt((1-values[1]/values[0])*100)}% lower ${key==='cpu'?'average CPU use':key==='combined'?'working memory plus swap':'resident working memory'} in this run`;
}document.getElementById('metric').addEventListener('change',draw);draw();</script></body></html>'''.replace('__DATA__', json.dumps(data))
out = args.data_dir / "comparison.html"
out.write_text(html + "\n")
print(out)
