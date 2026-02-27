#!/usr/bin/env python3
"""
build_heatmap.py — Generate a self-contained HPAI county-level heatmap from APHIS CSVs.

Usage:
    python build_heatmap.py
    python build_heatmap.py --poultry "A Table by Confirmation Date.csv" \
                            --wild-birds "HPAI Detections in Wild Birds.csv" \
                            -o heatmap.html
"""

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import addfips


# ── CSV Parsers ────────────────────────────────────────────────────────────

def parse_wild_birds_csv(path):
    events = []
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            state = row.get("State", "").strip().strip('"')
            county = row.get("County", "").strip().strip('"')
            date_str = row.get("Collection Date", "").strip().strip('"')
            if not state or not date_str:
                continue
            dt = _parse_date(date_str)
            if dt is None:
                continue
            events.append({"date": dt, "state": state, "county": county, "source": "wild_birds"})
    return events


def parse_poultry_csv(path):
    with open(path, encoding="utf-16") as f:
        lines = f.read().strip().replace("\r\n", "\n").replace("\r", "\n").split("\n")

    hdr_idx = None
    for i, line in enumerate(lines):
        low = line.lower()
        if "confirmed" in low and "production" in low:
            hdr_idx = i
            break
    if hdr_idx is None:
        print(f"  WARNING: Cannot find header row in {path}, skipping poultry")
        return []

    hdrs = lines[hdr_idx].split("\t")
    hdr_low = [h.strip().lower() for h in hdrs]
    ci = hdr_low.index("confirmed")
    si = hdr_low.index("state")
    cni = hdr_low.index("county name") if "county name" in hdr_low else None
    pi = hdr_low.index("production")
    data_start = pi + 1

    events = []
    prev_conf = prev_st = prev_cn = None

    for line in lines[hdr_idx + 1:]:
        cols = line.split("\t")
        if len(cols) <= data_start:
            continue
        confirmed = cols[ci].strip()
        state = cols[si].strip()
        county = cols[cni].strip() if cni is not None else ""
        production = cols[pi].strip()

        if confirmed:
            prev_conf = confirmed
        else:
            confirmed = prev_conf
        if state:
            prev_st = state
        else:
            state = prev_st
        if county:
            prev_cn = county
        else:
            county = prev_cn or ""

        if not confirmed or not production:
            continue

        flock = None
        for cell in cols[data_start:]:
            v = cell.strip().replace(",", "")
            if v:
                try:
                    flock = int(float(v))
                    break
                except ValueError:
                    continue
        if not flock:
            continue

        dt = _parse_date(confirmed)
        if dt is None:
            continue
        events.append({"date": dt, "state": state, "county": county, "source": "poultry"})

    return events


def _parse_date(s):
    for fmt in ("%m/%d/%Y", "%d-%b-%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


# ── FIPS Mapping ───────────────────────────────────────────────────────────

def build_fips_lookup():
    af = addfips.AddFIPS()

    OVERRIDES = {
        ("DC", "District of Columbia"): "11001",
        ("District of Columbia", "District of Columbia"): "11001",
        ("Alaska", "Matanuska Susitna"): "02170",
        ("Florida", "De Soto"): "12027",
        ("Louisiana", "Jefferson Davis Pari"): "22053",
        ("Louisiana", "LaSalle"): "22059",
        ("Michigan", "St Clair"): "26147",
        ("New York", "St Lawrence"): "36089",
        ("Wisconsin", "St Croix"): "55109",
    }

    def _clean(name):
        name = re.sub(r"\d+$", "", name).strip()
        name = name.replace("-", " ")
        # Normalize "St " → "St. " for addfips
        if name.startswith("St "):
            name = "St. " + name[3:]
        return name

    def lookup(state, county):
        key = (state, county)
        if key in OVERRIDES:
            return OVERRIDES[key]

        fips = af.get_county_fips(county, state=state)
        if fips:
            return fips

        cleaned = _clean(county)
        if cleaned != county:
            fips = af.get_county_fips(cleaned, state=state)
            if fips:
                return fips

        if state == "Virginia":
            if county.endswith(" City"):
                fips = af.get_county_fips(county.replace(" City", "") + " city", state=state)
            else:
                fips = af.get_county_fips(county + " city", state=state)
            if fips:
                return fips

        return None

    return lookup


# ── County Aggregation ─────────────────────────────────────────────────────

def aggregate_county_detections(all_events, fips_lookup):
    county_data = {}
    unmapped_set = set()
    unknown_count = 0

    for event in all_events:
        state = event["state"]
        county = event["county"]
        date = event["date"]
        source = event["source"]

        if not county or county.lower() == "unknown":
            unknown_count += 1
            continue

        fips = fips_lookup(state, county)
        if not fips:
            unmapped_set.add((state, county))
            continue

        if fips not in county_data:
            county_data[fips] = {
                "state": state, "county": county,
                "wild_birds": 0, "poultry": 0, "total": 0,
                "dated_sources": [], "latest_date": None,
            }

        entry = county_data[fips]
        entry[source] = entry.get(source, 0) + 1
        entry["total"] += 1
        iso = date.strftime("%Y-%m-%d")
        entry["dated_sources"].append((iso, source))
        if entry["latest_date"] is None or iso > entry["latest_date"]:
            entry["latest_date"] = iso

    if unmapped_set:
        print(f"  WARNING: {len(unmapped_set)} unique State+County pairs could not be FIPS-mapped:")
        for s, c in sorted(unmapped_set)[:25]:
            print(f"    {s} / {c}")
        if len(unmapped_set) > 25:
            print(f"    ... and {len(unmapped_set) - 25} more")

    return county_data, unknown_count


def compress_map_data(county_data):
    """Compress county_data into a compact JSON-friendly dict keyed by FIPS."""
    result = {}
    for fips, info in county_data.items():
        mo_wb = defaultdict(int)
        mo_p = defaultdict(int)

        for date_str, source in info["dated_sources"]:
            ym = date_str[:7]
            if source == "wild_birds":
                mo_wb[ym] += 1
            elif source == "poultry":
                mo_p[ym] += 1

        result[fips] = {
            "s": info["state"],
            "c": info["county"],
            "wb": info["wild_birds"],
            "p": info["poultry"],
            "t": info["total"],
            "ld": info["latest_date"],
            "mwb": dict(mo_wb),
            "mp": dict(mo_p),
        }
    return result


# ── HTML Template ──────────────────────────────────────────────────────────

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HPAI Detection Heatmap</title>
<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
<script src="https://cdn.jsdelivr.net/npm/topojson-client@3"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f0f4f8;color:#1e293b;line-height:1.5}
.wrap{max-width:1100px;margin:0 auto;padding:20px 16px}
header{text-align:center;margin-bottom:20px}
header h1{font-size:1.6rem;font-weight:700;color:#0f172a}
header .sub{color:#64748b;font-size:.8rem;margin-top:2px}

.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin-bottom:20px}
.kpi{background:#fff;border-radius:10px;padding:14px 18px;box-shadow:0 1px 3px rgba(0,0,0,.07)}
.kpi .lbl{font-size:.7rem;text-transform:uppercase;letter-spacing:.04em;color:#64748b;font-weight:600}
.kpi .val{font-size:1.4rem;font-weight:700;margin-top:2px}
.kpi .note{font-size:.7rem;color:#94a3b8;margin-top:1px}

.card{background:#fff;border-radius:10px;padding:20px;margin-bottom:14px;box-shadow:0 1px 3px rgba(0,0,0,.07)}
.card h2{font-size:1rem;font-weight:600;margin-bottom:2px}
.card .sub{font-size:.78rem;color:#64748b;margin-bottom:12px}

.controls{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;margin-bottom:10px}
.range-row{display:flex;gap:3px;flex-wrap:wrap}
.rbtn{padding:3px 10px;border:1px solid #e2e8f0;border-radius:16px;background:#f8fafc;font-size:.72rem;cursor:pointer;color:#475569;transition:all .15s}
.rbtn:hover{background:#e2e8f0}
.rbtn.active{background:#1e3a5f;color:#fff;border-color:#1e3a5f}

#mapContainer{width:100%;position:relative}
#mapContainer svg{width:100%;height:auto;display:block}
.county{stroke:#fff;stroke-width:.25px;transition:opacity .12s}
.county:hover{opacity:.8;stroke:#1e293b;stroke-width:1px}
.state-border{fill:none;stroke:#94a3b8;stroke-width:.7px;pointer-events:none}

.map-tooltip{
  position:absolute;pointer-events:none;background:#fff;
  border:1px solid #e2e8f0;border-radius:8px;padding:10px 14px;
  box-shadow:0 4px 12px rgba(0,0,0,.12);font-size:.8rem;
  max-width:280px;z-index:30;opacity:0;transition:opacity .12s;
}
.map-tooltip.visible{opacity:1}
.tt-county{font-weight:700;font-size:.9rem;color:#0f172a}
.tt-total{font-size:1.1rem;font-weight:700;margin:4px 0;color:#dc2626}
.tt-row{font-size:.75rem;color:#475569;line-height:1.6}
.tt-date{font-size:.7rem;color:#94a3b8;margin-top:4px}

.legend-wrap{display:flex;align-items:center;gap:6px;font-size:.7rem;color:#64748b}
.legend-bar{width:200px;height:12px;border-radius:3px}
.legend-labels{display:flex;justify-content:space-between;width:200px;font-size:.65rem;margin-top:1px}

.tbl-summary{font-size:.75rem;color:#94a3b8;margin-top:8px}

footer{text-align:center;padding:14px;color:#94a3b8;font-size:.7rem}
</style>
</head>
<body>
<div class="wrap">
<header>
  <h1>HPAI Detection Heatmap by County</h1>
  <div class="sub">Wild bird and poultry flock detections &middot; Updated __UPDATED__</div>
</header>

<div class="kpi-row">
  <div class="kpi">
    <div class="lbl">Counties with Detections</div>
    <div class="val">__KPI_COUNTIES__</div>
    <div class="note">Out of ~3,200 US counties</div>
  </div>
  <div class="kpi">
    <div class="lbl">Total Detections</div>
    <div class="val">__KPI_TOTAL__</div>
    <div class="note">Wild birds + poultry flocks</div>
  </div>
  <div class="kpi">
    <div class="lbl">Wild Bird Detections</div>
    <div class="val" style="color:#0ea5e9">__KPI_WB__</div>
    <div class="note">Individual confirmed cases</div>
  </div>
  <div class="kpi">
    <div class="lbl">Poultry Flock Detections</div>
    <div class="val" style="color:#dc2626">__KPI_POULTRY__</div>
    <div class="note">Confirmed flock infections</div>
  </div>
</div>

<div class="card">
  <h2>County Risk Map</h2>
  <div class="sub">Hover over any county to see detection details. Color intensity reflects total confirmed HPAI detections.</div>
  <div class="controls">
    <div class="range-row" data-chart="map">
      <button class="rbtn" data-r="14d">14D</button>
      <button class="rbtn active" data-r="30d">30D</button>
      <button class="rbtn" data-r="60d">60D</button>
    </div>
    <div class="range-row" data-chart="source">
      <button class="rbtn active" data-r="both">Both</button>
      <button class="rbtn" data-r="wild_birds">Wild Birds</button>
      <button class="rbtn" data-r="poultry">Poultry</button>
    </div>
    <div class="legend-wrap" id="legendWrap">
      <span>0</span>
      <canvas id="legendBar" class="legend-bar" width="200" height="12"></canvas>
      <span id="legendMax">—</span>
    </div>
  </div>
  <div id="mapContainer"></div>
  <div class="tbl-summary" id="mapSummary"></div>
</div>

</div>
<footer>Data: USDA APHIS &middot; Generated by build_heatmap.py</footer>

<script>
const D = __DATA_JSON__;
const TOPO_URL = 'https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json';
let mapRange = '30d';
let sourceFilter = 'both';
let mapSvg, mapPath, mapTooltip, countyPaths;

// ── Date utilities ──
function cutoffYM(range) {
  const now = new Date();
  let c;
  switch (range) {
    case '14d': c = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 14); break;
    case '30d': c = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 30); break;
    case '60d': c = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 60); break;
    default:    c = new Date(2000, 0, 1);
  }
  return c.getFullYear() + '-' + String(c.getMonth() + 1).padStart(2, '0');
}

// ── Filtered counts (respects both time range and source filter) ──
function sumMonths(moDict, cutM) {
  let n = 0;
  for (const [month, cnt] of Object.entries(moDict)) {
    if (month >= cutM) n += cnt;
  }
  return n;
}

function getFilteredCounts(fips) {
  const info = D.map_data[fips];
  if (!info) return { total: 0, wb: 0, p: 0 };
  const cutM = cutoffYM(mapRange);
  const wb = sumMonths(info.mwb || {}, cutM);
  const p  = sumMonths(info.mp  || {}, cutM);
  let total;
  if (sourceFilter === 'wild_birds') total = wb;
  else if (sourceFilter === 'poultry') total = p;
  else total = wb + p;
  return { total, wb, p };
}

// ── Color scale ──
function buildColorScale(maxVal) {
  if (maxVal <= 0) maxVal = 1;
  return d3.scaleSequentialLog(d3.interpolateYlOrRd).domain([1, maxVal]);
}

function paintLegend(maxVal) {
  const canvas = document.getElementById('legendBar');
  const ctx = canvas.getContext('2d');
  const w = canvas.width, h = canvas.height;
  const scale = buildColorScale(Math.max(maxVal, 2));
  for (let x = 0; x < w; x++) {
    const v = 1 + (x / (w - 1)) * (Math.max(maxVal, 2) - 1);
    ctx.fillStyle = scale(v);
    ctx.fillRect(x, 0, 1, h);
  }
  document.getElementById('legendMax').textContent = maxVal.toLocaleString();
}

// ── Map rendering ──
async function initMap() {
  const res = await fetch(TOPO_URL);
  const us = await res.json();
  const counties = topojson.feature(us, us.objects.counties);
  const stateMesh = topojson.mesh(us, us.objects.states, (a, b) => a !== b);

  const width = 975;
  const height = 610;

  const projection = d3.geoAlbersUsa().fitSize([width, height], counties);
  mapPath = d3.geoPath().projection(projection);

  mapSvg = d3.select('#mapContainer')
    .append('svg')
    .attr('viewBox', `0 0 ${width} ${height}`)
    .attr('preserveAspectRatio', 'xMidYMid meet');

  mapTooltip = d3.select('#mapContainer')
    .append('div')
    .attr('class', 'map-tooltip');

  countyPaths = mapSvg.append('g')
    .selectAll('path')
    .data(counties.features)
    .join('path')
    .attr('class', 'county')
    .attr('d', mapPath)
    .attr('data-fips', d => d.id)
    .on('mouseenter', onHover)
    .on('mousemove', onMove)
    .on('mouseleave', onLeave);

  mapSvg.append('path')
    .datum(stateMesh)
    .attr('class', 'state-border')
    .attr('d', mapPath);

  updateMapColors();
}

function updateMapColors() {
  const allFips = Object.keys(D.map_data);
  const totals = allFips.map(f => getFilteredCounts(f).total);
  const positives = totals.filter(c => c > 0);
  const maxCount = positives.length ? Math.max(...positives) : 1;
  const scale = buildColorScale(maxCount);

  countyPaths.attr('fill', function() {
    const fips = this.getAttribute('data-fips');
    const count = getFilteredCounts(fips).total;
    return count > 0 ? scale(count) : '#f1f5f9';
  });

  paintLegend(maxCount);

  const activeCounties = positives.length;
  const totalDet = positives.reduce((a, b) => a + b, 0);
  const unk = D.unknown_count || 0;
  const rangeLabel = {14:'14-day',30:'30-day',60:'60-day'}[parseInt(mapRange)] || mapRange;
  const srcLabel = sourceFilter === 'both' ? '' : (sourceFilter === 'wild_birds' ? ' (wild birds only)' : ' (poultry only)');
  let summary = activeCounties.toLocaleString() + ' counties with ' + totalDet.toLocaleString() + ' detections in ' + rangeLabel + ' window' + srcLabel;
  if (unk > 0) summary += ' · ' + unk.toLocaleString() + ' excluded (county unknown)';
  document.getElementById('mapSummary').textContent = summary;
}

// ── Tooltip (shows filtered counts for selected period + source) ──
function onHover(event, d) {
  const fips = d.id;
  const info = D.map_data[fips];
  const fc = getFilteredCounts(fips);
  const name = info ? (info.c + ', ' + info.s) : (d.properties.name || 'Unknown county');

  let html = '<div class="tt-county">' + name + '</div>';
  if (fc.total > 0) {
    html += '<div class="tt-total">' + fc.total.toLocaleString() + ' detection' + (fc.total !== 1 ? 's' : '') + '</div>';
    html += '<div class="tt-row">';
    if (fc.wb > 0) html += 'Wild birds: ' + fc.wb.toLocaleString() + '<br>';
    if (fc.p > 0) html += 'Poultry flocks: ' + fc.p.toLocaleString() + '<br>';
    html += '</div>';
    if (info && info.ld) html += '<div class="tt-date">Latest (all time): ' + info.ld + '</div>';
  } else {
    html += '<div class="tt-row" style="color:#94a3b8">No detections in this period</div>';
  }

  mapTooltip.html(html).classed('visible', true);
}

function onMove(event) {
  const container = document.getElementById('mapContainer');
  const rect = container.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  const ttNode = mapTooltip.node();
  const ttW = ttNode.offsetWidth;
  const ttH = ttNode.offsetHeight;
  const left = (x + ttW + 20 > rect.width) ? x - ttW - 10 : x + 14;
  const top = (y + ttH + 10 > rect.height) ? y - ttH - 10 : y + 14;
  mapTooltip.style('left', left + 'px').style('top', top + 'px');
}

function onLeave() {
  mapTooltip.classed('visible', false);
}

// ── Button wiring ──
document.querySelectorAll('.range-row').forEach(row => {
  const chart = row.dataset.chart;
  row.querySelectorAll('.rbtn').forEach(btn => {
    btn.addEventListener('click', () => {
      row.querySelectorAll('.rbtn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      const r = btn.dataset.r;
      if (chart === 'map') { mapRange = r; updateMapColors(); }
      if (chart === 'source') { sourceFilter = r; updateMapColors(); }
    });
  });
});

// ── Init ──
initMap();
</script>
</body>
</html>"""


# ── HTML Generation ────────────────────────────────────────────────────────

def generate_html(map_data_compressed, unknown_count, kpi):
    data_blob = {
        "map_data": map_data_compressed,
        "unknown_count": unknown_count,
    }

    html = HTML_TEMPLATE
    html = html.replace("__UPDATED__", datetime.now().strftime("%B %d, %Y"))
    html = html.replace("__KPI_COUNTIES__", f'{kpi["counties"]:,}')
    html = html.replace("__KPI_TOTAL__", f'{kpi["total"]:,}')
    html = html.replace("__KPI_WB__", f'{kpi["wild_birds"]:,}')
    html = html.replace("__KPI_POULTRY__", f'{kpi["poultry"]:,}')
    html = html.replace("__DATA_JSON__", json.dumps(data_blob, separators=(',', ':')).replace("</", "<\\/"))

    return html


# ── CLI ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Build HPAI county heatmap HTML")
    ap.add_argument("--poultry", default="A Table by Confirmation Date.csv",
                    help="Path to poultry confirmation CSV")
    ap.add_argument("--wild-birds", default="HPAI Detections in Wild Birds.csv",
                    help="Path to wild birds CSV")
    ap.add_argument("-o", "--output", default="heatmap.html",
                    help="Output HTML path")
    args = ap.parse_args()

    # 1. Parse all CSVs
    print("Parsing CSV data...")
    wb_events = []
    pt_events = []

    wb_path = Path(args.wild_birds)
    if wb_path.exists():
        wb_events = parse_wild_birds_csv(str(wb_path))
        print(f"  Wild birds: {len(wb_events)} detections")
    else:
        print(f"  WARNING: {wb_path} not found, skipping wild birds")

    pt_path = Path(args.poultry)
    if pt_path.exists():
        pt_events = parse_poultry_csv(str(pt_path))
        print(f"  Poultry: {len(pt_events)} detections")
    else:
        print(f"  WARNING: {pt_path} not found, skipping poultry")

    all_events = wb_events + pt_events
    if not all_events:
        sys.exit("ERROR: No detection events loaded from any CSV")

    # 2. Map to FIPS codes
    print("Mapping counties to FIPS codes...")
    fips_lookup = build_fips_lookup()
    county_data, unknown_count = aggregate_county_detections(all_events, fips_lookup)
    print(f"  {len(county_data)} counties mapped")
    print(f"  {unknown_count} detections excluded (county unknown)")

    # 3. Compress for JSON
    map_compressed = compress_map_data(county_data)

    # 4. Compute KPIs
    total_wb = sum(v["wb"] for v in map_compressed.values())
    total_pt = sum(v["p"] for v in map_compressed.values())
    kpi = {
        "counties": len(map_compressed),
        "total": total_wb + total_pt,
        "wild_birds": total_wb,
        "poultry": total_pt,
    }

    # 5. Generate HTML
    html = generate_html(map_compressed, unknown_count, kpi)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html)
    print(f"\nHeatmap written to: {out}")
    print(f"  Open in browser: file://{out.resolve()}")


if __name__ == "__main__":
    main()