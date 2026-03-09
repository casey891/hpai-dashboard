#!/usr/bin/env python3
"""
build_dashboard.py — Download APHIS data and generate the HPAI dashboard.

Downloads 3 datasets automatically (flocks, wild birds, mammals), then builds
the dashboard HTML + data.json. Livestock must be downloaded manually.

Usage:
    python build_dashboard.py                     # download data + build
    python build_dashboard.py --no-download        # build from existing CSVs
    python build_dashboard.py --no-prices          # skip egg price fetch
    python build_dashboard.py -o docs/index.html   # custom output path
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from parsers import (
    fetch_egg_prices, parse_hpai_csv, parse_livestock_csv,
    parse_mammals_csv, parse_wild_birds_csv,
)
from geo import HAS_ADDFIPS, build_fips_lookup, aggregate_county_detections, compress_map_data
from template import (
    HTML_TEMPLATE, COLORS,
    build_grouped_checkboxes, build_simple_checkboxes, RANGE_BUTTONS,
)


# ── Aggregation ─────────────────────────────────────────────────────────────

def _parse_mars_date(d):
    return datetime.strptime(d, "%m/%d/%Y")


def build_data(events, caged_prices, livestock=None, mammals=None, wild_birds=None, livestock_updated=None):
    # Monthly HPAI aggregates
    birds_m = defaultdict(lambda: defaultdict(int))
    inf_m = defaultdict(lambda: defaultdict(int))
    for e in events:
        mk = e["date"].strftime("%Y-%m")
        birds_m[mk][e["production"]] += e["flock"]
        inf_m[mk][e["production"]] += 1

    months = sorted(birds_m.keys())
    month_labels = [datetime.strptime(m, "%Y-%m").strftime("%b %Y") for m in months]

    totals = defaultdict(int)
    for m in months:
        for p, v in birds_m[m].items():
            totals[p] += v
    prod_types = sorted(totals, key=lambda p: -totals[p])

    total_inf = [sum(inf_m[m].values()) for m in months]

    # Egg prices — already in $/dozen
    all_raw_dates = sorted(caged_prices.keys(), key=lambda d: _parse_mars_date(d))
    egg_dates_display = [_parse_mars_date(d).strftime("%b %d, %Y") for d in all_raw_dates]
    egg_dates_iso = [_parse_mars_date(d).strftime("%Y-%m-%d") for d in all_raw_dates]
    caged_vals = [caged_prices[d] for d in all_raw_dates]

    # KPIs
    latest_price = caged_vals[-1] if caged_vals else None
    pct_chg = None
    if len(caged_vals) >= 22:
        old = caged_vals[-22]
        if old:
            pct_chg = round((caged_vals[-1] - old) / old * 100, 1)

    layer_birds = sum(birds_m[m].get("Commercial Table Egg Layer", 0) for m in months)
    total_sites = sum(total_inf)

    # Daily HPAI aggregates (for 30D / 3M drill-down)
    daily_birds_d = defaultdict(lambda: defaultdict(int))
    daily_inf_d = defaultdict(lambda: defaultdict(int))
    for e in events:
        dk = e["date"].strftime("%Y-%m-%d")
        daily_birds_d[dk][e["production"]] += e["flock"]
        daily_inf_d[dk][e["production"]] += 1
    daily_dates = sorted(daily_birds_d.keys())
    daily_labels = [datetime.strptime(d, "%Y-%m-%d").strftime("%b %d") for d in daily_dates]

    # Event-level rows for the data table (newest first)
    event_rows = sorted(
        [{"d": e["date"].strftime("%Y-%m-%d"), "s": e["state"],
          "c": e["county"], "p": e["production"], "f": e["flock"]}
         for e in events],
        key=lambda r: r["d"], reverse=True,
    )

    result = {
        "months": months,
        "month_labels": month_labels,
        "production_types": prod_types,
        "birds_by_month": {m: dict(birds_m[m]) for m in months},
        "infections_by_month": {m: dict(inf_m[m]) for m in months},
        "total_infections": total_inf,
        "egg_dates": egg_dates_display,
        "egg_dates_iso": egg_dates_iso,
        "caged_prices": caged_vals,
        "daily_dates": daily_dates,
        "daily_labels": daily_labels,
        "daily_birds": {d: dict(daily_birds_d[d]) for d in daily_dates},
        "daily_infections": {d: dict(daily_inf_d[d]) for d in daily_dates},
        "events": event_rows,
        "category_colors": {p: COLORS.get(p, "#6b7280") for p in prod_types},
        "kpi": {
            "layer_birds": layer_birds,
            "total_sites": total_sites,
            "latest_price": latest_price,
            "price_change": pct_chg,
        },
        "updated": datetime.now().strftime("%B %d, %Y"),
    }

    # ── Livestock aggregation ──
    if livestock:
        ls_monthly = defaultdict(int)
        for e in livestock:
            mk = e["date"].strftime("%Y-%m")
            ls_monthly[mk] += 1
        ls_months = sorted(ls_monthly.keys())
        ls_rows = sorted(
            [{"d": e["date"].strftime("%Y-%m-%d"), "s": e["state"],
              "id": e["special_id"], "p": e["production"], "sp": e["species"]}
             for e in livestock],
            key=lambda r: r["d"], reverse=True,
        )
        result["livestock"] = {
            "months": ls_months,
            "month_labels": [datetime.strptime(m, "%Y-%m").strftime("%b %Y") for m in ls_months],
            "monthly_counts": [ls_monthly[m] for m in ls_months],
            "events": ls_rows,
            "total": len(livestock),
            "data_updated": livestock_updated,
        }
        result["kpi"]["dairy_herds"] = len(livestock)

    # ── Mammals aggregation ──
    if mammals:
        mm_monthly = defaultdict(lambda: defaultdict(int))
        mm_groups = sorted(set(e["group"] for e in mammals))
        for e in mammals:
            mk = e["date"].strftime("%Y-%m")
            mm_monthly[mk][e["group"]] += 1
        mm_months = sorted(mm_monthly.keys())
        mm_rows = sorted(
            [{"d": e["date"].strftime("%Y-%m-%d"), "s": e["state"],
              "c": e["county"], "sp": e["species"], "g": e["group"],
              "st": e["strain"]}
             for e in mammals],
            key=lambda r: r["d"], reverse=True,
        )
        result["mammals"] = {
            "months": mm_months,
            "month_labels": [datetime.strptime(m, "%Y-%m").strftime("%b %Y") for m in mm_months],
            "groups": mm_groups,
            "monthly_by_group": {m: dict(mm_monthly[m]) for m in mm_months},
            "events": mm_rows,
            "total": len(mammals),
        }

    # ── Wild birds aggregation ──
    if wild_birds:
        wb_monthly = defaultdict(int)
        wb_daily = defaultdict(int)
        for e in wild_birds:
            mk = e["date"].strftime("%Y-%m")
            wb_monthly[mk] += 1
            dk = e["date"].strftime("%Y-%m-%d")
            wb_daily[dk] += 1
        wb_months = sorted(wb_monthly.keys())
        wb_days = sorted(wb_daily.keys())
        wb_day_labels = [datetime.strptime(d, "%Y-%m-%d").strftime("%b %d") for d in wb_days]
        # Cap event rows at source — table JS will also cap rendering at 500
        wb_rows = sorted(
            [{"d": e["date"].strftime("%Y-%m-%d"), "s": e["state"],
              "c": e["county"], "sp": e["species"], "st": e["strain"]}
             for e in wild_birds],
            key=lambda r: r["d"], reverse=True,
        )
        result["wild_birds"] = {
            "months": wb_months,
            "month_labels": [datetime.strptime(m, "%Y-%m").strftime("%b %Y") for m in wb_months],
            "monthly_counts": [wb_monthly[m] for m in wb_months],
            "daily_dates": wb_days,
            "daily_labels": wb_day_labels,
            "daily_counts": [wb_daily[d] for d in wb_days],
            "events": wb_rows,
            "total": len(wild_birds),
        }
        result["kpi"]["wild_bird_detections"] = len(wild_birds)

    return result


# ── CSV export ──────────────────────────────────────────────────────────────

def export_clean_csvs(data_dir, events, livestock=None, mammals=None, wild_birds=None):
    """Export clean, readable CSVs matching the dashboard table columns."""
    data_dir.mkdir(parents=True, exist_ok=True)

    # Poultry detections
    poultry_path = data_dir / "poultry_detections.csv"
    with open(poultry_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Date", "State", "County", "Operation Type", "Birds Impacted"])
        for e in sorted(events, key=lambda e: e["date"], reverse=True):
            w.writerow([e["date"].strftime("%m/%d/%Y"), e["state"], e["county"],
                        e["production"], e["flock"]])
    print(f"  {poultry_path} ({len(events)} rows)")

    # Livestock detections
    if livestock:
        ls_path = data_dir / "livestock_detections.csv"
        with open(ls_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Date", "State", "Special Id", "Production", "Species"])
            for e in sorted(livestock, key=lambda e: e["date"], reverse=True):
                w.writerow([e["date"].strftime("%m/%d/%Y"), e["state"],
                            e["special_id"], e["production"], e["species"]])
        print(f"  {ls_path} ({len(livestock)} rows)")

    # Wild bird detections
    if wild_birds:
        wb_path = data_dir / "wild_bird_detections.csv"
        with open(wb_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Date", "State", "County", "Species", "Strain"])
            for e in sorted(wild_birds, key=lambda e: e["date"], reverse=True):
                w.writerow([e["date"].strftime("%m/%d/%Y"), e["state"], e["county"],
                            e["species"], e["strain"]])
        print(f"  {wb_path} ({len(wild_birds)} rows)")

    # Mammal detections
    if mammals:
        mm_path = data_dir / "mammal_detections.csv"
        with open(mm_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Date", "State", "County", "Species", "Group", "Strain"])
            for e in sorted(mammals, key=lambda e: e["date"], reverse=True):
                w.writerow([e["date"].strftime("%m/%d/%Y"), e["state"], e["county"],
                            e["species"], e["group"], e["strain"]])
        print(f"  {mm_path} ({len(mammals)} rows)")


# ── HTML generation ─────────────────────────────────────────────────────────

def _fmt_big(n):
    if n >= 1e6:
        return f"{n/1e6:,.1f}M"
    if n >= 1e3:
        return f"{n:,.0f}"
    return str(n)


def generate_html(data, data_url="data"):
    # Birds checkboxes: default only Commercial Layers group selected
    birds_cbs = build_grouped_checkboxes(
        data["production_types"], data["category_colors"],
        {"Commercial Table Egg Layer", "Commercial Table Egg Pullets", "Commercial Table Egg Breeder"},
    )
    # Infections checkboxes: default ALL selected
    inf_cbs = build_grouped_checkboxes(
        data["production_types"], data["category_colors"],
        set(data["production_types"]),
    )
    # Table checkboxes: default ALL selected
    tbl_cbs = build_grouped_checkboxes(
        data["production_types"], data["category_colors"],
        set(data["production_types"]),
    )

    html = HTML_TEMPLATE
    html = html.replace("__UPDATED__", data["updated"])
    html = html.replace("__BIRDS_CHECKBOXES__", birds_cbs)
    html = html.replace("__INF_CHECKBOXES__", inf_cbs)
    html = html.replace("__TBL_CHECKBOXES__", tbl_cbs)
    html = html.replace("__DATA_URL__", data_url)

    # ── Heatmap card ──
    if "map_data" in data:
        map_dl = f' · <a href="{data_url}/poultry_detections.csv" download>Download Poultry Data</a>'
        if "wild_birds" in data:
            map_dl += f' · <a href="{data_url}/wild_bird_detections.csv" download>Download Wild Bird Data</a>'
        heatmap_card = f'''<div class="card">
  <h2>HPAI Detection Heatmap by County</h2>
  <div class="sub">Hover over any county to see detection details.</div>
  <div class="controls">
    <div class="range-row" data-chart="map">
      <button class="rbtn" data-r="7d">7D</button>
      <button class="rbtn" data-r="14d">14D</button>
      <button class="rbtn active" data-r="30d">30D</button>
      <button class="rbtn" data-r="60d">60D</button>
      <button class="rbtn" data-r="3m">3M</button>
      <button class="rbtn" data-r="1y">1Y</button>
    </div>
    <div class="range-row" data-chart="source">
      <button class="rbtn active" data-r="both">Both</button>
      <button class="rbtn" data-r="wild_birds">Wild Birds</button>
      <button class="rbtn" data-r="poultry">Poultry</button>
    </div>
    <div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px">
      <div class="legend-wrap" id="legendWrap">
        <span>0</span>
        <canvas id="legendBar" class="legend-bar" width="200" height="12"></canvas>
        <span id="legendMax">&mdash;</span>
      </div>
      <div class="sub" style="margin:0;font-size:.68rem">Color intensity reflects total confirmed HPAI detections.</div>
    </div>
  </div>
  <div id="mapContainer"></div>
  <div class="tbl-summary" id="mapSummary"></div>
  <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/commercial-backyard-flocks" target="_blank">USDA APHIS</a>{map_dl}</div>
</div>'''
        html = html.replace("__HEATMAP_CARD__", heatmap_card)
    else:
        html = html.replace("__HEATMAP_CARD__", "")

    # ── Tab buttons (hide tabs for missing data) ──
    if "livestock" in data:
        html = html.replace("__TAB_LIVESTOCK_BTN__",
            '<button class="tab-btn" data-tab="livestock">Livestock/Dairy</button>')
    else:
        html = html.replace("__TAB_LIVESTOCK_BTN__", "")

    if "wild_birds" in data:
        html = html.replace("__TAB_WILDBIRDS_BTN__",
            '<button class="tab-btn" data-tab="wildbirds">Wild Birds</button>')
    else:
        html = html.replace("__TAB_WILDBIRDS_BTN__", "")

    if "mammals" in data:
        html = html.replace("__TAB_MAMMALS_BTN__",
            '<button class="tab-btn" data-tab="mammals">Mammals</button>')
    else:
        html = html.replace("__TAB_MAMMALS_BTN__", "")

    # ── Tab content: Livestock ──
    if "livestock" in data:
        ls_updated = data["livestock"].get("data_updated") or ""
        ls_updated_note = f" · Data last updated: {ls_updated}" if ls_updated else ""
        ls_html = f'''<div class="tab-content" id="tab-livestock">
  <div class="card">
    <h2>Livestock/Dairy Detections by Month</h2>
    <div class="sub">Confirmed HPAI-affected herds{ls_updated_note}</div>
    <div class="controls">{RANGE_BUTTONS.format(chart="ls")}</div>
    <canvas id="cLivestock"></canvas>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/hpai-confirmed-cases-livestock" target="_blank">USDA APHIS</a> · <a href="{data_url}/livestock_detections.csv" download>Download Data</a></div>
  </div>
  <div class="card">
    <h2>Livestock/Dairy Detection Details</h2>
    <div class="sub">Individual confirmed herd detections{ls_updated_note}</div>
    <input type="text" class="tbl-search" id="lsSearch" placeholder="Filter by state, production, species..." oninput="updateLsTable()">
    <div class="tbl-wrap">
      <table>
        <thead><tr><th>Date</th><th>State</th><th>Special Id</th><th>Production</th><th>Species</th></tr></thead>
        <tbody id="lsTblBody"></tbody>
      </table>
    </div>
    <div class="tbl-summary" id="lsTblSummary"></div>
    <div class="tbl-pager" id="lsPager"><button onclick="pageTable('ls',-1)">\u2190 Prev</button><span id="lsPageInfo"></span><button onclick="pageTable('ls',1)">Next \u2192</button></div>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/hpai-confirmed-cases-livestock" target="_blank">USDA APHIS</a> · <a href="{data_url}/livestock_detections.csv" download>Download Data</a></div>
  </div>
</div>'''
        html = html.replace("__TAB_LIVESTOCK_HTML__", ls_html)
    else:
        html = html.replace("__TAB_LIVESTOCK_HTML__", "")

    # ── Tab content: Wild Birds ──
    if "wild_birds" in data:
        wb_html = f'''<div class="tab-content" id="tab-wildbirds">
  <div class="card">
    <h2 id="wbTitle">Wild Bird HPAI Detections by Month</h2>
    <div class="sub">Confirmed detections in wild bird populations</div>
    <div class="controls">{RANGE_BUTTONS.format(chart="wb")}</div>
    <canvas id="cWildBirds"></canvas>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/wild-birds?page=1" target="_blank">USDA APHIS</a> · <a href="{data_url}/wild_bird_detections.csv" download>Download Data</a></div>
  </div>
  <div class="card">
    <h2>Wild Bird Detection Details</h2>
    <div class="sub">Individual confirmed detections arranged by sample collection date</div>
    <input type="text" class="tbl-search" id="wbSearch" placeholder="Filter by state, county, species..." oninput="updateWbTable()">
    <div class="tbl-wrap">
      <table>
        <thead><tr><th>Date</th><th>State</th><th>County</th><th>Species</th><th>Strain</th></tr></thead>
        <tbody id="wbTblBody"></tbody>
      </table>
    </div>
    <div class="tbl-summary" id="wbTblSummary"></div>
    <div class="tbl-pager" id="wbPager"><button onclick="pageTable('wb',-1)">\u2190 Prev</button><span id="wbPageInfo"></span><button onclick="pageTable('wb',1)">Next \u2192</button></div>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/wild-birds?page=1" target="_blank">USDA APHIS</a> · <a href="{data_url}/wild_bird_detections.csv" download>Download Data</a></div>
  </div>
</div>'''
        html = html.replace("__TAB_WILDBIRDS_HTML__", wb_html)
    else:
        html = html.replace("__TAB_WILDBIRDS_HTML__", "")

    # ── Tab content: Mammals ──
    if "mammals" in data:
        mm_colors = {'Domestic/Companion': '#F6851F', 'Wild Carnivores': '#013046',
                     'Rodents/Small Mammals': '#FDB714', 'Marine Mammals': '#1F9EBC',
                     'Captive/Zoo': '#8FCAE6', 'Other': '#939598'}
        mm_groups = data["mammals"]["groups"]
        mm_cbs = build_simple_checkboxes(mm_groups, mm_colors, set(mm_groups))
        mm_html = f'''<div class="tab-content" id="tab-mammals">
  <div class="card">
    <h2>Mammal HPAI Detections by Month</h2>
    <div class="sub">Confirmed detections by species group by date of sample collection</div>
    <div class="controls">{RANGE_BUTTONS.format(chart="mm")}</div>
    <canvas id="cMammals"></canvas>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/mammals" target="_blank">USDA APHIS</a> · <a href="{data_url}/mammal_detections.csv" download>Download Data</a></div>
  </div>
  <div class="card">
    <h2>Mammal Detection Details</h2>
    <div class="sub">Individual confirmed detections by date of sample collection</div>
    <div class="controls">
      {RANGE_BUTTONS.format(chart="mmTbl")}
      <div class="ms-wrap" id="mmTblMS">
        <button class="ms-btn" id="mmTblMSBtn">All categories</button>
        <div class="ms-panel" id="mmTblMSPanel">
          <div class="ms-actions">
            <a onclick="msAll('mmTbl')">Select All</a> \u00b7 <a onclick="msNone('mmTbl')">Clear</a>
          </div>
          {mm_cbs}
        </div>
      </div>
    </div>
    <input type="text" class="tbl-search" id="mmSearch" placeholder="Filter by state, county, species, group..." oninput="updateMmTable()">
    <div class="tbl-wrap">
      <table>
        <thead><tr><th>Date</th><th>State</th><th>County</th><th>Species</th><th>Group</th><th>Strain</th></tr></thead>
        <tbody id="mmTblBody"></tbody>
      </table>
    </div>
    <div class="tbl-summary" id="mmTblSummary"></div>
    <div class="tbl-pager" id="mmPager"><button onclick="pageTable('mm',-1)">\u2190 Prev</button><span id="mmPageInfo"></span><button onclick="pageTable('mm',1)">Next \u2192</button></div>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/mammals" target="_blank">USDA APHIS</a> · <a href="{data_url}/mammal_detections.csv" download>Download Data</a></div>
  </div>
</div>'''
        html = html.replace("__TAB_MAMMALS_HTML__", mm_html)
    else:
        html = html.replace("__TAB_MAMMALS_HTML__", "")

    data_json = json.dumps(data, default=str)

    return html, data_json


# ── CLI ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Build HPAI dashboard HTML")
    ap.add_argument("-o", "--output", default="index.html", help="Output HTML path")
    ap.add_argument("--egg-start", default=None,
                    help="Start date for egg prices (YYYY-MM-DD). Default: 2022-01-01")
    ap.add_argument("--no-prices", action="store_true", help="Skip egg price fetch")
    ap.add_argument("--no-download", action="store_true",
                    help="Skip downloading fresh data (use existing CSVs)")
    ap.add_argument("--livestock", default="Table Details by Date.csv",
                    help="Path to livestock CSV (default: Table Details by Date.csv)")
    ap.add_argument("--data-url", default="data",
                    help="Base URL for download links (default: data)")
    args = ap.parse_args()

    base_dir = Path(args.output).parent or Path(".")

    # ── Download fresh data ──
    if not args.no_download:
        from download_data import DOWNLOADS, download_one
        print("Downloading fresh APHIS data...\n")
        for name, config in DOWNLOADS.items():
            download_one(name, config, base_dir)
            print()

    # ── CSV file paths (3 automated + 1 manual) ──
    csv_path = base_dir / "A Table by Confirmation Date.csv"
    wild_birds_path = base_dir / "HPAI Detections in Wild Birds.csv"
    mammals_path = base_dir / "HPAI Detections in Mammals.csv"
    livestock_path = Path(args.livestock)

    if not csv_path.exists():
        sys.exit(f"ERROR: File not found: {csv_path}")

    # 1. Parse HPAI CSV
    print(f"Parsing HPAI data: {csv_path}")
    events = parse_hpai_csv(str(csv_path))
    print(f"  {len(events)} flock detections loaded")

    # 2. Parse additional CSVs
    livestock = mammals = wild_birds = None
    livestock_updated = None
    if livestock_path.exists():
        print(f"Parsing livestock data: {livestock_path}")
        livestock = parse_livestock_csv(str(livestock_path))
        livestock_updated = datetime.fromtimestamp(livestock_path.stat().st_mtime).strftime("%B %d, %Y")
        print(f"  {len(livestock)} herd detections loaded (file updated {livestock_updated})")

    if mammals_path.exists():
        print(f"Parsing mammal data: {mammals_path}")
        mammals = parse_mammals_csv(str(mammals_path))
        print(f"  {len(mammals)} mammal detections loaded")

    if wild_birds_path.exists():
        print(f"Parsing wild bird data: {wild_birds_path}")
        wild_birds = parse_wild_birds_csv(str(wild_birds_path))
        print(f"  {len(wild_birds)} wild bird detections loaded")

    # Export clean CSVs
    out_dir = Path(args.output).parent
    data_dir = out_dir / args.data_url
    print("Exporting clean CSVs...")
    export_clean_csvs(data_dir, events, livestock=livestock, mammals=mammals, wild_birds=wild_birds)

    # 3. Fetch egg prices
    caged_prices = {}
    if not args.no_prices:
        today = datetime.today()
        start = datetime.strptime(args.egg_start, "%Y-%m-%d") if args.egg_start else datetime(2022, 1, 1)
        print("Fetching egg prices from MARS API...")
        try:
            caged_prices = fetch_egg_prices(start, today)
            print(f"  {len(caged_prices)} trading days")
        except Exception as e:
            print(f"  WARNING: Could not fetch egg prices: {e}")

    # 4. Build heatmap data (FIPS county mapping)
    if HAS_ADDFIPS:
        print("Building county heatmap data...")
        heatmap_events = [
            {"date": e["date"], "state": e["state"], "county": e["county"], "source": "poultry"}
            for e in events
        ]
        if wild_birds:
            heatmap_events += [
                {"date": e["date"], "state": e["state"], "county": e["county"], "source": "wild_birds"}
                for e in wild_birds
            ]
        fips_lookup = build_fips_lookup()
        county_data, unknown_count, unknown_events = aggregate_county_detections(heatmap_events, fips_lookup)
        map_compressed = compress_map_data(county_data)
        # Compress unknown events by year-month and source
        unk_by_month = defaultdict(lambda: {"wb": 0, "p": 0})
        for ue in unknown_events:
            ym = ue["date"].strftime("%Y-%m")
            if ue["source"] == "wild_birds":
                unk_by_month[ym]["wb"] += 1
            else:
                unk_by_month[ym]["p"] += 1
        unk_compressed = {ym: dict(v) for ym, v in unk_by_month.items()}
        print(f"  {len(map_compressed)} counties mapped, {unknown_count} excluded (county unknown)")
    else:
        print("  WARNING: addfips not installed, skipping heatmap")
        map_compressed = None
        unk_compressed = {}

    # 5. Build data & HTML
    data = build_data(events, caged_prices, livestock=livestock, mammals=mammals, wild_birds=wild_birds, livestock_updated=livestock_updated)
    if map_compressed:
        data["map_data"] = map_compressed
        data["unknown_by_month"] = unk_compressed
    html, data_json = generate_html(data, data_url=args.data_url)

    # 6. Write output
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html)

    data_out = out.parent / "data.json"
    data_out.write_text(data_json)

    html_kb = len(html.encode()) / 1024
    data_kb = len(data_json.encode()) / 1024
    print(f"\nDashboard written to: {out}  ({html_kb:.0f} KB)")
    print(f"Data written to:      {data_out}  ({data_kb:.0f} KB)")
    print(f"  Estimated gzip'd data.json: ~{data_kb * 0.15:.0f} KB")
    print(f"\n  To test locally:")
    print(f"    python3 -m http.server 8000 -d {out.parent}")
    print(f"    Open: http://localhost:8000")


if __name__ == "__main__":
    main()
