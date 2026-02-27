#!/usr/bin/env python3
"""
build_dashboard.py — Generate a self-contained HPAI dashboard from APHIS CSV + MARS API egg prices.

Usage:
    python build_dashboard.py "A Table by Confirmation Date.csv"
    python build_dashboard.py "A Table by Confirmation Date.csv" --output docs/index.html
    python build_dashboard.py "A Table by Confirmation Date.csv" --egg-start 2024-01-01
"""

import argparse
import base64
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

try:
    import addfips
    HAS_ADDFIPS = True
except ImportError:
    HAS_ADDFIPS = False

# ── MARS API config ─────────────────────────────────────────────────────────
MARS_BASE = "https://marsapi.ams.usda.gov/services/v1.2/reports"
MARS_REPORT = "2843"
MARS_KEY = "LIm1Mr7tz2NPJV9W/KJNe3aM/xyRvuWUzPdsu1S8k5E="


def _mars_get(url):
    req = Request(url)
    req.add_header("Authorization", "Basic " + base64.b64encode(f"{MARS_KEY}:".encode()).decode())
    req.add_header("Accept", "application/json")
    with urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


def fetch_egg_prices(start, end):
    """Fetch daily VWAP for Caged Large (National) from MARS API. Returns dict date->$/dz."""
    caged = defaultdict(lambda: {"pv": 0.0, "v": 0})
    cur = start
    while cur < end:
        chunk_end = min(cur + timedelta(days=180), end)
        sd, ed = cur.strftime("%m/%d/%Y"), chunk_end.strftime("%m/%d/%Y")
        url = f"{MARS_BASE}/{MARS_REPORT}?q=report_begin_date={sd}:{ed}&allSections=true"
        print(f"  Egg prices: {cur.date()} → {chunk_end.date()} ...", end=" ", flush=True)
        try:
            sections = _mars_get(url)
        except (HTTPError, URLError, Exception) as e:
            print(f"WARN: {e}")
            cur = chunk_end + timedelta(days=1)
            continue

        for sec in sections:
            if sec.get("reportSection") != "Report Detail Weighted":
                continue
            for r in sec["results"]:
                if r.get("class") != "Large" or not r.get("wtd_avg_price") or not r.get("volume"):
                    continue
                if r.get("environment") == "Caged" and r.get("origin") == "National":
                    d = r["report_date"]
                    p = float(r["wtd_avg_price"])
                    v = int(r["volume"])
                    caged[d]["pv"] += p * v
                    caged[d]["v"] += v

        nc = sum(1 for a in caged.values() if a["v"] > 0)
        print(f"{nc} days")
        cur = chunk_end + timedelta(days=1)

    # Convert cents/dozen → $/dozen
    return {d: round(a["pv"] / a["v"] / 100, 2) for d, a in caged.items() if a["v"] > 0}


# ── HPAI CSV parser ─────────────────────────────────────────────────────────

def parse_hpai_csv(path):
    """Parse APHIS 'A Table by Confirmation Date' CSV (UTF-16 tab-delimited)."""
    with open(path, encoding="utf-16") as f:
        lines = f.read().strip().replace("\r\n", "\n").replace("\r", "\n").split("\n")

    hdr_idx = None
    for i, line in enumerate(lines):
        low = line.lower()
        if "confirmed" in low and "production" in low:
            hdr_idx = i
            break
    if hdr_idx is None:
        sys.exit("ERROR: Cannot find header row (expected 'Confirmed' and 'Production' columns)")

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
        dt = None
        for fmt in ("%d-%b-%y", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(confirmed, fmt)
                break
            except ValueError:
                pass
        if dt is None:
            continue
        events.append({"date": dt, "state": state, "county": county, "production": production, "flock": flock})

    return events


# ── Livestock CSV parser ───────────────────────────────────────────────────

def parse_livestock_csv(path):
    """Parse APHIS 'Table Details by Date' CSV (UTF-16 tab-delimited)."""
    with open(path, encoding="utf-16") as f:
        lines = f.read().strip().replace("\r\n", "\n").replace("\r", "\n").split("\n")

    hdr_idx = None
    for i, line in enumerate(lines):
        low = line.lower()
        if "confirmed" in low and "production" in low and "species" in low:
            hdr_idx = i
            break
    if hdr_idx is None:
        sys.exit("ERROR: Cannot find header row in livestock CSV")

    hdrs = lines[hdr_idx].split("\t")
    hdr_low = [h.strip().lower() for h in hdrs]
    ci = hdr_low.index("confirmed")
    si = hdr_low.index("state")
    idi = hdr_low.index("special id")
    pi = hdr_low.index("production")
    spi = hdr_low.index("species")

    events = []
    for line in lines[hdr_idx + 1:]:
        cols = line.split("\t")
        if len(cols) < max(ci, si, idi, pi, spi) + 1:
            continue
        confirmed = cols[ci].strip()
        state = cols[si].strip()
        special_id = cols[idi].strip()
        production = cols[pi].strip()
        species = cols[spi].strip()
        if not confirmed or not state:
            continue
        dt = None
        for fmt in ("%d-%b-%y", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(confirmed, fmt)
                break
            except ValueError:
                pass
        if dt is None:
            continue
        events.append({
            "date": dt, "state": state, "special_id": special_id,
            "production": production, "species": species,
        })
    return events


# ── Mammal species classifier ─────────────────────────────────────────────

MAMMAL_GROUPS = {
    "Domestic/Companion": [
        "domestic cat", "cat", "kitten", "dog", "puppy", "pet", "goat",
    ],
    "Wild Carnivores": [
        "fox", "bobcat", "coyote", "mountain lion", "cougar", "lion",
        "fisher", "mink", "weasel", "otter", "badger", "bear",
        "skunk", "raccoon", "marten", "wolverine", "lynx", "ocelot",
        "coati", "ringtail", "ermine", "polecat", "ferret",
    ],
    "Rodents/Small Mammals": [
        "mouse", "mice", "rat", "squirrel", "chipmunk", "vole",
        "rabbit", "hare", "opossum", "possum", "porcupine",
        "shrew", "mole", "woodchuck", "groundhog", "cottontail",
        "pygmy rabbit", "pikas",
    ],
    "Marine Mammals": [
        "seal", "sea lion", "dolphin", "porpoise", "whale", "walrus",
        "manatee", "otter",
    ],
    "Captive/Zoo": [
        "tiger", "leopard", "snow leopard", "cheetah", "jaguar",
        "gorilla", "primate", "binturong", "genet", "civet",
        "amur", "bengal", "captive", "zoo",
    ],
}


def classify_mammal_species(name):
    """Classify a mammal species name into one of 6 groups."""
    low = name.lower().strip()
    # Check Captive/Zoo first (Amur tiger etc.)
    for kw in MAMMAL_GROUPS["Captive/Zoo"]:
        if kw in low:
            return "Captive/Zoo"
    # Marine before Wild Carnivores (sea otter vs otter)
    for kw in MAMMAL_GROUPS["Marine Mammals"]:
        if kw in low:
            return "Marine Mammals"
    for kw in MAMMAL_GROUPS["Domestic/Companion"]:
        if kw in low:
            return "Domestic/Companion"
    for kw in MAMMAL_GROUPS["Wild Carnivores"]:
        if kw in low:
            return "Wild Carnivores"
    for kw in MAMMAL_GROUPS["Rodents/Small Mammals"]:
        if kw in low:
            return "Rodents/Small Mammals"
    return "Other"


def parse_mammals_csv(path):
    """Parse APHIS 'HPAI Detections in Mammals' CSV (UTF-8 comma-delimited)."""
    import csv
    events = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_str = row.get("Date Detected", "").strip()
            if not date_str:
                continue
            dt = None
            for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d-%b-%y"):
                try:
                    dt = datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    pass
            if dt is None:
                continue
            species = row.get("Species", "").strip()
            events.append({
                "date": dt,
                "state": row.get("State", "").strip(),
                "county": row.get("County", "").strip(),
                "species": species,
                "group": classify_mammal_species(species),
                "strain": row.get("HPAI Strain", "").strip(),
            })
    return events


# ── Wild bird species classifier ──────────────────────────────────────────

BIRD_GROUPS = {
    "Waterfowl": [
        "goose", "geese", "duck", "mallard", "teal", "pintail", "wigeon",
        "shoveler", "gadwall", "canvasback", "scaup", "scoter", "eider",
        "bufflehead", "goldeneye", "merganser", "swan", "brant", "garganey",
        "redhead", "pochard", "ruddy", "wood duck", "ring-necked duck",
        "long-tailed duck", "harlequin", "smew", "shelduck",
    ],
    "Raptors": [
        "eagle", "hawk", "falcon", "owl", "osprey", "kite", "harrier",
        "vulture", "condor", "kestrel", "merlin", "accipiter", "buteo",
        "peregrine", "goshawk",
    ],
    "Seabirds": [
        "pelican", "gull", "tern", "cormorant", "gannet", "booby",
        "albatross", "shearwater", "petrel", "skua", "jaeger", "murre",
        "puffin", "auk", "guillemot", "frigatebird", "tropicbird",
        "loon", "grebe", "phalarope", "skimmer",
    ],
    "Corvids": [
        "crow", "raven", "jay", "magpie", "jackdaw", "rook",
    ],
}


def classify_bird_species(name):
    """Classify a bird species name into one of 5 groups."""
    low = name.lower().strip()
    for kw in BIRD_GROUPS["Waterfowl"]:
        if kw in low:
            return "Waterfowl"
    for kw in BIRD_GROUPS["Raptors"]:
        if kw in low:
            return "Raptors"
    for kw in BIRD_GROUPS["Seabirds"]:
        if kw in low:
            return "Seabirds"
    for kw in BIRD_GROUPS["Corvids"]:
        if kw in low:
            return "Corvids"
    return "Other"


def parse_wild_birds_csv(path):
    """Parse APHIS 'HPAI Detections in Wild Birds' CSV (UTF-8 comma-delimited)."""
    import csv
    events = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_str = row.get("Collection Date", "").strip()
            if not date_str:
                continue
            dt = None
            for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d-%b-%y"):
                try:
                    dt = datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    pass
            if dt is None:
                continue
            species = row.get("Bird Species", "").strip()
            events.append({
                "date": dt,
                "state": row.get("State", "").strip(),
                "county": row.get("County", "").strip(),
                "species": species,
                "group": classify_bird_species(species),
                "strain": row.get("HPAI Strain", "").strip(),
            })
    return events


# ── FIPS Mapping (for heatmap) ────────────────────────────────────────────

def build_fips_lookup():
    """Build a FIPS lookup function using addfips with manual overrides."""
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


def aggregate_county_detections(all_events, fips_lookup):
    """Aggregate detection events by FIPS county code."""
    county_data = {}
    unmapped_set = set()
    unknown_count = 0
    unknown_events = []

    for event in all_events:
        state = event["state"]
        county = event["county"]
        date = event["date"]
        source = event["source"]

        if not county or county.lower() == "unknown":
            unknown_count += 1
            unknown_events.append({"date": date, "source": source})
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

    return county_data, unknown_count, unknown_events


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


# ── Aggregation ─────────────────────────────────────────────────────────────

COLORS = {
    # Commercial — navy/teal family (#013046 → #1F9EBC → #8FCAE6)
    "Commercial Table Egg Layer": "#F6851F",
    "Commercial Table Egg Pullets": "#F9A54E",
    "Commercial Broiler Production": "#013046",
    "Commercial Broiler Breeder": "#0A4A6B",
    "Commercial Turkey Meat Bird": "#1F9EBC",
    "Commercial Turkey Breeder Hens": "#3DB3CF",
    "Commercial Duck Meat Bird": "#FDB714",
    "Commercial Duck Breeder": "#FDCB56",
    "Commercial Upland Gamebird Producer": "#8FCAE6",
    "Commercial Raised for Release Upland Game Bird": "#B0D9ED",
    "Commercial Raised for Release Waterfowl": "#D0E8F4",
    "Commercial Breeder Operation": "#165E7A",
    "Commercial Breeder (Multiple Bird Species)": "#2B7D9E",
    "Commercial Turkey Breeder Replacement Hens": "#5DB8D4",
    "Commercial Turkey Breeder Toms": "#78C4DC",
    "Commercial Turkey Poult Supplier": "#A3D5E8",
    "Commercial Table Egg Breeder": "#FAB87B",
    "Commercial Broiler Breeder Pullets": "#0F3D5C",
    "Primary Broiler Breeder Pedigree Farm": "#4AADCA",
    # Backyard/Other — orange/gold family
    "Live Bird Market": "#E5700A",
    "Live Bird Sales  (non-slaughter)": "#D4920E",
    "WOAH Poultry": "#939598",
    "WOAH Non-Poultry": "#A7A9AC",
}


def _parse_mars_date(d):
    return datetime.strptime(d, "%m/%d/%Y")


def build_data(events, caged_prices, livestock=None, mammals=None, wild_birds=None):
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


# ── HTML template ───────────────────────────────────────────────────────────

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
<meta http-equiv="Pragma" content="no-cache">
<meta http-equiv="Expires" content="0">
<title>HPAI Impact Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Lexend:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
<script src="https://cdn.jsdelivr.net/npm/topojson-client@3"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Lexend',sans-serif;background:#f0f4f8;color:#013046;line-height:1.5}
.wrap{max-width:1100px;margin:0 auto;padding:20px 16px}
header{text-align:center;margin-bottom:20px}
header h1{font-size:1.6rem;font-weight:700;color:#013046}
header .sub{color:#939598;font-size:.8rem;margin-top:2px}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:20px}
.kpi{background:#fff;border-radius:10px;padding:14px 18px;box-shadow:0 1px 3px rgba(0,0,0,.07)}
.kpi .lbl{font-size:.7rem;text-transform:uppercase;letter-spacing:.04em;color:#939598;font-weight:600}
.kpi .val{font-size:1.4rem;font-weight:700;margin-top:2px}
.kpi .note{font-size:.7rem;color:#A7A9AC;margin-top:1px}
.card{background:#fff;border-radius:10px;padding:20px;margin-bottom:14px;box-shadow:0 1px 3px rgba(0,0,0,.07)}
.card h2{font-size:1rem;font-weight:600;margin-bottom:2px;color:#013046}
.card .sub{font-size:.78rem;color:#939598;margin-bottom:12px}
.card-source{font-size:.68rem;color:#939598;margin-top:10px}
.card-source a{color:#939598;text-decoration:none}
.card-source a:hover{text-decoration:underline}
footer{text-align:center;padding:14px;color:#A7A9AC;font-size:.7rem}
.up{color:#F6851F}.dn{color:#1F9EBC}
/* range buttons */
.controls{display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:8px;margin-bottom:10px}
.range-row{display:flex;gap:3px;flex-wrap:wrap}
.rbtn{padding:3px 10px;border:1px solid #e2e8f0;border-radius:16px;background:#f8fafc;font-size:.72rem;cursor:pointer;color:#939598;transition:all .15s}
.rbtn:hover{background:#e2e8f0}
.rbtn.active{background:#013046;color:#fff;border-color:#013046}
/* multi-select */
.ms-wrap{position:relative;display:inline-block}
.ms-btn{padding:5px 12px;border:1px solid #e2e8f0;border-radius:6px;background:#f8fafc;font-size:.8rem;cursor:pointer;color:#013046;min-width:160px;text-align:left}
.ms-btn::after{content:'▾';float:right;margin-left:8px;color:#A7A9AC}
.ms-panel{display:none;position:absolute;right:0;z-index:20;background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:8px 10px;max-height:320px;overflow-y:auto;min-width:290px;box-shadow:0 4px 16px rgba(0,0,0,.12);margin-top:4px}
.ms-panel.open{display:block}
.ms-actions{padding:2px 0 6px;border-bottom:1px solid #f1f5f9;margin-bottom:6px;font-size:.75rem}
.ms-actions a{color:#1F9EBC;text-decoration:none;cursor:pointer}
.ms-actions a:hover{text-decoration:underline}
.ms-item{display:flex;align-items:center;padding:3px 0;cursor:pointer;font-size:.8rem;gap:6px}
.ms-item input{margin:0;cursor:pointer}
.ms-dot{width:10px;height:10px;border-radius:2px;flex-shrink:0}
/* data table */
.tbl-wrap{max-height:400px;overflow-y:auto;border:1px solid #e2e8f0;border-radius:6px;margin-top:8px}
.tbl-wrap table{width:100%;border-collapse:collapse;font-size:.78rem}
.tbl-wrap thead{position:sticky;top:0;z-index:2}
.tbl-wrap th{background:#f8fafc;padding:7px 10px;text-align:left;font-weight:600;color:#939598;border-bottom:2px solid #e2e8f0;white-space:nowrap}
.tbl-wrap td{padding:5px 10px;border-bottom:1px solid #f1f5f9;color:#013046}
.tbl-wrap tr:nth-child(even) td{background:#f8fafc}
.tbl-wrap .num{text-align:right;font-variant-numeric:tabular-nums}
.tbl-search{width:100%;padding:7px 12px;border:1px solid #e2e8f0;border-radius:6px;font-size:.8rem;font-family:'Lexend',sans-serif;color:#013046;margin-top:8px;box-sizing:border-box}
.tbl-search::placeholder{color:#A7A9AC}
.tbl-search:focus{outline:none;border-color:#1F9EBC}
.tbl-summary{font-size:.75rem;color:#939598;margin-top:6px}
.tbl-pager{display:flex;align-items:center;gap:8px;margin-top:6px;font-size:.75rem;color:#939598}
.tbl-pager button{padding:3px 10px;border:1px solid #e2e8f0;border-radius:6px;background:#f8fafc;font-size:.72rem;cursor:pointer;color:#013046;font-family:'Lexend',sans-serif}
.tbl-pager button:hover:not(:disabled){background:#e2e8f0}
.tbl-pager button:disabled{opacity:.4;cursor:default}
.ms-group{margin-bottom:2px}
.ms-group-hdr{display:flex;align-items:center;padding:5px 0 3px;cursor:pointer;font-size:.8rem;font-weight:600;gap:6px;color:#013046}
.ms-group-hdr input{margin:0;cursor:pointer}
.ms-group-children{padding-left:20px}
/* tabs */
.tab-bar{display:flex;gap:0;margin-bottom:14px;border-bottom:2px solid #e2e8f0}
.tab-btn{padding:10px 20px;border:none;background:none;font-size:.85rem;font-weight:600;color:#939598;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-2px;white-space:nowrap;transition:all .15s;font-family:'Lexend',sans-serif}
.tab-btn:hover{color:#013046}
.tab-btn.active{color:#1F9EBC;border-bottom-color:#1F9EBC}
.tab-content{display:none}
.tab-content.active{display:block}
/* heatmap */
#mapContainer{width:100%;position:relative}
#mapContainer svg{width:100%;height:auto;display:block}
.county{stroke:#fff;stroke-width:.25px;transition:opacity .12s}
.county:hover{opacity:.8;stroke:#013046;stroke-width:1px}
.state-border{fill:none;stroke:#A7A9AC;stroke-width:.7px;pointer-events:none}
.map-tooltip{position:absolute;pointer-events:none;background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:10px 14px;box-shadow:0 4px 12px rgba(0,0,0,.12);font-size:.8rem;max-width:280px;z-index:30;opacity:0;transition:opacity .12s}
.map-tooltip.visible{opacity:1}
.tt-county{font-weight:700;font-size:.9rem;color:#013046}
.tt-total{font-size:1.1rem;font-weight:700;margin:4px 0;color:#F6851F}
.tt-row{font-size:.75rem;color:#939598;line-height:1.6}
.tt-date{font-size:.7rem;color:#A7A9AC;margin-top:4px}
.legend-wrap{display:flex;align-items:center;gap:6px;font-size:.7rem;color:#939598}
.legend-bar{width:200px;height:12px;border-radius:3px}
@keyframes spin{to{transform:rotate(360deg)}}
#loadingOverlay{position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(240,244,248,.95);z-index:1000;display:flex;align-items:center;justify-content:center}
</style>
</head>
<body>
<div id="loadingOverlay">
  <div style="text-align:center">
    <div style="width:40px;height:40px;border:3px solid #e2e8f0;border-top-color:#1F9EBC;border-radius:50%;animation:spin 1s linear infinite;margin:0 auto 12px"></div>
    <div style="font-size:.9rem;color:#939598;font-weight:600">Loading dashboard data&hellip;</div>
  </div>
</div>
<div id="dashboardContent" class="wrap">
<header>
  <h1>HPAI Dashboard</h1>
  <div class="sub">Updated __UPDATED__</div>
</header>

<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
  <div class="sub" id="kpiPeriodLabel" style="margin:0;font-weight:600;font-size:.8rem">Last 30 Days</div>
  <div class="range-row" data-chart="kpi">
    <button class="rbtn active" data-r="30d">30D</button>
    <button class="rbtn" data-r="3m">3M</button>
    <button class="rbtn" data-r="6m">6M</button>
    <button class="rbtn" data-r="1y">1Y</button>
    <button class="rbtn" data-r="ytd">YTD</button>
    <button class="rbtn" data-r="all">All</button>
  </div>
</div>
<div class="kpi-row">
  <div class="kpi">
    <div class="lbl">Hens Depopulated</div>
    <div class="val" style="color:#F6851F" id="kpiLayers">&mdash;</div>
    <div class="note" id="kpiLayersNote">Commercial egg layers</div>
  </div>
  <div class="kpi">
    <div class="lbl">Number of Poultry Operations Infected</div>
    <div class="val" style="color:#FDB714" id="kpiSites">&mdash;</div>
    <div class="note" id="kpiSitesNote">All flock types</div>
  </div>
  <div class="kpi">
    <div class="lbl">Wild Bird Detections</div>
    <div class="val" style="color:#1F9EBC" id="kpiWB">&mdash;</div>
    <div class="note" id="kpiWBNote">Confirmed cases</div>
  </div>
  <div class="kpi">
    <div class="lbl">Detection Change (M/M)</div>
    <div class="val" id="kpiChg">&mdash;</div>
    <div class="note">Wild Bird Detections and Poultry Operation Infection vs prior 30 days</div>
  </div>
  <div class="kpi">
    <div class="lbl">Wholesale Egg Price</div>
    <div class="val" style="color:#013046" id="kpiPrice">&mdash;</div>
    <div class="note">National FOB, Caged, $ per Dozen</div>
  </div>
</div>

<!-- Heatmap (always visible) -->
__HEATMAP_CARD__

<!-- Tab bar -->
<div class="tab-bar" id="tabBar">
  <button class="tab-btn active" data-tab="poultry">Poultry</button>
  __TAB_WILDBIRDS_BTN__
  __TAB_LIVESTOCK_BTN__
  __TAB_MAMMALS_BTN__
</div>

<!-- Tab: Commercial Poultry -->
<div class="tab-content active" id="tab-poultry">
  <div class="card">
    <h2 id="birdsTitle">Birds Impacted by Month</h2>
    <div class="sub">Total flock size of confirmed HPAI detections</div>
    <div class="controls">
      <div class="range-row" data-chart="birds">
        <button class="rbtn" data-r="30d">30D</button>
        <button class="rbtn" data-r="3m">3M</button>
        <button class="rbtn" data-r="6m">6M</button>
        <button class="rbtn" data-r="1y">1Y</button>
        <button class="rbtn" data-r="ytd">YTD</button>
        <button class="rbtn active" data-r="all">All</button>
      </div>
      <div class="ms-wrap" id="birdsMS">
        <button class="ms-btn" id="birdsMSBtn">1 category</button>
        <div class="ms-panel" id="birdsMSPanel">
          <div class="ms-actions">
            <a onclick="msAll('birds')">Select All</a> · <a onclick="msNone('birds')">Clear</a>
          </div>
          __BIRDS_CHECKBOXES__
        </div>
      </div>
    </div>
    <canvas id="cBirds"></canvas>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/commercial-backyard-flocks" target="_blank">USDA APHIS</a></div>
  </div>
  <div class="card">
    <h2 id="infTitle">Confirmed HPAI Detections by Month</h2>
    <div class="sub">Number of confirmed sites</div>
    <div class="controls">
      <div class="range-row" data-chart="inf">
        <button class="rbtn" data-r="30d">30D</button>
        <button class="rbtn" data-r="3m">3M</button>
        <button class="rbtn" data-r="6m">6M</button>
        <button class="rbtn" data-r="1y">1Y</button>
        <button class="rbtn" data-r="ytd">YTD</button>
        <button class="rbtn active" data-r="all">All</button>
      </div>
      <div class="ms-wrap" id="infMS">
        <button class="ms-btn" id="infMSBtn">All categories</button>
        <div class="ms-panel" id="infMSPanel">
          <div class="ms-actions">
            <a onclick="msAll('inf')">Select All</a> · <a onclick="msNone('inf')">Clear</a>
          </div>
          __INF_CHECKBOXES__
        </div>
      </div>
    </div>
    <canvas id="cInf"></canvas>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/commercial-backyard-flocks" target="_blank">USDA APHIS</a></div>
  </div>
  <div class="card">
    <h2>Wholesale Egg Prices</h2>
    <div class="sub">Large, National Wholesale, Volume-Weighted ($ per dozen)</div>
    <div class="controls">
      <div class="range-row" data-chart="egg">
        <button class="rbtn" data-r="30d">30D</button>
        <button class="rbtn" data-r="3m">3M</button>
        <button class="rbtn" data-r="6m">6M</button>
        <button class="rbtn" data-r="1y">1Y</button>
        <button class="rbtn" data-r="ytd">YTD</button>
        <button class="rbtn active" data-r="all">All</button>
      </div>
    </div>
    <canvas id="cEgg"></canvas>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://mymarketnews.ams.usda.gov/viewReport/2843" target="_blank">USDA AMS</a></div>
  </div>
  <div class="card">
    <h2>Poultry Detection Details</h2>
    <div class="sub">Individual confirmed flock detections</div>
    <div class="controls">
      <div class="range-row" data-chart="tbl">
        <button class="rbtn" data-r="30d">30D</button>
        <button class="rbtn" data-r="3m">3M</button>
        <button class="rbtn" data-r="6m">6M</button>
        <button class="rbtn" data-r="1y">1Y</button>
        <button class="rbtn" data-r="ytd">YTD</button>
        <button class="rbtn active" data-r="all">All</button>
      </div>
      <div class="ms-wrap" id="tblMS">
        <button class="ms-btn" id="tblMSBtn">All categories</button>
        <div class="ms-panel" id="tblMSPanel">
          <div class="ms-actions">
            <a onclick="msAll('tbl')">Select All</a> · <a onclick="msNone('tbl')">Clear</a>
          </div>
          __TBL_CHECKBOXES__
        </div>
      </div>
    </div>
    <input type="text" class="tbl-search" id="tblSearch" placeholder="Filter by state, county, type..." oninput="updateTable()">
    <div class="tbl-wrap">
      <table>
        <thead><tr><th>Date</th><th>State</th><th>County</th><th>Operation Type</th><th class="num">Birds Impacted</th></tr></thead>
        <tbody id="tblBody"></tbody>
      </table>
    </div>
    <div class="tbl-summary" id="tblSummary"></div>
    <div class="tbl-pager" id="tblPager"><button onclick="pageTable('tbl',-1)">← Prev</button><span id="tblPageInfo"></span><button onclick="pageTable('tbl',1)">Next →</button></div>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/commercial-backyard-flocks" target="_blank">USDA APHIS</a></div>
  </div>
</div>

<!-- Tab: Wild Birds -->
__TAB_WILDBIRDS_HTML__

<!-- Tab: Livestock -->
__TAB_LIVESTOCK_HTML__

<!-- Tab: Mammals -->
__TAB_MAMMALS_HTML__

</div>

<script>
/* ── Global state ── */
let D = null;
let eggChart, birdsChart, infChart;
let lsChart=null, wbChart=null, mmChart=null;
const TOPO_URL='https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json';
let mapRange='30d',sourceFilter='both';
let mapSvg,mapPath,mapTooltip,countyPaths;
let eggRange='all',birdsRange='all',infRange='all',tblRange='all';
let lsRange='all',wbRange='all',mmRange='all',mmTblRange='all';
let kpiRange='30d';
const PAGE_SIZE=100;
let tblPage=0,lsPage=0,wbPage=0,mmPage=0;
const panelMap={birds:'birdsMSPanel',inf:'infMSPanel',tbl:'tblMSPanel'};
const btnMap={birds:'birdsMSBtn',inf:'infMSBtn',tbl:'tblMSBtn'};
const MM_COLORS={'Domestic/Companion':'#F6851F','Wild Carnivores':'#013046','Rodents/Small Mammals':'#FDB714','Marine Mammals':'#1F9EBC','Captive/Zoo':'#8FCAE6','Other':'#939598'};

/* ── Chart.js global font ── */
Chart.defaults.font.family="'Lexend',sans-serif";

/* ── Loading UI ── */
function hideLoading(){document.getElementById('loadingOverlay').style.display='none';}
function showError(msg){
  document.getElementById('loadingOverlay').innerHTML='<div style="text-align:center;color:#F6851F"><div style="font-size:1.2rem;font-weight:700;margin-bottom:8px">Failed to load data</div><div style="font-size:.85rem;color:#939598">'+msg+'</div><button onclick="location.reload()" style="margin-top:12px;padding:8px 20px;border:1px solid #e2e8f0;border-radius:6px;background:#fff;color:#013046;cursor:pointer">Retry</button></div>';
}

/* ── Heatmap helpers ── */
function cutoffYM(range){
  const now=new Date();let c;
  switch(range){
    case '7d':c=new Date(now.getFullYear(),now.getMonth(),now.getDate()-7);break;
    case '14d':c=new Date(now.getFullYear(),now.getMonth(),now.getDate()-14);break;
    case '30d':c=new Date(now.getFullYear(),now.getMonth(),now.getDate()-30);break;
    case '60d':c=new Date(now.getFullYear(),now.getMonth(),now.getDate()-60);break;
    case '3m':c=new Date(now.getFullYear(),now.getMonth()-3,now.getDate());break;
    case '1y':c=new Date(now.getFullYear()-1,now.getMonth(),now.getDate());break;
    default:c=new Date(2000,0,1);
  }
  return c.getFullYear()+'-'+String(c.getMonth()+1).padStart(2,'0');
}
function sumMonths(moDict,cutM){let n=0;for(const[month,cnt]of Object.entries(moDict)){if(month>=cutM)n+=cnt;}return n;}
function getFilteredCounts(fips){
  const info=D.map_data[fips];
  if(!info)return{total:0,wb:0,p:0};
  const cutM=cutoffYM(mapRange);
  const wb=sumMonths(info.mwb||{},cutM);
  const p=sumMonths(info.mp||{},cutM);
  let total;
  if(sourceFilter==='wild_birds')total=wb;
  else if(sourceFilter==='poultry')total=p;
  else total=wb+p;
  return{total,wb,p};
}
function buildColorScale(maxVal){if(maxVal<=0)maxVal=1;return d3.scaleSequentialLog(d3.interpolateYlOrRd).domain([1,maxVal]);}
function paintLegend(maxVal){
  const canvas=document.getElementById('legendBar');
  const ctx=canvas.getContext('2d');
  const w=canvas.width,h=canvas.height;
  const scale=buildColorScale(Math.max(maxVal,2));
  for(let x=0;x<w;x++){const v=1+(x/(w-1))*(Math.max(maxVal,2)-1);ctx.fillStyle=scale(v);ctx.fillRect(x,0,1,h);}
  document.getElementById('legendMax').textContent=maxVal.toLocaleString();
}
async function initMap(){
  if(!D.map_data)return;
  const res=await fetch(TOPO_URL);
  const us=await res.json();
  const counties=topojson.feature(us,us.objects.counties);
  const stateMesh=topojson.mesh(us,us.objects.states,(a,b)=>a!==b);
  const width=975,height=610;
  const projection=d3.geoAlbersUsa().fitSize([width,height],counties);
  mapPath=d3.geoPath().projection(projection);
  mapSvg=d3.select('#mapContainer').append('svg').attr('viewBox','0 0 '+width+' '+height).attr('preserveAspectRatio','xMidYMid meet');
  mapTooltip=d3.select('#mapContainer').append('div').attr('class','map-tooltip');
  countyPaths=mapSvg.append('g').selectAll('path').data(counties.features).join('path')
    .attr('class','county').attr('d',mapPath).attr('data-fips',d=>d.id)
    .on('mouseenter',onMapHover).on('mousemove',onMapMove).on('mouseleave',onMapLeave);
  mapSvg.append('path').datum(stateMesh).attr('class','state-border').attr('d',mapPath);
  updateMapColors();
}
function updateMapColors(){
  if(!countyPaths)return;
  const allFips=Object.keys(D.map_data);
  const totals=allFips.map(f=>getFilteredCounts(f).total);
  const positives=totals.filter(c=>c>0);
  const maxCount=positives.length?Math.max(...positives):1;
  const scale=buildColorScale(maxCount);
  countyPaths.attr('fill',function(){const fips=this.getAttribute('data-fips');const count=getFilteredCounts(fips).total;return count>0?scale(count):'#f1f5f9';});
  paintLegend(maxCount);
  const activeCounties=positives.length;
  const totalDet=positives.reduce((a,b)=>a+b,0);
  const cutM=cutoffYM(mapRange);
  let unk=0;
  if(D.unknown_by_month){for(const[m,v]of Object.entries(D.unknown_by_month)){if(m>=cutM){if(sourceFilter==='wild_birds')unk+=v.wb||0;else if(sourceFilter==='poultry')unk+=v.p||0;else unk+=(v.wb||0)+(v.p||0);}}}
  const rangeLabel={'7d':'7-day','14d':'14-day','30d':'30-day','60d':'60-day','3m':'3-month','1y':'1-year'}[mapRange]||mapRange;
  const srcLabel=sourceFilter==='both'?'':(sourceFilter==='wild_birds'?' (wild birds only)':' (poultry only)');
  let summary=activeCounties.toLocaleString()+' counties with '+totalDet.toLocaleString()+' detections in '+rangeLabel+' window'+srcLabel;
  if(unk>0)summary+=' \u00b7 '+unk.toLocaleString()+' excluded (county unknown)';
  document.getElementById('mapSummary').textContent=summary;
}
function onMapHover(event,d){
  const fips=d.id;const info=D.map_data[fips];const fc=getFilteredCounts(fips);
  const name=info?(info.c+', '+info.s):(d.properties.name||'Unknown county');
  let html='<div class="tt-county">'+name+'</div>';
  if(fc.total>0){
    html+='<div class="tt-total">'+fc.total.toLocaleString()+' detection'+(fc.total!==1?'s':'')+'</div>';
    html+='<div class="tt-row">';
    if(fc.wb>0)html+='Wild birds: '+fc.wb.toLocaleString()+'<br>';
    if(fc.p>0)html+='Poultry flocks: '+fc.p.toLocaleString()+'<br>';
    html+='</div>';
    if(info&&info.ld)html+='<div class="tt-date">Latest (all time): '+info.ld+'</div>';
  } else {
    html+='<div class="tt-row" style="color:#A7A9AC">No detections in this period</div>';
  }
  mapTooltip.html(html).classed('visible',true);
}
function onMapMove(event){
  const container=document.getElementById('mapContainer');
  const rect=container.getBoundingClientRect();
  const x=event.clientX-rect.left,y=event.clientY-rect.top;
  const ttNode=mapTooltip.node();
  const ttW=ttNode.offsetWidth,ttH=ttNode.offsetHeight;
  const left=(x+ttW+20>rect.width)?x-ttW-10:x+14;
  const top=(y+ttH+10>rect.height)?y-ttH-10:y+14;
  mapTooltip.style('left',left+'px').style('top',top+'px');
}
function onMapLeave(){mapTooltip.classed('visible',false);}

/* ── Date range helpers ── */
function cutoffDate(range){
  const now=new Date();
  switch(range){
    case '30d':return new Date(now.getFullYear(),now.getMonth(),now.getDate()-30);
    case '3m':return new Date(now.getFullYear(),now.getMonth()-3,now.getDate());
    case '6m':return new Date(now.getFullYear(),now.getMonth()-6,now.getDate());
    case '1y':return new Date(now.getFullYear()-1,now.getMonth(),now.getDate());
    case 'ytd':return new Date(now.getFullYear(),0,1);
    default:return new Date(2000,0,1);
  }
}
function cutoffISO(range){const c=cutoffDate(range);return c.getFullYear()+'-'+String(c.getMonth()+1).padStart(2,'0')+'-'+String(c.getDate()).padStart(2,'0');}
function cutoffMonth(range){const c=cutoffDate(range);return c.getFullYear()+'-'+String(c.getMonth()+1).padStart(2,'0');}
function eggIndices(range){const cutoff=cutoffDate(range);const idx=[];D.egg_dates_iso.forEach((d,i)=>{if(new Date(d)>=cutoff)idx.push(i);});return idx;}
function monthIndices(range){const cutM=cutoffMonth(range);const idx=[];D.months.forEach((m,i)=>{if(m>=cutM)idx.push(i);});return idx;}
function dailyIndices(range){const cutoff=cutoffDate(range);const idx=[];D.daily_dates.forEach((d,i)=>{if(new Date(d)>=cutoff)idx.push(i);});return idx;}
function sliceByIdx(arr,idx){return idx.map(i=>arr[i]);}
function isDaily(range){return range==='30d'||range==='3m';}
function fmtBirds(v){
  if(v>=1e6){const m=v/1e6;return(m%1===0?m.toFixed(0):m.toFixed(1))+'M';}
  if(v>=1e3){const k=v/1e3;return(k%1===0?k.toFixed(0):k.toFixed(1))+'K';}
  return v;
}
function fmtDate(iso){const p=iso.split('-');return parseInt(p[1])+'/'+parseInt(p[2])+'/'+p[0];}

/* ── Multi-select helpers ── */
function getLeaves(panelId){return Array.from(document.querySelectorAll('#'+panelId+' input[type=checkbox][value]'));}
function getSelected(panelId){return getLeaves(panelId).filter(cb=>cb.checked).map(cb=>cb.value);}
function chartUpdate(chart){
  if(chart==='birds')updateBirds();
  else if(chart==='inf')updateInf();
  else if(chart==='tbl')updateTable();
  else if(chart==='mmTbl')updateMmTable();
}
function msAll(chart){document.querySelectorAll('#'+panelMap[chart]+' input[type=checkbox]').forEach(cb=>cb.checked=true);chartUpdate(chart);updateMSLabel(chart);}
function msNone(chart){document.querySelectorAll('#'+panelMap[chart]+' input[type=checkbox]').forEach(cb=>cb.checked=false);chartUpdate(chart);updateMSLabel(chart);}
function updateMSLabel(chart){
  const leaves=getLeaves(panelMap[chart]);
  const sel=leaves.filter(cb=>cb.checked);
  const el=document.getElementById(btnMap[chart]);
  if(sel.length===0)el.textContent='None selected';
  else if(sel.length===leaves.length)el.textContent='All categories';
  else if(sel.length===1){const n=sel[0].value;el.textContent=n.length>28?n.slice(0,26)+'\u2026':n;}
  else el.textContent=sel.length+' categories';
}
function syncGroupHdr(hdr){
  const children=hdr.closest('.ms-group').querySelectorAll('.ms-group-children input[type=checkbox]');
  const checked=Array.from(children).filter(cb=>cb.checked).length;
  hdr.checked=checked===children.length;
  hdr.indeterminate=checked>0&&checked<children.length;
}
function panelToChart(panel){
  if(panel.id==='birdsMSPanel')return 'birds';
  if(panel.id==='infMSPanel')return 'inf';
  if(panel.id==='mmTblMSPanel')return 'mmTbl';
  return 'tbl';
}

/* ── Update functions ── */
function updateEgg(){const idx=eggIndices(eggRange);eggChart.data.labels=sliceByIdx(D.egg_dates,idx);eggChart.data.datasets[0].data=sliceByIdx(D.caged_prices,idx);eggChart.update();}
function updateBirds(){
  const sel=getSelected('birdsMSPanel');
  if(isDaily(birdsRange)){const idx=dailyIndices(birdsRange);birdsChart.data.labels=sliceByIdx(D.daily_labels,idx);birdsChart.data.datasets=sel.map(p=>({label:p,data:idx.map(i=>(D.daily_birds[D.daily_dates[i]]||{})[p]||0),backgroundColor:D.category_colors[p]||'#939598'}));}
  else{const idx=monthIndices(birdsRange);birdsChart.data.labels=sliceByIdx(D.month_labels,idx);birdsChart.data.datasets=sel.map(p=>({label:p,data:idx.map(i=>(D.birds_by_month[D.months[i]]||{})[p]||0),backgroundColor:D.category_colors[p]||'#939598'}));}
  birdsChart.update();document.getElementById('birdsTitle').textContent='Birds Impacted by '+(isDaily(birdsRange)?'Day':'Month');
}
function updateInf(){
  const sel=getSelected('infMSPanel');
  if(isDaily(infRange)){const idx=dailyIndices(infRange);infChart.data.labels=sliceByIdx(D.daily_labels,idx);infChart.data.datasets=sel.map(p=>({label:p,data:idx.map(i=>(D.daily_infections[D.daily_dates[i]]||{})[p]||0),backgroundColor:D.category_colors[p]||'#939598'}));}
  else{const idx=monthIndices(infRange);infChart.data.labels=sliceByIdx(D.month_labels,idx);infChart.data.datasets=sel.map(p=>({label:p,data:idx.map(i=>(D.infections_by_month[D.months[i]]||{})[p]||0),backgroundColor:D.category_colors[p]||'#939598'}));}
  infChart.update();document.getElementById('infTitle').textContent='Confirmed HPAI Detections by '+(isDaily(infRange)?'Day':'Month');
}
function renderPager(id,page,total){
  const pages=Math.ceil(total/PAGE_SIZE)||1;
  const el=document.getElementById(id);if(!el)return;
  const btns=el.querySelectorAll('button');
  btns[0].disabled=page<=0;btns[1].disabled=page>=pages-1;
  document.getElementById(id.replace('Pager','PageInfo')).textContent='Page '+(page+1)+' of '+pages;
  el.style.display=total>PAGE_SIZE?'flex':'none';
}
function pageTable(tbl,dir){
  if(tbl==='tbl'){tblPage+=dir;updateTable(false);}
  if(tbl==='ls'){lsPage+=dir;updateLsTable(false);}
  if(tbl==='wb'){wbPage+=dir;updateWbTable(false);}
  if(tbl==='mm'){mmPage+=dir;updateMmTable(false);}
}
function updateTable(resetPage){
  if(resetPage!==false)tblPage=0;
  const cutoff=cutoffISO(tblRange);const sel=new Set(getSelected('tblMSPanel'));
  const q=(document.getElementById('tblSearch').value||'').toLowerCase().trim();
  const filtered=D.events.filter(e=>e.d>=cutoff&&sel.has(e.p)&&(!q||e.s.toLowerCase().includes(q)||e.c.toLowerCase().includes(q)||e.p.toLowerCase().includes(q)));
  const start=tblPage*PAGE_SIZE;const show=filtered.slice(start,start+PAGE_SIZE);
  document.getElementById('tblBody').innerHTML=show.map(e=>'<tr><td>'+fmtDate(e.d)+'</td><td>'+e.s+'</td><td>'+e.c+'</td><td>'+e.p+'</td><td class="num">'+e.f.toLocaleString()+'</td></tr>').join('');
  document.getElementById('tblSummary').textContent='Showing '+(start+1)+'-'+(start+show.length)+' of '+filtered.length.toLocaleString()+' detections';
  renderPager('tblPager',tblPage,filtered.length);
}
function initTab(tab){
  if(tab==='livestock'&&D.livestock)initLivestock();
  if(tab==='wildbirds'&&D.wild_birds)initWildBirds();
  if(tab==='mammals'&&D.mammals)initMammals();
}
function initLivestock(){
  const LS=D.livestock;
  lsChart=new Chart(document.getElementById('cLivestock'),{type:'bar',data:{labels:LS.month_labels,datasets:[{label:'Herds Affected',data:LS.monthly_counts,backgroundColor:'#013046'}]},options:{responsive:true,aspectRatio:2.5,plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>c.parsed.y+' herds'}}},scales:{x:{ticks:{maxRotation:45},grid:{display:false}},y:{title:{display:true,text:'Herds Affected'},grid:{color:'#f1f5f9'}}}}});
  updateLsTable();
}
function updateLivestock(){
  if(!lsChart||!D.livestock)return;const LS=D.livestock;const cutM=cutoffMonth(lsRange);
  const idx=[];LS.months.forEach((m,i)=>{if(m>=cutM)idx.push(i);});
  lsChart.data.labels=sliceByIdx(LS.month_labels,idx);lsChart.data.datasets[0].data=sliceByIdx(LS.monthly_counts,idx);lsChart.update();updateLsTable();
}
function updateLsTable(resetPage){
  if(!D.livestock)return;if(resetPage!==false)lsPage=0;
  const cutoff=cutoffISO(lsRange);
  const q=(document.getElementById('lsSearch')?.value||'').toLowerCase().trim();
  const filtered=D.livestock.events.filter(e=>e.d>=cutoff&&(!q||e.s.toLowerCase().includes(q)||e.p.toLowerCase().includes(q)||e.sp.toLowerCase().includes(q)||(e.id||'').toLowerCase().includes(q)));
  const start=lsPage*PAGE_SIZE;const show=filtered.slice(start,start+PAGE_SIZE);
  document.getElementById('lsTblBody').innerHTML=show.map(e=>'<tr><td>'+fmtDate(e.d)+'</td><td>'+e.s+'</td><td>'+e.id+'</td><td>'+e.p+'</td><td>'+e.sp+'</td></tr>').join('');
  document.getElementById('lsTblSummary').textContent='Showing '+(start+1)+'-'+(start+show.length)+' of '+filtered.length.toLocaleString()+' detections';
  renderPager('lsPager',lsPage,filtered.length);
}
function initWildBirds(){
  const WB=D.wild_birds;
  wbChart=new Chart(document.getElementById('cWildBirds'),{type:'bar',data:{labels:WB.month_labels,datasets:[{label:'Detections',data:WB.monthly_counts,backgroundColor:'#1F9EBC'}]},options:{responsive:true,aspectRatio:2.5,plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>c.parsed.y.toLocaleString()+' detections'}}},scales:{x:{ticks:{maxRotation:45},grid:{display:false}},y:{title:{display:true,text:'Detections'},grid:{color:'#f1f5f9'},ticks:{callback:fmtBirds}}}}});
  updateWbTable();
}
function wbDailyIndices(range){const cutoff=cutoffDate(range);const idx=[];D.wild_birds.daily_dates.forEach((d,i)=>{if(new Date(d)>=cutoff)idx.push(i);});return idx;}
function updateWildBirds(){
  if(!wbChart||!D.wild_birds)return;const WB=D.wild_birds;
  if(isDaily(wbRange)){const idx=wbDailyIndices(wbRange);wbChart.data.labels=sliceByIdx(WB.daily_labels,idx);wbChart.data.datasets[0].data=sliceByIdx(WB.daily_counts,idx);}
  else{const cutM=cutoffMonth(wbRange);const idx=[];WB.months.forEach((m,i)=>{if(m>=cutM)idx.push(i);});wbChart.data.labels=sliceByIdx(WB.month_labels,idx);wbChart.data.datasets[0].data=sliceByIdx(WB.monthly_counts,idx);}
  wbChart.update();document.getElementById('wbTitle').textContent='Wild Bird HPAI Detections by '+(isDaily(wbRange)?'Day':'Month');updateWbTable();
}
function updateWbTable(resetPage){
  if(!D.wild_birds)return;if(resetPage!==false)wbPage=0;
  const cutoff=cutoffISO(wbRange);
  const q=(document.getElementById('wbSearch')?.value||'').toLowerCase().trim();
  const filtered=D.wild_birds.events.filter(e=>e.d>=cutoff&&(!q||e.s.toLowerCase().includes(q)||e.c.toLowerCase().includes(q)||e.sp.toLowerCase().includes(q)||e.st.toLowerCase().includes(q)));
  const start=wbPage*PAGE_SIZE;const show=filtered.slice(start,start+PAGE_SIZE);
  document.getElementById('wbTblBody').innerHTML=show.map(e=>'<tr><td>'+fmtDate(e.d)+'</td><td>'+e.s+'</td><td>'+e.c+'</td><td>'+e.sp+'</td><td>'+e.st+'</td></tr>').join('');
  document.getElementById('wbTblSummary').textContent='Showing '+(start+1)+'-'+(start+show.length)+' of '+filtered.length.toLocaleString()+' detections';
  renderPager('wbPager',wbPage,filtered.length);
}
function initMammals(){
  const MM=D.mammals;
  mmChart=new Chart(document.getElementById('cMammals'),{type:'bar',data:{labels:MM.month_labels,datasets:MM.groups.map(g=>({label:g,data:MM.months.map(m=>(MM.monthly_by_group[m]||{})[g]||0),backgroundColor:MM_COLORS[g]||'#939598'}))},options:{responsive:true,aspectRatio:2.5,plugins:{legend:{display:true,position:'bottom',labels:{boxWidth:12,font:{size:11}}},tooltip:{callbacks:{label:c=>c.dataset.label+': '+c.parsed.y}}},scales:{x:{stacked:true,ticks:{maxRotation:45},grid:{display:false}},y:{stacked:true,title:{display:true,text:'Detections'},grid:{color:'#f1f5f9'}}}}});
  updateMmTable();
}
function updateMammals(){
  if(!mmChart||!D.mammals)return;const MM=D.mammals;const cutM=cutoffMonth(mmRange);
  const idx=[];MM.months.forEach((m,i)=>{if(m>=cutM)idx.push(i);});
  mmChart.data.labels=sliceByIdx(MM.month_labels,idx);
  mmChart.data.datasets=MM.groups.map(g=>({label:g,data:idx.map(i=>(MM.monthly_by_group[MM.months[i]]||{})[g]||0),backgroundColor:MM_COLORS[g]||'#939598'}));
  mmChart.update();updateMmTable();
}
function updateMmTable(resetPage){
  if(!D.mammals)return;if(resetPage!==false)mmPage=0;
  const cutoff=cutoffISO(mmTblRange);
  const sel=D.mammals.groups?new Set(getSelected('mmTblMSPanel')):null;
  const q=(document.getElementById('mmSearch')?.value||'').toLowerCase().trim();
  const filtered=D.mammals.events.filter(e=>e.d>=cutoff&&(!sel||sel.has(e.g))&&(!q||e.s.toLowerCase().includes(q)||e.c.toLowerCase().includes(q)||e.sp.toLowerCase().includes(q)||e.g.toLowerCase().includes(q)||e.st.toLowerCase().includes(q)));
  const start=mmPage*PAGE_SIZE;const show=filtered.slice(start,start+PAGE_SIZE);
  document.getElementById('mmTblBody').innerHTML=show.map(e=>'<tr><td>'+fmtDate(e.d)+'</td><td>'+e.s+'</td><td>'+e.c+'</td><td>'+e.sp+'</td><td>'+e.g+'</td><td>'+e.st+'</td></tr>').join('');
  document.getElementById('mmTblSummary').textContent='Showing '+(start+1)+'-'+(start+show.length)+' of '+filtered.length.toLocaleString()+' detections';
  renderPager('mmPager',mmPage,filtered.length);
}

/* ── KPI update ── */
function updateKPIs(){
  const cut=cutoffISO(kpiRange);
  const labels={'30d':'Last 30 Days','3m':'Last 3 Months','6m':'Last 6 Months','1y':'Last 12 Months','ytd':'Year to Date','all':'All Time'};
  document.getElementById('kpiPeriodLabel').textContent=labels[kpiRange]||kpiRange;
  const layerBirds=D.events.filter(e=>e.d>=cut&&e.p==='Commercial Table Egg Layer').reduce((s,e)=>s+e.f,0);
  document.getElementById('kpiLayers').textContent=fmtBirds(layerBirds);
  const pSites=D.events.filter(e=>e.d>=cut).length;
  document.getElementById('kpiSites').textContent=pSites.toLocaleString();
  const wbDet=D.wild_birds?D.wild_birds.events.filter(e=>e.d>=cut).length:0;
  document.getElementById('kpiWB').textContent=wbDet.toLocaleString();
  /* M/M change always uses 30-day windows */
  const now=new Date();
  const d30=new Date(now.getFullYear(),now.getMonth(),now.getDate()-30);
  const d60=new Date(now.getFullYear(),now.getMonth(),now.getDate()-60);
  function isoD(d){return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');}
  const cut30=isoD(d30),cut60=isoD(d60);
  const curP=D.events.filter(e=>e.d>=cut30).length;
  const curW=D.wild_birds?D.wild_birds.events.filter(e=>e.d>=cut30).length:0;
  const cur=curP+curW;
  const prevP=D.events.filter(e=>e.d>=cut60&&e.d<cut30).length;
  const prevW=D.wild_birds?D.wild_birds.events.filter(e=>e.d>=cut60&&e.d<cut30).length:0;
  const prev=prevP+prevW;
  const chgEl=document.getElementById('kpiChg');
  if(prev>0){const pct=((cur-prev)/prev*100).toFixed(1);chgEl.textContent=(pct>0?'+':'')+pct+'%';chgEl.className='val '+(pct>0?'up':pct<0?'dn':'');}
  else{chgEl.textContent=cur>0?'New':'0';}
  /* Egg price: average over selected period */
  if(D.caged_prices&&D.caged_prices.length>0&&D.egg_dates_iso){
    const priceDates=D.egg_dates_iso;
    const prices=D.caged_prices;
    const inRange=[];
    for(let i=0;i<priceDates.length;i++){if(priceDates[i]>=cut&&prices[i]!=null)inRange.push(prices[i]);}
    if(inRange.length>0){const avg=inRange.reduce((a,b)=>a+b,0)/inRange.length;document.getElementById('kpiPrice').textContent='$'+avg.toFixed(2);}
    else{document.getElementById('kpiPrice').textContent='N/A';}
  }else{document.getElementById('kpiPrice').textContent='N/A';}
}

/* ── Boot: fetch data and initialize ── */
async function boot(){
  try{
    const resp=await fetch('data.json?v='+Date.now());
    if(!resp.ok)throw new Error('HTTP '+resp.status);
    D=await resp.json();
  }catch(e){
    if(location.protocol==='file:')showError('Cannot load data.json via file:// protocol.<br>Run: <code>python3 -m http.server 8000</code> in the output folder, then open localhost:8000');
    else showError(e.message);
    return;
  }
  hideLoading();
  initDashboard();
}

function initDashboard(){
  /* Mammal panel maps */
  if(D.mammals){panelMap.mmTbl='mmTblMSPanel';btnMap.mmTbl='mmTblMSBtn';}

  /* Create charts */
  eggChart=new Chart(document.getElementById('cEgg'),{type:'line',
    data:{labels:D.egg_dates,datasets:[{label:'Caged Large',data:D.caged_prices,borderColor:'#F6851F',backgroundColor:'rgba(246,133,31,.08)',fill:true,tension:.2,pointRadius:0,pointHitRadius:8,borderWidth:3}]},
    options:{responsive:true,aspectRatio:2.5,plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>c.parsed.y!=null?'$'+c.parsed.y.toFixed(2)+'/dz':''}}},scales:{x:{ticks:{maxTicksLimit:12,maxRotation:0},grid:{display:false}},y:{title:{display:true,text:'$ / Dozen'},grid:{color:'#f1f5f9'},ticks:{callback:v=>'$'+v.toFixed(2)}}}}});

  birdsChart=new Chart(document.getElementById('cBirds'),{type:'bar',
    data:{labels:D.month_labels,datasets:[{label:'Commercial Table Egg Layer',data:D.months.map(m=>(D.birds_by_month[m]||{})['Commercial Table Egg Layer']||0),backgroundColor:D.category_colors['Commercial Table Egg Layer']||'#dc2626'}]},
    options:{responsive:true,aspectRatio:2.5,plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>{const v=c.parsed.y;return c.dataset.label+': '+(v>=1e6?(v/1e6).toFixed(2)+'M':v>=1e3?(v/1e3).toFixed(0)+'K':v)+' birds';}}}},scales:{x:{stacked:true,ticks:{maxRotation:45},grid:{display:false}},y:{stacked:true,title:{display:true,text:'Total Birds'},grid:{color:'#f1f5f9'},ticks:{callback:fmtBirds}}}}});

  infChart=new Chart(document.getElementById('cInf'),{type:'bar',
    data:{labels:D.month_labels,datasets:D.production_types.map(p=>({label:p,data:D.months.map(m=>(D.infections_by_month[m]||{})[p]||0),backgroundColor:D.category_colors[p]||'#939598'}))},
    options:{responsive:true,aspectRatio:2.5,plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>c.dataset.label+': '+c.parsed.y+' sites'}}},scales:{x:{stacked:true,ticks:{maxRotation:45},grid:{display:false}},y:{stacked:true,title:{display:true,text:'Number of Sites'},grid:{color:'#f1f5f9'}}}}});

  /* Heatmap */
  initMap();

  /* Tab switching */
  const tabBtns=document.querySelectorAll('.tab-btn');
  const tabPanes=document.querySelectorAll('.tab-content');
  const tabInitialized={};
  tabBtns.forEach(btn=>{btn.addEventListener('click',()=>{
    tabBtns.forEach(b=>b.classList.remove('active'));tabPanes.forEach(p=>p.classList.remove('active'));
    btn.classList.add('active');const t=btn.dataset.tab;document.getElementById('tab-'+t).classList.add('active');
    if(!tabInitialized[t]){initTab(t);tabInitialized[t]=true;}
    if(t==='poultry'){birdsChart.resize();infChart.resize();eggChart.resize();}
    if(t==='livestock'&&lsChart)lsChart.resize();
    if(t==='wildbirds'&&wbChart)wbChart.resize();
    if(t==='mammals'&&mmChart)mmChart.resize();
    if(window.parent!==window){setTimeout(()=>{window.parent.postMessage({type:'resize',height:document.documentElement.scrollHeight},'*');},50);}
  });});

  /* Multi-select panel wiring */
  document.querySelectorAll('.ms-btn').forEach(btn=>{btn.addEventListener('click',e=>{e.stopPropagation();const panel=btn.nextElementSibling;document.querySelectorAll('.ms-panel.open').forEach(p=>{if(p!==panel)p.classList.remove('open');});panel.classList.toggle('open');});});
  document.addEventListener('click',()=>{document.querySelectorAll('.ms-panel.open').forEach(p=>p.classList.remove('open'));});
  document.querySelectorAll('.ms-panel').forEach(p=>p.addEventListener('click',e=>e.stopPropagation()));
  document.querySelectorAll('input[data-group]').forEach(hdr=>{hdr.addEventListener('change',()=>{const children=hdr.closest('.ms-group').querySelectorAll('.ms-group-children input[type=checkbox]');children.forEach(cb=>cb.checked=hdr.checked);const chart=panelToChart(hdr.closest('.ms-panel'));chartUpdate(chart);updateMSLabel(chart);});});
  document.querySelectorAll('.ms-group-children input[type=checkbox]').forEach(cb=>{cb.addEventListener('change',()=>{const hdr=cb.closest('.ms-group').querySelector('input[data-group]');syncGroupHdr(hdr);const chart=panelToChart(cb.closest('.ms-panel'));chartUpdate(chart);updateMSLabel(chart);});});
  document.querySelectorAll('input[data-group]').forEach(hdr=>syncGroupHdr(hdr));

  /* Range button wiring */
  document.querySelectorAll('.range-row').forEach(row=>{const chart=row.dataset.chart;row.querySelectorAll('.rbtn').forEach(btn=>{btn.addEventListener('click',()=>{
    row.querySelectorAll('.rbtn').forEach(b=>b.classList.remove('active'));btn.classList.add('active');const r=btn.dataset.r;
    if(chart==='map'){mapRange=r;updateMapColors();}
    if(chart==='source'){sourceFilter=r;updateMapColors();}
    if(chart==='egg'){eggRange=r;updateEgg();}
    if(chart==='birds'){birdsRange=r;updateBirds();}
    if(chart==='inf'){infRange=r;updateInf();}
    if(chart==='tbl'){tblRange=r;updateTable();}
    if(chart==='ls'){lsRange=r;updateLivestock();}
    if(chart==='wb'){wbRange=r;updateWildBirds();}
    if(chart==='mm'){mmRange=r;updateMammals();}
    if(chart==='kpi'){kpiRange=r;updateKPIs();}
    if(chart==='mmTbl'){mmTblRange=r;updateMmTable();}
  });});});

  /* Initial data population */
  updateTable();
  updateKPIs();

  /* MS label init */
  updateMSLabel('birds');updateMSLabel('inf');updateMSLabel('tbl');
  if(D.mammals)updateMSLabel('mmTbl');

  /* Iframe height reporter */
  if(window.parent!==window){
    document.documentElement.style.overflow='hidden';
    function reportHeight(){document.documentElement.style.height='auto';window.parent.postMessage({type:'resize',height:document.documentElement.scrollHeight},'*');}
    reportHeight();window.addEventListener('resize',reportHeight);
    new ResizeObserver(reportHeight).observe(document.body);
  }
}

boot();
</script>
</body>
</html>"""


# ── HTML generation ─────────────────────────────────────────────────────────

def _fmt_big(n):
    if n >= 1e6:
        return f"{n/1e6:,.1f}M"
    if n >= 1e3:
        return f"{n:,.0f}"
    return str(n)


COMMERCIAL_TYPES = {
    "Commercial Table Egg Layer", "Commercial Table Egg Pullets",
    "Commercial Broiler Production", "Commercial Broiler Breeder",
    "Commercial Turkey Meat Bird", "Commercial Turkey Breeder Hens",
    "Commercial Duck Meat Bird", "Commercial Duck Breeder",
    "Commercial Upland Gamebird Producer",
    "Commercial Raised for Release Upland Game Bird",
    "Commercial Raised for Release Waterfowl",
    "Commercial Breeder Operation",
    "Commercial Breeder (Multiple Bird Species)",
    "Commercial Turkey Breeder Replacement Hens",
    "Commercial Turkey Breeder Toms",
    "Commercial Turkey Poult Supplier",
    "Commercial Table Egg Breeder",
    "Commercial Broiler Breeder Pullets",
    "Primary Broiler Breeder Pedigree Farm",
}


def _build_grouped_checkboxes(prod_types, colors, default_checked):
    """Build grouped checkbox HTML: Commercial vs Backyard."""
    commercial = [p for p in prod_types if p in COMMERCIAL_TYPES]
    backyard = [p for p in prod_types if p not in COMMERCIAL_TYPES]

    def _group(group_label, members):
        # Are all members checked?
        all_chk = all(m in default_checked for m in members)
        some_chk = any(m in default_checked for m in members)
        hdr_chk = "checked" if all_chk else ""
        lines = [
            f'<div class="ms-group">',
            f'  <label class="ms-group-hdr"><input type="checkbox" data-group="1" {hdr_chk}>{group_label}</label>',
            f'  <div class="ms-group-children">',
        ]
        for p in members:
            chk = "checked" if p in default_checked else ""
            color = colors.get(p, "#6b7280")
            # Strip "Commercial " prefix for cleaner display
            display = p.replace("Commercial ", "") if p.startswith("Commercial ") else p
            lines.append(
                f'    <label class="ms-item"><input type="checkbox" value="{p}" {chk}>'
                f'<span class="ms-dot" style="background:{color}"></span>{display}</label>'
            )
        lines.append('  </div>')
        lines.append('</div>')
        return "\n        ".join(lines)

    parts = []
    if commercial:
        parts.append(_group("Commercial", commercial))
    if backyard:
        parts.append(_group("Backyard", backyard))
    return "\n        ".join(parts)


def _build_simple_checkboxes(groups, colors, default_checked):
    """Build flat checkbox list for mammal groups."""
    lines = []
    for g in groups:
        chk = "checked" if g in default_checked else ""
        color = colors.get(g, "#6b7280")
        lines.append(
            f'<label class="ms-item"><input type="checkbox" value="{g}" {chk}>'
            f'<span class="ms-dot" style="background:{color}"></span>{g}</label>'
        )
    return "\n        ".join(lines)


_RANGE_BUTTONS = """<div class="range-row" data-chart="{chart}">
      <button class="rbtn" data-r="30d">30D</button>
      <button class="rbtn" data-r="3m">3M</button>
      <button class="rbtn" data-r="6m">6M</button>
      <button class="rbtn" data-r="1y">1Y</button>
      <button class="rbtn" data-r="ytd">YTD</button>
      <button class="rbtn active" data-r="all">All</button>
    </div>"""


def generate_html(data):
    # Birds checkboxes: default only "Commercial Table Egg Layer"
    birds_cbs = _build_grouped_checkboxes(
        data["production_types"], data["category_colors"],
        {"Commercial Table Egg Layer"},
    )
    # Infections checkboxes: default ALL selected
    inf_cbs = _build_grouped_checkboxes(
        data["production_types"], data["category_colors"],
        set(data["production_types"]),
    )
    # Table checkboxes: default ALL selected
    tbl_cbs = _build_grouped_checkboxes(
        data["production_types"], data["category_colors"],
        set(data["production_types"]),
    )

    html = HTML_TEMPLATE
    html = html.replace("__UPDATED__", data["updated"])
    html = html.replace("__BIRDS_CHECKBOXES__", birds_cbs)
    html = html.replace("__INF_CHECKBOXES__", inf_cbs)
    html = html.replace("__TBL_CHECKBOXES__", tbl_cbs)

    # ── Heatmap card ──
    if "map_data" in data:
        heatmap_card = '''<div class="card">
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
  <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/commercial-backyard-flocks" target="_blank">USDA APHIS</a></div>
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
        ls_html = f'''<div class="tab-content" id="tab-livestock">
  <div class="card">
    <h2>Livestock/Dairy Detections by Month</h2>
    <div class="sub">Confirmed HPAI-affected herds</div>
    <div class="controls">{_RANGE_BUTTONS.format(chart="ls")}</div>
    <canvas id="cLivestock"></canvas>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/hpai-confirmed-cases-livestock" target="_blank">USDA APHIS</a></div>
  </div>
  <div class="card">
    <h2>Livestock/Dairy Detection Details</h2>
    <div class="sub">Individual confirmed herd detections</div>
    <input type="text" class="tbl-search" id="lsSearch" placeholder="Filter by state, production, species..." oninput="updateLsTable()">
    <div class="tbl-wrap">
      <table>
        <thead><tr><th>Date</th><th>State</th><th>Special Id</th><th>Production</th><th>Species</th></tr></thead>
        <tbody id="lsTblBody"></tbody>
      </table>
    </div>
    <div class="tbl-summary" id="lsTblSummary"></div>
    <div class="tbl-pager" id="lsPager"><button onclick="pageTable('ls',-1)">← Prev</button><span id="lsPageInfo"></span><button onclick="pageTable('ls',1)">Next →</button></div>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/hpai-confirmed-cases-livestock" target="_blank">USDA APHIS</a></div>
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
    <div class="controls">{_RANGE_BUTTONS.format(chart="wb")}</div>
    <canvas id="cWildBirds"></canvas>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/wild-birds?page=1" target="_blank">USDA APHIS</a></div>
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
    <div class="tbl-pager" id="wbPager"><button onclick="pageTable('wb',-1)">← Prev</button><span id="wbPageInfo"></span><button onclick="pageTable('wb',1)">Next →</button></div>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/wild-birds?page=1" target="_blank">USDA APHIS</a></div>
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
        mm_cbs = _build_simple_checkboxes(mm_groups, mm_colors, set(mm_groups))
        mm_html = f'''<div class="tab-content" id="tab-mammals">
  <div class="card">
    <h2>Mammal HPAI Detections by Month</h2>
    <div class="sub">Confirmed detections by species group</div>
    <div class="controls">{_RANGE_BUTTONS.format(chart="mm")}</div>
    <canvas id="cMammals"></canvas>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/mammals" target="_blank">USDA APHIS</a></div>
  </div>
  <div class="card">
    <h2>Mammal Detection Details</h2>
    <div class="sub">Individual confirmed detections</div>
    <div class="controls">
      {_RANGE_BUTTONS.format(chart="mmTbl")}
      <div class="ms-wrap" id="mmTblMS">
        <button class="ms-btn" id="mmTblMSBtn">All categories</button>
        <div class="ms-panel" id="mmTblMSPanel">
          <div class="ms-actions">
            <a onclick="msAll('mmTbl')">Select All</a> · <a onclick="msNone('mmTbl')">Clear</a>
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
    <div class="tbl-pager" id="mmPager"><button onclick="pageTable('mm',-1)">← Prev</button><span id="mmPageInfo"></span><button onclick="pageTable('mm',1)">Next →</button></div>
    <div class="card-source">Chart: Innovate Animal Ag · Source: <a href="https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/mammals" target="_blank">USDA APHIS</a></div>
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
    ap.add_argument("csv", help="Path to APHIS 'A Table by Confirmation Date' CSV")
    ap.add_argument("-o", "--output", default="dashboard/index.html", help="Output HTML path")
    ap.add_argument("--egg-start", default=None,
                    help="Start date for egg prices (YYYY-MM-DD). Default: 1 year ago")
    ap.add_argument("--no-prices", action="store_true", help="Skip egg price fetch")
    ap.add_argument("--livestock", default=None,
                    help="Path to 'Table Details by Date' CSV (livestock/dairy)")
    ap.add_argument("--mammals", default=None,
                    help="Path to 'HPAI Detections in Mammals' CSV")
    ap.add_argument("--wild-birds", default=None,
                    help="Path to 'HPAI Detections in Wild Birds' CSV")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        sys.exit(f"ERROR: File not found: {csv_path}")

    # 1. Parse HPAI CSV
    print(f"Parsing HPAI data: {csv_path}")
    events = parse_hpai_csv(str(csv_path))
    print(f"  {len(events)} flock detections loaded")

    # 2. Parse optional CSVs
    livestock = mammals = wild_birds = None
    if args.livestock:
        lp = Path(args.livestock)
        if lp.exists():
            print(f"Parsing livestock data: {lp}")
            livestock = parse_livestock_csv(str(lp))
            print(f"  {len(livestock)} herd detections loaded")

    if args.mammals:
        mp = Path(args.mammals)
        if mp.exists():
            print(f"Parsing mammal data: {mp}")
            mammals = parse_mammals_csv(str(mp))
            print(f"  {len(mammals)} mammal detections loaded")

    if args.wild_birds:
        wp = Path(args.wild_birds)
        if wp.exists():
            print(f"Parsing wild bird data: {wp}")
            wild_birds = parse_wild_birds_csv(str(wp))
            print(f"  {len(wild_birds)} wild bird detections loaded")

    # 3. Fetch egg prices
    caged_prices = {}
    if not args.no_prices:
        today = datetime.today()
        start = datetime.strptime(args.egg_start, "%Y-%m-%d") if args.egg_start else (today - timedelta(days=365))
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
    data = build_data(events, caged_prices, livestock=livestock, mammals=mammals, wild_birds=wild_birds)
    if map_compressed:
        data["map_data"] = map_compressed
        data["unknown_by_month"] = unk_compressed
    html, data_json = generate_html(data)

    # 5. Write output
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
