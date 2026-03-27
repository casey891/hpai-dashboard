"""
geo.py — FIPS county lookup, detection aggregation, and map data compression.
"""

import re
from collections import defaultdict

try:
    import addfips
    HAS_ADDFIPS = True
except ImportError:
    HAS_ADDFIPS = False


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
                "wild_birds_by_day": defaultdict(int),
                "poultry_by_day": defaultdict(int),
                "poultry_by_month_prod": defaultdict(lambda: defaultdict(lambda: {"count": 0, "birds": 0})),
                "poultry_by_day_prod": defaultdict(lambda: defaultdict(lambda: {"count": 0, "birds": 0})),
            }

        entry = county_data[fips]
        entry[source] = entry.get(source, 0) + 1
        entry["total"] += 1
        iso = date.strftime("%Y-%m-%d")
        entry["dated_sources"].append((iso, source))
        if entry["latest_date"] is None or iso > entry["latest_date"]:
            entry["latest_date"] = iso
        if source == "wild_birds":
            entry["wild_birds_by_day"][iso] += 1
        if source == "poultry":
            month = iso[:7]
            entry["poultry_by_day"][iso] += 1
            production = event.get("production") or "Unknown / Unspecified"
            flock = int(event.get("flock") or 0)
            prod_entry = entry["poultry_by_month_prod"][month][production]
            prod_entry["count"] += 1
            prod_entry["birds"] += flock
            day_prod_entry = entry["poultry_by_day_prod"][iso][production]
            day_prod_entry["count"] += 1
            day_prod_entry["birds"] += flock

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
        day_wb = dict(info.get("wild_birds_by_day", {}))
        day_p = dict(info.get("poultry_by_day", {}))
        poultry_breakdown = {}
        poultry_breakdown_by_day = {}
        for date_str, source in info["dated_sources"]:
            ym = date_str[:7]
            if source == "wild_birds":
                mo_wb[ym] += 1
            elif source == "poultry":
                mo_p[ym] += 1
        for month, prod_map in info.get("poultry_by_month_prod", {}).items():
            poultry_breakdown[month] = {
                production: [vals["count"], vals["birds"]]
                for production, vals in prod_map.items()
                if vals["count"] or vals["birds"]
            }
        for day, prod_map in info.get("poultry_by_day_prod", {}).items():
            poultry_breakdown_by_day[day] = {
                production: [vals["count"], vals["birds"]]
                for production, vals in prod_map.items()
                if vals["count"] or vals["birds"]
            }
        result[fips] = {
            "s": info["state"],
            "c": info["county"],
            "wb": info["wild_birds"],
            "p": info["poultry"],
            "t": info["total"],
            "ld": info["latest_date"],
            "dwb": day_wb,
            "dp": day_p,
            "dpp": poultry_breakdown_by_day,
            "mwb": dict(mo_wb),
            "mp": dict(mo_p),
            "mpp": poultry_breakdown,
        }
    return result
