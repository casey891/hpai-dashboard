"""
parsers.py — CSV parsers, species classifiers, and MARS API egg price fetcher.
"""

import base64
import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


# ── Shared date parser ─────────────────────────────────────────────────────

def _parse_date(s):
    """Try multiple date formats and return a datetime, or None."""
    for fmt in ("%d-%b-%y", "%m/%d/%Y", "%Y-%m-%d", "%m/%d/%Y %I:%M:%S %p"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


# ── MARS API config ────────────────────────────────────────────────────────

MARS_BASE = "https://marsapi.ams.usda.gov/services/v1.2/reports"
MARS_REPORT = "2843"
MARS_KEY = os.environ.get("MARS_API_KEY", "")


def _mars_get(url):
    req = Request(url)
    req.add_header("Authorization", "Basic " + base64.b64encode(f"{MARS_KEY}:".encode()).decode())
    req.add_header("Accept", "application/json")
    with urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


def fetch_egg_prices(start, end):
    """Fetch daily VWAP for Caged Large (National) from MARS API. Returns dict date->$/dz."""
    if not MARS_KEY:
        print("  WARNING: MARS_API_KEY not set, skipping egg prices")
        return {}
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


# ── HPAI CSV parser ────────────────────────────────────────────────────────

def _detect_csv_format(path):
    """Detect whether a CSV is UTF-16 tab-delimited (Tableau crosstab) or UTF-8 comma-delimited (Tableau .csv endpoint)."""
    raw = open(path, "rb").read(4)
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return "crosstab"
    return "flat"


def _parse_birds_affected(s):
    """Parse Birds Affected value, handling 'M' suffix (millions) from Tableau."""
    s = s.strip().replace(",", "")
    if not s:
        return None
    if s.upper().endswith("M"):
        try:
            return int(float(s[:-1]) * 1_000_000)
        except ValueError:
            return None
    if s.upper().endswith("K"):
        try:
            return int(float(s[:-1]) * 1_000)
        except ValueError:
            return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _parse_hpai_flat(path):
    """Parse flat CSV from Tableau .csv endpoint (UTF-8 comma-delimited)."""
    events = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            confirmed = (row.get("Confirmed Diagnosis") or row.get("Confirmed") or "").strip()
            if not confirmed:
                continue
            dt = _parse_date(confirmed)
            if dt is None:
                continue
            flock = _parse_birds_affected(row.get("Birds Affected", ""))
            if flock is None:
                continue
            events.append({
                "date": dt,
                "state": row.get("State", "").strip(),
                "county": row.get("County Name", "").strip(),
                "production": row.get("Production", "").strip(),
                "flock": flock,
            })
    return events


def _find_col(hdr_low, *candidates):
    """Return index of first header containing any candidate substring."""
    for c in candidates:
        for i, h in enumerate(hdr_low):
            if c in h:
                return i
    raise ValueError(f"Column not found: {candidates}")


def _parse_hpai_crosstab(path):
    """Parse crosstab CSV from Tableau 'Download Data' (UTF-16 tab-delimited)."""
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
    ci = _find_col(hdr_low, "confirmed diagnosis", "confirmed")
    si = _find_col(hdr_low, "state")
    try:
        cni = _find_col(hdr_low, "county name")
    except ValueError:
        cni = None
    pi = _find_col(hdr_low, "production")

    # Collect indices of all known named columns
    named_cols = {ci, si, pi}
    if cni is not None:
        named_cols.add(cni)

    # Detect optional columns added in the 2026 format refresh
    try:
        idi = _find_col(hdr_low, "special id")
        named_cols.add(idi)
    except ValueError:
        idi = None
    try:
        cri = _find_col(hdr_low, "control area released")
        named_cols.add(cri)
    except ValueError:
        cri = None

    # Data (bird-count) columns = everything not identified as a named column
    data_cols = [i for i in range(len(hdrs)) if i not in named_cols]

    events = []
    prev_conf = prev_st = prev_cn = None

    for line in lines[hdr_idx + 1:]:
        cols = line.split("\t")
        if not any(idx < len(cols) for idx in data_cols):
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
        for idx in data_cols:
            if idx < len(cols):
                v = cols[idx].strip().replace(",", "")
                if v:
                    try:
                        flock = int(float(v))
                        break
                    except ValueError:
                        continue
        if flock is None:
            continue
        dt = _parse_date(confirmed)
        if dt is None:
            continue
        car = cols[cri].strip() if cri is not None and cri < len(cols) else None
        events.append({"date": dt, "state": state, "county": county, "production": production, "flock": flock, "control_area_released": car})

    return events


def parse_hpai_csv(path):
    """Parse APHIS 'A Table by Confirmation Date' CSV.

    Supports both formats:
    - UTF-16 tab-delimited crosstab (from Tableau 'Download Data' dialog)
    - UTF-8 comma-delimited flat CSV (from Tableau .csv endpoint)
    """
    fmt = _detect_csv_format(path)
    if fmt == "flat":
        return _parse_hpai_flat(path)
    return _parse_hpai_crosstab(path)


# ── Livestock CSV parser ───────────────────────────────────────────────────

def parse_livestock_csv(path):
    """Parse APHIS 'Table Details by Date' CSV.

    Supports both formats:
    - UTF-16 tab-delimited crosstab (from Tableau 'Download Data' dialog)
    - UTF-8 comma-delimited flat CSV (from Tableau .csv endpoint)
    """
    fmt = _detect_csv_format(path)
    if fmt == "flat":
        return _parse_livestock_flat(path)
    return _parse_livestock_crosstab(path)


def _parse_livestock_flat(path):
    """Parse flat CSV from Tableau .csv endpoint (UTF-8 comma-delimited)."""
    events = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            confirmed = (row.get("Confirmed Diagnosis") or row.get("Confirmed") or "").strip()
            if not confirmed:
                continue
            dt = _parse_date(confirmed)
            if dt is None:
                continue
            state = row.get("State", "").strip()
            if not state:
                continue
            events.append({
                "date": dt,
                "state": state,
                "special_id": row.get("Special Id", "").strip(),
                "production": row.get("Production", "").strip(),
                "species": row.get("Species", "").strip(),
            })
    return events


def _parse_livestock_crosstab(path):
    """Parse crosstab CSV from Tableau 'Download Data' (UTF-16 tab-delimited)."""
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
    ci = _find_col(hdr_low, "confirmed diagnosis", "confirmed")
    si = _find_col(hdr_low, "state")
    idi = _find_col(hdr_low, "special id")
    pi = _find_col(hdr_low, "production")
    spi = _find_col(hdr_low, "species")

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
        dt = _parse_date(confirmed)
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
    events = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_str = row.get("Date Collected", "").strip()
            if not date_str:
                continue
            dt = _parse_date(date_str)
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
    events = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_str = row.get("Collection Date", "").strip()
            if not date_str:
                continue
            dt = _parse_date(date_str)
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
