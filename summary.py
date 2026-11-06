"""
summary.py — a small headline-numbers file (summary.json) next to data.json.

innovateanimalag.org/hpai-dashboard frames this dashboard. Search engines see
little of a framed page, so that page also writes the latest headline numbers
as plain text, read from this file. It's a few hundred bytes, so the site page
doesn't have to download the full data.json (about 350 KB compressed) that the
dashboard itself loads.

    python3 summary.py            # rebuild summary.json from the current data.json
"""

import json
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path


def _iso(value):
    return value if isinstance(value, str) else value.strftime("%Y-%m-%d")


def build_summary(data, today=None):
    """Headline numbers from a data.json-shaped dict (see build_dashboard.build_data)."""
    today = today or date.today()
    cutoff = (today - timedelta(days=30)).isoformat()
    year = str(today.year)
    summary = {"updated": today.isoformat()}

    events = data.get("events") or []
    if events:
        recent = [e for e in events if e["d"] > cutoff]
        by_state = defaultdict(lambda: {"flocks": 0, "birds": 0})
        for e in recent:
            by_state[e["s"]]["flocks"] += 1
            by_state[e["s"]]["birds"] += e.get("f") or 0
        summary["poultry"] = {
            "since": min(e["d"] for e in events),
            "latest": max(e["d"] for e in events),
            "flocks": len(events),
            "birds": sum(e.get("f") or 0 for e in events),
            "year": int(year),
            "flocks_year": sum(1 for e in events if e["d"].startswith(year)),
            "birds_year": sum(e.get("f") or 0 for e in events if e["d"].startswith(year)),
            "flocks_30d": len(recent),
            "birds_30d": sum(e.get("f") or 0 for e in recent),
            # Most birds first, so the page can name the hardest-hit states.
            "states_30d": [
                {"state": s, **v}
                for s, v in sorted(by_state.items(), key=lambda kv: (-kv[1]["birds"], kv[0]))
            ],
        }

    livestock = (data.get("livestock") or {}).get("events") or []
    if livestock:
        recent = [e for e in livestock if e["d"] > cutoff]
        summary["dairy"] = {
            "since": min(e["d"] for e in livestock),
            "latest": max(e["d"] for e in livestock),
            "herds": len(livestock),
            "herds_year": sum(1 for e in livestock if e["d"].startswith(year)),
            "herds_30d": len(recent),
            "states": len({e["s"] for e in livestock}),
            "states_30d": [s for s, _ in Counter(e["s"] for e in recent).most_common()],
        }

    wild = (data.get("wild_birds") or {}).get("events") or []
    if wild:
        summary["wild_birds"] = {
            "since": min(e["d"] for e in wild),
            "latest": max(e["d"] for e in wild),
            "detections": len(wild),
            "detections_30d": sum(1 for e in wild if e["d"] > cutoff),
        }

    mammals = (data.get("mammals") or {}).get("events") or []
    if mammals:
        summary["mammals"] = {
            "latest": max(e["d"] for e in mammals),
            "detections": len(mammals),
        }

    prices, dates = data.get("caged_prices") or [], data.get("egg_dates_iso") or []
    if prices and dates:
        summary["egg_price"] = {"date": _iso(dates[-1]), "dollars_per_dozen": prices[-1]}

    return summary


def write_summary(data, out_dir, today=None):
    path = Path(out_dir) / "summary.json"
    path.write_text(json.dumps(build_summary(data, today), indent=1) + "\n")
    return path


if __name__ == "__main__":
    here = Path(__file__).parent
    data = json.loads((here / "data.json").read_text())
    # data.json's "updated" is the build date, e.g. "November 06, 2026".
    built = datetime.strptime(data["updated"], "%B %d, %Y").date()
    print(f"Wrote {write_summary(data, here, built)}")
