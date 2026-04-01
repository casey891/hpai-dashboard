#!/usr/bin/env python3
"""
download_data.py — Download 3 of 4 USDA APHIS HPAI datasets.

- Wild Birds & Mammals: direct CSV from APHIS static files
- Flocks: Tableau Server .csv endpoint (flat CSV format).
  NOTE: The Tableau CSV endpoint often lags 1-2 days behind the actual
  dashboard. If a manually-downloaded file already exists (UTF-16 crosstab
  from the Tableau "Download Data" dialog), it is kept as-is.
- Livestock: NOT automatable (Tableau view doesn't expose data table).
  Download manually via Claude in Chrome or the APHIS website.

Usage:
    python download_data.py                # download all 3 to current directory
    python download_data.py --output-dir . # specify output directory
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import requests

# ── Download sources ─────────────────────────────────────────────────────────

DOWNLOADS = {
    "HPAI Detections in Wild Birds.csv": {
        "url": "https://www.aphis.usda.gov/sites/default/files/hpai-wild-birds.csv",
        "type": "direct",
    },
    "HPAI Detections in Mammals.csv": {
        "url": "https://www.aphis.usda.gov/sites/default/files/hpai-mammals.csv",
        "type": "direct",
    },
    "A Table by Confirmation Date.csv": {
        "url": "https://publicdashboards.dl.usda.gov/t/MRP_PUB/views/VS_Avian_HPAIConfirmedDetections2022/HPAI2022ConfirmedDetections.csv",
        "type": "tableau_csv",
        "validate": lambda text: "Confirmed" in text.split("\n")[0] and "State" in text.split("\n")[0],
        "prefer_local": True,
    },
}


def download_one(name, config, output_dir):
    """Download a single dataset. Returns True on success, False on failure."""
    url = config["url"]
    out_path = output_dir / name
    print(f"  {name}")

    # If prefer_local is set and a local file exists, keep it
    if config.get("prefer_local") and out_path.exists():
        size_kb = out_path.stat().st_size / 1024
        print(f"    SKIPPED — local file exists ({size_kb:.0f} KB)")
        print(f"    (Tableau CSV endpoint lags behind manual downloads; keeping local copy)")
        return True

    print(f"    URL: {url}")

    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"    FAILED — {type(e).__name__}: {e}")
        return False

    # Validate Tableau CSV responses (they return 200 even for wrong views)
    validator = config.get("validate")
    if validator and not validator(resp.text):
        print(f"    FAILED — Response doesn't contain expected data")
        print(f"    Got: {resp.text[:100]!r}")
        return False

    out_path.write_bytes(resp.content)

    # Count rows for reporting
    lines = resp.text.strip().split("\n")
    rows = len(lines) - 1
    size_kb = len(resp.content) / 1024
    print(f"    OK — {rows} rows, {size_kb:.0f} KB")
    return True


def main():
    ap = argparse.ArgumentParser(description="Download USDA APHIS HPAI datasets")
    ap.add_argument("-o", "--output-dir", default=".",
                    help="Directory to save CSVs (default: current directory)")
    args = ap.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"USDA HPAI Data Downloader — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Output directory: {output_dir.resolve()}\n")

    results = {}
    for name, config in DOWNLOADS.items():
        results[name] = download_one(name, config, output_dir)
        print()

    # Summary
    print("── Summary ──")
    for name, ok in results.items():
        status = "OK" if ok else "FAILED"
        print(f"  [{status:>6}] {name}")

    failed = [n for n, ok in results.items() if not ok]
    if failed:
        print(f"\n{len(failed)} download(s) failed.")
        print("For Tableau failures, download manually or use Claude in Chrome.")
        return 1

    print("\nAll downloads successful!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
