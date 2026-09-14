#!/usr/bin/env python3
"""
tableau_export.py — Export APHIS Tableau detail tables as UTF-16 crosstab CSVs.

The poultry and livestock detail tables are not reachable over plain HTTP: the
Tableau crosstab export only works inside a live vizql session, which the server
creates when a browser actually boots the dashboard. So we drive a headless
Chromium through the same "Download Data" dialog a person would use.

Each export is validated before it replaces the file on disk, so a failed or
truncated download never clobbers a known-good local copy.

Usage:
    python3 tableau_export.py                  # export both datasets
    python3 tableau_export.py --only poultry   # just one
    python3 tableau_export.py -o data/         # write elsewhere
    python3 tableau_export.py --headed         # watch it run (debugging)
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path

TABLEAU_BASE = "https://publicdashboards.dl.usda.gov/t/MRP_PUB/views"

EXPORTS = {
    "poultry": {
        "filename": "A Table by Confirmation Date.csv",
        "workbook": "VS_Avian_HPAIConfirmedDetections2022",
        "view": "HPAI2022ConfirmedDetections",
        "sheet": "X - Table by Confirmation Date",
        "required_headers": ("confirmed diagnosis", "state", "production"),
        "min_rows": 2000,
    },
    "livestock": {
        "filename": "Table Details by Date.csv",
        "workbook": "VS_Cattle_HPAIConfirmedDetections2024",
        "view": "HPAI2022ConfirmedDetections",
        "sheet": "Table Details by Date",
        "required_headers": ("confirmed diagnosis", "state", "production"),
        "min_rows": 1000,
    },
}


def _validate(path, required_headers, min_rows):
    """Check a downloaded crosstab looks like the real detail table.

    Returns (ok, message, row_count).
    """
    raw = open(path, "rb").read(2)
    if raw not in (b"\xff\xfe", b"\xfe\xff"):
        return False, f"not a UTF-16 crosstab (BOM={raw!r})", 0

    try:
        text = open(path, encoding="utf-16").read()
    except (UnicodeError, ValueError) as e:
        return False, f"undecodable as UTF-16: {e}", 0

    lines = [l for l in text.replace("\r\n", "\n").split("\n") if l.strip()]

    # Tableau emits a group-header row above the real header; find the header
    # row the same way parsers.py does.
    hdr_idx = None
    for i, line in enumerate(lines[:5]):
        low = line.lower()
        if all(h in low for h in required_headers):
            hdr_idx = i
            break
    if hdr_idx is None:
        return False, f"header row not found (need {', '.join(required_headers)})", 0

    rows = len(lines) - hdr_idx - 1
    if rows < min_rows:
        return False, f"only {rows} data rows, expected >= {min_rows}", rows
    return True, "ok", rows


def export_one(key, out_dir, headed=False, timeout_ms=90_000):
    """Export one dataset. Returns (ok, message)."""
    from playwright.sync_api import TimeoutError as PWTimeout
    from playwright.sync_api import sync_playwright

    cfg = EXPORTS[key]
    out_path = Path(out_dir) / cfg["filename"]
    url = (f"{TABLEAU_BASE}/{cfg['workbook']}/{cfg['view']}"
           f"?:embed=y&:isGuestRedirectFromVizportal=y&:refresh=yes")

    print(f"  {key}: {cfg['filename']}")
    print(f"    sheet: {cfg['sheet']}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headed)
        ctx = browser.new_context(accept_downloads=True,
                                  viewport={"width": 1600, "height": 1200})
        page = ctx.new_page()
        try:
            page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")

            # The viz boots asynchronously; the export button only exists once
            # the dashboard has rendered.
            btn = page.get_by_role("button", name="Download Crosstab")
            btn.wait_for(state="visible", timeout=timeout_ms)
            btn.click()

            dialog = page.get_by_role("dialog", name="Download Crosstab")
            dialog.wait_for(state="visible", timeout=timeout_ms)

            # The sheet picker scrolls, and its thumbnails lazy-load, so the
            # list never settles long enough to pass Playwright's stability
            # check. Scroll the target in ourselves and force the clicks.
            opt = page.get_by_role("option", name=cfg["sheet"], exact=True)
            opt.scroll_into_view_if_needed(timeout=timeout_ms)
            opt.click(force=True, timeout=timeout_ms)
            if opt.get_attribute("aria-selected") != "true":
                raise RuntimeError(f"sheet {cfg['sheet']!r} did not select")

            page.get_by_role("radio", name="csv").click(force=True, timeout=timeout_ms)

            with page.expect_download(timeout=timeout_ms) as dl:
                dialog.get_by_role("button", name="Download").click(
                    force=True, timeout=timeout_ms)
            download = dl.value

            tmp = Path(tempfile.mkstemp(suffix=".csv")[1])
            download.save_as(tmp)
        except PWTimeout as e:
            browser.close()
            return False, f"timed out driving the dashboard: {str(e).splitlines()[0]}"
        except Exception as e:
            browser.close()
            return False, f"{type(e).__name__}: {e}"
        finally:
            try:
                browser.close()
            except Exception:
                pass

    ok, msg, rows = _validate(tmp, cfg["required_headers"], cfg["min_rows"])
    size_kb = tmp.stat().st_size / 1024
    if not ok:
        tmp.unlink(missing_ok=True)
        return False, f"downloaded file rejected — {msg}"

    # Only now replace the good local copy.
    prev = f"{out_path.stat().st_size / 1024:.0f} KB" if out_path.exists() else "none"
    os.replace(tmp, out_path)
    print(f"    OK — {rows} rows, {size_kb:.0f} KB (was {prev})")
    return True, f"{rows} rows"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--output-dir", default=".", help="Where to write the CSVs")
    ap.add_argument("--only", choices=sorted(EXPORTS), help="Export just one dataset")
    ap.add_argument("--headed", action="store_true", help="Show the browser window")
    args = ap.parse_args()

    keys = [args.only] if args.only else list(EXPORTS)
    print("Exporting APHIS Tableau detail tables...\n")

    failures = []
    for key in keys:
        ok, msg = export_one(key, args.output_dir, headed=args.headed)
        if not ok:
            print(f"    FAILED — {msg}")
            print(f"    (keeping existing local file)")
            failures.append(key)
        print()

    if failures:
        print(f"{len(failures)} of {len(keys)} export(s) failed: {', '.join(failures)}")
        return 1
    print(f"All {len(keys)} export(s) succeeded.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
