# HPAI Impact Dashboard

A self-contained interactive dashboard for tracking Highly Pathogenic Avian Influenza (HPAI) detections across the United States. Built from USDA APHIS CSV data and MARS API egg prices, it generates a static HTML dashboard with Chart.js visualizations and a D3.js county-level heatmap.

**Live dashboard:** Hosted via GitHub Pages or any static file server.

## Features

- **KPI summary cards** with configurable time windows (30D / 3M / 6M / 1Y / YTD / All)
- **Poultry detections** — birds impacted and confirmed site counts by month/day, with Combined and By Category views
- **Wholesale egg prices** — daily volume-weighted average for caged large eggs (National), fetched from the USDA MARS API
- **County heatmap** — interactive D3.js map with FIPS-coded county data, filterable by time range and source (wild birds / poultry / both)
- **Livestock/dairy tab** — herd detections by month with searchable detail table
- **Wild birds tab** — detection counts by month/day with species detail table
- **Mammals tab** — detections by species group (Domestic/Companion, Wild Carnivores, Rodents, Marine, Captive/Zoo) with stacked bar chart and filterable table
- **Fully self-contained** — output is a single `index.html` + `data.json`, no server-side dependencies

## Data Sources

| Source | Description |
|--------|-------------|
| [USDA APHIS — Commercial/Backyard Flocks](https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/commercial-backyard-flocks) | "A Table by Confirmation Date" CSV (required) |
| [USDA APHIS — Livestock](https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/hpai-confirmed-cases-livestock) | "Table Details by Date" CSV (optional) |
| [USDA APHIS — Wild Birds](https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/wild-birds) | "HPAI Detections in Wild Birds" CSV (optional) |
| [USDA APHIS — Mammals](https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/mammals) | "HPAI Detections in Mammals" CSV (optional) |
| [USDA AMS MARS API](https://mymarketnews.ams.usda.gov/viewReport/2843) | Daily wholesale egg prices (fetched at build time) |

## Requirements

- Python 3.8+
- [`addfips`](https://github.com/fitnr/addfips) — for county heatmap FIPS mapping (optional but recommended)

```bash
pip install addfips
```

All other dependencies are Python standard library.

### Egg prices (MARS API)

Wholesale egg prices are fetched from the USDA MARS API at build time. Set your API key as an environment variable:

```bash
export MARS_API_KEY="your-key-here"
```

If `MARS_API_KEY` is not set, egg prices will be skipped automatically. You can also skip them explicitly with `--no-prices`.

## Usage

Download the CSV files from the APHIS links above, then run:

```bash
# Minimal — poultry data only
python3 build_dashboard.py "A Table by Confirmation Date.csv"

# Full — all data sources
python3 build_dashboard.py "A Table by Confirmation Date.csv" \
  --livestock "Table Details by Date.csv" \
  --mammals "HPAI Detections in Mammals.csv" \
  --wild-birds "HPAI Detections in Wild Birds.csv"

# Custom output path
python3 build_dashboard.py "A Table by Confirmation Date.csv" -o docs/index.html

# Skip egg price fetch (offline mode)
python3 build_dashboard.py "A Table by Confirmation Date.csv" --no-prices

# Custom egg price start date
python3 build_dashboard.py "A Table by Confirmation Date.csv" --egg-start 2024-01-01
```

Output is written to `index.html` and `data.json` in the current directory by default.

### Preview locally

```bash
python3 -m http.server 8000 -d .
# Open http://localhost:8000
```

## Project Structure

```
build_dashboard.py   — CLI entry point, data aggregation, HTML generation
parsers.py           — CSV parsers, species classifiers, MARS API egg price fetcher
geo.py               — FIPS county lookup, detection aggregation, map data compression
template.py          — HTML/CSS/JS template, color constants, checkbox builders
```

## License

Chart: [Innovate Animal Ag](https://www.innovateanimalag.org/)
