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
- [`requests`](https://docs.python-requests.org/) — for downloading APHIS data
- [`addfips`](https://github.com/fitnr/addfips) — for county heatmap FIPS mapping (optional but recommended)

```bash
pip install requests addfips
```

### Egg prices (MARS API)

Wholesale egg prices are fetched from the USDA MARS API at build time. Set your API key as an environment variable:

```bash
export MARS_API_KEY="your-key-here"
```

If `MARS_API_KEY` is not set, egg prices will be skipped automatically. You can also skip them explicitly with `--no-prices`.

## Usage

The build script automatically downloads 3 of 4 datasets (flocks, wild birds, mammals) and builds the dashboard:

```bash
# Download fresh data + build dashboard
python3 build_dashboard.py

# Build from existing CSVs (skip download)
python3 build_dashboard.py --no-download

# Skip egg price fetch (offline mode)
python3 build_dashboard.py --no-prices

# Custom output path
python3 build_dashboard.py -o docs/index.html
```

Output is written to `index.html` and `data.json` in the current directory by default.

**Livestock data** must be downloaded manually from the [APHIS livestock page](https://www.aphis.usda.gov/livestock-poultry-disease/avian/avian-influenza/hpai-detections/hpai-confirmed-cases-livestock) — the Tableau view doesn't expose an automated endpoint. Place it as `Table Details by Date.csv` in the project directory.

You can also download datasets independently:

```bash
python3 download_data.py                # download all 3 to current directory
python3 download_data.py -o data/       # specify output directory
```

### Preview locally

```bash
python3 -m http.server 8000 -d .
# Open http://localhost:8000
```

## Project Structure

```
build_dashboard.py   — CLI entry point: downloads data, aggregates, generates HTML
download_data.py     — APHIS dataset downloader (3 of 4 datasets automated)
parsers.py           — CSV parsers, species classifiers, MARS API egg price fetcher
geo.py               — FIPS county lookup, detection aggregation, map data compression
template.py          — HTML/CSS/JS template, color constants, checkbox builders
```

## License

Chart: [Innovate Animal Ag](https://www.innovateanimalag.org/)
