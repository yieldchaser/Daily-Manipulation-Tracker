# Daily Manipulation Tracker

A Python-based system that ingests daily NSE stock data, computes manipulation risk signals, and surfaces them through an interactive Streamlit dashboard.

---

## Features

- **5 SQLite tables** — `daily_prices`, `rolling_stats`, `corporate_events`, `bulk_deals`, `manipulation_scores`
- **Streamlit dashboard** — interactive, wide-layout UI with 4 analysis tabs
- **Manipulation scoring** — composite risk score (0–1) built from 7 independent signals
- **Plotly charts** — candlestick OHLCV, volume bars, score timelines, rolling metrics
- **Corporate events & bulk deals** — tabular views with timeline and distribution charts
- **Graceful error handling** — empty-table messages, connection error recovery, input validation

---

## Project Structure

```
Daily-Manipulation-Tracker/
├── data/
│   └── tracker.db          # SQLite database (auto-created by create_db.py)
├── dashboard/
│   ├── app.py              # Streamlit dashboard entry point
│   └── utils.py            # Database helper functions
├── src/
│   └── create_db.py        # Schema initialisation script
├── .github/
│   └── workflows/
│       └── ci.yml          # GitHub Actions CI workflow
├── requirements.txt
└── README.md
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Initialise the database schema

```bash
python src/create_db.py
```

This creates `data/tracker.db` with all five tables if they do not already exist.

### 3. Run the dashboard

```bash
streamlit run dashboard/app.py
```

The app will open at `http://localhost:8501` by default.

---

## Dashboard Tabs

| Tab | Contents |
|-----|----------|
| **Price & Volume** | Plotly candlestick chart (OHLC) + colour-coded volume bars + delivery volume overlay |
| **Manipulation Scores** | Total score line chart with green/yellow/red risk zones + per-signal breakdown |
| **Rolling Stats** | 30-day average volume, delivery, vol ratio, 30/60-day price change, 52-week range, circuit streak |
| **Events & Deals** | Corporate events table + bulk/block deals table + combined timeline |

---

## Sidebar Controls

- **Stock Symbol** — dropdown populated from `daily_prices`; falls back to free-text input when the table is empty
- **Date Range** — start and end date pickers (default: last 90 days)
- **Refresh** — clears the 5-minute data cache and re-queries the database

---

## Screenshots

> _Add screenshots here after first run._

```
[ Price & Volume tab screenshot ]
[ Manipulation Scores tab screenshot ]
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11 |
| Database | SQLite 3 (stdlib) |
| Dashboard | Streamlit 1.32 |
| Charts | Plotly 5.19 |
| Data wrangling | Pandas 2.2, NumPy 1.26 |
| Data ingestion | jugaad-data 0.27 |
| CI | GitHub Actions |

---

## CI

The `validate` job in `.github/workflows/ci.yml` runs on every push to `main`:

1. Checks out the repository
2. Sets up Python 3.11
3. Installs `requirements.txt`
4. Runs `python src/create_db.py` to verify the schema initialises without error

---

## License

See [LICENSE](LICENSE).
