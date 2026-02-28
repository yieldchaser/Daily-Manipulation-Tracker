"""
data_pipeline.py — Daily data ingestion for the Daily Manipulation Tracker.

Downloads NSE Bhavcopy (equity prices + delivery), Bulk Deals, Corporate
Announcements, and NSE Index bhavcopy (for NIFTY 500 benchmark) for a given
date and stores them in the SQLite database.

Usage:
    python src/data_pipeline.py                  # uses today's date
    python src/data_pipeline.py --date 2024-01-15
"""

import argparse
import io
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "tracker.db")

# ── NSE request headers ───────────────────────────────────────────────────────
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

NSE_API_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
    "X-Requested-With": "XMLHttpRequest",
}

# ── Retry helper ──────────────────────────────────────────────────────────────

def http_get_with_retry(session, url, headers=None, max_retries=3, timeout=15):
    """GET with exponential backoff. Returns requests.Response or raises."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 404:
                log.warning("HTTP 404 for %s — data not available", url)
                return None
            else:
                log.warning(
                    "Attempt %d/%d: HTTP %d for %s",
                    attempt, max_retries, resp.status_code, url,
                )
        except requests.exceptions.RequestException as exc:
            log.warning("Attempt %d/%d: request error for %s: %s", attempt, max_retries, url, exc)
        if attempt < max_retries:
            wait = 2 ** attempt
            log.info("Retrying in %ds…", wait)
            time.sleep(wait)
    log.error("All %d attempts failed for %s", max_retries, url)
    return None


def build_nse_session():
    """Create a requests.Session pre-loaded with NSE cookies."""
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    try:
        log.info("Fetching NSE homepage to obtain cookies…")
        resp = session.get("https://www.nseindia.com", timeout=15)
        log.info("NSE homepage status: %d, cookies: %s", resp.status_code, list(session.cookies.keys()))
        time.sleep(1)
    except requests.exceptions.RequestException as exc:
        log.warning("Could not fetch NSE homepage: %s", exc)
    return session


# ── Date helpers ──────────────────────────────────────────────────────────────

def is_weekend(dt: date) -> bool:
    return dt.weekday() >= 5  # Saturday=5, Sunday=6


def most_recent_trading_day(target: date, lookback: int = 5) -> date:
    """Walk backwards from target to find a non-weekend day."""
    dt = target
    for _ in range(lookback):
        if not is_weekend(dt):
            return dt
        dt -= timedelta(days=1)
    return dt


# ── 1. Bhavcopy (daily_prices) ────────────────────────────────────────────────

def download_full_bhavcopy(dt: date):
    """
    Download the full bhavcopy (with delivery data) for the given date via direct HTTP.
    Returns a pandas DataFrame or None if unavailable.

    The full bhavcopy URL pattern:
      https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{dd}{mm}{yyyy}.csv
    Columns include: SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE,
                     LOW_PRICE, LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY,
                     TURNOVER_LACS, NO_OF_TRADES, DELIV_QTY, DELIV_PER
    """
    ddmmyyyy = dt.strftime("%d%m%Y")
    url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv"
    log.info("Downloading full bhavcopy for %s from %s…", dt, url)
    try:
        session = requests.Session()
        # First hit NSE homepage to get cookies
        session.get("https://www.nseindia.com/", headers=NSE_HEADERS, timeout=10)
        time.sleep(1)
        resp = session.get(url, headers=NSE_HEADERS, timeout=30)
        if resp.status_code == 404:
            log.warning("Full bhavcopy not available (404) for %s", dt)
            return None
        resp.raise_for_status()
        text = resp.text
        if not text or len(text.strip()) < 100:
            log.warning("Full bhavcopy returned empty/short content for %s", dt)
            return None
        df = pd.read_csv(io.StringIO(text))
        df.columns = [c.strip() for c in df.columns]
        log.info("Full bhavcopy columns: %s", list(df.columns))
        log.info("Full bhavcopy rows: %d", len(df))
        return df
    except Exception as exc:
        log.warning("download_full_bhavcopy failed for %s: %s", dt, exc)
        return None


def download_cm_bhavcopy(dt: date):
    """
    Fallback: download the standard CM bhavcopy (no delivery data) via direct HTTP.
    Columns: SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, PREVCLOSE, TOTTRDQTY,
             TOTALTRADES, ISIN, TIMESTAMP
    """
    # CM bhavcopy URL pattern: BhavCopy_<DDMMMYYYY>_1.csv (e.g. BhavCopy_27FEB2026_1.csv)
    ddmmmyyyy = dt.strftime("%d%b%Y").upper()
    url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_{ddmmmyyyy}_1.csv"
    log.info("Downloading CM bhavcopy for %s from %s…", dt, url)
    try:
        session = requests.Session()
        # First hit NSE homepage to get cookies
        session.get("https://www.nseindia.com/", headers=NSE_HEADERS, timeout=10)
        time.sleep(1)
        resp = session.get(url, headers=NSE_HEADERS, timeout=30)
        if resp.status_code == 404:
            log.warning("CM bhavcopy not available (404) for %s", dt)
            return None
        resp.raise_for_status()
        text = resp.text
        if not text or len(text.strip()) < 100:
            log.warning("CM bhavcopy returned empty/short content for %s", dt)
            return None
        df = pd.read_csv(io.StringIO(text))
        df.columns = [c.strip() for c in df.columns]
        log.info("CM bhavcopy columns: %s", list(df.columns))
        log.info("CM bhavcopy rows: %d", len(df))
        return df
    except Exception as exc:
        log.warning("download_cm_bhavcopy failed for %s: %s", dt, exc)
        return None


def normalise_full_bhavcopy(df: pd.DataFrame, dt: date) -> pd.DataFrame:
    """
    Map full bhavcopy columns to our schema columns.
    Full bhavcopy columns (typical):
      SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE,
      LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS,
      NO_OF_TRADES, DELIV_QTY, DELIV_PER
    """
    col_map = {
        "SYMBOL": "symbol",
        "SERIES": "series",
        "OPEN_PRICE": "open",
        "HIGH_PRICE": "high",
        "LOW_PRICE": "low",
        "CLOSE_PRICE": "close",
        "PREV_CLOSE": "prev_close",
        "TTL_TRD_QNTY": "total_volume",
        "NO_OF_TRADES": "trades",
        "DELIV_QTY": "delivery_volume",
        "DELIV_PER": "delivery_pct",
    }
    # Keep only columns that exist
    existing = {k: v for k, v in col_map.items() if k in df.columns}
    out = df[list(existing.keys())].rename(columns=existing).copy()
    out["date"] = dt.strftime("%Y-%m-%d")

    # Compute pct_change
    if "close" in out.columns and "prev_close" in out.columns:
        out["pct_change"] = (
            (out["close"] - out["prev_close"]) / out["prev_close"].replace(0, float("nan")) * 100
        )
    else:
        out["pct_change"] = None

    # Ensure all schema columns exist
    for col in ["open", "high", "low", "close", "prev_close", "pct_change",
                "total_volume", "delivery_volume", "delivery_pct", "trades", "series"]:
        if col not in out.columns:
            out[col] = None

    return out


def normalise_cm_bhavcopy(df: pd.DataFrame, dt: date) -> pd.DataFrame:
    """
    Map CM bhavcopy columns to our schema columns.
    CM bhavcopy columns (typical):
      SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, PREVCLOSE, TOTTRDQTY,
      TOTALTRADES, ISIN, TIMESTAMP
    """
    col_map = {
        "SYMBOL": "symbol",
        "SERIES": "series",
        "OPEN": "open",
        "HIGH": "high",
        "LOW": "low",
        "CLOSE": "close",
        "PREVCLOSE": "prev_close",
        "TOTTRDQTY": "total_volume",
        "TOTALTRADES": "trades",
    }
    existing = {k: v for k, v in col_map.items() if k in df.columns}
    out = df[list(existing.keys())].rename(columns=existing).copy()
    out["date"] = dt.strftime("%Y-%m-%d")
    out["delivery_volume"] = None
    out["delivery_pct"] = None

    if "close" in out.columns and "prev_close" in out.columns:
        out["pct_change"] = (
            (out["close"] - out["prev_close"]) / out["prev_close"].replace(0, float("nan")) * 100
        )
    else:
        out["pct_change"] = None

    for col in ["open", "high", "low", "close", "prev_close", "pct_change",
                "total_volume", "delivery_volume", "delivery_pct", "trades", "series"]:
        if col not in out.columns:
            out[col] = None

    return out


def upsert_daily_prices(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Insert or replace rows into daily_prices. Returns count inserted."""
    rows = []
    for _, row in df.iterrows():
        rows.append((
            row.get("date"),
            row.get("symbol"),
            row.get("series"),
            _safe_float(row.get("open")),
            _safe_float(row.get("high")),
            _safe_float(row.get("low")),
            _safe_float(row.get("close")),
            _safe_float(row.get("prev_close")),
            _safe_float(row.get("pct_change")),
            _safe_int(row.get("total_volume")),
            _safe_int(row.get("delivery_volume")),
            _safe_float(row.get("delivery_pct")),
            _safe_int(row.get("trades")),
        ))

    sql = """
        INSERT OR REPLACE INTO daily_prices
            (date, symbol, series, open, high, low, close, prev_close,
             pct_change, total_volume, delivery_volume, delivery_pct, trades)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    c = conn.cursor()
    c.executemany(sql, rows)
    conn.commit()
    return c.rowcount if c.rowcount >= 0 else len(rows)


# ── 2. Rolling stats ──────────────────────────────────────────────────────────

def compute_and_upsert_rolling_stats(conn: sqlite3.Connection, target_date: str) -> int:
    """
    Compute rolling statistics for each symbol using historical data in
    daily_prices, then upsert into rolling_stats.
    """
    log.info("Computing rolling stats for %s…", target_date)
    c = conn.cursor()

    # Fetch all historical data up to and including target_date (252 trading days max)
    c.execute("""
        SELECT date, symbol, close, total_volume, delivery_pct, high
        FROM daily_prices
        WHERE date <= ?
        ORDER BY symbol, date
    """, (target_date,))
    rows = c.fetchall()

    if not rows:
        log.warning("No data in daily_prices to compute rolling stats.")
        return 0

    df = pd.DataFrame(rows, columns=["date", "symbol", "close", "total_volume", "delivery_pct", "high"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["symbol", "date"])

    # Get symbols present on target_date
    target_dt = pd.to_datetime(target_date)
    today_symbols = df[df["date"] == target_dt]["symbol"].unique()

    if len(today_symbols) == 0:
        log.warning("No symbols found for target date %s in daily_prices.", target_date)
        return 0

    stats_rows = []
    for symbol in today_symbols:
        sym_df = df[df["symbol"] == symbol].copy().reset_index(drop=True)
        # Find today's row index
        today_idx_list = sym_df.index[sym_df["date"] == target_dt].tolist()
        if not today_idx_list:
            continue
        today_idx = today_idx_list[-1]

        today_row = sym_df.iloc[today_idx]
        today_close = today_row["close"]
        today_volume = today_row["total_volume"]

        # Last 30 rows (including today)
        window_30 = sym_df.iloc[max(0, today_idx - 29): today_idx + 1]
        # Last 60 rows
        window_60 = sym_df.iloc[max(0, today_idx - 59): today_idx + 1]
        # Last 252 rows
        window_252 = sym_df.iloc[max(0, today_idx - 251): today_idx + 1]

        avg_volume_30d = window_30["total_volume"].mean() if len(window_30) > 0 else None
        avg_delivery_30d = window_30["delivery_pct"].mean() if len(window_30) > 0 else None

        vol_ratio = None
        if avg_volume_30d and avg_volume_30d > 0 and today_volume is not None:
            vol_ratio = float(today_volume) / float(avg_volume_30d)

        # Price change 30d: compare today vs 30 days ago
        price_change_30d = None
        if len(window_30) >= 2:
            close_30d_ago = window_30.iloc[0]["close"]
            if close_30d_ago and close_30d_ago != 0:
                price_change_30d = (float(today_close) - float(close_30d_ago)) / float(close_30d_ago) * 100

        # Price change 60d
        price_change_60d = None
        if len(window_60) >= 2:
            close_60d_ago = window_60.iloc[0]["close"]
            if close_60d_ago and close_60d_ago != 0:
                price_change_60d = (float(today_close) - float(close_60d_ago)) / float(close_60d_ago) * 100

        # Upper circuit streak: consecutive days where close == high
        streak = 0
        for i in range(today_idx, -1, -1):
            r = sym_df.iloc[i]
            if r["close"] is not None and r["high"] is not None and r["close"] == r["high"]:
                streak += 1
            else:
                break

        # 52-week high/low
        week_52_high = window_252["close"].max() if len(window_252) > 0 else None
        week_52_low = window_252["close"].min() if len(window_252) > 0 else None

        stats_rows.append((
            target_date,
            symbol,
            _safe_float(avg_volume_30d),
            _safe_float(avg_delivery_30d),
            _safe_float(vol_ratio),
            _safe_float(price_change_30d),
            _safe_float(price_change_60d),
            int(streak),
            _safe_float(week_52_high),
            _safe_float(week_52_low),
        ))

    sql = """
        INSERT OR REPLACE INTO rolling_stats
            (date, symbol, avg_volume_30d, avg_delivery_30d, vol_ratio,
             price_change_30d, price_change_60d, upper_circuit_streak,
             week_52_high, week_52_low)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    c.executemany(sql, stats_rows)
    conn.commit()
    log.info("Rolling stats upserted: %d rows", len(stats_rows))
    return len(stats_rows)


# ── 3. Bulk Deals ─────────────────────────────────────────────────────────────
#
# SIGNAL UNAVAILABILITY NOTE (as of 2026-02):
# The NSE bulk deals API endpoint has changed and all known variants return 404:
#   - https://www.nseindia.com/api/bulk-deal-archives?from=...&to=...&type=bulk_deals  → 404
#   - https://www.nseindia.com/api/bulk-deal-archives?from=...&to=...                  → 404
#   - https://www.nseindia.com/api/bulk-deals?from=...&to=...                          → 404
# The suggested alternative https://www.nseindia.com/api/historical/bulk-deals
# returns an HTML page (not a JSON API endpoint).
# As a result, Signal 7 (Bulk Deal Anomaly) will always score 0 until NSE
# restores or documents a working API endpoint.
#
def download_bulk_deals(session: requests.Session, dt: date) -> list:
    """
    Download NSE bulk deals for the given date.
    Returns list of dicts or empty list.

    NOTE: As of 2026-02, the NSE bulk deals API endpoint
    (https://www.nseindia.com/api/bulk-deal-archives) returns HTTP 404.
    All known alternative endpoints also fail. This function will return
    an empty list until NSE restores a working API endpoint.
    Signal 7 (Bulk Deal Anomaly) is therefore unavailable.
    """
    date_str = dt.strftime("%d-%m-%Y")
    # Primary endpoint (returns 404 as of 2026-02 — NSE API change)
    url = (
        f"https://www.nseindia.com/api/bulk-deal-archives"
        f"?from={date_str}&to={date_str}&type=bulk_deals"
    )
    log.info("Downloading bulk deals from: %s", url)
    resp = http_get_with_retry(session, url, headers=NSE_API_HEADERS)
    if resp is None:
        log.warning(
            "Bulk deals API unavailable for %s (HTTP 404). "
            "NSE has changed/removed the bulk-deal-archives endpoint. "
            "Signal 7 (Bulk Deal Anomaly) will score 0.",
            dt,
        )
        return []
    try:
        data = resp.json()
        deals = data.get("data", [])
        log.info("Bulk deals fetched: %d records", len(deals))
        return deals
    except (json.JSONDecodeError, ValueError) as exc:
        log.warning("Could not parse bulk deals JSON: %s", exc)
        log.debug("Response text: %s", resp.text[:500])
        return []


def upsert_bulk_deals(conn: sqlite3.Connection, deals: list, dt: date) -> int:
    """Insert or replace bulk deal rows. Returns count."""
    if not deals:
        return 0
    date_str = dt.strftime("%Y-%m-%d")
    rows = []
    for deal in deals:
        rows.append((
            date_str,
            str(deal.get("symbol", deal.get("Symbol", ""))).strip(),
            str(deal.get("clientName", deal.get("Client Name", ""))).strip(),
            str(deal.get("buySell", deal.get("Buy/Sell", ""))).strip(),
            _safe_int(deal.get("dealQuantity", deal.get("Quantity Traded", 0))),
            _safe_float(deal.get("dealPrice", deal.get("Trade Price / Wt. Avg. Price", 0))),
        ))
    sql = """
        INSERT OR REPLACE INTO bulk_deals
            (date, symbol, client_name, buy_sell, quantity, price)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    c = conn.cursor()
    c.executemany(sql, rows)
    conn.commit()
    return len(rows)


# ── 4. Corporate Announcements ────────────────────────────────────────────────
#
# API STATUS (as of 2026-02):
# The NSE corporate announcements API endpoint works and returns JSON:
#   https://www.nseindia.com/api/corporate-announcements?index=equities&from_date=DD-MM-YYYY&to_date=DD-MM-YYYY
# Response is a JSON list with fields:
#   symbol, desc (category), an_dt (announcement date), attchmntText (full text),
#   sm_name (company name), sm_isin, sort_date, seq_id, etc.
# NOTE: Field names differ from the original code's expectations.
#   Use 'desc' for event category, 'an_dt' for date, 'attchmntText' for description.
#
CORP_KEYWORDS = ["bonus", "split", "preferential", "allotment", "mou", "order", "partnership"]


def download_corporate_announcements(session: requests.Session, dt: date) -> list:
    """
    Download NSE corporate announcements for the given date.
    Returns list of dicts or empty list.

    API endpoint: https://www.nseindia.com/api/corporate-announcements
    Parameters: index=equities, from_date=DD-MM-YYYY, to_date=DD-MM-YYYY
    Response fields: symbol, desc, an_dt, attchmntText, sm_name, sort_date, etc.
    """
    date_str = dt.strftime("%d-%m-%Y")
    url = (
        f"https://www.nseindia.com/api/corporate-announcements"
        f"?index=equities&from_date={date_str}&to_date={date_str}"
    )
    log.info("Downloading corporate announcements from: %s", url)
    resp = http_get_with_retry(session, url, headers=NSE_API_HEADERS)
    if resp is None:
        log.warning("No corporate announcements response for %s", dt)
        return []
    try:
        data = resp.json()
        # Response is a JSON list directly or may have a 'data' key
        if isinstance(data, list):
            announcements = data
        elif isinstance(data, dict):
            announcements = data.get("data", [])
        else:
            announcements = []
        log.info("Corporate announcements fetched: %d records", len(announcements))
        return announcements
    except (json.JSONDecodeError, ValueError) as exc:
        log.warning("Could not parse corporate announcements JSON: %s", exc)
        log.debug("Response text: %s", resp.text[:500])
        return []


def match_keyword(text: str) -> str | None:
    """Return the first matching keyword from CORP_KEYWORDS, or None."""
    if not text:
        return None
    text_lower = text.lower()
    for kw in CORP_KEYWORDS:
        if kw in text_lower:
            return kw
    return None


def upsert_corporate_events(conn: sqlite3.Connection, announcements: list, dt: date) -> int:
    """Filter and insert corporate events. Returns count inserted.

    Handles the NSE corporate-announcements API response format (as of 2026-02):
      - symbol: stock symbol
      - desc: announcement category (e.g. "Bonus Issue", "General Updates")
      - an_dt: announcement date string like "27-Feb-2026 23:48:08"
      - sort_date: ISO-like date string "2026-02-27 23:48:08"
      - attchmntText: full announcement text (used for keyword matching)
    Also handles legacy field names for backward compatibility.
    """
    if not announcements:
        return 0
    date_str = dt.strftime("%Y-%m-%d")
    rows = []
    for ann in announcements:
        # symbol field (consistent across old and new API)
        symbol = str(ann.get("symbol", ann.get("Symbol", ""))).strip()

        # Event category / subject — new API uses 'desc', old used 'subject'/'description'
        event_category = str(ann.get("desc", ann.get("subject", ann.get("description", "")))).strip()

        # Full description text — new API uses 'attchmntText', old used 'subject'/'description'
        full_text = str(ann.get("attchmntText", ann.get("subject", ann.get("description", "")))).strip()

        # Combine category + full text for keyword matching
        combined_text = f"{event_category} {full_text}"

        # Announcement date — new API uses 'sort_date' (YYYY-MM-DD HH:MM:SS) or 'an_dt'
        # Old API used 'ann_date' or 'date'
        ann_date = date_str
        for date_field in ["sort_date", "an_dt", "ann_date", "date"]:
            raw_date = ann.get(date_field, "")
            if raw_date:
                raw_date = str(raw_date).strip()
                try:
                    # "2026-02-27 23:48:08" format
                    if len(raw_date) >= 10 and raw_date[4] == "-":
                        ann_date = raw_date[:10]
                        break
                    # "27-Feb-2026 23:48:08" format
                    elif "-" in raw_date and len(raw_date) >= 11:
                        ann_date = datetime.strptime(raw_date[:11].strip(), "%d-%b-%Y").strftime("%Y-%m-%d")
                        break
                    # "DD/MM/YYYY" format
                    elif "/" in raw_date:
                        ann_date = datetime.strptime(raw_date[:10], "%d/%m/%Y").strftime("%Y-%m-%d")
                        break
                except ValueError:
                    continue

        keyword = match_keyword(combined_text)
        if keyword is None:
            continue

        # Use event_category as event_type, full_text as description
        rows.append((
            ann_date,
            symbol,
            keyword,
            combined_text[:500],
            "NSE",
        ))

    if not rows:
        log.info("No corporate events matched keywords.")
        return 0

    sql = """
        INSERT OR REPLACE INTO corporate_events
            (date, symbol, event_type, description, source)
        VALUES (?, ?, ?, ?, ?)
    """
    c = conn.cursor()
    c.executemany(sql, rows)
    conn.commit()
    log.info("Corporate events upserted: %d rows", len(rows))
    return len(rows)


# ── 5. NSE Index Bhavcopy (index_prices) ─────────────────────────────────────

def ensure_index_prices_table(conn: sqlite3.Connection):
    """Create index_prices table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_prices (
            index_name  TEXT NOT NULL,
            date        TEXT NOT NULL,
            close       REAL,
            UNIQUE(index_name, date)
        )
    """)
    conn.commit()


def download_index_data(dt: date) -> pd.DataFrame | None:
    """
    Download NSE index bhavcopy for the given date.

    URL pattern:
      https://nsearchives.nseindia.com/content/indices/ind_close_all_DDMMYYYY.csv

    The CSV contains closing prices for all NSE indices including NIFTY 500.
    Returns a pandas DataFrame or None if unavailable.

    Relevant columns in the CSV (typical):
      Index Name, Closing Index Value, Opening Index Value, High Index Value,
      Low Index Value, Change(Points), Change(%), Volume, Turnover (Rs. Cr.),
      P/E, P/B, Div Yield
    """
    ddmmyyyy = dt.strftime("%d%m%Y")
    url = f"https://nsearchives.nseindia.com/content/indices/ind_close_all_{ddmmyyyy}.csv"
    log.info("Downloading NSE index bhavcopy for %s from %s…", dt, url)
    try:
        session = requests.Session()
        # First hit NSE homepage to get cookies
        session.get("https://www.nseindia.com/", headers=NSE_HEADERS, timeout=10)
        time.sleep(1)
        resp = session.get(url, headers=NSE_HEADERS, timeout=30)
        if resp.status_code == 404:
            log.warning("NSE index bhavcopy not available (404) for %s", dt)
            return None
        resp.raise_for_status()
        text = resp.text
        if not text or len(text.strip()) < 50:
            log.warning("NSE index bhavcopy returned empty/short content for %s", dt)
            return None
        df = pd.read_csv(io.StringIO(text))
        df.columns = [c.strip() for c in df.columns]
        log.info("NSE index bhavcopy columns: %s", list(df.columns))
        log.info("NSE index bhavcopy rows: %d", len(df))
        return df
    except Exception as exc:
        log.warning("download_index_data failed for %s: %s", dt, exc)
        return None


def upsert_index_prices(conn: sqlite3.Connection, df: pd.DataFrame, dt: date) -> int:
    """
    Parse the NSE index bhavcopy DataFrame and store NIFTY 500 (and other
    indices) into the index_prices table.

    The CSV typically has a column named 'Index Name' and 'Closing Index Value'.
    We store all indices but primarily care about 'NIFTY 500'.

    Returns count of rows inserted.
    """
    if df is None or len(df) == 0:
        return 0

    date_str = dt.strftime("%Y-%m-%d")

    # Identify the index name column and closing value column
    # Common column names in NSE index bhavcopy:
    name_col = None
    close_col = None

    for col in df.columns:
        col_lower = col.lower().strip()
        if col_lower in ("index name", "indexname", "index_name"):
            name_col = col
        if col_lower in ("closing index value", "close", "closingindexvalue",
                         "closing_index_value", "close index value"):
            close_col = col

    # Fallback: use first column as name, look for 'closing' in column names
    if name_col is None and len(df.columns) > 0:
        name_col = df.columns[0]
        log.warning("Could not identify index name column; using first column: %s", name_col)

    if close_col is None:
        for col in df.columns:
            if "clos" in col.lower():
                close_col = col
                break

    if name_col is None or close_col is None:
        log.warning(
            "Could not identify required columns in index bhavcopy. "
            "Columns found: %s", list(df.columns)
        )
        return 0

    rows = []
    for _, row in df.iterrows():
        index_name = str(row[name_col]).strip()
        if not index_name or index_name.lower() in ("nan", ""):
            continue
        close_val = _safe_float(row.get(close_col))
        if close_val is None:
            continue
        rows.append((index_name, date_str, close_val))

    if not rows:
        log.warning("No valid index rows found in bhavcopy for %s", dt)
        return 0

    sql = """
        INSERT OR REPLACE INTO index_prices (index_name, date, close)
        VALUES (?, ?, ?)
    """
    c = conn.cursor()
    c.executemany(sql, rows)
    conn.commit()
    log.info("index_prices: %d rows inserted/replaced for %s", len(rows), dt)

    # Log whether NIFTY 500 was found
    nifty500_rows = [r for r in rows if "NIFTY 500" in r[0].upper() or r[0].upper() == "NIFTY500"]
    if nifty500_rows:
        log.info("NIFTY 500 close for %s: %.2f", dt, nifty500_rows[0][2])
    else:
        log.warning("NIFTY 500 not found in index bhavcopy for %s. Available: %s",
                    dt, [r[0] for r in rows[:10]])

    return len(rows)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_float(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_int(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return int(float(val))
    except (TypeError, ValueError):
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="NSE Daily Data Pipeline")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to process in YYYY-MM-DD format (default: today)",
    )
    args = parser.parse_args()

    # Determine target date
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            log.error("Invalid date format: %s. Use YYYY-MM-DD.", args.date)
            sys.exit(1)
    else:
        target_date = date.today()

    log.info("Target date: %s", target_date)

    # Skip weekends
    if is_weekend(target_date):
        log.info("Target date %s is a weekend. Finding most recent trading day…", target_date)
        target_date = most_recent_trading_day(target_date - timedelta(days=1))
        log.info("Adjusted to: %s", target_date)

    # ── Connect to DB ──────────────────────────────────────────────────────────
    if not os.path.exists(DB_PATH):
        log.error("Database not found at %s. Run src/create_db.py first.", DB_PATH)
        sys.exit(1)
    conn = sqlite3.connect(DB_PATH)
    log.info("Connected to database: %s", DB_PATH)

    # Ensure index_prices table exists
    ensure_index_prices_table(conn)

    # ── Build NSE session ──────────────────────────────────────────────────────
    nse_session = build_nse_session()

    # ── 1. Bhavcopy ───────────────────────────────────────────────────────────
    prices_inserted = 0
    rolling_inserted = 0
    bhavcopy_date = target_date

    # Try up to 5 previous trading days if today's data isn't available
    for attempt in range(5):
        if is_weekend(bhavcopy_date):
            bhavcopy_date -= timedelta(days=1)
            continue

        log.info("Attempting bhavcopy download for %s (attempt %d)…", bhavcopy_date, attempt + 1)

        # Try full bhavcopy first (has delivery data)
        df_raw = download_full_bhavcopy(bhavcopy_date)
        if df_raw is not None and len(df_raw) > 0:
            df_norm = normalise_full_bhavcopy(df_raw, bhavcopy_date)
        else:
            # Fallback to CM bhavcopy
            log.info("Falling back to CM bhavcopy for %s…", bhavcopy_date)
            df_raw = download_cm_bhavcopy(bhavcopy_date)
            if df_raw is not None and len(df_raw) > 0:
                df_norm = normalise_cm_bhavcopy(df_raw, bhavcopy_date)
            else:
                log.warning("No bhavcopy data for %s, trying previous day…", bhavcopy_date)
                bhavcopy_date -= timedelta(days=1)
                time.sleep(2)
                continue

        # Filter to EQ series only
        if "series" in df_norm.columns:
            df_eq = df_norm[df_norm["series"].str.strip() == "EQ"].copy()
            log.info("EQ series rows: %d (total: %d)", len(df_eq), len(df_norm))
        else:
            df_eq = df_norm.copy()
            log.warning("No 'series' column found; using all rows.")

        if len(df_eq) == 0:
            log.warning("No EQ series rows found for %s.", bhavcopy_date)
            bhavcopy_date -= timedelta(days=1)
            continue

        # Upsert into daily_prices
        try:
            prices_inserted = upsert_daily_prices(conn, df_eq)
            log.info("daily_prices: %d rows inserted/replaced for %s", prices_inserted, bhavcopy_date)
        except Exception as exc:
            log.error("Failed to upsert daily_prices: %s", exc)

        # Compute rolling stats
        try:
            rolling_inserted = compute_and_upsert_rolling_stats(
                conn, bhavcopy_date.strftime("%Y-%m-%d")
            )
        except Exception as exc:
            log.error("Failed to compute rolling stats: %s", exc)

        break  # Success
    else:
        log.error("Could not obtain bhavcopy data after 5 attempts.")

    # ── 2. Bulk Deals ─────────────────────────────────────────────────────────
    bulk_inserted = 0
    try:
        time.sleep(2)
        deals = download_bulk_deals(nse_session, bhavcopy_date)
        bulk_inserted = upsert_bulk_deals(conn, deals, bhavcopy_date)
        log.info("bulk_deals: %d rows inserted/replaced", bulk_inserted)
    except Exception as exc:
        log.error("Bulk deals pipeline failed: %s", exc)

    # ── 3. Corporate Announcements ────────────────────────────────────────────
    corp_inserted = 0
    try:
        time.sleep(2)
        announcements = download_corporate_announcements(nse_session, bhavcopy_date)
        corp_inserted = upsert_corporate_events(conn, announcements, bhavcopy_date)
        log.info("corporate_events: %d rows inserted/replaced", corp_inserted)
    except Exception as exc:
        log.error("Corporate announcements pipeline failed: %s", exc)

    # ── 4. NSE Index Bhavcopy (NIFTY 500 benchmark) ───────────────────────────
    index_inserted = 0
    try:
        time.sleep(2)
        log.info("Downloading NSE index bhavcopy for %s…", bhavcopy_date)
        index_df = download_index_data(bhavcopy_date)
        if index_df is not None and len(index_df) > 0:
            index_inserted = upsert_index_prices(conn, index_df, bhavcopy_date)
            log.info("index_prices: %d rows inserted/replaced", index_inserted)
        else:
            log.warning(
                "NSE index bhavcopy not available for %s. "
                "Signal 4 (Price Detachment) will score 0 until index data is available.",
                bhavcopy_date,
            )
    except Exception as exc:
        log.error("NSE index bhavcopy pipeline failed: %s", exc)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"  DATA PIPELINE SUMMARY — {bhavcopy_date}")
    print("=" * 60)
    print(f"  daily_prices    : {prices_inserted:>6} rows inserted/replaced")
    print(f"  rolling_stats   : {rolling_inserted:>6} rows inserted/replaced")
    print(f"  bulk_deals      : {bulk_inserted:>6} rows inserted/replaced")
    print(f"  corporate_events: {corp_inserted:>6} rows inserted/replaced")
    print(f"  index_prices    : {index_inserted:>6} rows inserted/replaced")
    print("=" * 60)

    # ── Sample rows from daily_prices ─────────────────────────────────────────
    try:
        c = conn.cursor()
        c.execute("""
            SELECT date, symbol, series, open, high, low, close, pct_change,
                   total_volume, delivery_pct
            FROM daily_prices
            WHERE date = ?
            ORDER BY total_volume DESC
            LIMIT 5
        """, (bhavcopy_date.strftime("%Y-%m-%d"),))
        sample_rows = c.fetchall()
        if sample_rows:
            print("\n  Top 5 rows from daily_prices (by volume):")
            print(f"  {'DATE':<12} {'SYMBOL':<12} {'SER':<4} {'OPEN':>8} {'HIGH':>8} "
                  f"{'LOW':>8} {'CLOSE':>8} {'CHG%':>7} {'VOLUME':>12} {'DEL%':>7}")
            print("  " + "-" * 95)
            for row in sample_rows:
                date_v, sym, ser, opn, high, low, close, pct, vol, del_pct = row
                print(
                    f"  {str(date_v):<12} {str(sym):<12} {str(ser or ''):<4} "
                    f"{_fmt(opn):>8} {_fmt(high):>8} {_fmt(low):>8} {_fmt(close):>8} "
                    f"{_fmt(pct):>7} {_fmt_int(vol):>12} {_fmt(del_pct):>7}"
                )
        else:
            print("\n  No rows found in daily_prices for this date.")
    except Exception as exc:
        log.error("Could not fetch sample rows: %s", exc)

    conn.close()
    print("\n  Pipeline complete.\n")


def _fmt(val):
    if val is None:
        return "N/A"
    try:
        return f"{float(val):.2f}"
    except (TypeError, ValueError):
        return str(val)


def _fmt_int(val):
    if val is None:
        return "N/A"
    try:
        return f"{int(val):,}"
    except (TypeError, ValueError):
        return str(val)


if __name__ == "__main__":
    main()
