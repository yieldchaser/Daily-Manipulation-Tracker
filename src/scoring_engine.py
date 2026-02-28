"""
scoring_engine.py — Time-series based manipulation scoring engine.

Philosophy: "Has something suspicious been happening consistently for weeks?"
A stock that grinds up 0.5-2% EVERY SINGLE DAY for 30-60 days with no news
and low delivery = manipulation. Every signal looks at a WINDOW of days.

Usage:
    python src/scoring_engine.py                  # uses most recent date in daily_prices
    python src/scoring_engine.py --date 2026-02-27
"""

import argparse
import os
import sqlite3
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "tracker.db")


# ── Noise Eliminator: Large-cap index constituents ────────────────────────────
# Nifty 50 constituents (hardcoded — current as of early 2026)
NIFTY50 = {
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BHARTIARTL", "BPCL",
    "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY",
    "EICHERMOT", "GRASIM", "HCLTECH", "HDFC", "HDFCBANK",
    "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK",
    "INDUSINDBK", "INFY", "ITC", "JSWSTEEL", "KOTAKBANK",
    "LT", "M&M", "MARUTI", "NESTLEIND", "NTPC",
    "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SBIN",
    "SHRIRAMFIN", "SUNPHARMA", "TATACONSUM", "TATAMOTORS", "TATASTEEL",
    "TCS", "TECHM", "TITAN", "ULTRACEMCO", "WIPRO",
}

# Nifty Next 50 constituents (hardcoded — current as of early 2026)
NIFTY_NEXT50 = {
    "ABB", "ADANIGREEN", "ADANITRANS", "AMBUJACEM", "AUROPHARMA",
    "BAJAJHLDNG", "BANKBARODA", "BERGEPAINT", "BEL", "BOSCHLTD",
    "CANBK", "CHOLAFIN", "COLPAL", "DABUR", "DLF",
    "DMART", "GAIL", "GODREJCP", "GODREJPROP", "HAVELLS",
    "ICICIGI", "ICICIPRULI", "INDUSTOWER", "IOC", "IRCTC",
    "JINDALSTEL", "LICI", "LUPIN", "MARICO", "MCDOWELL-N",
    "MUTHOOTFIN", "NAUKRI", "NHPC", "NMDC", "OFSS",
    "PAGEIND", "PIDILITIND", "PIIND", "PNB", "RECLTD",
    "SAIL", "SIEMENS", "SRF", "TATAPOWER", "TORNTPHARM",
    "TRENT", "TVSMOTOR", "UBL", "UNIONBANK", "VBL",
    "VEDL", "ZOMATO",
}

# Combined large-cap set — these stocks cannot be manipulated via pump-dump
LARGE_CAP_SYMBOLS = NIFTY50 | NIFTY_NEXT50


# ── Signal 1: Abnormal Consistency Score ─────────────────────────────────────

def signal_abnormal_consistency(hist_df: pd.DataFrame) -> float:
    """
    Signal 1 — Abnormal Consistency Score (max 2.0 pts).

    Look back 30 days. Count days where:
      - Volume > 2x its own 90-day average AND
      - No major news/results event (we skip this check if data unavailable)

    ratio > 0.4 (12+ days out of 30) = 1.0 pt
    ratio > 0.6 (18+ days out of 30) = 2.0 pts
    """
    if hist_df is None or len(hist_df) < 10:
        return 0.0

    # Need at least some history for 90-day avg
    # Use all available data for 90-day avg volume
    avg_90d_vol = hist_df["total_volume"].mean()
    if avg_90d_vol is None or avg_90d_vol == 0 or pd.isna(avg_90d_vol):
        return 0.0

    # Last 30 days
    window_30 = hist_df.tail(30)
    if len(window_30) < 10:
        return 0.0

    # Count days with volume > 2x 90-day average
    suspicious_days = (window_30["total_volume"] > 2.0 * avg_90d_vol).sum()
    ratio = suspicious_days / len(window_30)

    if ratio > 0.6:
        return 2.0
    elif ratio > 0.4:
        return 1.0
    return 0.0


# ── Signal 2: Chronic Low Delivery on Up Days ─────────────────────────────────

def signal_chronic_low_delivery(hist_df: pd.DataFrame) -> float:
    """
    Signal 2 — Chronic Low Delivery on Up Days (max 2.0 pts).

    Look back 30 days. Count days where BOTH:
      - price change > 0.5% (up day)
      - delivery_pct < 25%

    8-15 such days in 30 = 1.0 pt
    15+ such days in 30  = 2.0 pts
    """
    if hist_df is None or len(hist_df) < 10:
        return 0.0

    window_30 = hist_df.tail(30)
    if len(window_30) < 10:
        return 0.0

    # Need delivery data
    if "delivery_pct" not in window_30.columns or window_30["delivery_pct"].isna().all():
        return 0.0
    if "pct_change" not in window_30.columns or window_30["pct_change"].isna().all():
        return 0.0

    # Count days: up day AND low delivery
    up_days = window_30["pct_change"] > 0.5
    low_delivery = window_30["delivery_pct"] < 25.0
    chronic_days = (up_days & low_delivery).sum()

    if chronic_days >= 15:
        return 2.0
    elif chronic_days >= 8:
        return 1.0
    return 0.0


# ── Signal 3: Steady Grind Pattern ───────────────────────────────────────────

def signal_steady_grind(hist_df: pd.DataFrame) -> float:
    """
    Signal 3 — Steady Grind Pattern (max 2.0 pts).

    Look back 45 days. Calculate:
      a) How many of those days were positive (close > open)?
      b) What was the standard deviation of daily returns?

    positive days > 70% AND std dev < 1.5% = 1.5 pts
    positive days > 80% AND std dev < 1.0% = 2.0 pts
    """
    if hist_df is None or len(hist_df) < 20:
        return 0.0

    window_45 = hist_df.tail(45)
    if len(window_45) < 20:
        return 0.0

    # Need open and close
    if "open" not in window_45.columns or "close" not in window_45.columns:
        return 0.0

    valid = window_45.dropna(subset=["open", "close"])
    if len(valid) < 15:
        return 0.0

    # Positive days: close > open
    positive_days = (valid["close"] > valid["open"]).sum()
    positive_ratio = positive_days / len(valid)

    # Daily returns std dev
    if "pct_change" in valid.columns and not valid["pct_change"].isna().all():
        daily_returns = valid["pct_change"].dropna()
    else:
        # Compute from close prices
        daily_returns = valid["close"].pct_change().dropna() * 100

    if len(daily_returns) < 10:
        return 0.0

    std_dev = daily_returns.std()

    if positive_ratio > 0.80 and std_dev < 1.0:
        return 2.0
    elif positive_ratio > 0.70 and std_dev < 1.5:
        return 1.5
    return 0.0


# ── Signal 4: Price Detachment from Market ────────────────────────────────────

def signal_price_detachment(hist_df: pd.DataFrame, index_df: pd.DataFrame) -> float:
    """
    Signal 4 — Price Detachment from Market (max 1.5 pts).

    Calculate the stock's return vs Nifty 500 over the last 60 days.
    If the stock is up >40% while Nifty is flat or down = clear detachment.

    detachment > 40% vs index over 60 days = 1.0 pt
    detachment > 80% vs index over 60 days = 1.5 pts

    If index data is not available, skip this signal (score 0).
    """
    if hist_df is None or len(hist_df) < 30:
        return 0.0
    if index_df is None or len(index_df) < 30:
        return 0.0

    window_60 = hist_df.tail(60)
    if len(window_60) < 30:
        return 0.0

    # Stock 60-day return
    first_close = window_60.iloc[0]["close"]
    last_close = window_60.iloc[-1]["close"]
    if first_close is None or first_close == 0 or pd.isna(first_close):
        return 0.0
    stock_return = (float(last_close) - float(first_close)) / float(first_close) * 100

    # Index 60-day return — align dates
    # Get the date range from stock window
    stock_start_date = window_60.iloc[0]["date"] if "date" in window_60.columns else None
    stock_end_date = window_60.iloc[-1]["date"] if "date" in window_60.columns else None

    if stock_start_date is None or stock_end_date is None:
        return 0.0

    # Filter index data to same date range
    idx_window = index_df[
        (index_df["date"] >= stock_start_date) &
        (index_df["date"] <= stock_end_date)
    ].sort_values("date")

    if len(idx_window) < 20:
        return 0.0

    idx_first = idx_window.iloc[0]["close"]
    idx_last = idx_window.iloc[-1]["close"]
    if idx_first is None or idx_first == 0 or pd.isna(idx_first):
        return 0.0
    index_return = (float(idx_last) - float(idx_first)) / float(idx_first) * 100

    # Detachment = stock return - index return
    detachment = stock_return - index_return

    if detachment > 80:
        return 1.5
    elif detachment > 40:
        return 1.0
    return 0.0


# ── Signal 5: Velocity Fingerprint ───────────────────────────────────────────

def signal_velocity_fingerprint(hist_df: pd.DataFrame) -> float:
    """
    Signal 5 — Velocity Fingerprint (max 1.5 pts).

    Calculate the 60-day return AND the smoothness of that return.
    Smoothness = 1 - (number of down days / total days)
    Manipulation fingerprint = high return + high smoothness

    60d return > 50% AND smoothness > 0.75 = 1.0 pt
    60d return > 100% AND smoothness > 0.80 = 1.5 pts
    """
    if hist_df is None or len(hist_df) < 30:
        return 0.0

    window_60 = hist_df.tail(60)
    if len(window_60) < 30:
        return 0.0

    # 60-day return
    first_close = window_60.iloc[0]["close"]
    last_close = window_60.iloc[-1]["close"]
    if first_close is None or first_close == 0 or pd.isna(first_close):
        return 0.0
    return_60d = (float(last_close) - float(first_close)) / float(first_close) * 100

    # Smoothness: 1 - (down days / total days)
    if "pct_change" in window_60.columns and not window_60["pct_change"].isna().all():
        valid_days = window_60["pct_change"].dropna()
    else:
        valid_days = window_60["close"].pct_change().dropna() * 100

    if len(valid_days) < 20:
        return 0.0

    down_days = (valid_days < 0).sum()
    smoothness = 1.0 - (down_days / len(valid_days))

    if return_60d > 100 and smoothness > 0.80:
        return 1.5
    elif return_60d > 50 and smoothness > 0.75:
        return 1.0
    return 0.0


# ── Signal 6: Micro Cap Detachment ───────────────────────────────────────────

def signal_micro_cap_detachment(hist_df: pd.DataFrame) -> float:
    """
    Signal 6 — Micro Cap Detachment (max 1.5 pts).

    Market cap proxy = close price * avg volume (as proxy if shares not available)
    If stock's 60-day avg daily turnover is under ₹5 crore but price has moved >50%
    = extremely easy to manipulate.

    Turnover = close * volume (in rupees)
    ₹5 crore = 5,000,000 (5e6)
    ₹1 crore = 1,000,000 (1e6)

    low liquidity (avg turnover <₹5Cr) + high move (>50%) = 1.0 pt
    very low liquidity (avg turnover <₹1Cr) + high move (>50%) = 1.5 pts
    """
    if hist_df is None or len(hist_df) < 30:
        return 0.0

    window_60 = hist_df.tail(60)
    if len(window_60) < 30:
        return 0.0

    # Compute daily turnover
    if "turnover" in window_60.columns and not window_60["turnover"].isna().all():
        # Use existing turnover column if available (in lakhs typically, convert to rupees)
        avg_turnover = window_60["turnover"].dropna().mean()
        # If turnover is in lakhs, multiply by 100000
        # Check magnitude: if avg < 10000, likely in lakhs
        if avg_turnover < 10000:
            avg_turnover_rupees = avg_turnover * 100000  # lakhs to rupees
        else:
            avg_turnover_rupees = avg_turnover
    elif "close" in window_60.columns and "total_volume" in window_60.columns:
        # Compute turnover as close * volume
        valid = window_60.dropna(subset=["close", "total_volume"])
        if len(valid) < 20:
            return 0.0
        daily_turnover = valid["close"] * valid["total_volume"]
        avg_turnover_rupees = daily_turnover.mean()
    else:
        return 0.0

    if avg_turnover_rupees <= 0 or pd.isna(avg_turnover_rupees):
        return 0.0

    # 60-day price move
    first_close = window_60.iloc[0]["close"]
    last_close = window_60.iloc[-1]["close"]
    if first_close is None or first_close == 0 or pd.isna(first_close):
        return 0.0
    price_move_60d = (float(last_close) - float(first_close)) / float(first_close) * 100

    if price_move_60d <= 50:
        return 0.0

    # 1 crore = 10,000,000 (1e7); 5 crore = 50,000,000 (5e7)
    FIVE_CRORE = 50_000_000   # ₹5 crore
    ONE_CRORE = 10_000_000    # ₹1 crore

    if avg_turnover_rupees < ONE_CRORE:
        return 1.5
    elif avg_turnover_rupees < FIVE_CRORE:
        return 1.0
    return 0.0


# ── Signal 7: Reversal Risk Score ────────────────────────────────────────────

def signal_reversal_risk(hist_df: pd.DataFrame) -> float:
    """
    Signal 7 — Reversal Risk Score (max 2.0 pts).

    Look at the last 5 trading days specifically:
      - Is price starting to decline after a long up-trend? (last 5d return < -2%)
      - Is delivery % suddenly SPIKING on down days? (delivery > 50% on down days)
      - Did volume suddenly DROP after being elevated? (last 5d avg vol < 50% of 30d avg vol)

    2 of 3 reversal signs present = 1.0 pt (DISTRIBUTION)
    All 3 reversal signs present = 2.0 pts (DUMP STARTING)
    """
    if hist_df is None or len(hist_df) < 15:
        return 0.0

    window_30 = hist_df.tail(30)
    window_5 = hist_df.tail(5)

    if len(window_5) < 3 or len(window_30) < 10:
        return 0.0

    signs = 0

    # Sign 1: Price declining after up-trend (last 5d return < -2%)
    first_5d_close = window_5.iloc[0]["close"]
    last_5d_close = window_5.iloc[-1]["close"]
    if (first_5d_close is not None and first_5d_close != 0 and
            not pd.isna(first_5d_close) and not pd.isna(last_5d_close)):
        last_5d_return = (float(last_5d_close) - float(first_5d_close)) / float(first_5d_close) * 100
        if last_5d_return < -2.0:
            signs += 1

    # Sign 2: Delivery spiking on down days (delivery > 50% on down days in last 5d)
    if "delivery_pct" in window_5.columns and "pct_change" in window_5.columns:
        down_days_5d = window_5[window_5["pct_change"] < 0]
        if len(down_days_5d) > 0:
            high_delivery_down = (down_days_5d["delivery_pct"] > 50.0).any()
            if high_delivery_down:
                signs += 1

    # Sign 3: Volume suddenly dropped (last 5d avg vol < 50% of 30d avg vol)
    avg_vol_30d = window_30["total_volume"].mean()
    avg_vol_5d = window_5["total_volume"].mean()
    if (avg_vol_30d is not None and avg_vol_30d > 0 and
            not pd.isna(avg_vol_30d) and not pd.isna(avg_vol_5d)):
        if avg_vol_5d < 0.5 * avg_vol_30d:
            signs += 1

    if signs >= 3:
        return 2.0
    elif signs >= 2:
        return 1.0
    return 0.0


# ── Phase Classification ──────────────────────────────────────────────────────

def classify_phase(total_score: float, signal_7: float) -> str:
    """
    Classify the manipulation phase based on total_score and signal_7.

    Score 0-3:   CLEAN
    Score 3-5:   WATCH
    Score 5-7:   PUMP PHASE
    Score 6-8 + Signal 7 firing: DISTRIBUTION
    Score 8-10:  EXTREME
    """
    if total_score >= 8.0:
        return "EXTREME"
    elif total_score >= 6.0 and signal_7 > 0:
        return "DISTRIBUTION"
    elif total_score >= 5.0:
        return "PUMP PHASE"
    elif total_score >= 3.0:
        return "WATCH"
    else:
        return "CLEAN"


# ── Noise Eliminator ──────────────────────────────────────────────────────────

def apply_noise_filter(
    symbol: str,
    hist_df: pd.DataFrame,
    target_date: str,
    results_symbols_5d: set,
) -> tuple[bool, str]:
    """
    Apply pre-filters. Returns (should_skip, reason).

    Filters:
    1. Stock is in Nifty 50 or Nifty Next 50
    2. Stock had quarterly results in last 5 trading days
    3. Stock's 90-day average daily turnover > ₹100 crore
    4. Stock has been listed for less than 6 months (< 120 days of data ideally).
       Practical minimum: 60 trading days (needed for 60-day window signals).
       When the database has < 120 days of total history, we use 60 days as the
       minimum so the engine can still score stocks with available data.
    """
    # Filter 1: Large cap
    if symbol in LARGE_CAP_SYMBOLS:
        return True, "large_cap"

    # Filter 4: Insufficient history
    # Require at least 60 days of data (minimum for 60-day window signals).
    # The ideal is 120 days (~6 months), but we use 60 as the practical floor.
    if hist_df is None or len(hist_df) < 60:
        return True, "insufficient_history"

    # Filter 2: Recent results announcement
    if symbol in results_symbols_5d:
        return True, "recent_results"

    # Filter 3: Too liquid (90-day avg turnover > ₹100 crore)
    # 1 crore = 10,000,000 (1e7); 100 crore = 1,000,000,000 (1e9)
    HUNDRED_CRORE = 1_000_000_000  # ₹100 crore in rupees
    window_90 = hist_df.tail(90)
    if "close" in window_90.columns and "total_volume" in window_90.columns:
        valid = window_90.dropna(subset=["close", "total_volume"])
        if len(valid) > 0:
            daily_turnover = valid["close"] * valid["total_volume"]
            avg_turnover = daily_turnover.mean()
            if avg_turnover > HUNDRED_CRORE:
                return True, "too_liquid"

    return False, ""


# ── Main Scoring Logic ────────────────────────────────────────────────────────

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


def ensure_manipulation_scores_schema(conn: sqlite3.Connection):
    """
    Ensure manipulation_scores table has the new signal columns.
    Adds new columns if they don't exist (for backward compatibility).
    """
    c = conn.cursor()
    c.execute("PRAGMA table_info(manipulation_scores)")
    existing_cols = {row[1] for row in c.fetchall()}

    new_cols = [
        ("signal_1", "REAL"),
        ("signal_2", "REAL"),
        ("signal_3", "REAL"),
        ("signal_4", "REAL"),
        ("signal_5", "REAL"),
        ("signal_6", "REAL"),
        ("signal_7", "REAL"),
    ]
    for col_name, col_type in new_cols:
        if col_name not in existing_cols:
            try:
                conn.execute(f"ALTER TABLE manipulation_scores ADD COLUMN {col_name} {col_type}")
            except Exception:
                pass
    conn.commit()


def run_scoring(target_date: str):
    """
    Compute time-series based manipulation scores for all symbols.
    Uses last 90 days of daily_prices for each stock.
    Returns count of rows inserted.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print(f"\n{'='*70}")
    print(f"  Daily Manipulation Tracker — Time-Series Scoring Engine")
    print(f"  Date: {target_date}")
    print(f"  Philosophy: Catching grinding pumps, not single-day moves")
    print(f"{'='*70}\n")

    # Ensure schema is up to date
    ensure_index_prices_table(conn)
    ensure_manipulation_scores_schema(conn)

    # ── Load all symbols present on target_date ───────────────────────────────
    c = conn.cursor()
    c.execute("""
        SELECT DISTINCT symbol FROM daily_prices WHERE date = ?
    """, (target_date,))
    symbol_rows = c.fetchall()

    if not symbol_rows:
        print(f"❌ No data in daily_prices for date {target_date}. Exiting.")
        conn.close()
        return 0

    all_symbols = [row[0] for row in symbol_rows]
    print(f"✅ Found {len(all_symbols)} symbols on {target_date}")

    # ── Load 90 days of historical data for all symbols ───────────────────────
    date_90d_ago = (
        datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=120)
    ).strftime("%Y-%m-%d")

    print(f"📊 Loading historical data from {date_90d_ago} to {target_date}...")
    hist_data = pd.read_sql_query(
        """
        SELECT symbol, date, open, high, low, close, prev_close,
               pct_change, total_volume, delivery_volume, delivery_pct, trades
        FROM daily_prices
        WHERE date >= ? AND date <= ?
        ORDER BY symbol, date ASC
        """,
        conn,
        params=(date_90d_ago, target_date),
    )
    print(f"✅ Loaded {len(hist_data)} rows of historical data")

    # ── Load Nifty 500 index data ─────────────────────────────────────────────
    index_df = None
    try:
        index_df = pd.read_sql_query(
            """
            SELECT date, close FROM index_prices
            WHERE index_name = 'NIFTY 500'
              AND date >= ? AND date <= ?
            ORDER BY date ASC
            """,
            conn,
            params=(date_90d_ago, target_date),
        )
        if len(index_df) > 0:
            print(f"✅ Loaded {len(index_df)} rows of NIFTY 500 index data")
        else:
            print("⚠️  No NIFTY 500 index data found — Signal 4 will score 0")
            index_df = None
    except Exception as e:
        print(f"⚠️  Could not load index data: {e} — Signal 4 will score 0")
        index_df = None

    # ── Load recent results announcements (last 5 trading days) ──────────────
    date_5d_ago = (
        datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=7)
    ).strftime("%Y-%m-%d")

    results_symbols_5d = set()
    try:
        c.execute("""
            SELECT DISTINCT symbol FROM corporate_events
            WHERE date >= ? AND date <= ?
              AND (event_type LIKE '%results%' OR event_type LIKE '%dividend%'
                   OR description LIKE '%quarterly results%'
                   OR description LIKE '%financial results%')
        """, (date_5d_ago, target_date))
        results_symbols_5d = {row[0] for row in c.fetchall()}
        print(f"✅ Found {len(results_symbols_5d)} symbols with recent results/dividends")
    except Exception:
        print("⚠️  Could not load corporate events — results filter skipped")

    # ── Group historical data by symbol ──────────────────────────────────────
    print(f"\n🔄 Processing {len(all_symbols)} symbols in batches of 200...")
    symbol_hist = {}
    for sym, grp in hist_data.groupby("symbol"):
        symbol_hist[sym] = grp.reset_index(drop=True)

    # ── Score each symbol ─────────────────────────────────────────────────────
    score_rows = []
    filtered_count = 0
    above_5_count = 0
    scored_count = 0

    # Process in batches of 200
    batch_size = 200
    symbol_batches = [
        all_symbols[i:i + batch_size]
        for i in range(0, len(all_symbols), batch_size)
    ]

    all_results = []

    for batch_idx, batch in enumerate(symbol_batches):
        for symbol in batch:
            hist_df = symbol_hist.get(symbol)

            # Apply noise filter
            should_skip, skip_reason = apply_noise_filter(
                symbol, hist_df, target_date, results_symbols_5d
            )
            if should_skip:
                filtered_count += 1
                continue

            scored_count += 1

            # Compute all 7 signals
            s1 = signal_abnormal_consistency(hist_df)
            s2 = signal_chronic_low_delivery(hist_df)
            s3 = signal_steady_grind(hist_df)
            s4 = signal_price_detachment(hist_df, index_df)
            s5 = signal_velocity_fingerprint(hist_df)
            s6 = signal_micro_cap_detachment(hist_df)
            s7 = signal_reversal_risk(hist_df)

            total = s1 + s2 + s3 + s4 + s5 + s6 + s7
            phase = classify_phase(total, s7)

            if total > 5.0:
                above_5_count += 1

            # Build signals triggered string
            triggered = []
            if s1 > 0: triggered.append(f"s1={s1:.1f}")
            if s2 > 0: triggered.append(f"s2={s2:.1f}")
            if s3 > 0: triggered.append(f"s3={s3:.1f}")
            if s4 > 0: triggered.append(f"s4={s4:.1f}")
            if s5 > 0: triggered.append(f"s5={s5:.1f}")
            if s6 > 0: triggered.append(f"s6={s6:.1f}")
            if s7 > 0: triggered.append(f"s7={s7:.1f}")
            signals_triggered_str = ",".join(triggered) if triggered else ""

            all_results.append({
                "symbol": symbol,
                "total": round(total, 4),
                "s1": round(s1, 4),
                "s2": round(s2, 4),
                "s3": round(s3, 4),
                "s4": round(s4, 4),
                "s5": round(s5, 4),
                "s6": round(s6, 4),
                "s7": round(s7, 4),
                "phase": phase,
                "signals_triggered": signals_triggered_str,
            })

            score_rows.append((
                target_date,
                symbol,
                round(total, 4),
                round(s1, 4),
                round(s2, 4),
                round(s3, 4),
                round(s4, 4),
                round(s5, 4),
                round(s6, 4),
                round(s7, 4),
                phase,
                signals_triggered_str,
            ))

    # ── INSERT OR REPLACE into manipulation_scores ────────────────────────────
    sql = """
        INSERT OR REPLACE INTO manipulation_scores
            (date, symbol, total_score,
             signal_1, signal_2, signal_3, signal_4,
             signal_5, signal_6, signal_7,
             phase, signals_triggered)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    c.executemany(sql, score_rows)
    conn.commit()

    # ── Print summary ─────────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  SCORING SUMMARY")
    print(f"{'='*70}")
    print(f"  Total stocks scored:                  {scored_count}")
    print(f"  Stocks filtered out (noise eliminator): {filtered_count}")
    print(f"  Stocks scoring above 5:               {above_5_count}")
    print(f"  Total rows written to DB:             {len(score_rows)}")
    print(f"{'='*70}\n")

    # ── Print top 10 ──────────────────────────────────────────────────────────
    all_results.sort(key=lambda x: x["total"], reverse=True)
    top10 = all_results[:10]

    header = (
        f"{'SYMBOL':<15} {'TOTAL':>6} {'S1':>5} {'S2':>5} {'S3':>5} "
        f"{'S4':>5} {'S5':>5} {'S6':>5} {'S7':>5} {'PHASE':<15} SIGNALS"
    )
    sep = "─" * 100
    print("TOP 10 SCORED STOCKS")
    print(sep)
    print(header)
    print(sep)

    for r in top10:
        row_str = (
            f"{r['symbol']:<15} {r['total']:>6.2f} {r['s1']:>5.2f} {r['s2']:>5.2f} "
            f"{r['s3']:>5.2f} {r['s4']:>5.2f} {r['s5']:>5.2f} {r['s6']:>5.2f} "
            f"{r['s7']:>5.2f} {r['phase']:<15} {r['signals_triggered']}"
        )
        print(row_str)

    print(sep)
    print()
    print("SIGNAL LEGEND:")
    print("  S1 = Abnormal Consistency (vol >2x avg for many days)")
    print("  S2 = Chronic Low Delivery on Up Days")
    print("  S3 = Steady Grind Pattern (low volatility, relentlessly positive)")
    print("  S4 = Price Detachment from Market (vs NIFTY 500)")
    print("  S5 = Velocity Fingerprint (high return + high smoothness)")
    print("  S6 = Micro Cap Detachment (low liquidity + high move)")
    print("  S7 = Reversal Risk Score (distribution/dump starting)")
    print()
    print("PHASE LEGEND:")
    print("  CLEAN        = Score 0-3, no unusual pattern")
    print("  WATCH        = Score 3-5, unusual pattern developing")
    print("  PUMP PHASE   = Score 5-7, active accumulation/pumping likely")
    print("  DISTRIBUTION = Score 6-8 + S7 firing, operators exiting")
    print("  EXTREME      = Score 8+, textbook manipulation pattern")
    print(sep)

    # ── RMDRIP specific query ─────────────────────────────────────────────────
    print("\n" + "="*70)
    print("  RMDRIP — LAST 30 DAYS OF SCORES")
    print("="*70)

    try:
        date_30d_ago = (
            datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=45)
        ).strftime("%Y-%m-%d")

        rmdrip_scores = pd.read_sql_query(
            """
            SELECT date, total_score, signal_1, signal_2, signal_3,
                   signal_4, signal_5, signal_6, signal_7, phase, signals_triggered
            FROM manipulation_scores
            WHERE symbol = 'RMDRIP'
              AND date >= ?
            ORDER BY date DESC
            LIMIT 30
            """,
            conn,
            params=(date_30d_ago,),
        )

        if len(rmdrip_scores) == 0:
            # Check if RMDRIP exists in daily_prices at all
            c.execute("SELECT COUNT(*) FROM daily_prices WHERE symbol = 'RMDRIP'")
            count = c.fetchone()[0]
            if count == 0:
                print("  ℹ️  RMDRIP not found in daily_prices database.")
                print("  (Stock may not be in NSE EQ series or data not yet downloaded)")
            else:
                print(f"  ℹ️  RMDRIP found in daily_prices ({count} rows) but no scores yet.")
                print("  (Run scoring engine after data pipeline to generate scores)")
        else:
            rmdrip_header = (
                f"  {'DATE':<12} {'TOTAL':>6} {'S1':>5} {'S2':>5} {'S3':>5} "
                f"{'S4':>5} {'S5':>5} {'S6':>5} {'S7':>5} {'PHASE'}"
            )
            print(rmdrip_header)
            print("  " + "─" * 80)
            for _, row in rmdrip_scores.iterrows():
                print(
                    f"  {str(row['date']):<12} {row['total_score']:>6.2f} "
                    f"{_safe_val(row.get('signal_1')):>5} "
                    f"{_safe_val(row.get('signal_2')):>5} "
                    f"{_safe_val(row.get('signal_3')):>5} "
                    f"{_safe_val(row.get('signal_4')):>5} "
                    f"{_safe_val(row.get('signal_5')):>5} "
                    f"{_safe_val(row.get('signal_6')):>5} "
                    f"{_safe_val(row.get('signal_7')):>5} "
                    f"{row['phase']}"
                )
    except Exception as e:
        print(f"  ⚠️  Could not query RMDRIP scores: {e}")

    print("="*70 + "\n")

    conn.close()
    return len(score_rows)


def _safe_val(val):
    """Format a float value safely for display."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return " N/A"
    try:
        return f"{float(val):.2f}"
    except (TypeError, ValueError):
        return " N/A"


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Time-series based manipulation scoring engine. "
            "Detects grinding pumps over weeks, not single-day moves."
        )
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date in YYYY-MM-DD format (default: most recent date in daily_prices)",
    )
    args = parser.parse_args()

    # Determine target date
    if args.date:
        target_date = args.date
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            print(f"❌ Invalid date format: {target_date}. Use YYYY-MM-DD.")
            return
    else:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT MAX(date) FROM daily_prices")
        row = c.fetchone()
        conn.close()
        if not row or not row[0]:
            print("❌ No data found in daily_prices. Run data_pipeline.py first.")
            return
        target_date = row[0]
        print(f"ℹ️  No --date specified. Using most recent date: {target_date}")

    inserted = run_scoring(target_date)
    print(f"✅ Done. {inserted} rows written to manipulation_scores for {target_date}.")


if __name__ == "__main__":
    main()
