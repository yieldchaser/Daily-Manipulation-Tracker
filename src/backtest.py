"""
backtest.py — 90-day historical backtest for the Daily Manipulation Tracker.

Downloads 90 calendar days of NSE Bhavcopy data for target symbols,
runs the scoring engine day-by-day, and prints a score timeline table
with goal verification summary.

Usage:
    python src/backtest.py
"""

import io
import logging
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "tracker.db")

# Add src to sys.path so we can import scoring_engine
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.WARNING,  # Suppress INFO noise during backtest
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Import scoring functions ───────────────────────────────────────────────────
from scoring_engine import (
    signal_volume,
    signal_delivery,
    signal_circuit,
    signal_velocity,
    signal_corp_event,
    signal_pref_allot,
    signal_bulk_deal,
    classify_phase,
)

# ── Import data pipeline helpers ──────────────────────────────────────────────
from data_pipeline import (
    normalise_full_bhavcopy,
    normalise_cm_bhavcopy,
    upsert_daily_prices,
    compute_and_upsert_rolling_stats,
    _safe_float,
    _safe_int,
)

# ── Target symbols ────────────────────────────────────────────────────────────
PRIMARY_SYMBOLS = ["RMDRIP", "SILVERLINE", "BGDL"]
FALLBACK_SYMBOLS = ["TEJASNET", "JPPOWER"]


# ── Date helpers ──────────────────────────────────────────────────────────────

def is_weekend(dt: date) -> bool:
    return dt.weekday() >= 5  # Saturday=5, Sunday=6


def get_trading_days(start: date, end: date) -> list:
    """Return list of weekday dates from start to end (inclusive)."""
    days = []
    current = start
    while current <= end:
        if not is_weekend(current):
            days.append(current)
        current += timedelta(days=1)
    return days


# ── Bhavcopy download ─────────────────────────────────────────────────────────

def download_bhavcopy_for_date(dt: date):
    """
    Try full_bhavcopy_raw first, then bhavcopy_raw as fallback.
    Returns a normalised DataFrame or None.
    """
    from jugaad_data.nse import NSEArchives
    nse = NSEArchives()

    # Try full bhavcopy (has delivery data)
    try:
        text = nse.full_bhavcopy_raw(dt)
        if text and len(text.strip()) > 100:
            df = pd.read_csv(io.StringIO(text))
            df.columns = [c.strip() for c in df.columns]
            if len(df) > 0:
                return normalise_full_bhavcopy(df, dt), "full"
    except Exception as exc:
        log.debug("full_bhavcopy_raw failed for %s: %s", dt, exc)

    # Fallback to CM bhavcopy
    try:
        text = nse.bhavcopy_raw(dt)
        if text and len(text.strip()) > 100:
            df = pd.read_csv(io.StringIO(text))
            df.columns = [c.strip() for c in df.columns]
            if len(df) > 0:
                return normalise_cm_bhavcopy(df, dt), "cm"
    except Exception as exc:
        log.debug("bhavcopy_raw failed for %s: %s", dt, exc)

    return None, None


# ── Fallback symbol selection ─────────────────────────────────────────────────

def get_fallback_symbols_from_db(conn: sqlite3.Connection, n: int = 3) -> list:
    """
    Get top N symbols by total_score from manipulation_scores table.
    Used when primary symbols have no data.
    """
    c = conn.cursor()
    c.execute("""
        SELECT symbol, SUM(total_score) as total
        FROM manipulation_scores
        GROUP BY symbol
        ORDER BY total DESC
        LIMIT ?
    """, (n,))
    rows = c.fetchall()
    return [row[0] for row in rows]


# ── Per-symbol scoring (without DB write) ─────────────────────────────────────

def score_symbol_for_date(conn: sqlite3.Connection, symbol: str, target_date: str) -> dict | None:
    """
    Compute manipulation score for a single symbol on target_date.
    Returns a dict with all signal scores, or None if no data.
    """
    c = conn.cursor()

    # Load daily_prices for this symbol/date
    c.execute("""
        SELECT pct_change, delivery_pct, close
        FROM daily_prices
        WHERE date = ? AND symbol = ?
    """, (target_date, symbol))
    price_row = c.fetchone()
    if price_row is None:
        return None

    pct_change, delivery_pct, close = price_row

    # Load rolling_stats
    c.execute("""
        SELECT vol_ratio, price_change_30d, price_change_60d,
               upper_circuit_streak, week_52_high, week_52_low
        FROM rolling_stats
        WHERE date = ? AND symbol = ?
    """, (target_date, symbol))
    rolling_row = c.fetchone()

    vol_ratio = None
    price_change_30d = None
    price_change_60d = None
    upper_circuit_streak = 0
    week_52_high = None

    if rolling_row:
        vol_ratio, price_change_30d, price_change_60d, upper_circuit_streak, week_52_high, _ = rolling_row
        upper_circuit_streak = upper_circuit_streak or 0

    # Load corporate events
    date_30d_ago = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
    date_548d_ago = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=548)).strftime("%Y-%m-%d")

    c.execute("""
        SELECT date, event_type, description
        FROM corporate_events
        WHERE symbol = ? AND date >= ? AND date <= ?
    """, (symbol, date_548d_ago, target_date))
    corp_rows = c.fetchall()

    results_dividend_today = False
    events_30d = []
    events_548d = []

    for ev_date, event_type, description in corp_rows:
        et = (event_type or "").lower()
        if ev_date == target_date and ("results" in et or "dividend" in et):
            results_dividend_today = True
        if ev_date >= date_30d_ago:
            events_30d.append((event_type, description))
        events_548d.append((event_type, description))

    # Load bulk deals
    date_90d_ago = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=90)).strftime("%Y-%m-%d")
    date_7d_ago = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")

    c.execute("""
        SELECT date, client_name
        FROM bulk_deals
        WHERE symbol = ? AND date >= ? AND date <= ?
    """, (symbol, date_90d_ago, target_date))
    bulk_rows = c.fetchall()

    today_clients = []
    deals_7d = 0
    clients_90d = set()

    for ev_date, client_name in bulk_rows:
        if ev_date == target_date:
            today_clients.append(client_name)
        if ev_date >= date_7d_ago:
            deals_7d += 1
        if ev_date < target_date:
            clients_90d.add(client_name)

    # Compute signals
    s_volume = signal_volume(vol_ratio, results_dividend_today)
    s_delivery = signal_delivery(pct_change, delivery_pct)
    s_circuit = signal_circuit(upper_circuit_streak)
    s_velocity = signal_velocity(price_change_60d)
    s_corp_event = signal_corp_event(events_30d, price_change_30d)
    s_pref_allot = signal_pref_allot(events_548d)
    s_bulk_deal = signal_bulk_deal(today_clients, deals_7d, clients_90d)

    total = s_volume + s_delivery + s_circuit + s_velocity + s_corp_event + s_pref_allot + s_bulk_deal
    phase = classify_phase(total, vol_ratio, price_change_30d, delivery_pct, week_52_high, close)

    return {
        "date": target_date,
        "symbol": symbol,
        "total_score": round(total, 4),
        "signal_volume": round(s_volume, 4),
        "signal_delivery": round(s_delivery, 4),
        "signal_circuit": round(s_circuit, 4),
        "signal_velocity": round(s_velocity, 4),
        "signal_corp_event": round(s_corp_event, 4),
        "signal_pref_allot": round(s_pref_allot, 4),
        "signal_bulk_deal": round(s_bulk_deal, 4),
        "phase": phase,
    }


def upsert_score(conn: sqlite3.Connection, score: dict):
    """Insert or replace a score row into manipulation_scores."""
    sql = """
        INSERT OR REPLACE INTO manipulation_scores
            (date, symbol, total_score, signal_volume, signal_delivery,
             signal_circuit, signal_velocity, signal_corp_event,
             signal_pref_allot, signal_bulk_deal, phase, signals_triggered)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    triggered = []
    if score["signal_volume"] > 0:
        triggered.append("volume")
    if score["signal_delivery"] > 0:
        triggered.append("delivery")
    if score["signal_circuit"] > 0:
        triggered.append("circuit")
    if score["signal_velocity"] > 0:
        triggered.append("velocity")
    if score["signal_corp_event"] > 0:
        triggered.append("corp_event")
    if score["signal_pref_allot"] > 0:
        triggered.append("pref_allot")
    if score["signal_bulk_deal"] > 0:
        triggered.append("bulk_deal")

    conn.execute(sql, (
        score["date"],
        score["symbol"],
        score["total_score"],
        score["signal_volume"],
        score["signal_delivery"],
        score["signal_circuit"],
        score["signal_velocity"],
        score["signal_corp_event"],
        score["signal_pref_allot"],
        score["signal_bulk_deal"],
        score["phase"],
        ",".join(triggered),
    ))
    conn.commit()


# ── Print score timeline ───────────────────────────────────────────────────────

def print_score_timeline(symbol: str, timeline: list):
    """
    Print a formatted score timeline table for a symbol.
    timeline: list of score dicts sorted by date.
    """
    print(f"\n{'='*80}")
    print(f"=== {symbol} Score Timeline (90 days) ===")
    print(f"{'='*80}")

    header = (
        f"{'DATE':<12} {'SCORE':>6} {'VOL':>5} {'DELIV':>6} {'CIRC':>5} "
        f"{'VEL':>5} {'CORP':>5} {'PREF':>5} {'BULK':>5}  {'PHASE'}"
    )
    sep = "-" * len(header)
    print(header)
    print(sep)

    for s in timeline:
        print(
            f"{s['date']:<12} {s['total_score']:>6.1f} {s['signal_volume']:>5.1f} "
            f"{s['signal_delivery']:>6.1f} {s['signal_circuit']:>5.1f} "
            f"{s['signal_velocity']:>5.1f} {s['signal_corp_event']:>5.1f} "
            f"{s['signal_pref_allot']:>5.1f} {s['signal_bulk_deal']:>5.1f}  "
            f"{s['phase']}"
        )

    print(sep)


# ── Goal verification ─────────────────────────────────────────────────────────

def print_goal_verification(symbol: str, timeline: list):
    """Print goal verification summary for a symbol."""
    if not timeline:
        print(f"\n  {symbol}: No data available.")
        return

    scores = [(s["date"], s["total_score"]) for s in timeline]
    max_score = max(s for _, s in scores)
    max_date = next(d for d, s in scores if s == max_score)

    reached_6 = any(s >= 6.0 for _, s in scores)
    first_3 = next((d for d, s in scores if s >= 3.0), None)
    first_5 = next((d for d, s in scores if s >= 5.0), None)
    first_7 = next((d for d, s in scores if s >= 7.0), None)

    print(f"\n--- {symbol} Goal Verification ---")
    print(f"  Max score: {max_score:.1f} on {max_date}")
    print(f"  Score reached 6+: {'YES' if reached_6 else 'NO'}")
    print(f"  First date score crossed 3: {first_3 if first_3 else 'Never'}")
    print(f"  First date score crossed 5: {first_5 if first_5 else 'Never'}")
    print(f"  First date score crossed 7: {first_7 if first_7 else 'Never'}")


# ── Main backtest logic ───────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 80)
    print("  Daily Manipulation Tracker — 90-Day Backtest")
    print("=" * 80)

    # ── Connect to DB ──────────────────────────────────────────────────────────
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found at {DB_PATH}. Run src/create_db.py first.")
        sys.exit(1)
    conn = sqlite3.connect(DB_PATH)
    print(f"✅ Connected to database: {DB_PATH}")

    # ── Determine date range ───────────────────────────────────────────────────
    today = date.today()
    start_date = today - timedelta(days=90)
    trading_days = get_trading_days(start_date, today)
    print(f"\n📅 Date range: {start_date} → {today}")
    print(f"📅 Trading days to process: {len(trading_days)}")

    # ── Step 1: Determine target symbols ──────────────────────────────────────
    print(f"\n🎯 Primary target symbols: {', '.join(PRIMARY_SYMBOLS)}")

    # ── Step 2: Download historical data ──────────────────────────────────────
    print(f"\n{'='*80}")
    print("  PHASE 1: Downloading historical bhavcopy data")
    print(f"{'='*80}")

    skipped_dates = []
    loaded_dates = []
    symbol_date_counts = {sym: 0 for sym in PRIMARY_SYMBOLS}

    for dt in trading_days:
        date_str = dt.strftime("%Y-%m-%d")
        print(f"Loading {date_str}...", end=" ", flush=True)

        df_norm, bhavcopy_type = download_bhavcopy_for_date(dt)

        if df_norm is None:
            print("skipped (no data)")
            skipped_dates.append(date_str)
            time.sleep(1)
            continue

        # Filter to EQ series only
        if "series" in df_norm.columns:
            df_eq = df_norm[df_norm["series"].str.strip() == "EQ"].copy()
        else:
            df_eq = df_norm.copy()

        if len(df_eq) == 0:
            print("skipped (no EQ rows)")
            skipped_dates.append(date_str)
            time.sleep(1)
            continue

        # Filter to target symbols only (to keep DB small for backtest)
        df_target = df_eq[df_eq["symbol"].isin(PRIMARY_SYMBOLS)].copy()

        # Count how many target symbols found
        found_symbols = df_target["symbol"].tolist()
        for sym in found_symbols:
            if sym in symbol_date_counts:
                symbol_date_counts[sym] += 1

        # Insert ALL EQ data (needed for rolling stats computation)
        # But only insert target symbols to keep it manageable
        # Actually we need all data for rolling stats context — insert target symbols only
        # since we only care about scoring those symbols
        if len(df_target) > 0:
            try:
                upsert_daily_prices(conn, df_target)
            except Exception as exc:
                print(f"  ⚠️  daily_prices insert error: {exc}")

            # Compute rolling stats for target symbols
            try:
                compute_and_upsert_rolling_stats(conn, date_str)
            except Exception as exc:
                print(f"  ⚠️  rolling_stats error: {exc}")

            loaded_dates.append(date_str)
            print(f"done ({len(found_symbols)} symbols: {', '.join(found_symbols)})")
        else:
            print(f"done (0 target symbols found in bhavcopy)")
            loaded_dates.append(date_str)

        time.sleep(1)

    print(f"\n✅ Data loading complete.")
    print(f"   Loaded dates: {len(loaded_dates)}")
    print(f"   Skipped dates: {len(skipped_dates)}")
    if skipped_dates:
        print(f"   Skipped: {', '.join(skipped_dates[:10])}{'...' if len(skipped_dates) > 10 else ''}")

    # ── Check if primary symbols have data ────────────────────────────────────
    print(f"\n📊 Symbol data counts (primary symbols):")
    for sym, count in symbol_date_counts.items():
        print(f"   {sym}: {count} days")

    symbols_with_data = [sym for sym, count in symbol_date_counts.items() if count > 0]

    if not symbols_with_data:
        print(f"\n⚠️  No data found for primary symbols {PRIMARY_SYMBOLS}.")
        print("   Falling back to top 3 symbols from existing manipulation_scores...")
        fallback = get_fallback_symbols_from_db(conn, 3)
        if not fallback:
            print("❌ No fallback symbols found in manipulation_scores. Exiting.")
            conn.close()
            sys.exit(1)
        target_symbols = fallback
        print(f"   Using fallback symbols: {', '.join(target_symbols)}")

        # Re-download data for fallback symbols
        print(f"\n{'='*80}")
        print("  PHASE 1b: Re-downloading data for fallback symbols")
        print(f"{'='*80}")

        fallback_date_counts = {sym: 0 for sym in target_symbols}

        for dt in trading_days:
            date_str = dt.strftime("%Y-%m-%d")
            print(f"Loading {date_str}...", end=" ", flush=True)

            df_norm, bhavcopy_type = download_bhavcopy_for_date(dt)

            if df_norm is None:
                print("skipped (no data)")
                time.sleep(1)
                continue

            if "series" in df_norm.columns:
                df_eq = df_norm[df_norm["series"].str.strip() == "EQ"].copy()
            else:
                df_eq = df_norm.copy()

            df_target = df_eq[df_eq["symbol"].isin(target_symbols)].copy()
            found_symbols = df_target["symbol"].tolist()

            for sym in found_symbols:
                if sym in fallback_date_counts:
                    fallback_date_counts[sym] += 1

            if len(df_target) > 0:
                try:
                    upsert_daily_prices(conn, df_target)
                    compute_and_upsert_rolling_stats(conn, date_str)
                except Exception as exc:
                    print(f"  ⚠️  Error: {exc}")
                print(f"done ({len(found_symbols)} symbols)")
            else:
                print(f"done (0 target symbols)")

            time.sleep(1)

        print(f"\n📊 Fallback symbol data counts:")
        for sym, count in fallback_date_counts.items():
            print(f"   {sym}: {count} days")
    else:
        target_symbols = symbols_with_data
        # Also check fallback symbols if we have fewer than 3 primary symbols with data
        if len(target_symbols) < 3:
            print(f"\n⚠️  Only {len(target_symbols)} primary symbol(s) have data.")
            print("   Checking fallback symbols...")
            for sym in FALLBACK_SYMBOLS:
                if len(target_symbols) >= 3:
                    break
                # Check if fallback symbol has data in DB
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM daily_prices WHERE symbol = ?", (sym,))
                cnt = c.fetchone()[0]
                if cnt > 0:
                    target_symbols.append(sym)
                    print(f"   Added fallback symbol: {sym} ({cnt} rows)")

    print(f"\n🎯 Final target symbols for backtest: {', '.join(target_symbols)}")

    # ── Step 3: Run scoring engine day-by-day ─────────────────────────────────
    print(f"\n{'='*80}")
    print("  PHASE 2: Running scoring engine day-by-day")
    print(f"{'='*80}")

    # Get all dates that have data for at least one target symbol
    c = conn.cursor()
    c.execute("""
        SELECT DISTINCT date FROM daily_prices
        WHERE symbol IN ({})
        ORDER BY date
    """.format(",".join("?" * len(target_symbols))), target_symbols)
    dates_with_data = [row[0] for row in c.fetchall()]

    print(f"📅 Dates with data for target symbols: {len(dates_with_data)}")

    # Score each symbol for each date
    all_timelines = {sym: [] for sym in target_symbols}

    for date_str in dates_with_data:
        for symbol in target_symbols:
            score = score_symbol_for_date(conn, symbol, date_str)
            if score is not None:
                all_timelines[symbol].append(score)
                upsert_score(conn, score)

    print(f"✅ Scoring complete.")
    for sym in target_symbols:
        print(f"   {sym}: {len(all_timelines[sym])} scored dates")

    # ── Step 4: Print score timelines ─────────────────────────────────────────
    print(f"\n{'='*80}")
    print("  PHASE 3: Score Timelines")
    print(f"{'='*80}")

    for symbol in target_symbols:
        timeline = sorted(all_timelines[symbol], key=lambda x: x["date"])
        if not timeline:
            print(f"\n⚠️  {symbol}: No scored data available.")
            continue
        print_score_timeline(symbol, timeline)

    # ── Step 5: Goal verification ──────────────────────────────────────────────
    print(f"\n{'='*80}")
    print("  PHASE 4: Goal Verification Summary")
    print(f"{'='*80}")

    for symbol in target_symbols:
        timeline = sorted(all_timelines[symbol], key=lambda x: x["date"])
        print_goal_verification(symbol, timeline)

    # ── Final summary ──────────────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print("  BACKTEST COMPLETE")
    print(f"{'='*80}")
    print(f"  Symbols backtested: {', '.join(target_symbols)}")
    print(f"  Date range: {start_date} → {today}")
    print(f"  Trading days attempted: {len(trading_days)}")
    print(f"  Dates with data loaded: {len(loaded_dates)}")
    print(f"  Dates skipped (no data): {len(skipped_dates)}")
    total_scored = sum(len(all_timelines[sym]) for sym in target_symbols)
    print(f"  Total score records: {total_scored}")
    print(f"{'='*80}\n")

    conn.close()


if __name__ == "__main__":
    main()
