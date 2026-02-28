"""
scoring_engine.py — Compute manipulation scores for all symbols on a given date.

Usage:
    python src/scoring_engine.py                  # uses most recent date in daily_prices
    python src/scoring_engine.py --date 2026-02-27
"""

import argparse
import os
import sqlite3
from datetime import datetime, timedelta

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "tracker.db")


# ── Signal helpers ────────────────────────────────────────────────────────────

def signal_volume(vol_ratio, has_results_or_dividend):
    """
    Signal 1 — Volume Anomaly (max 2 pts).
    vol_ratio = today_vol / 30d_avg_vol
    Deduct 1.0 if verified results/dividend event exists.
    """
    if vol_ratio is None:
        return 0.0
    if vol_ratio >= 5.0:
        score = 2.0
    elif vol_ratio >= 3.0:
        score = 1.0
    elif vol_ratio >= 2.0:
        score = 0.5
    else:
        score = 0.0
    if has_results_or_dividend:
        score -= 1.0
    return max(0.0, score)


def signal_delivery(pct_change, delivery_pct):
    """
    Signal 2 — Delivery Divergence (max 2 pts).
    price up >2% AND delivery_pct <25% → 1.0 pt
    price up >2% AND delivery_pct <15% → 2.0 pts (overrides)
    """
    if pct_change is None or delivery_pct is None:
        return 0.0
    if pct_change > 2.0:
        if delivery_pct < 15.0:
            return 2.0
        elif delivery_pct < 25.0:
            return 1.0
    return 0.0


def signal_circuit(upper_circuit_streak):
    """
    Signal 3 — Upper Circuit Streak (max 2 pts).
    3–5 consecutive days → 1.0 pt
    6+ consecutive days → 2.0 pts
    """
    if upper_circuit_streak is None:
        return 0.0
    if upper_circuit_streak >= 6:
        return 2.0
    elif upper_circuit_streak >= 3:
        return 1.0
    return 0.0


def signal_velocity(price_change_60d):
    """
    Signal 4 — Price Velocity (max 1 pt).
    price up >100% vs 60 days ago → 1.0 pt
    """
    if price_change_60d is None:
        return 0.0
    return 1.0 if price_change_60d > 100.0 else 0.0


def signal_corp_event(corp_events_30d, price_change_30d):
    """
    Signal 5 — Corporate Event Cover (max 1 pt).
    bonus or split event in last 30d while price elevated (price_change_30d > 20%) → 0.5 pt
    vague MoU or partnership keyword in last 30d → 0.5 pt
    Both can stack up to 1.0 pt max.
    corp_events_30d: list of (event_type, description) tuples
    """
    if not corp_events_30d:
        return 0.0

    score = 0.0
    bonus_split_score = 0.0
    mou_score = 0.0

    for event_type, description in corp_events_30d:
        et = (event_type or "").lower()
        desc = (description or "").lower()
        combined = et + " " + desc

        # bonus or split while price elevated
        if ("bonus" in combined or "split" in combined):
            if price_change_30d is not None and price_change_30d > 20.0:
                bonus_split_score = 0.5

        # vague MoU or partnership
        if "mou" in combined or "partnership" in combined:
            mou_score = 0.5

    score = bonus_split_score + mou_score
    return min(1.0, score)


def signal_pref_allot(corp_events_548d):
    """
    Signal 6 — Preferential Allotment (max 1 pt).
    Any event with event_type or description containing 'preferential' or 'allotment' → 1.0 pt
    corp_events_548d: list of (event_type, description) tuples
    """
    if not corp_events_548d:
        return 0.0

    for event_type, description in corp_events_548d:
        et = (event_type or "").lower()
        desc = (description or "").lower()
        combined = et + " " + desc
        if "preferential" in combined or "allotment" in combined:
            return 1.0
    return 0.0


def signal_bulk_deal(bulk_deals_today, bulk_deals_7d, bulk_deals_90d_clients):
    """
    Signal 7 — Bulk Deal Anomaly (max 1 pt).
    A bulk deal by a new/unknown entity today (client_name not seen in last 90 days) → 0.5 pt
    Multiple bulk deals for this symbol in the same week (last 7 days) → 1.0 pt (overrides 0.5)
    bulk_deals_today: list of client_name strings for today
    bulk_deals_7d: count of bulk deals in last 7 days (including today)
    bulk_deals_90d_clients: set of client_names seen in last 90 days (excluding today)
    """
    if not bulk_deals_today:
        return 0.0

    # Multiple bulk deals in last 7 days → 1.0 pt
    if bulk_deals_7d > 1:
        return 1.0

    # New/unknown entity today
    for client in bulk_deals_today:
        if client not in bulk_deals_90d_clients:
            return 0.5

    return 0.0


def classify_phase(total_score, vol_ratio, price_change_30d, delivery_pct, week_52_high, close):
    """
    Classify the manipulation phase based on total_score and market conditions.
    """
    if total_score >= 7.0:
        return "EXTREME ALERT"
    elif total_score >= 5.0:
        # DISTRIBUTION: close > 85% of 52-week high AND delivery_pct > 40
        if (week_52_high and week_52_high > 0 and close is not None and
                close > 0.85 * week_52_high and
                delivery_pct is not None and delivery_pct > 40.0):
            return "DISTRIBUTION"
        else:
            return "ELEVATED"
    elif total_score >= 3.0:
        # PUMP: vol_ratio > 2 OR price_change_30d > 20, AND delivery_pct < 30
        vr = vol_ratio if vol_ratio is not None else 0.0
        pc30 = price_change_30d if price_change_30d is not None else 0.0
        dp = delivery_pct if delivery_pct is not None else 100.0
        if (vr > 2.0 or pc30 > 20.0) and dp < 30.0:
            return "PUMP"
        else:
            return "WATCH"
    else:
        return "CLEAN"


def build_plain_english_summary(symbol, vol_ratio, delivery_pct, pct_change,
                                 upper_circuit_streak, phase):
    """
    Generate a plain-English summary for a stock.
    Only include facts that are non-zero/triggered.
    Format: "SYMBOL: volume Xx avg, delivery Y% on up day, Z consecutive upper circuits. Phase: PHASE"
    """
    parts = []

    # Volume
    if vol_ratio is not None and vol_ratio >= 2.0:
        parts.append(f"volume {vol_ratio:.1f}x avg")

    # Delivery on up day
    if pct_change is not None and pct_change > 2.0 and delivery_pct is not None:
        parts.append(f"delivery {delivery_pct:.1f}% on up day")

    # Upper circuits
    if upper_circuit_streak is not None and upper_circuit_streak > 0:
        parts.append(f"{upper_circuit_streak} consecutive upper circuits")

    if parts:
        detail = ", ".join(parts)
        return f"{symbol}: {detail}. Phase: {phase}"
    else:
        return f"{symbol}: no anomalies detected. Phase: {phase}"


# ── Main scoring logic ────────────────────────────────────────────────────────

def run_scoring(target_date: str):
    """
    Compute manipulation scores for all symbols on target_date.
    Returns count of rows inserted.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    print(f"\n{'='*60}")
    print(f"  Daily Manipulation Tracker — Scoring Engine")
    print(f"  Date: {target_date}")
    print(f"{'='*60}\n")

    # ── Load daily_prices for target_date ─────────────────────────────────────
    c.execute("""
        SELECT symbol, pct_change, delivery_pct, close
        FROM daily_prices
        WHERE date = ?
    """, (target_date,))
    prices_rows = c.fetchall()

    if not prices_rows:
        print(f"❌ No data in daily_prices for date {target_date}. Exiting.")
        conn.close()
        return 0

    prices = {row["symbol"]: dict(row) for row in prices_rows}
    symbols = list(prices.keys())
    print(f"✅ Loaded {len(symbols)} symbols from daily_prices")

    # ── Load rolling_stats for target_date ────────────────────────────────────
    c.execute("""
        SELECT symbol, vol_ratio, price_change_30d, price_change_60d,
               upper_circuit_streak, week_52_high, week_52_low
        FROM rolling_stats
        WHERE date = ?
    """, (target_date,))
    rolling_rows = c.fetchall()
    rolling = {row["symbol"]: dict(row) for row in rolling_rows}
    print(f"✅ Loaded {len(rolling)} symbols from rolling_stats")

    # ── Load corporate_events (bulk load, filter in Python) ───────────────────
    # For Signal 1: results/dividend on target_date
    # For Signal 5: bonus/split/mou/partnership in last 30 days
    # For Signal 6: preferential/allotment in last 548 days

    date_30d_ago = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
    date_548d_ago = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=548)).strftime("%Y-%m-%d")

    c.execute("""
        SELECT symbol, date, event_type, description
        FROM corporate_events
        WHERE date >= ? AND date <= ?
    """, (date_548d_ago, target_date))
    corp_event_rows = c.fetchall()

    # Build lookup structures
    # results_dividend_today[symbol] = True if results/dividend on target_date
    results_dividend_today = set()
    # corp_events_30d[symbol] = list of (event_type, description)
    corp_events_30d = {}
    # corp_events_548d[symbol] = list of (event_type, description)
    corp_events_548d = {}

    for row in corp_event_rows:
        sym = row["symbol"]
        et = (row["event_type"] or "").lower()
        desc = (row["description"] or "").lower()
        ev_date = row["date"]

        # Signal 1: results/dividend on target_date
        if ev_date == target_date:
            if "results" in et or "dividend" in et:
                results_dividend_today.add(sym)

        # Signal 5: last 30 days
        if ev_date >= date_30d_ago:
            if sym not in corp_events_30d:
                corp_events_30d[sym] = []
            corp_events_30d[sym].append((row["event_type"], row["description"]))

        # Signal 6: last 548 days
        if sym not in corp_events_548d:
            corp_events_548d[sym] = []
        corp_events_548d[sym].append((row["event_type"], row["description"]))

    print(f"✅ Loaded {len(corp_event_rows)} corporate events")

    # ── Load bulk_deals (bulk load, filter in Python) ─────────────────────────
    date_90d_ago = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=90)).strftime("%Y-%m-%d")
    date_7d_ago = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")

    c.execute("""
        SELECT symbol, date, client_name
        FROM bulk_deals
        WHERE date >= ? AND date <= ?
    """, (date_90d_ago, target_date))
    bulk_deal_rows = c.fetchall()

    # Build lookup structures
    # bulk_deals_today[symbol] = list of client_names
    bulk_deals_today = {}
    # bulk_deals_7d_count[symbol] = count of deals in last 7 days
    bulk_deals_7d_count = {}
    # bulk_deals_90d_clients[symbol] = set of client_names (excluding today)
    bulk_deals_90d_clients = {}

    for row in bulk_deal_rows:
        sym = row["symbol"]
        client = row["client_name"]
        ev_date = row["date"]

        if ev_date == target_date:
            if sym not in bulk_deals_today:
                bulk_deals_today[sym] = []
            bulk_deals_today[sym].append(client)

        if ev_date >= date_7d_ago:
            bulk_deals_7d_count[sym] = bulk_deals_7d_count.get(sym, 0) + 1

        if ev_date < target_date:
            if sym not in bulk_deals_90d_clients:
                bulk_deals_90d_clients[sym] = set()
            bulk_deals_90d_clients[sym].add(client)

    print(f"✅ Loaded {len(bulk_deal_rows)} bulk deal records")

    # ── Compute scores for each symbol ────────────────────────────────────────
    score_rows = []
    summaries = []

    for symbol in symbols:
        p = prices.get(symbol, {})
        r = rolling.get(symbol, {})

        pct_change = p.get("pct_change")
        delivery_pct = p.get("delivery_pct")
        close = p.get("close")

        vol_ratio = r.get("vol_ratio")
        price_change_30d = r.get("price_change_30d")
        price_change_60d = r.get("price_change_60d")
        upper_circuit_streak = r.get("upper_circuit_streak", 0)
        week_52_high = r.get("week_52_high")

        # Signal 1 — Volume Anomaly
        has_results_div = symbol in results_dividend_today
        s_volume = signal_volume(vol_ratio, has_results_div)

        # Signal 2 — Delivery Divergence
        s_delivery = signal_delivery(pct_change, delivery_pct)

        # Signal 3 — Upper Circuit Streak
        s_circuit = signal_circuit(upper_circuit_streak)

        # Signal 4 — Price Velocity
        s_velocity = signal_velocity(price_change_60d)

        # Signal 5 — Corporate Event Cover
        events_30d = corp_events_30d.get(symbol, [])
        s_corp_event = signal_corp_event(events_30d, price_change_30d)

        # Signal 6 — Preferential Allotment
        events_548d = corp_events_548d.get(symbol, [])
        s_pref_allot = signal_pref_allot(events_548d)

        # Signal 7 — Bulk Deal Anomaly
        today_clients = bulk_deals_today.get(symbol, [])
        deals_7d = bulk_deals_7d_count.get(symbol, 0)
        clients_90d = bulk_deals_90d_clients.get(symbol, set())
        s_bulk_deal = signal_bulk_deal(today_clients, deals_7d, clients_90d)

        # Total score
        total = s_volume + s_delivery + s_circuit + s_velocity + s_corp_event + s_pref_allot + s_bulk_deal

        # Phase classification
        phase = classify_phase(total, vol_ratio, price_change_30d, delivery_pct, week_52_high, close)

        # Signals triggered
        triggered = []
        if s_volume > 0:
            triggered.append("volume")
        if s_delivery > 0:
            triggered.append("delivery")
        if s_circuit > 0:
            triggered.append("circuit")
        if s_velocity > 0:
            triggered.append("velocity")
        if s_corp_event > 0:
            triggered.append("corp_event")
        if s_pref_allot > 0:
            triggered.append("pref_allot")
        if s_bulk_deal > 0:
            triggered.append("bulk_deal")

        signals_triggered_str = ",".join(triggered) if triggered else ""

        score_rows.append((
            target_date,
            symbol,
            round(total, 4),
            round(s_volume, 4),
            round(s_delivery, 4),
            round(s_circuit, 4),
            round(s_velocity, 4),
            round(s_corp_event, 4),
            round(s_pref_allot, 4),
            round(s_bulk_deal, 4),
            phase,
            signals_triggered_str,
        ))

        # Plain-English summary
        summary = build_plain_english_summary(
            symbol, vol_ratio, delivery_pct, pct_change, upper_circuit_streak, phase
        )
        summaries.append((total, symbol, summary, {
            "total_score": round(total, 4),
            "signal_volume": round(s_volume, 4),
            "signal_delivery": round(s_delivery, 4),
            "signal_circuit": round(s_circuit, 4),
            "signal_velocity": round(s_velocity, 4),
            "signal_corp_event": round(s_corp_event, 4),
            "signal_pref_allot": round(s_pref_allot, 4),
            "signal_bulk_deal": round(s_bulk_deal, 4),
            "phase": phase,
            "signals_triggered": signals_triggered_str,
        }))

    # ── INSERT OR REPLACE into manipulation_scores ────────────────────────────
    sql = """
        INSERT OR REPLACE INTO manipulation_scores
            (date, symbol, total_score, signal_volume, signal_delivery,
             signal_circuit, signal_velocity, signal_corp_event,
             signal_pref_allot, signal_bulk_deal, phase, signals_triggered)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    c.executemany(sql, score_rows)
    conn.commit()
    inserted = len(score_rows)
    print(f"\n✅ Inserted/replaced {inserted} rows into manipulation_scores\n")

    # ── Print top 10 scored stocks ────────────────────────────────────────────
    summaries.sort(key=lambda x: x[0], reverse=True)
    top10 = summaries[:10]

    # Table header
    header = (
        f"{'SYMBOL':<15} {'TOTAL':>6} {'VOL':>5} {'DELIV':>6} {'CIRC':>5} "
        f"{'VEL':>5} {'CORP':>5} {'PREF':>5} {'BULK':>5} {'PHASE':<15} {'SIGNALS'}"
    )
    sep = "-" * len(header)
    print("TOP 10 SCORED STOCKS")
    print(sep)
    print(header)
    print(sep)

    for total_score, symbol, summary, d in top10:
        row_str = (
            f"{symbol:<15} {d['total_score']:>6.2f} {d['signal_volume']:>5.2f} "
            f"{d['signal_delivery']:>6.2f} {d['signal_circuit']:>5.2f} "
            f"{d['signal_velocity']:>5.2f} {d['signal_corp_event']:>5.2f} "
            f"{d['signal_pref_allot']:>5.2f} {d['signal_bulk_deal']:>5.2f} "
            f"{d['phase']:<15} {d['signals_triggered']}"
        )
        print(row_str)

    print(sep)

    # Plain-English summaries
    print("\nPLAIN-ENGLISH SUMMARIES (Top 10)")
    print(sep)
    for total_score, symbol, summary, d in top10:
        print(f"  {summary}")
    print(sep)

    conn.close()
    return inserted


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Compute manipulation scores for all symbols on a given date."
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
        # Validate format
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            print(f"❌ Invalid date format: {target_date}. Use YYYY-MM-DD.")
            return
    else:
        # Use most recent date in daily_prices
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
    print(f"\n✅ Done. {inserted} rows written to manipulation_scores for {target_date}.")


if __name__ == "__main__":
    main()
