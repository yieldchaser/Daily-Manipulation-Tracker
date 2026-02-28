"""
social_monitor.py — Monitor public Indian stock tips Telegram channels for NSE stock ticker mentions.

================================================================================
TELEGRAM API CREDENTIALS REQUIRED
================================================================================
This module requires Telegram API credentials (free from https://my.telegram.org).

Steps to get credentials:
  1. Go to https://my.telegram.org
  2. Log in with your phone number
  3. Click on "API Development Tools"
  4. Create a new App (fill in app title, short name, etc.)
  5. You will receive an api_id (integer) and api_hash (string)

Set the following environment variables before running:
  export TELEGRAM_API_ID=<your_api_id>
  export TELEGRAM_API_HASH=<your_api_hash>
  export TELEGRAM_PHONE=<your_phone_number_with_country_code>

NOTE: This script only reads PUBLIC channels — no private channel access is performed.
================================================================================

Usage:
    python src/social_monitor.py
    python src/social_monitor.py  # uses today's date automatically
"""

import asyncio
import os
import re
import sqlite3
from datetime import datetime, timezone

# ── Telegram channels to monitor ──────────────────────────────────────────────
CHANNELS = [
    "nse_bse_tips",
    "stockmarketindia",
    "dalal_street_bulls",
    "nifty_banknifty_calls",
    "multibagger_stocks",
    "indian_stock_market_tips",
    "stocktipsindia",
    "nse_tips_free",
    "equity_research_india",
    "trading_calls_india",
]

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "tracker.db")


# ── Database helpers ──────────────────────────────────────────────────────────

def get_db_connection():
    """Connect to data/tracker.db (path relative to the script's parent directory)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def create_table(conn):
    """Create the social_mentions table if it does not already exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS social_mentions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            mention_count INTEGER DEFAULT 0,
            channel_count INTEGER DEFAULT 0,
            sample_text TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, date)
        )
    """)
    conn.commit()


def get_nse_symbols(conn):
    """Fetch all distinct symbols from the daily_ohlcv table."""
    try:
        cursor = conn.execute("SELECT DISTINCT symbol FROM daily_ohlcv")
        rows = cursor.fetchall()
        return [row["symbol"] for row in rows]
    except sqlite3.OperationalError:
        # Table may not exist yet
        return []


# ── Telegram scanning ─────────────────────────────────────────────────────────

async def scan_channels(api_id, api_hash, phone, date_str=None):
    """
    Async function using telethon to scan public Telegram channels for NSE stock mentions.

    Connects to Telegram using provided credentials, fetches the last 200 messages
    from each channel in CHANNELS for the given date (today if date_str is None),
    and records whole-word, case-insensitive matches of NSE symbols.

    Returns:
        dict: {symbol: {"count": int, "channels": set(), "sample_texts": list}}
    """
    from telethon import TelegramClient
    from telethon.errors import (
        ChannelPrivateError,
        UsernameNotOccupiedError,
        UsernameInvalidError,
        FloodWaitError,
    )

    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    target_date = datetime.strptime(date_str, "%Y-%m-%d").date()

    # Get DB connection to fetch symbols
    conn = get_db_connection()
    symbols = get_nse_symbols(conn)
    conn.close()

    if not symbols:
        print("⚠️  No NSE symbols found in daily_ohlcv. Cannot scan for mentions.")
        return {}

    # Build a set for fast lookup (uppercase)
    symbol_set = set(s.upper() for s in symbols)

    # Results dict: symbol -> {count, channels, sample_texts}
    mentions = {}

    # Session file stored in data/ directory
    session_path = os.path.join(BASE_DIR, "data", "telegram_session")

    client = TelegramClient(session_path, api_id, api_hash)

    try:
        await client.start(phone=phone)
        print(f"✅ Connected to Telegram as {phone}")

        for channel_username in CHANNELS:
            print(f"  📡 Scanning @{channel_username} ...")
            try:
                entity = await client.get_entity(channel_username)
                messages = await client.get_messages(entity, limit=200)

                for msg in messages:
                    if msg.date is None:
                        continue
                    # Only process messages from the target date (UTC)
                    msg_date = msg.date.astimezone(timezone.utc).date()
                    if msg_date != target_date:
                        continue

                    text = msg.text or msg.message or ""
                    if not text:
                        continue

                    text_upper = text.upper()

                    # Find all whole-word symbol matches
                    for symbol in symbol_set:
                        # Whole-word match using regex word boundaries
                        pattern = r'\b' + re.escape(symbol) + r'\b'
                        if re.search(pattern, text_upper):
                            if symbol not in mentions:
                                mentions[symbol] = {
                                    "count": 0,
                                    "channels": set(),
                                    "sample_texts": [],
                                }
                            mentions[symbol]["count"] += 1
                            mentions[symbol]["channels"].add(channel_username)
                            # Keep up to 3 sample texts per symbol
                            if len(mentions[symbol]["sample_texts"]) < 3:
                                # Truncate long texts
                                sample = text[:500] if len(text) > 500 else text
                                mentions[symbol]["sample_texts"].append(sample)

            except ChannelPrivateError:
                print(f"  ⚠️  @{channel_username} is private — skipping")
            except (UsernameNotOccupiedError, UsernameInvalidError):
                print(f"  ⚠️  @{channel_username} not found — skipping")
            except FloodWaitError as e:
                print(f"  ⚠️  Rate limited — waiting {e.seconds}s before continuing")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                print(f"  ⚠️  Error scanning @{channel_username}: {e}")

    finally:
        await client.disconnect()

    print(f"\n✅ Scan complete. Found mentions for {len(mentions)} symbols.")
    return mentions


# ── Persistence ───────────────────────────────────────────────────────────────

def save_mentions(conn, mentions_data, date_str):
    """
    Save/upsert scan results to the social_mentions table.

    Args:
        conn: SQLite connection
        mentions_data: dict returned by scan_channels()
        date_str: date string in YYYY-MM-DD format
    """
    rows_saved = 0
    for symbol, data in mentions_data.items():
        mention_count = data["count"]
        channel_count = len(data["channels"])
        sample_texts = data.get("sample_texts", [])
        sample_text = " | ".join(sample_texts) if sample_texts else None

        conn.execute("""
            INSERT INTO social_mentions (symbol, date, mention_count, channel_count, sample_text)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(symbol, date) DO UPDATE SET
                mention_count = mention_count + excluded.mention_count,
                channel_count = MAX(channel_count, excluded.channel_count),
                sample_text = COALESCE(excluded.sample_text, sample_text)
        """, (symbol, date_str, mention_count, channel_count, sample_text))
        rows_saved += 1

    conn.commit()
    return rows_saved


# ── Orchestrator ──────────────────────────────────────────────────────────────

async def run_monitor(api_id, api_hash, phone, date_str=None):
    """
    Main orchestrator: get DB connection, create table, get symbols,
    scan channels, save results, print summary.
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    print(f"\n{'='*60}")
    print(f"  Social Monitor — Telegram Pump Detection")
    print(f"  Date: {date_str}")
    print(f"{'='*60}\n")

    conn = get_db_connection()
    create_table(conn)

    symbols = get_nse_symbols(conn)
    print(f"✅ Loaded {len(symbols)} NSE symbols from daily_ohlcv")

    if not symbols:
        print("❌ No symbols to scan. Ensure daily_ohlcv table is populated.")
        conn.close()
        return

    if api_id == 0 or not api_hash or not phone:
        print("❌ Telegram credentials not set. Please set environment variables:")
        print("   TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE")
        conn.close()
        return

    # Scan channels
    mentions_data = await scan_channels(api_id, api_hash, phone, date_str)

    # Save results
    rows_saved = save_mentions(conn, mentions_data, date_str)
    print(f"✅ Saved {rows_saved} symbol mention records to social_mentions")

    # Print summary
    if mentions_data:
        print(f"\n{'─'*60}")
        print(f"  TOP MENTIONED SYMBOLS on {date_str}")
        print(f"{'─'*60}")
        sorted_mentions = sorted(
            mentions_data.items(),
            key=lambda x: (x[1]["count"], len(x[1]["channels"])),
            reverse=True,
        )
        for symbol, data in sorted_mentions[:20]:
            channels_list = ", ".join(sorted(data["channels"]))
            print(
                f"  {symbol:<15} mentions={data['count']:>4}  "
                f"channels={len(data['channels']):>2}  [{channels_list}]"
            )
        print(f"{'─'*60}\n")
    else:
        print(f"\nℹ️  No mentions found for {date_str}.\n")

    conn.close()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    """
    Entry point. Reads Telegram credentials from environment variables and
    runs the async monitor.
    """
    API_ID = int(os.environ.get("TELEGRAM_API_ID", "0"))
    API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
    PHONE = os.environ.get("TELEGRAM_PHONE", "")

    asyncio.run(run_monitor(API_ID, API_HASH, PHONE))


if __name__ == "__main__":
    main()
