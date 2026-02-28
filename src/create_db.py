"""
create_db.py — Initialize the SQLite database schema for Daily Manipulation Tracker.
Run once before first use: python src/create_db.py
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'tracker.db')


def create_database():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # ── daily_prices ──────────────────────────────────────────────────────────
    c.execute('''
        CREATE TABLE IF NOT EXISTS daily_prices (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,
            symbol          TEXT NOT NULL,
            series          TEXT,
            open            REAL,
            high            REAL,
            low             REAL,
            close           REAL,
            prev_close      REAL,
            pct_change      REAL,
            total_volume    INTEGER,
            delivery_volume INTEGER,
            delivery_pct    REAL,
            trades          INTEGER,
            UNIQUE(date, symbol)
        )
    ''')

    # ── rolling_stats ─────────────────────────────────────────────────────────
    c.execute('''
        CREATE TABLE IF NOT EXISTS rolling_stats (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,
            symbol          TEXT NOT NULL,
            avg_volume_30d  REAL,
            avg_delivery_30d REAL,
            vol_ratio       REAL,
            price_change_30d REAL,
            price_change_60d REAL,
            upper_circuit_streak INTEGER DEFAULT 0,
            week_52_high    REAL,
            week_52_low     REAL,
            UNIQUE(date, symbol)
        )
    ''')

    # ── corporate_events ──────────────────────────────────────────────────────
    c.execute('''
        CREATE TABLE IF NOT EXISTS corporate_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            event_type  TEXT,
            description TEXT,
            source      TEXT,
            UNIQUE(date, symbol, event_type)
        )
    ''')

    # ── bulk_deals ────────────────────────────────────────────────────────────
    c.execute('''
        CREATE TABLE IF NOT EXISTS bulk_deals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            client_name TEXT,
            buy_sell    TEXT,
            quantity    INTEGER,
            price       REAL,
            UNIQUE(date, symbol, client_name, buy_sell)
        )
    ''')

    # ── manipulation_scores ───────────────────────────────────────────────────
    c.execute('''
        CREATE TABLE IF NOT EXISTS manipulation_scores (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            date                TEXT NOT NULL,
            symbol              TEXT NOT NULL,
            total_score         REAL,
            signal_volume       REAL,
            signal_delivery     REAL,
            signal_circuit      REAL,
            signal_velocity     REAL,
            signal_corp_event   REAL,
            signal_pref_allot   REAL,
            signal_bulk_deal    REAL,
            signal_1            REAL,
            signal_2            REAL,
            signal_3            REAL,
            signal_4            REAL,
            signal_5            REAL,
            signal_6            REAL,
            signal_7            REAL,
            phase               TEXT,
            signals_triggered   TEXT,
            UNIQUE(date, symbol)
        )
    ''')

    # ── index_prices ──────────────────────────────────────────────────────────
    c.execute('''
        CREATE TABLE IF NOT EXISTS index_prices (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            index_name  TEXT NOT NULL,
            date        TEXT NOT NULL,
            close       REAL,
            UNIQUE(index_name, date)
        )
    ''')

    conn.commit()
    conn.close()
    print(f"✅ Database created at: {os.path.abspath(DB_PATH)}")
    print("Tables created: daily_prices, rolling_stats, corporate_events, bulk_deals, manipulation_scores, index_prices")


if __name__ == '__main__':
    create_database()
