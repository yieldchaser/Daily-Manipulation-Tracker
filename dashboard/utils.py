"""
utils.py — Database helper functions for the Daily Manipulation Tracker dashboard.
"""

import os
import sqlite3
import pandas as pd

# Resolve DB path relative to this file's location (dashboard/ -> ../data/tracker.db)
_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'tracker.db')


def get_db_connection() -> sqlite3.Connection:
    """Return a sqlite3 connection to the tracker database."""
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_symbols() -> list[str]:
    """Return a sorted list of unique stock symbols from daily_prices."""
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(
            "SELECT DISTINCT symbol FROM daily_prices ORDER BY symbol",
            conn
        )
        conn.close()
        return df['symbol'].tolist()
    except Exception:
        return []


def get_price_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Return OHLCV price data for a given symbol and date range.

    Parameters
    ----------
    symbol : str
        Stock ticker symbol.
    start_date : str
        Start date in 'YYYY-MM-DD' format (inclusive).
    end_date : str
        End date in 'YYYY-MM-DD' format (inclusive).

    Returns
    -------
    pd.DataFrame
        Columns: date, symbol, open, high, low, close, total_volume,
                 delivery_volume, delivery_pct, pct_change, trades, prev_close, series
    """
    try:
        conn = get_db_connection()
        query = """
            SELECT date, symbol, series, open, high, low, close, prev_close,
                   pct_change, total_volume, delivery_volume, delivery_pct, trades
            FROM daily_prices
            WHERE symbol = ?
              AND date BETWEEN ? AND ?
            ORDER BY date ASC
        """
        df = pd.read_sql_query(query, conn, params=(symbol, start_date, end_date))
        conn.close()
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception:
        return pd.DataFrame()


def get_manipulation_scores(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Return manipulation score data for a given symbol and date range.

    Parameters
    ----------
    symbol : str
        Stock ticker symbol.
    start_date : str
        Start date in 'YYYY-MM-DD' format (inclusive).
    end_date : str
        End date in 'YYYY-MM-DD' format (inclusive).

    Returns
    -------
    pd.DataFrame
        Columns: date, symbol, total_score, signal_volume, signal_delivery,
                 signal_circuit, signal_velocity, signal_corp_event,
                 signal_pref_allot, signal_bulk_deal, phase, signals_triggered
    """
    try:
        conn = get_db_connection()
        query = """
            SELECT date, symbol, total_score, signal_volume, signal_delivery,
                   signal_circuit, signal_velocity, signal_corp_event,
                   signal_pref_allot, signal_bulk_deal, phase, signals_triggered
            FROM manipulation_scores
            WHERE symbol = ?
              AND date BETWEEN ? AND ?
            ORDER BY date ASC
        """
        df = pd.read_sql_query(query, conn, params=(symbol, start_date, end_date))
        conn.close()
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception:
        return pd.DataFrame()


def get_rolling_stats(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Return rolling statistical metrics for a given symbol and date range.

    Parameters
    ----------
    symbol : str
        Stock ticker symbol.
    start_date : str
        Start date in 'YYYY-MM-DD' format (inclusive).
    end_date : str
        End date in 'YYYY-MM-DD' format (inclusive).

    Returns
    -------
    pd.DataFrame
        Columns: date, symbol, avg_volume_30d, avg_delivery_30d, vol_ratio,
                 price_change_30d, price_change_60d, upper_circuit_streak,
                 week_52_high, week_52_low
    """
    try:
        conn = get_db_connection()
        query = """
            SELECT date, symbol, avg_volume_30d, avg_delivery_30d, vol_ratio,
                   price_change_30d, price_change_60d, upper_circuit_streak,
                   week_52_high, week_52_low
            FROM rolling_stats
            WHERE symbol = ?
              AND date BETWEEN ? AND ?
            ORDER BY date ASC
        """
        df = pd.read_sql_query(query, conn, params=(symbol, start_date, end_date))
        conn.close()
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception:
        return pd.DataFrame()


def get_corporate_events(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Return corporate events for a given symbol and date range.

    Parameters
    ----------
    symbol : str
        Stock ticker symbol.
    start_date : str
        Start date in 'YYYY-MM-DD' format (inclusive).
    end_date : str
        End date in 'YYYY-MM-DD' format (inclusive).

    Returns
    -------
    pd.DataFrame
        Columns: date, symbol, event_type, description, source
    """
    try:
        conn = get_db_connection()
        query = """
            SELECT date, symbol, event_type, description, source
            FROM corporate_events
            WHERE symbol = ?
              AND date BETWEEN ? AND ?
            ORDER BY date DESC
        """
        df = pd.read_sql_query(query, conn, params=(symbol, start_date, end_date))
        conn.close()
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception:
        return pd.DataFrame()


def get_bulk_deals(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Return bulk/block deal data for a given symbol and date range.

    Parameters
    ----------
    symbol : str
        Stock ticker symbol.
    start_date : str
        Start date in 'YYYY-MM-DD' format (inclusive).
    end_date : str
        End date in 'YYYY-MM-DD' format (inclusive).

    Returns
    -------
    pd.DataFrame
        Columns: date, symbol, client_name, buy_sell, quantity, price
    """
    try:
        conn = get_db_connection()
        query = """
            SELECT date, symbol, client_name, buy_sell, quantity, price
            FROM bulk_deals
            WHERE symbol = ?
              AND date BETWEEN ? AND ?
            ORDER BY date DESC
        """
        df = pd.read_sql_query(query, conn, params=(symbol, start_date, end_date))
        conn.close()
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception:
        return pd.DataFrame()
