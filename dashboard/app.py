"""
app.py — Streamlit dashboard for the Daily Manipulation Tracker.

Run from the project root:
    streamlit run dashboard/app.py
"""

import sys
import os
from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st

# Ensure the dashboard directory is on the path so utils can be imported
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import (
    get_symbols,
    get_price_data,
    get_manipulation_scores,
    get_rolling_stats,
    get_corporate_events,
    get_bulk_deals,
)

# ── Page configuration ────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Daily Manipulation Tracker",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
        .risk-low    { color: #2ecc71; font-weight: bold; }
        .risk-medium { color: #f39c12; font-weight: bold; }
        .risk-high   { color: #e74c3c; font-weight: bold; }
        .footer {
            position: fixed;
            bottom: 0;
            left: 0;
            width: 100%;
            background-color: #0e1117;
            color: #888;
            text-align: center;
            padding: 6px 0;
            font-size: 0.75rem;
            border-top: 1px solid #333;
            z-index: 999;
        }
        .metric-card {
            background: #1e2130;
            border-radius: 8px;
            padding: 12px 16px;
            margin-bottom: 8px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📈 Manipulation Tracker")
    st.markdown("---")

    # Stock symbol selector
    symbols = get_symbols()
    if symbols:
        selected_symbol = st.selectbox(
            "Stock Symbol",
            options=symbols,
            index=0,
            help="Select a stock symbol to analyse",
        )
    else:
        selected_symbol = st.text_input(
            "Stock Symbol",
            value="",
            placeholder="e.g. RELIANCE",
            help="No symbols found in DB — type a symbol manually",
        ).strip().upper()

    st.markdown("---")

    # Date range picker
    default_end = date.today()
    default_start = default_end - timedelta(days=90)

    start_date = st.date_input("Start Date", value=default_start)
    end_date = st.date_input("End Date", value=default_end)

    if start_date > end_date:
        st.error("⚠️ Start date must be before end date.")
        st.stop()

    st.markdown("---")

    # Refresh button
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.caption("Data sourced from NSE via jugaad-data")

# ── Validate inputs ───────────────────────────────────────────────────────────
if not selected_symbol:
    st.warning("Please select or enter a stock symbol in the sidebar.")
    st.stop()

start_str = start_date.strftime("%Y-%m-%d")
end_str = end_date.strftime("%Y-%m-%d")

# ── Page header ───────────────────────────────────────────────────────────────
st.title(f"📊 {selected_symbol} — Manipulation Analysis")
st.caption(f"Period: {start_str} → {end_str}")

# ── Load data (cached) ────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_all_data(symbol: str, start: str, end: str):
    return {
        "prices":       get_price_data(symbol, start, end),
        "scores":       get_manipulation_scores(symbol, start, end),
        "rolling":      get_rolling_stats(symbol, start, end),
        "corp_events":  get_corporate_events(symbol, start, end),
        "bulk_deals":   get_bulk_deals(symbol, start, end),
    }


data = load_all_data(selected_symbol, start_str, end_str)

# ── Summary metrics row ───────────────────────────────────────────────────────
prices_df = data["prices"]
scores_df = data["scores"]

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    if not prices_df.empty and "close" in prices_df.columns:
        latest_close = prices_df["close"].iloc[-1]
        prev_close = prices_df["close"].iloc[-2] if len(prices_df) > 1 else latest_close
        delta = latest_close - prev_close
        st.metric("Latest Close", f"₹{latest_close:,.2f}", delta=f"{delta:+.2f}")
    else:
        st.metric("Latest Close", "N/A")

with col2:
    if not prices_df.empty and "total_volume" in prices_df.columns:
        latest_vol = prices_df["total_volume"].iloc[-1]
        st.metric("Latest Volume", f"{latest_vol:,.0f}" if pd.notna(latest_vol) else "N/A")
    else:
        st.metric("Latest Volume", "N/A")

with col3:
    if not prices_df.empty and "pct_change" in prices_df.columns:
        latest_pct = prices_df["pct_change"].iloc[-1]
        st.metric("Day Change %", f"{latest_pct:+.2f}%" if pd.notna(latest_pct) else "N/A")
    else:
        st.metric("Day Change %", "N/A")

with col4:
    if not scores_df.empty and "total_score" in scores_df.columns:
        latest_score = scores_df["total_score"].iloc[-1]
        if pd.notna(latest_score):
            if latest_score < 0.3:
                risk_label = "🟢 Low"
            elif latest_score < 0.7:
                risk_label = "🟡 Medium"
            else:
                risk_label = "🔴 High"
            st.metric("Risk Score", f"{latest_score:.3f}", delta=risk_label, delta_color="off")
        else:
            st.metric("Risk Score", "N/A")
    else:
        st.metric("Risk Score", "N/A")

with col5:
    if not scores_df.empty and "phase" in scores_df.columns:
        latest_phase = scores_df["phase"].iloc[-1]
        st.metric("Phase", latest_phase if pd.notna(latest_phase) else "N/A")
    else:
        st.metric("Phase", "N/A")

st.markdown("---")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📉 Price & Volume",
    "🚨 Manipulation Scores",
    "📊 Rolling Stats",
    "📋 Events & Deals",
])

# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — Price & Volume
# ─────────────────────────────────────────────────────────────────────────────
with tab1:
    st.subheader(f"Price & Volume — {selected_symbol}")

    if prices_df.empty:
        st.info("ℹ️ No price data available for the selected symbol and date range.")
    else:
        required_cols = {"date", "open", "high", "low", "close"}
        if not required_cols.issubset(prices_df.columns):
            st.warning("Price data is missing required OHLC columns.")
        else:
            # Build candlestick + volume subplot
            fig = make_subplots(
                rows=2,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.05,
                row_heights=[0.7, 0.3],
                subplot_titles=("OHLC Candlestick", "Volume"),
            )

            # Candlestick
            fig.add_trace(
                go.Candlestick(
                    x=prices_df["date"],
                    open=prices_df["open"],
                    high=prices_df["high"],
                    low=prices_df["low"],
                    close=prices_df["close"],
                    name="OHLC",
                    increasing_line_color="#2ecc71",
                    decreasing_line_color="#e74c3c",
                ),
                row=1,
                col=1,
            )

            # Volume bars — colour by up/down day
            if "total_volume" in prices_df.columns:
                vol_colors = [
                    "#2ecc71" if (c >= o) else "#e74c3c"
                    for c, o in zip(prices_df["close"], prices_df["open"])
                ]
                fig.add_trace(
                    go.Bar(
                        x=prices_df["date"],
                        y=prices_df["total_volume"],
                        name="Volume",
                        marker_color=vol_colors,
                        opacity=0.7,
                    ),
                    row=2,
                    col=1,
                )

                # Delivery volume overlay
                if "delivery_volume" in prices_df.columns:
                    fig.add_trace(
                        go.Bar(
                            x=prices_df["date"],
                            y=prices_df["delivery_volume"],
                            name="Delivery Vol",
                            marker_color="#3498db",
                            opacity=0.6,
                        ),
                        row=2,
                        col=1,
                    )

            fig.update_layout(
                height=600,
                xaxis_rangeslider_visible=False,
                template="plotly_dark",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(l=40, r=40, t=60, b=40),
            )
            fig.update_yaxes(title_text="Price (₹)", row=1, col=1)
            fig.update_yaxes(title_text="Volume", row=2, col=1)

            st.plotly_chart(fig, use_container_width=True)

            # Raw data expander
            with st.expander("📄 Raw Price Data"):
                display_df = prices_df.copy()
                display_df["date"] = display_df["date"].dt.strftime("%Y-%m-%d")
                st.dataframe(display_df, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — Manipulation Scores
# ─────────────────────────────────────────────────────────────────────────────
with tab2:
    st.subheader(f"Manipulation Risk Scores — {selected_symbol}")

    if scores_df.empty:
        st.info("ℹ️ No manipulation score data available for the selected symbol and date range.")
    else:
        # ── Main score line chart with risk zones ──────────────────────────
        fig_score = go.Figure()

        # Risk zone shading
        x_min = scores_df["date"].min()
        x_max = scores_df["date"].max()

        fig_score.add_hrect(y0=0, y1=0.3, fillcolor="green", opacity=0.08,
                            line_width=0, annotation_text="Low Risk", annotation_position="left")
        fig_score.add_hrect(y0=0.3, y1=0.7, fillcolor="orange", opacity=0.08,
                            line_width=0, annotation_text="Medium Risk", annotation_position="left")
        fig_score.add_hrect(y0=0.7, y1=1.0, fillcolor="red", opacity=0.08,
                            line_width=0, annotation_text="High Risk", annotation_position="left")

        # Threshold lines
        fig_score.add_hline(y=0.3, line_dash="dot", line_color="#2ecc71", opacity=0.6)
        fig_score.add_hline(y=0.7, line_dash="dot", line_color="#e74c3c", opacity=0.6)

        # Score line
        fig_score.add_trace(
            go.Scatter(
                x=scores_df["date"],
                y=scores_df["total_score"],
                mode="lines+markers",
                name="Manipulation Score",
                line=dict(color="#f39c12", width=2),
                marker=dict(
                    size=6,
                    color=scores_df["total_score"],
                    colorscale=[[0, "#2ecc71"], [0.3, "#2ecc71"],
                                [0.3, "#f39c12"], [0.7, "#f39c12"],
                                [0.7, "#e74c3c"], [1.0, "#e74c3c"]],
                    cmin=0,
                    cmax=1,
                    showscale=True,
                    colorbar=dict(title="Score", thickness=12),
                ),
                hovertemplate=(
                    "<b>%{x|%Y-%m-%d}</b><br>"
                    "Score: %{y:.3f}<extra></extra>"
                ),
            )
        )

        fig_score.update_layout(
            height=400,
            template="plotly_dark",
            yaxis=dict(title="Manipulation Score", range=[0, 1.05]),
            xaxis=dict(title="Date"),
            margin=dict(l=40, r=40, t=40, b=40),
        )
        st.plotly_chart(fig_score, use_container_width=True)

        # ── Individual signal breakdown ────────────────────────────────────
        signal_cols = [c for c in [
            "signal_volume", "signal_delivery", "signal_circuit",
            "signal_velocity", "signal_corp_event", "signal_pref_allot",
            "signal_bulk_deal",
        ] if c in scores_df.columns]

        if signal_cols:
            st.markdown("#### Signal Breakdown")
            fig_signals = go.Figure()
            colors = px.colors.qualitative.Plotly
            for i, col in enumerate(signal_cols):
                fig_signals.add_trace(
                    go.Scatter(
                        x=scores_df["date"],
                        y=scores_df[col],
                        mode="lines",
                        name=col.replace("signal_", "").replace("_", " ").title(),
                        line=dict(color=colors[i % len(colors)], width=1.5),
                        hovertemplate=f"<b>%{{x|%Y-%m-%d}}</b><br>{col}: %{{y:.3f}}<extra></extra>",
                    )
                )
            fig_signals.update_layout(
                height=350,
                template="plotly_dark",
                yaxis=dict(title="Signal Value"),
                xaxis=dict(title="Date"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(l=40, r=40, t=40, b=40),
            )
            st.plotly_chart(fig_signals, use_container_width=True)

        # ── Signals triggered table ────────────────────────────────────────
        if "signals_triggered" in scores_df.columns:
            with st.expander("📄 Signals Triggered Log"):
                trig_df = scores_df[["date", "total_score", "phase", "signals_triggered"]].copy()
                trig_df["date"] = trig_df["date"].dt.strftime("%Y-%m-%d")
                trig_df = trig_df[trig_df["signals_triggered"].notna() & (trig_df["signals_triggered"] != "")]
                if trig_df.empty:
                    st.write("No signals triggered in this period.")
                else:
                    st.dataframe(trig_df, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 — Rolling Stats
# ─────────────────────────────────────────────────────────────────────────────
with tab3:
    st.subheader(f"Rolling Statistics — {selected_symbol}")

    rolling_df = data["rolling"]

    if rolling_df.empty:
        st.info("ℹ️ No rolling statistics available for the selected symbol and date range.")
    else:
        col_left, col_right = st.columns(2)

        # ── Volume metrics ─────────────────────────────────────────────────
        with col_left:
            st.markdown("#### Volume Metrics")
            fig_vol = go.Figure()

            if "avg_volume_30d" in rolling_df.columns:
                fig_vol.add_trace(go.Scatter(
                    x=rolling_df["date"], y=rolling_df["avg_volume_30d"],
                    mode="lines", name="Avg Volume 30d",
                    line=dict(color="#3498db", width=2),
                ))
            if "avg_delivery_30d" in rolling_df.columns:
                fig_vol.add_trace(go.Scatter(
                    x=rolling_df["date"], y=rolling_df["avg_delivery_30d"],
                    mode="lines", name="Avg Delivery 30d",
                    line=dict(color="#9b59b6", width=2),
                ))
            if "vol_ratio" in rolling_df.columns:
                fig_vol.add_trace(go.Scatter(
                    x=rolling_df["date"], y=rolling_df["vol_ratio"],
                    mode="lines", name="Vol Ratio",
                    line=dict(color="#e67e22", width=2),
                    yaxis="y2",
                ))
                fig_vol.update_layout(
                    yaxis2=dict(
                        title="Vol Ratio",
                        overlaying="y",
                        side="right",
                        showgrid=False,
                    )
                )

            fig_vol.update_layout(
                height=320,
                template="plotly_dark",
                yaxis=dict(title="Volume"),
                xaxis=dict(title="Date"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(l=40, r=40, t=40, b=40),
            )
            st.plotly_chart(fig_vol, use_container_width=True)

        # ── Price change metrics ───────────────────────────────────────────
        with col_right:
            st.markdown("#### Price Change Metrics")
            fig_price = go.Figure()

            if "price_change_30d" in rolling_df.columns:
                fig_price.add_trace(go.Scatter(
                    x=rolling_df["date"], y=rolling_df["price_change_30d"],
                    mode="lines", name="Price Change 30d (%)",
                    line=dict(color="#2ecc71", width=2),
                ))
            if "price_change_60d" in rolling_df.columns:
                fig_price.add_trace(go.Scatter(
                    x=rolling_df["date"], y=rolling_df["price_change_60d"],
                    mode="lines", name="Price Change 60d (%)",
                    line=dict(color="#e74c3c", width=2),
                ))

            fig_price.add_hline(y=0, line_dash="dash", line_color="#888", opacity=0.5)

            fig_price.update_layout(
                height=320,
                template="plotly_dark",
                yaxis=dict(title="Price Change (%)"),
                xaxis=dict(title="Date"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(l=40, r=40, t=40, b=40),
            )
            st.plotly_chart(fig_price, use_container_width=True)

        # ── 52-week range & circuit streak ─────────────────────────────────
        st.markdown("#### 52-Week Range & Circuit Streak")
        col_a, col_b = st.columns(2)

        with col_a:
            if {"week_52_high", "week_52_low"}.issubset(rolling_df.columns):
                fig_52 = go.Figure()
                fig_52.add_trace(go.Scatter(
                    x=rolling_df["date"], y=rolling_df["week_52_high"],
                    mode="lines", name="52W High",
                    line=dict(color="#2ecc71", width=1.5),
                ))
                fig_52.add_trace(go.Scatter(
                    x=rolling_df["date"], y=rolling_df["week_52_low"],
                    mode="lines", name="52W Low",
                    line=dict(color="#e74c3c", width=1.5),
                    fill="tonexty",
                    fillcolor="rgba(52,152,219,0.1)",
                ))
                # Overlay close price if available
                if not prices_df.empty and "close" in prices_df.columns:
                    merged = pd.merge(
                        rolling_df[["date", "week_52_high", "week_52_low"]],
                        prices_df[["date", "close"]],
                        on="date", how="left",
                    )
                    fig_52.add_trace(go.Scatter(
                        x=merged["date"], y=merged["close"],
                        mode="lines", name="Close",
                        line=dict(color="#f39c12", width=2),
                    ))
                fig_52.update_layout(
                    height=280,
                    template="plotly_dark",
                    yaxis=dict(title="Price (₹)"),
                    xaxis=dict(title="Date"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    margin=dict(l=40, r=40, t=40, b=40),
                )
                st.plotly_chart(fig_52, use_container_width=True)
            else:
                st.info("52-week range data not available.")

        with col_b:
            if "upper_circuit_streak" in rolling_df.columns:
                fig_streak = go.Figure()
                fig_streak.add_trace(go.Bar(
                    x=rolling_df["date"],
                    y=rolling_df["upper_circuit_streak"],
                    name="Upper Circuit Streak",
                    marker_color="#e74c3c",
                    opacity=0.8,
                ))
                fig_streak.update_layout(
                    height=280,
                    template="plotly_dark",
                    yaxis=dict(title="Consecutive Days"),
                    xaxis=dict(title="Date"),
                    margin=dict(l=40, r=40, t=40, b=40),
                )
                st.plotly_chart(fig_streak, use_container_width=True)
            else:
                st.info("Circuit streak data not available.")

        # Raw data expander
        with st.expander("📄 Raw Rolling Stats Data"):
            display_rolling = rolling_df.copy()
            display_rolling["date"] = display_rolling["date"].dt.strftime("%Y-%m-%d")
            st.dataframe(display_rolling, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# TAB 4 — Events & Deals
# ─────────────────────────────────────────────────────────────────────────────
with tab4:
    st.subheader(f"Corporate Events & Bulk Deals — {selected_symbol}")

    corp_df = data["corp_events"]
    bulk_df = data["bulk_deals"]

    col_ev, col_bd = st.columns(2)

    # ── Corporate Events ───────────────────────────────────────────────────
    with col_ev:
        st.markdown("#### 🏢 Corporate Events")
        if corp_df.empty:
            st.info("ℹ️ No corporate events found for the selected period.")
        else:
            display_corp = corp_df.copy()
            display_corp["date"] = display_corp["date"].dt.strftime("%Y-%m-%d")

            # Colour-code by event type
            event_type_counts = display_corp["event_type"].value_counts()
            st.caption(f"Total events: **{len(display_corp)}**")

            # Event type distribution
            if len(event_type_counts) > 0:
                fig_ev_pie = px.pie(
                    values=event_type_counts.values,
                    names=event_type_counts.index,
                    title="Event Type Distribution",
                    template="plotly_dark",
                    hole=0.4,
                )
                fig_ev_pie.update_layout(
                    height=250,
                    margin=dict(l=20, r=20, t=40, b=20),
                    showlegend=True,
                )
                st.plotly_chart(fig_ev_pie, use_container_width=True)

            st.dataframe(
                display_corp[["date", "event_type", "description", "source"]],
                use_container_width=True,
                hide_index=True,
            )

    # ── Bulk Deals ─────────────────────────────────────────────────────────
    with col_bd:
        st.markdown("#### 💼 Bulk / Block Deals")
        if bulk_df.empty:
            st.info("ℹ️ No bulk/block deals found for the selected period.")
        else:
            display_bulk = bulk_df.copy()
            display_bulk["date"] = display_bulk["date"].dt.strftime("%Y-%m-%d")

            st.caption(f"Total deals: **{len(display_bulk)}**")

            # Buy vs Sell summary
            if "buy_sell" in display_bulk.columns:
                bs_counts = display_bulk["buy_sell"].value_counts()
                fig_bs = px.bar(
                    x=bs_counts.index,
                    y=bs_counts.values,
                    title="Buy vs Sell Count",
                    template="plotly_dark",
                    color=bs_counts.index,
                    color_discrete_map={"BUY": "#2ecc71", "SELL": "#e74c3c"},
                )
                fig_bs.update_layout(
                    height=250,
                    margin=dict(l=20, r=20, t=40, b=20),
                    showlegend=False,
                    xaxis_title="",
                    yaxis_title="Count",
                )
                st.plotly_chart(fig_bs, use_container_width=True)

            st.dataframe(
                display_bulk[["date", "client_name", "buy_sell", "quantity", "price"]],
                use_container_width=True,
                hide_index=True,
            )

    # ── Combined timeline ──────────────────────────────────────────────────
    if not corp_df.empty or not bulk_df.empty:
        st.markdown("#### 📅 Events Timeline")

        timeline_rows = []
        if not corp_df.empty:
            for _, row in corp_df.iterrows():
                timeline_rows.append({
                    "date": row["date"],
                    "type": "Corporate Event",
                    "detail": f"{row.get('event_type', '')} — {row.get('description', '')}",
                })
        if not bulk_df.empty:
            for _, row in bulk_df.iterrows():
                timeline_rows.append({
                    "date": row["date"],
                    "type": "Bulk Deal",
                    "detail": f"{row.get('buy_sell', '')} {row.get('quantity', '')} @ ₹{row.get('price', '')} by {row.get('client_name', '')}",
                })

        if timeline_rows:
            timeline_df = pd.DataFrame(timeline_rows).sort_values("date", ascending=False)
            timeline_df["date"] = timeline_df["date"].dt.strftime("%Y-%m-%d")
            st.dataframe(timeline_df, use_container_width=True, hide_index=True)

# ── Footer ────────────────────────────────────────────────────────────────────
from datetime import datetime, timezone

last_updated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
st.markdown(
    f'<div class="footer">Daily Manipulation Tracker &nbsp;|&nbsp; Last updated: {last_updated}</div>',
    unsafe_allow_html=True,
)
