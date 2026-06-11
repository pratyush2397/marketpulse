import streamlit as st
import pandas as pd
import plotly.express as px
import os
import glob

# ─── Data Loading ───────────────────────────────────────────
@st.cache_data
def load_agg_daily():
    path = "D:/marketpulse/data/gold/aggregations/agg_daily"
    files = glob.glob(f"{path}/**/*.parquet", recursive=True)
    return pd.concat([pd.read_parquet(f) for f in files])

@st.cache_data
def load_agg_sector():
    path = "D:/marketpulse/data/gold/aggregations/agg_sector"
    files = glob.glob(f"{path}/**/*.parquet", recursive=True)
    return pd.concat([pd.read_parquet(f) for f in files])

@st.cache_data
def load_top_stocks():
    path = "D:/marketpulse/data/gold/aggregations/agg_top_stocks"
    files = glob.glob(f"{path}/**/*.parquet", recursive=True)
    return pd.concat([pd.read_parquet(f) for f in files])

# ─── Load Data ───────────────────────────────────────────────
df_daily    = load_agg_daily()
df_sector   = load_agg_sector()
df_top      = load_top_stocks()

# ─── Sidebar ─────────────────────────────────────────────────
st.sidebar.title("🔍 Filters")
selected_ticker = st.sidebar.selectbox(
    "Select Stock",
    sorted(df_daily["ticker"].unique())
)

# ─── Main Title ──────────────────────────────────────────────
st.title("📈 MarketPulse — Stock Analytics Dashboard")
st.markdown("Real-time NSE stock analytics powered by Delta Lake + PySpark")

# ─── KPI Cards ───────────────────────────────────────────────
st.markdown("---")
col1, col2, col3, col4 = st.columns(4)

ticker_df = df_daily[df_daily["ticker"] == selected_ticker].sort_values("trade_date")
latest = ticker_df.iloc[-1]

col1.metric("Latest Close", f"₹{latest['avg_close']:,.2f}")
col2.metric("MA7",          f"₹{latest['avg_ma7']:,.2f}")
col3.metric("MA30",         f"₹{latest['avg_ma30']:,.2f}")
col4.metric("Daily Return", f"{latest['avg_daily_return']:.4f}%")

# ─── Price Chart ─────────────────────────────────────────────
st.markdown("---")
st.subheader(f"📊 Price History — {selected_ticker}")

fig = px.line(
    ticker_df,
    x="trade_date",
    y=["avg_close", "avg_ma7", "avg_ma30"],
    title=f"{selected_ticker} — Close vs MA7 vs MA30",
    labels={"value": "Price (₹)", "trade_date": "Date"},
    color_discrete_map={
        "avg_close": "#00d4ff",
        "avg_ma7":   "#ff6b35",
        "avg_ma30":  "#7bc67e"
    }
)
fig.update_layout(
    plot_bgcolor="#0e1117",
    paper_bgcolor="#0e1117",
    font_color="white"
)
st.plotly_chart(fig, use_container_width=True)

# ─── Volume Chart ────────────────────────────────────────────
st.subheader(f"📦 Volume — {selected_ticker}")

fig2 = px.bar(
    ticker_df,
    x="trade_date",
    y="total_volume",
    title=f"{selected_ticker} — Daily Volume",
    color_discrete_sequence=["#7bc67e"]
)
fig2.update_layout(
    plot_bgcolor="#0e1117",
    paper_bgcolor="#0e1117",
    font_color="white"
)
st.plotly_chart(fig2, use_container_width=True)

# ─── Sector Analysis ─────────────────────────────────────────
st.markdown("---")
st.subheader("🏭 Sector Performance")

fig3 = px.bar(
    df_sector.groupby("sector")["avg_return"].mean().reset_index(),
    x="sector",
    y="avg_return",
    title="Average Daily Return by Sector",
    color="avg_return",
    color_continuous_scale="RdYlGn"
)
fig3.update_layout(
    plot_bgcolor="#0e1117",
    paper_bgcolor="#0e1117",
    font_color="white"
)
st.plotly_chart(fig3, use_container_width=True)

# ─── Top Stocks ──────────────────────────────────────────────
st.markdown("---")
st.subheader("🏆 Top Stocks by Average Daily Return")

fig4 = px.bar(
    df_top.sort_values("avg_daily_return", ascending=True),
    x="avg_daily_return",
    y="ticker",
    orientation="h",
    title="Top Stocks — Avg Daily Return",
    color="avg_daily_return",
    color_continuous_scale="RdYlGn"
)
fig4.update_layout(
    plot_bgcolor="#0e1117",
    paper_bgcolor="#0e1117",
    font_color="white"
)
st.plotly_chart(fig4, use_container_width=True)

# ─── Raw Data ────────────────────────────────────────────────
st.markdown("---")
st.subheader("🗃️ Raw Data")
if st.checkbox("Show raw daily data"):
    st.dataframe(ticker_df)