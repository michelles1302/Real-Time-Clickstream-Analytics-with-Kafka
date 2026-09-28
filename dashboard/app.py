import glob
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Add project root directory to path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from app.config import PROCESSED_DATA_DIR, SAMPLE_DATA_DIR, STORAGE_MODE
from app.analytics.analytics import ClickstreamAnalytics

# Streamlit Page Config
st.set_page_config(
    page_title="Near Real-Time Clickstream Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling (Dark Glassmorphic Modern Aesthetic)
st.markdown("""
<style>
    .main {
        background-color: #0e1117;
    }
    .metric-card {
        background: linear-gradient(135deg, rgba(255,255,255,0.05), rgba(255,255,255,0.02));
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
    }
    .metric-value {
        font-size: 2.2rem;
        font-weight: 700;
        background: linear-gradient(90deg, #4FACFE 0%, #00F2FE 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #a0aec0;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 5px;
    }
    .status-badge-live {
        background-color: #10B981;
        color: #ffffff;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
    }
    .status-badge-demo {
        background-color: #F59E0B;
        color: #ffffff;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=5)
def load_parquet_data() -> tuple[pd.DataFrame, str, int]:
    """
    Load all Parquet files from processed directory.
    Falls back to sample directory if no live data exists.
    Returns (dataframe, mode_label, file_count).
    """
    live_files = glob.glob(str(PROCESSED_DATA_DIR / "**" / "*.parquet"), recursive=True)
    
    if live_files:
        dfs = [pd.read_parquet(f) for f in live_files]
        combined = pd.concat(dfs, ignore_index=True)
        return combined, "LIVE_PIPELINE", len(live_files)
    
    # Fallback to sample data
    sample_files = glob.glob(str(SAMPLE_DATA_DIR / "**" / "*.parquet"), recursive=True)
    if sample_files:
        dfs = [pd.read_parquet(f) for f in sample_files]
        combined = pd.concat(dfs, ignore_index=True)
        return combined, "DEMO_SAMPLE", len(sample_files)

    return pd.DataFrame(), "NO_DATA", 0


def main():
    # Load Data
    df, mode_label, file_count = load_parquet_data()

    # Header section
    col_title, col_status = st.columns([3, 1])
    with col_title:
        st.title("⚡ Near Real-Time Clickstream Analytics")
        st.caption("Apache Kafka + Parquet Data Pipeline Dashboard")

    with col_status:
        st.write("")
        if mode_label == "LIVE_PIPELINE":
            st.markdown('<span class="status-badge-live">● Near Real-Time Live Pipeline</span>', unsafe_allow_html=True)
        elif mode_label == "DEMO_SAMPLE":
            st.markdown('<span class="status-badge-demo">▲ Demo / Sample Data Mode</span>', unsafe_allow_html=True)
        else:
            st.warning("No Data Found")

        st.caption(f"Last updated: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")

    # Sidebar Controls & Filters
    st.sidebar.header("🔍 Pipeline Filters")

    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.subheader("System Info")
    st.sidebar.text(f"Storage Target: {STORAGE_MODE.upper()}")
    st.sidebar.text(f"Parquet Batches: {file_count}")
    st.sidebar.text(f"Total Loaded Records: {len(df):,}")
    st.sidebar.markdown("---")

    if df.empty:
        st.info("No clickstream events available yet. Run the Producer & Consumer or execute `python scripts/generate_sample_data.py` to populate data.")
        return

    # Filter Options
    event_types = sorted(df["event_type"].dropna().unique().tolist()) if "event_type" in df.columns else []
    selected_event_types = st.sidebar.multiselect("Event Type", options=event_types, default=event_types)

    devices = sorted(df["device_type"].dropna().unique().tolist()) if "device_type" in df.columns else []
    selected_devices = st.sidebar.multiselect("Device Type", options=devices, default=devices)

    countries = sorted(df["country"].dropna().unique().tolist()) if "country" in df.columns else []
    selected_countries = st.sidebar.multiselect("Country", options=countries, default=countries)

    # Apply filters
    filtered_df = df.copy()
    if selected_event_types:
        filtered_df = filtered_df[filtered_df["event_type"].isin(selected_event_types)]
    if selected_devices:
        filtered_df = filtered_df[filtered_df["device_type"].isin(selected_devices)]
    if selected_countries:
        filtered_df = filtered_df[filtered_df["country"].isin(selected_countries)]

    # Calculate Analytics
    analytics = ClickstreamAnalytics(filtered_df)
    kpis = analytics.get_overview_kpis()

    # KPI Metric Row
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    with k1:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{kpis["total_events"]:,}</div><div class="metric-label">Total Events</div></div>', unsafe_allow_html=True)
    with k2:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{kpis["unique_users"]:,}</div><div class="metric-label">Unique Users</div></div>', unsafe_allow_html=True)
    with k3:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{kpis["unique_sessions"]:,}</div><div class="metric-label">Sessions</div></div>', unsafe_allow_html=True)
    with k4:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{kpis["total_purchases"]:,}</div><div class="metric-label">Purchases</div></div>', unsafe_allow_html=True)
    with k5:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{kpis["add_to_cart_rate"]}%</div><div class="metric-label">Add-To-Cart Rate</div></div>', unsafe_allow_html=True)
    with k6:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{kpis["purchase_conversion_rate"]}%</div><div class="metric-label">Conversion Rate</div></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Charts Row 1: Time Series & Distribution
    c1, c2 = st.columns([2, 1])

    with c1:
        st.subheader("📈 Event Stream Timeline")
        ts_df = analytics.get_events_over_time(freq="1Min")
        if not ts_df.empty:
            fig_ts = px.line(
                ts_df,
                x="timestamp",
                y="count",
                labels={"timestamp": "Time (UTC)", "count": "Events / Min"},
                template="plotly_dark",
                color_discrete_sequence=["#4FACFE"]
            )
            fig_ts.update_layout(margin=dict(l=20, r=20, t=20, b=20), height=320)
            st.plotly_chart(fig_ts, use_container_width=True)
        else:
            st.caption("Insufficient timestamp data for time series plot.")

    with c2:
        st.subheader("🍩 Event Type Breakdown")
        type_df = analytics.get_events_by_type()
        if not type_df.empty:
            fig_pie = px.pie(
                type_df,
                names="event_type",
                values="count",
                hole=0.4,
                template="plotly_dark",
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_pie.update_layout(margin=dict(l=20, r=20, t=20, b=20), height=320)
            st.plotly_chart(fig_pie, use_container_width=True)

    # Charts Row 2: Top Products & Demographics
    c3, c4, c5 = st.columns([1, 1, 1])

    with c3:
        st.subheader("🛍️ Most Viewed Products")
        prod_views = analytics.get_top_products(limit=8, event_type="product_view")
        if not prod_views.empty:
            fig_prod = px.bar(
                prod_views,
                x="count",
                y="product_id",
                orientation="h",
                template="plotly_dark",
                color="count",
                color_continuous_scale="Viridis"
            )
            fig_prod.update_layout(yaxis=dict(autorange="reversed"), margin=dict(l=20, r=20, t=20, b=20), height=300)
            st.plotly_chart(fig_prod, use_container_width=True)
        else:
            st.caption("No product view data.")

    with c4:
        st.subheader("📱 Device Distribution")
        dev_df = analytics.get_device_distribution()
        if not dev_df.empty:
            fig_dev = px.bar(
                dev_df,
                x="device_type",
                y="count",
                template="plotly_dark",
                color="device_type",
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            fig_dev.update_layout(margin=dict(l=20, r=20, t=20, b=20), height=300)
            st.plotly_chart(fig_dev, use_container_width=True)

    with c5:
        st.subheader("🌍 Country Distribution")
        ctry_df = analytics.get_country_distribution()
        if not ctry_df.empty:
            fig_ctry = px.bar(
                ctry_df,
                x="country",
                y="count",
                template="plotly_dark",
                color="country",
                color_discrete_sequence=px.colors.qualitative.Vivid
            )
            fig_ctry.update_layout(margin=dict(l=20, r=20, t=20, b=20), height=300)
            st.plotly_chart(fig_ctry, use_container_width=True)

    # Raw Data Table Preview
    with st.expander("📋 View Sample Processed Events Data"):
        st.dataframe(filtered_df.head(100), use_container_width=True)


if __name__ == "__main__":
    main()
