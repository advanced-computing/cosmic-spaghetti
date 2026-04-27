from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import datetime, timedelta

import pandas as pd
import pandas_gbq
import plotly.express as px
import requests
import streamlit as st
from google.oauth2 import service_account

from complaint_categories import COMPLAINT_CATEGORY_MAP

st.set_page_config(page_title="NYC Building Complaints", layout="wide")
st.title("NYC Building Complaints Dashboard")

# Configuration
PROJECT_ID = "sipa-adv-c-cosmic-spaghetti"
DATASET = "cosmic_spaghetti"
TABLE = "complaints"

date_col = "date_entered"
borough_col = "borough"
category_col = "complaint_category"
status_col = "status"


# Page load time context manager
@contextmanager
def display_load_time():
    start_time = time.time()
    try:
        yield
    finally:
        elapsed = time.time() - start_time
        st.caption(f"Page loaded in {elapsed:.2f} seconds")


# GeoJSON loader with caching
@st.cache_data(ttl=86400, show_spinner=False)
def get_geojson():
    geojson_url = "https://raw.githubusercontent.com/dwillis/nyc-maps/master/boroughs.geojson"
    response = requests.get(geojson_url)
    return response.json()


# Load from BQ
@st.cache_data(ttl=3600, show_spinner=False)
def load_complaints() -> pd.DataFrame:
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    query = f"""
    SELECT
        community_board,
        date_entered,
        complaint_category,
        status
    FROM `{PROJECT_ID}.{DATASET}.complaints`
    WHERE date_entered IS NOT NULL
    LIMIT 10000
    """
    df = pandas_gbq.read_gbq(
        query,
        project_id=PROJECT_ID,
        credentials=credentials,
        progress_bar_type=None,
        dtypes={
            "community_board": "str",
            "complaint_category": "str",
            "status": "str",
        },
    )
    df["date_entered"] = pd.to_datetime(df["date_entered"], errors="coerce")

    # extract borough from community_board (first digit = borough code)
    borough_map = {
        "1": "Manhattan",
        "2": "Bronx",
        "3": "Brooklyn",
        "4": "Queens",
        "5": "Staten Island",
    }
    df["borough"] = df["community_board"].str[0].map(borough_map).fillna("Unknown")
    df["complaint_desc"] = (
        df["complaint_category"].map(COMPLAINT_CATEGORY_MAP).fillna(df["complaint_category"])
    )
    return df


# Main page
with display_load_time():
    with st.spinner("Loading complaints data from BigQuery..."):
        df = load_complaints()

    if df.empty:
        st.error("No rows returned from BigQuery")
        st.stop()

    st.success(f"Loaded {len(df):,} rows (last 12 months)")

    # ── Filters ───────────────────────────────────────────────────────────────
    st.subheader("Filters")

    with st.expander("Set Filters", expanded=False):
        borough_options = sorted(df[borough_col].dropna().astype(str).unique().tolist())
        selected_borough = st.multiselect("Borough", borough_options, default=borough_options)

        category_options = sorted(df[category_col].dropna().astype(str).unique().tolist())
        selected_category = st.multiselect(
            "Complaint Category", category_options, default=category_options
        )

        status_options = sorted(df[status_col].dropna().astype(str).unique().tolist())
        selected_status = st.multiselect("Status", status_options, default=status_options)

    df_filtered = df.copy()
    if selected_borough:
        df_filtered = df_filtered[df_filtered[borough_col].astype(str).isin(selected_borough)]
    if selected_category:
        df_filtered = df_filtered[df_filtered[category_col].astype(str).isin(selected_category)]
    if selected_status:
        df_filtered = df_filtered[df_filtered[status_col].astype(str).isin(selected_status)]

    st.caption(f"Filtered rows: {len(df_filtered):,}")

    # summary metrics
    bucket = st.selectbox(
        "Time bucket", ["Monthly", "Weekly", "Daily"], index=0, key="complaints_bucket"
    )

    st.subheader(f"Summary of Complaints — Current {bucket}")

    if not df_filtered.empty:
        end_period = df_filtered[date_col].max()
        offsets_map = {
            "Monthly": pd.DateOffset(months=1),
            "Weekly": pd.DateOffset(weeks=1),
            "Daily": pd.DateOffset(days=1),
        }
        offsets = offsets_map[bucket]
        period_label = bucket.lower().rstrip("ly")

        start_period = end_period - offsets
        start_prev = start_period - offsets

        current = df_filtered[df_filtered[date_col] > start_period]
        previous = df_filtered[
            (df_filtered[date_col] > start_prev) & (df_filtered[date_col] <= start_period)
        ]

        # total complaints
        current_total = len(current)
        previous_total = len(previous)
        if previous_total > 0:
            total_pct = ((current_total - previous_total) / previous_total) * 100
            total_delta = f"{total_pct:+.1f}%"
        else:
            total_delta = None

        # borough with most complaints
        current_boro = current[borough_col].value_counts()
        previous_boro = previous[borough_col].value_counts()
        top_boro = current_boro.idxmax() if not current_boro.empty else "N/A"
        top_boro_count = int(current_boro.max()) if not current_boro.empty else 0
        prev_boro_count = int(previous_boro.get(top_boro, 0))
        boro_delta = f"{top_boro_count - prev_boro_count:+,}"

        # most common complaint category
        current_cat = current[category_col].value_counts()
        top_cat = current_cat.idxmax() if not current_cat.empty else "N/A"

        col1, col2, col3 = st.columns(3)
        col1.metric(
            label=f"Total Complaints (vs previous {period_label})",
            value=f"{current_total:,}",
            delta=total_delta,
            border=True,
        )
        col2.metric(
            label="Borough with Most Complaints",
            value=top_boro,
            delta=boro_delta,
            border=True,
        )
        col3.metric(
            label="Most Common Category",
            value=top_cat,
            border=True,
        )

    # complaints by borough
    st.subheader("Complaints by Borough")

    counts = df_filtered[borough_col].fillna("Missing").value_counts().reset_index()
    counts.columns = ["Borough", "Total Complaints"]

    fig_bar = px.bar(
        counts,
        x="Borough",
        y="Total Complaints",
        title="NYC Building Complaints by Borough",
        color="Borough",
        color_discrete_sequence=px.colors.qualitative.Dark24_r,
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # complaints by borough (map)
    st.subheader("Complaints by Borough (Map)")

    if not df_filtered.empty:
        nyc_geo = get_geojson()
        borough_counts = df_filtered[borough_col].value_counts().reset_index()
        borough_counts.columns = ["Borough", "Complaints"]
        borough_counts["Borough"] = borough_counts["Borough"].str.strip().str.title()

        fig_map = px.choropleth_mapbox(
            borough_counts,
            geojson=nyc_geo,
            locations="Borough",
            featureidkey="properties.BoroName",
            color="Complaints",
            color_continuous_scale="Reds",
            mapbox_style="carto-positron",
            zoom=9.5,
            center={"lat": 40.7128, "lon": -74.0060},
            title="Building Complaints by Borough (Map)",
            hover_name="Borough",
            hover_data={"Complaints": True},
        )
        fig_map.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
        st.plotly_chart(fig_map, use_container_width=True)

    # Complaint categories
    st.subheader("Top 10 Complaint Categories")

    top_categories = (
        df_filtered["complaint_desc"].fillna("Unknown").value_counts().head(10).reset_index()
    )
    top_categories.columns = ["Category", "Count"]

    fig_cat = px.bar(
        top_categories,
        x="Count",
        y="Category",
        orientation="h",
        title="Top 10 Complaint Categories",
        color="Count",
        color_continuous_scale="Reds",
    )
    fig_cat.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_cat, use_container_width=True)

    # Complaints over time
    st.subheader("Complaints Over Time")

    bucket_ts = st.selectbox(
        "Time bucket", ["Monthly", "Weekly", "Daily"], index=0, key="complaints_timeseries"
    )
    freq_map = {"Monthly": "M", "Weekly": "W-MON", "Daily": "D"}
    freq = freq_map[bucket_ts]

    df_ts = df_filtered.copy()
    df_ts["Period"] = df_ts[date_col].dt.to_period(freq).dt.to_timestamp()
    ts = df_ts.groupby(["Period", borough_col]).size().reset_index(name="Complaints")

    fig_ts = px.line(
        ts,
        x="Period",
        y="Complaints",
        color=borough_col,
        markers=True,
        title="Building Complaints by Borough Over Time",
    )
    st.plotly_chart(fig_ts, use_container_width=True)
