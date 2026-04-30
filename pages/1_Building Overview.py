from __future__ import annotations

import time
from datetime import datetime, timedelta

import pandas as pd
import pandas_gbq
import plotly.express as px
import requests
import streamlit as st
from google.oauth2 import service_account

import functions.permit_page as _pp  # noqa: F401
from functions.permit_page import (
    apply_filter,
    filter_last_12_months,
    first_column,
    permit_timeseries_by_borough,
)

st.set_page_config(page_title="NYC Buildings Overview", layout="wide")
st.title("NYC Buildings Overview")

start_time = time.time()

PROJECT_ID = "sipa-adv-c-cosmic-spaghetti"
DATASET = "cosmic_spaghetti"
MIN_CONSTRUCTION_YEAR = 1900
MAX_CONSTRUCTION_YEAR = 2025


@st.cache_data(ttl=86400, show_spinner=False)
def get_geojson():
    response = requests.get(
        "https://raw.githubusercontent.com/dwillis/nyc-maps/master/boroughs.geojson"
    )
    return response.json()


def get_credentials():
    return service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )


@st.cache_data(ttl=86400, show_spinner=False)
def load_buildings_summary() -> pd.DataFrame:
    query = f"""
    SELECT borough, cnstrct_yr, total_buildings, avg_height
    FROM `{PROJECT_ID}.{DATASET}.buildings_summary`
    WHERE borough IS NOT NULL AND cnstrct_yr <= {MAX_CONSTRUCTION_YEAR}
    """
    return pandas_gbq.read_gbq(
        query,
        project_id=PROJECT_ID,
        credentials=get_credentials(),
        progress_bar_type=None,
        dtypes={"borough": "str"},
    )


@st.cache_data(ttl=3600, show_spinner=False)
def load_new_buildings() -> pd.DataFrame:
    query = f"""
    SELECT borough, permit_date, permit_type, permit_type_desc,
        status, latitude, longitude
    FROM `{PROJECT_ID}.{DATASET}.permits`
    WHERE permit_type = 'NB'
    AND borough IS NOT NULL
    AND permit_date IS NOT NULL
    """
    df = pandas_gbq.read_gbq(
        query,
        project_id=PROJECT_ID,
        credentials=get_credentials(),
        progress_bar_type=None,
        dtypes={"borough": "str"},
    )
    df["permit_date"] = pd.to_datetime(df["permit_date"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def load_other_permits() -> pd.DataFrame:
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    query = f"""
    SELECT borough, permit_date, permit_type, permit_type_desc,
           status, latitude, longitude, source
    FROM `{PROJECT_ID}.{DATASET}.permits`
    WHERE permit_type != 'NB'
    AND permit_date >= '{one_year_ago}'
    AND borough IS NOT NULL
    LIMIT 50000
    """
    df = pandas_gbq.read_gbq(
        query,
        project_id=PROJECT_ID,
        credentials=get_credentials(),
        progress_bar_type=None,
        dtypes={"borough": "str", "permit_type": "str", "status": "str"},
    )
    df["permit_date"] = pd.to_datetime(df["permit_date"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    return df


@st.cache_data(ttl=86400, show_spinner=False)
def load_facades() -> pd.DataFrame:
    query = f"""
    SELECT borough, filing_status, current_status, cycle, filing_date
    FROM `{PROJECT_ID}.{DATASET}.facades`
    WHERE borough IS NOT NULL
    """
    df = pandas_gbq.read_gbq(
        query,
        project_id=PROJECT_ID,
        credentials=get_credentials(),
        progress_bar_type=None,
        dtypes={"borough": "str", "filing_status": "str"},
    )
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    return df


# ── Load all data ─────────────────────────────────────────────────────────────
with st.spinner("Loading data..."):
    df_summary = load_buildings_summary()
    df_new = load_new_buildings()
    df_other = load_other_permits()
    df_facades = load_facades()

nyc_geo = get_geojson()
df_other_2025 = df_other[df_other["permit_date"] >= "2025-01-01"]

# ── Top metrics ───────────────────────────────────────────────────────────────
total_unsafe = (
    len(df_facades[df_facades["filing_status"].str.contains("UNSAFE", na=False)])
    if not df_facades.empty
    else 0
)

col1, col2, col3, col4 = st.columns(4)
col1.metric(
    label="Total Buildings (end of 2025)",
    value=f"{int(df_summary['total_buildings'].sum()):,}",
    border=True,
)
col2.metric(
    label="New Building Jobs (2008–2020)",
    value=f"{len(df_new):,}",
    border=True,
)
col3.metric(
    label="Other Permits (Jan 2025+)",
    value=f"{len(df_other_2025):,}",
    border=True,
)
col4.metric(
    label="Unsafe Facade Filings",
    value=f"{total_unsafe:,}",
    border=True,
)

st.divider()

tab1, tab2, tab3, tab4 = st.tabs(
    [
        "🏙️ Total Buildings",
        "🏗️ New Building Jobs",
        "🔨 Other Building Jobs",
        "🔍 Facade Inspection (FISP)",
    ]
)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — TOTAL BUILDINGS IN NYC
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("Total Buildings in NYC (end of 2025)")
    st.caption("Source: NYC Building Footprints (5zhs-2jue) — cnstrct_yr ≤ 2025")

    boro_summary = df_summary.groupby("borough")["total_buildings"].sum().reset_index()
    boro_summary["Borough"] = boro_summary["borough"].str.title()

    col1, col2 = st.columns(2)
    with col1:
        fig_map = px.choropleth_mapbox(
            boro_summary,
            geojson=nyc_geo,
            locations="Borough",
            featureidkey="properties.BoroName",
            color="total_buildings",
            color_continuous_scale="Blues",
            mapbox_style="carto-positron",
            zoom=9.5,
            center={"lat": 40.7128, "lon": -74.0060},
            title="Total Buildings by Borough",
            hover_name="Borough",
            labels={"total_buildings": "Buildings"},
        )
        fig_map.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
        st.plotly_chart(fig_map, use_container_width=True)
    with col2:
        fig_bar = px.bar(
            boro_summary.sort_values("total_buildings", ascending=False),
            x="Borough",
            y="total_buildings",
            title="Total Buildings by Borough",
            color="Borough",
            color_discrete_sequence=px.colors.qualitative.Dark24_r,
            labels={"total_buildings": "Buildings"},
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    st.subheader("Buildings Constructed Per Year")
    df_yr = (
        df_summary[
            (df_summary["cnstrct_yr"] >= MIN_CONSTRUCTION_YEAR)
            & (df_summary["cnstrct_yr"] <= MAX_CONSTRUCTION_YEAR)
        ]
        .groupby(["cnstrct_yr", "borough"])["total_buildings"]
        .sum()
        .reset_index()
        .rename(columns={"cnstrct_yr": "Year", "total_buildings": "Buildings"})
    )
    fig_yr = px.line(
        df_yr,
        x="Year",
        y="Buildings",
        color="borough",
        title="Buildings Constructed Per Year by Borough",
        labels={"borough": "Borough"},
    )
    st.plotly_chart(fig_yr, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — NEW BUILDING JOBS (2008–2020)
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("New Building Jobs (2008–2020)")
    st.caption(
        "Source: DOB Permit Issuance (ipu4-2q9a) — job_type = NB. "
        "Data available 2008–2020. New buildings after 2020 are not yet in any public dataset."
    )

    if df_new.empty:
        st.info("No new building permits found.")
    else:
        new_by_boro = (
            df_new["borough"]
            .value_counts()
            .reset_index()
            .rename(columns={"borough": "Borough", "count": "New Buildings"})
        )
        new_by_boro["Borough"] = new_by_boro["Borough"].str.title()

        col1, col2 = st.columns(2)
        with col1:
            fig_new_map = px.choropleth_mapbox(
                new_by_boro,
                geojson=nyc_geo,
                locations="Borough",
                featureidkey="properties.BoroName",
                color="New Buildings",
                color_continuous_scale="Greens",
                mapbox_style="carto-positron",
                zoom=9.5,
                center={"lat": 40.7128, "lon": -74.0060},
                title="New Building Permits by Borough",
                hover_name="Borough",
            )
            fig_new_map.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
            st.plotly_chart(fig_new_map, use_container_width=True)
        with col2:
            fig_new_bar = px.bar(
                new_by_boro.sort_values("New Buildings", ascending=False),
                x="Borough",
                y="New Buildings",
                title="New Building Permits by Borough",
                color="Borough",
                color_discrete_sequence=px.colors.qualitative.Dark24_r,
            )
            st.plotly_chart(fig_new_bar, use_container_width=True)

        df_new_coords = df_new.dropna(subset=["latitude", "longitude"])
        if not df_new_coords.empty:
            st.subheader("Where Are New Buildings Being Built?")
            fig_scatter = px.scatter_mapbox(
                df_new_coords,
                lat="latitude",
                lon="longitude",
                color="borough",
                mapbox_style="carto-positron",
                zoom=10,
                center={"lat": 40.7128, "lon": -74.0060},
                title="New Building Permit Locations",
                opacity=0.6,
                hover_data={"borough": True, "status": True, "permit_date": True},
            )
            fig_scatter.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
            st.plotly_chart(fig_scatter, use_container_width=True)

        df_new_yr = df_new.dropna(subset=["permit_date"]).copy()
        if not df_new_yr.empty:
            df_new_yr["Year"] = df_new_yr["permit_date"].dt.year
            yearly = df_new_yr.groupby("Year").size().reset_index(name="Count")
            fig_trend = px.bar(
                yearly,
                x="Year",
                y="Count",
                title="New Building Permits Per Year (2008–2020)",
                color_discrete_sequence=["#1D9E75"],
            )
            st.plotly_chart(fig_trend, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — OTHER BUILDING JOBS
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Other Building Jobs")
    st.caption(
        "Source: DOB NOW (rbx6-tga4) — all work permit types except New Building. "
        "Filtered to January 2025 onwards."
    )

    if df_other.empty:
        st.info("No other permits found.")
    else:
        st.markdown("#### Overview (January 2025 onwards)")

        other_by_boro_2025 = (
            df_other_2025["borough"]
            .value_counts()
            .reset_index()
            .rename(columns={"borough": "Borough", "count": "Permits"})
        )
        other_by_boro_2025["Borough"] = other_by_boro_2025["Borough"].str.title()

        by_type_2025 = (
            df_other_2025["permit_type_desc"]
            .fillna(df_other_2025["permit_type"])
            .value_counts()
            .head(10)
            .reset_index()
            .rename(columns={"permit_type_desc": "Permit Type", "count": "Count"})
        )

        col1, col2 = st.columns(2)
        with col1:
            fig_other_map = px.choropleth_mapbox(
                other_by_boro_2025,
                geojson=nyc_geo,
                locations="Borough",
                featureidkey="properties.BoroName",
                color="Permits",
                color_continuous_scale="Oranges",
                mapbox_style="carto-positron",
                zoom=9.5,
                center={"lat": 40.7128, "lon": -74.0060},
                title="Other Permits by Borough (Jan 2025+)",
                hover_name="Borough",
            )
            fig_other_map.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
            st.plotly_chart(fig_other_map, use_container_width=True)
        with col2:
            fig_type = px.bar(
                by_type_2025,
                x="Count",
                y="Permit Type",
                orientation="h",
                title="Top 10 Permit Types (Jan 2025+)",
                color="Count",
                color_continuous_scale="Oranges",
            )
            fig_type.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig_type, use_container_width=True)

        df_other_coords = df_other_2025.dropna(subset=["latitude", "longitude"])
        if not df_other_coords.empty:
            st.subheader("Where Are Permits Being Filed? (Jan 2025+)")
            fig_other_scatter = px.scatter_mapbox(
                df_other_coords,
                lat="latitude",
                lon="longitude",
                color="permit_type_desc",
                mapbox_style="carto-positron",
                zoom=10,
                center={"lat": 40.7128, "lon": -74.0060},
                title="Permit Locations",
                opacity=0.5,
                hover_data={"borough": True, "permit_type_desc": True, "status": True},
            )
            fig_other_scatter.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
            st.plotly_chart(fig_other_scatter, use_container_width=True)

        st.divider()
        st.markdown("#### Detailed View (Last 12 Months)")
        st.success(f"Loaded {len(df_other):,} rows (last 12 months)")

        date_col = first_column(df_other, ["permit_date"])
        borough_col = "borough" if "borough" in df_other.columns else None
        type_col = first_column(
            df_other, ["permit_type_desc", "permit_type", "work_type", "job_type"]
        )
        status_col = first_column(df_other, ["status", "permit_status"])
        df_detail = filter_last_12_months(df_other, date_col=date_col)

        with st.expander("Set Filters", expanded=False):
            if borough_col:
                borough_options = sorted(
                    df_detail[borough_col].dropna().astype(str).unique().tolist()
                )
                selected_borough = st.multiselect(
                    "Borough",
                    borough_options,
                    default=borough_options,
                    key="t3_boro",
                )
            else:
                selected_borough = None

            if type_col:
                type_options = sorted(df_detail[type_col].dropna().astype(str).unique().tolist())
                selected_types = st.multiselect(
                    type_col.replace("_", " ").title(),
                    type_options,
                    default=type_options,
                    key="t3_type",
                )
            else:
                selected_types = None

            if status_col:
                status_options = sorted(
                    df_detail[status_col].dropna().astype(str).unique().tolist()
                )
                selected_status = st.multiselect(
                    "Permit Status",
                    status_options,
                    default=status_options,
                    key="t3_status",
                )
            else:
                selected_status = None

        df_filtered = apply_filter(
            df_detail,
            borough_col=borough_col,
            selected_borough=selected_borough,
            type_col=type_col,
            selected_types=selected_types,
            status_col=status_col,
            selected_status=selected_status,
        )
        st.caption(f"Filtered rows: {len(df_filtered):,}")

        bucket = st.selectbox(
            "Time bucket",
            ["Monthly", "Weekly", "Daily"],
            index=0,
            key="bucket_permits",
        )
        freq = {"Monthly": "MS", "Weekly": "W-MON", "Daily": "D"}[bucket]

        if not df_filtered.empty and date_col in df_filtered.columns:
            df_filtered[date_col] = pd.to_datetime(df_filtered[date_col], errors="coerce")
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

            current_total = len(current)
            previous_total = len(previous)
            total_delta = (
                f"{((current_total - previous_total) / previous_total * 100):+.1f}%"
                if previous_total > 0
                else None
            )

            if borough_col and borough_col in df_filtered.columns:
                current_boro = current[borough_col].value_counts()
                previous_boro = previous[borough_col].value_counts()
                top_boro = current_boro.idxmax() if not current_boro.empty else "N/A"
                top_boro_count = int(current_boro.max()) if not current_boro.empty else 0
                prev_boro_count = int(previous_boro.get(top_boro, 0))
                boro_delta = f"{top_boro_count - prev_boro_count:+,}"
            else:
                top_boro, boro_delta = "N/A", None

            if type_col and type_col in df_filtered.columns:
                current_type = current[type_col].value_counts()
                previous_type = previous[type_col].value_counts()
                top_type = current_type.idxmax() if not current_type.empty else "N/A"
                top_type_count = int(current_type.max()) if not current_type.empty else 0
                prev_type_count = int(previous_type.get(top_type, 0))
                type_delta = (
                    f"{((top_type_count - prev_type_count) / prev_type_count * 100):+.1f}%"
                    if prev_type_count > 0
                    else None
                )
            else:
                top_type, type_delta = "N/A", None

            st.subheader(f"Summary — Current {bucket} Period")
            col1, col2, col3 = st.columns(3)
            col1.metric(
                label=f"Total Permits (vs previous {period_label})",
                value=f"{current_total:,}",
                delta=total_delta,
                border=True,
            )
            col2.metric(
                label=f"Borough with Highest Permits (vs previous {period_label})",
                value=top_boro,
                delta=boro_delta,
                border=True,
            )
            col3.metric(
                label=f"Most Common Work Type (vs previous {period_label})",
                value=top_type,
                delta=type_delta,
                border=True,
            )

        if not df_filtered.empty and borough_col:
            boro_counts = (
                df_filtered[borough_col]
                .value_counts()
                .reset_index()
                .rename(columns={borough_col: "Borough", "count": "Count"})
            )
            boro_counts["Borough"] = boro_counts["Borough"].str.strip().str.title()
            fig_detail_map = px.choropleth_mapbox(
                boro_counts,
                geojson=nyc_geo,
                locations="Borough",
                featureidkey="properties.BoroName",
                color="Count",
                color_continuous_scale="Reds",
                mapbox_style="carto-positron",
                zoom=9.5,
                center={"lat": 40.7128, "lon": -74.0060},
                title="Number of Permits by Borough",
                hover_name="Borough",
                hover_data={"Count": True},
            )
            fig_detail_map.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
            st.plotly_chart(fig_detail_map, use_container_width=True)
        else:
            st.info("No data to display")

        ts = permit_timeseries_by_borough(
            df_filtered,
            date_col=date_col,
            borough_col=borough_col or "borough",
            status_col=None,
            freq=freq,
        )
        if ts.empty:
            st.info("No rows to visualize")
        else:
            fig_ts = px.line(
                ts,
                x="Period",
                y="Count",
                color="Borough",
                markers=True,
                title="Building Job Permits by Borough Over Time",
            )
            st.plotly_chart(fig_ts, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — FACADE INSPECTION (FISP)
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.subheader("Facade Inspection Safety Program (FISP)")
    st.caption(
        "Buildings taller than 6 stories must be inspected every 5 years. "
        "Current cycle: Cycle 10 (2025–2030). Data available: 2001–present. "
        "Classifications: Safe · SWARMP (needs repair within 5 years) · Unsafe."
    )

    if df_facades.empty:
        st.info("No facade inspection data found.")
    else:
        unsafe_count = len(df_facades[df_facades["filing_status"].str.contains("UNSAFE", na=False)])
        swarmp_count = len(df_facades[df_facades["filing_status"].str.contains("SWARMP", na=False)])
        safe_count = len(df_facades[df_facades["filing_status"].str.contains("SAFE", na=False)])

        col1, col2, col3 = st.columns(3)
        col1.metric(label="Safe Filings", value=f"{safe_count:,}", border=True)
        col2.metric(label="SWARMP Filings", value=f"{swarmp_count:,}", border=True)
        col3.metric(label="Unsafe Filings", value=f"{unsafe_count:,}", border=True)

        df_cycle9 = df_facades[df_facades["cycle"] == "9"]
        status_counts = (
            df_cycle9["filing_status"]
            .fillna("UNKNOWN")
            .value_counts()
            .reset_index()
            .rename(columns={"filing_status": "Status", "count": "Count"})
        )

        col1, col2 = st.columns(2)
        with col1:
            fig_pie = px.pie(
                status_counts,
                names="Status",
                values="Count",
                title="Facade Filing Status (Cycle 9, Previous Cycle)",
                color="Status",
                color_discrete_map={
                    "SAFE": "#639922",
                    "SWARMP": "#BA7517",
                    "UNSAFE": "#E24B4A",
                    "NO REPORT FILED": "#888780",
                },
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        with col2:
            unsafe_by_boro = (
                df_cycle9[df_cycle9["filing_status"].str.contains("UNSAFE", na=False)]["borough"]
                .value_counts()
                .reset_index()
                .rename(columns={"borough": "Borough", "count": "Unsafe Filings"})
            )
            unsafe_by_boro["Borough"] = unsafe_by_boro["Borough"].str.title()
            fig_unsafe = px.bar(
                unsafe_by_boro.sort_values("Unsafe Filings", ascending=False),
                x="Borough",
                y="Unsafe Filings",
                title="Unsafe Facade Filings by Borough",
                color="Borough",
                color_discrete_sequence=px.colors.qualitative.Dark24_r,
            )
            st.plotly_chart(fig_unsafe, use_container_width=True)

        fisp_by_boro = (
            df_facades[df_facades["filing_status"].str.contains("UNSAFE|SWARMP", na=False)]
            .groupby("borough")["filing_status"]
            .count()
            .reset_index()
            .rename(columns={"borough": "Borough", "filing_status": "At Risk Filings"})
        )
        fisp_by_boro["Borough"] = fisp_by_boro["Borough"].str.title()

        fig_fisp_map = px.choropleth_mapbox(
            fisp_by_boro,
            geojson=nyc_geo,
            locations="Borough",
            featureidkey="properties.BoroName",
            color="At Risk Filings",
            color_continuous_scale="Reds",
            mapbox_style="carto-positron",
            zoom=9.5,
            center={"lat": 40.7128, "lon": -74.0060},
            title="At Risk Facades (SWARMP + Unsafe) by Borough",
            hover_name="Borough",
            hover_data={"At Risk Filings": True},
        )
        fig_fisp_map.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
        st.plotly_chart(fig_fisp_map, use_container_width=True)

        if "cycle" in df_facades.columns:
            cycle_counts = (
                df_facades.groupby(["cycle", "filing_status"]).size().reset_index(name="Count")
            )
            fig_cycle = px.bar(
                cycle_counts,
                x="cycle",
                y="Count",
                color="filing_status",
                title="Facade Filing Status by Cycle",
                barmode="group",
                color_discrete_map={
                    "SAFE": "#639922",
                    "SWARMP": "#BA7517",
                    "UNSAFE": "#E24B4A",
                },
                labels={"filing_status": "Status", "cycle": "Cycle"},
            )
            st.plotly_chart(fig_cycle, use_container_width=True)

st.caption(f"Page loaded in {time.time() - start_time:.2f} seconds")
