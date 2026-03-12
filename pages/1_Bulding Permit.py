from __future__ import annotations

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

# temporary fix
import functions.permit_page as _pp  # noqa: F401
from functions.permit_page import (
    apply_filter,
    filter_last_12_months,
    first_column,
    load_paginated,
    permit_timeseries_by_borough,
)

st.set_page_config(page_title="NYC Building Job", layout="wide")
st.write("permit_page location:", _pp.__file__)
st.title("NYC Building Job (Last 12 Months)")

URL = "https://data.cityofnewyork.us/resource/rbx6-tga4.json"

DESIRED_COLUMNS = [
    "borough",
    "issued_date",
    "approved_date",
    "expired_date",
    "work_type",
    "permit_status",
    "community_board",
]


@st.cache_data(ttl=3600, show_spinner=False)
def get_permits():
    return load_paginated(
        URL,
        desired_columns=DESIRED_COLUMNS,
        limit=50_000,
        max_rows=250_000,
        order_by="issued_date",
        date_col="issued_date",
    )


# load geojson
@st.cache_data(ttl=86400, show_spinner=False)
def get_geojson():
    geojson_url = "https://raw.githubusercontent.com/dwillis/nyc-maps/master/boroughs.geojson"
    response = requests.get(geojson_url)
    return response.json()


with st.spinner("Loading permits..."):
    df = get_permits()

if df.empty:
    st.error("No rows returned from API")
    st.stop()

# finding columns for 12 months filter
date_col = first_column(df, ["issued_date", "approved_date", "expired_date"])
if not date_col:
    st.error("Could not find suitable date columns for filtering")
    st.write("Columns found:", df.columns.tolist())
    st.stop()

df = filter_last_12_months(df, date_col=date_col)

if df.empty:
    st.error("No permits found in the last 12 months")
    st.write("Using date column:", date_col)
    st.stop()

st.success(f"Loaded {len(df):,} rows (last 12 months) using `{date_col}`")
# st.dataframe(df.head(5), use_container_width=True)
# st.write("Loaded DF columns:", df.columns.tolist())

borough_col = "borough" if "borough" in df.columns else None
type_col = first_column(df, ["permit_type", "work_type", "job_type", "permit_subtype"])
status_col = first_column(df, ["permit_status"])

st.subheader("Filters")

with st.expander("Set Filters", expanded=False):
    if borough_col:
        borough_options = sorted(df[borough_col].dropna().astype(str).unique().tolist())
        selected_borough = st.multiselect("Borough", borough_options, default=borough_options)
    else:
        selected_borough = None
        st.info("No borough column found to filter by")

    if type_col:
        type_options = sorted(df[type_col].dropna().astype(str).unique().tolist())
        selected_types = st.multiselect(
            type_col.replace("_", " ").title(), type_options, default=type_options
        )
    else:
        selected_types = None
        st.info("No type column found to filter by")

    if status_col:
        status_options = sorted(df[status_col].dropna().astype(str).unique().tolist())
        selected_status = st.multiselect("Permit Status", status_options, default=status_options)
    else:
        selected_status = None

df_filtered = apply_filter(
    df,
    borough_col=borough_col,
    selected_borough=selected_borough,
    type_col=type_col,
    selected_types=selected_types,
    status_col=status_col,
    selected_status=selected_status,
)

st.caption(f"Filtered rows: {len(df_filtered):,}")
# st.dataframe(df_filtered.head(5), use_container_width=True)

st.subheader("Approved and Issued Building Job Permits by Borough Over Time")

bucket = st.selectbox("Time bucket", ["Monthly", "Weekly", "Daily"], index=0)
freq = {"Monthly": "MS", "Weekly": "W-MON", "Daily": "D"}[bucket]

# Metrics

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

    # a. total permit metrics
    current_total = len(current)
    previous_total = len(previous)
    if previous_total > 0:
        total_percentage = ((current_total - previous_total) / previous_total) * 100
        total_delta = f"{total_percentage:+.1f}%"
    else:
        total_delta = None

    # b. borough w/highest permt
    if borough_col and borough_col in df_filtered.columns:
        current_boro = current[borough_col].value_counts()
        previous_boro = previous[borough_col].value_counts()
        top_boro = current_boro.idxmax() if not current_boro.empty else "N/A"
        top_boro_count = int(current_boro.max()) if not current_boro.empty else 0
        previous_boro_count = int(previous_boro.get(top_boro, 0))
        boro_delta = f"{top_boro_count - previous_boro_count:+,}"
    else:
        top_boro = "N/A"
        top_boro_count = 0
        boro_delta = None

    # c. work type with highest permit
    if type_col and type_col in df_filtered.columns:
        current_type = current[type_col].value_counts()
        previous_type = previous[type_col].value_counts()
        top_type = current_type.idxmax() if not current_type.empty else "N/A"
        top_type_count = int(current_type.max()) if not current_type.empty else 0
        previous_type_count = int(previous_type.get(top_type, 0))
        if previous_type_count > 0:
            type_pct = ((top_type_count - previous_type_count) / previous_type_count) * 100
            type_delta = f"{type_pct:+.1f}%"
        else:
            type_delta = None
    else:
        top_type = "N/A"
        top_type_count = 0
        type_delta = None

    # show metric
    st.subheader(f"Summary-Current {bucket} Period")
    col1, col2, col3 = st.columns(3)
    col1.metric(
        label=f"Total Permits (compared to previous {period_label})",
        value=f"{current_total:,}",
        delta=total_delta,
        border=True,
    )
    col2.metric(
        label=f"Borough with Highest Permits (compared to previous {period_label})",
        value=top_boro,
        delta=boro_delta,
        border=True,
    )

    col3.metric(
        label=f"Most Work Type  (compared to previous {period_label})",
        value=top_type,
        delta=type_delta,
        border=True,
    )
# st.write(df_filtered[borough_col].unique())

st.subheader("Permits by Borough")
if not df_filtered.empty and borough_col:
    nyc_geo = get_geojson()

    boro_counts = (
        df_filtered[borough_col]
        .value_counts()
        .reset_index()
        .rename(columns={borough_col: "Borough", "count": "Count"})
    )

    # match boro name to geoJSON
    boro_counts["Borough"] = boro_counts["Borough"].str.strip().str.title()

    fig_map = px.choropleth_mapbox(
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
    fig_map.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
    st.plotly_chart(fig_map, use_container_width=True)


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
    fig = px.line(
        ts,
        x="Period",
        y="Count",
        color="Borough",
        markers=True,
        title="Approved and Issued Building Job Permits by Borough Over Time",
    )
    st.plotly_chart(fig, use_container_width=True)
