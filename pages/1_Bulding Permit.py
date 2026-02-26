from __future__ import annotations

import plotly.express as px
import streamlit as st

from functions.permit_page import (
    apply_filter,
    filter_last_12_months,
    first_column,
    load_paginated,
    map_borough,
    permit_timeseries_by_borough,
)

st.set_page_config(page_title="NYC Building Job", layout="wide")
st.title("NYC Building Job (Last 12 Months)")

url = "https://data.cityofnewyork.us/resource/rbx6-tga4.json"  # url api soda 2 (as of february 25th)
limit = 5000

import json

import requests

# st.subheader("Debug: raw API sample (first 1 row)")
# sample = requests.get(url, params={"$limit": 1}).json()
# st.write("Keys in first row:", sorted(sample[0].keys()) if sample else "No rows")
# st.json(sample[0] if sample else {})


# choose colums to use
desired_columns = [
    "borough",
    "issued_date",
    "approved_date",
    "expired_date",
    "work_type",
    "permit_status",
    "community_board",
]

with st.spinner("Loading permits (pagination)..."):
    df = load_paginated(
        url,
        desired_columns=desired_columns,
        limit=50_000,
        max_rows=250_000,
        order_by="issued_date",
    )

if df.empty:
    st.error("No rows returned from API")
    st.stop()

# detecting columns for 12 months filter

date_col = first_column(df, ["issued_date", "approved_date", "expired_date"])
if not date_col:
    st.error("Could not find suitable date columns for filtering")
    st.write("Colums found:", df.columns.tolist())
    st.stop()

df = map_borough(df, borough_col="borough")
df = filter_last_12_months(df, date_col=date_col)

if df.empty:
    st.error("No permits found in the last 12 months")
    st.write("Using date colum", date_col)
    st.stop()

st.success(f"Loaded{len(df):,} rows (for the last 12 months) using `{date_col}`")
st.dataframe(df.head(5), use_container_width=True)

st.write("Loaded DF columns:", df.columns.tolist())

borough_col = "borough" if "borough" in df.columns else None
type_col = first_column(df, ["permit_type", "work_type", "job_type", "permit_subtype"])
status_col = first_column(df, ["permit_status"])

st.subheader("Filters")

if borough_col:
    borough_options = sorted(df[borough_col].dropna().astype(str).unique().tolist())
    selected_borough = st.multiselect(
        "Borough", borough_options, default=borough_options
    )
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
    selected_status = st.multiselect(
        "Permit Status", status_options, default=status_options
    )
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

st.caption(f"Filtered rows:{len(df_filtered):,}")
st.dataframe(df_filtered.head(5), use_container_width=True)

st.subheader("Approved and Issued Building Job Permits by Borough Over Time")

bucket = st.selectbox("Time bucket", ["Monthly", "Weekly", "Daily"], index=0)
freq = {"Monthly": "MS", "Weekly": "W-MON", "Daily": "D"}[bucket]

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
