from datetime import datetime

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

st.set_page_config(page_title="NYC Job Filings", layout="wide")
st.title("NYC Building Job Filings (2026 → Today)")

url = "https://data.cityofnewyork.us/resource/w9ak-ipjd.json"
limit = 5000

# prepare filter in streamlit app
date_col = "filing_date"
status_col = "filing_status"
jobtype_col = "job_type"
borough_col = "borough"

# filter for 2026 to today
start_date = "2026-01-01T00:00:00"
end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


# ---------- helpers to avoid guessing column names ----------
@st.cache_data(ttl=3600)
def load_paginated_date_filtered(url: str) -> pd.DataFrame:
    all_record = []
    offset = 0

    where = f"{date_col} >= '{start_date}' AND {date_col} <= '{end_date}'"

    while True:
        params = {"$limit": limit, "$offset": offset, "$where": where}
        r = requests.get(url, params=params)
        r.raise_for_status()
        chunk = r.json()

        if not chunk:
            break

        all_record.extend(chunk)
        offset += limit

    return pd.json_normalize(all_record)


# load data
with st.spinner("Loading data (pagination + 2026→today filter)..."):
    df_job = load_paginated_date_filtered(url)

st.success(f"Loaded {len(df_job):,} rows (2026 → today)")

# show first 5 rows to confirm data loaded correctly and columns are as expected
st.dataframe(df_job.head(5), use_container_width=True)


# Build options safely
status_options = sorted(df_job[status_col].dropna().astype(str).unique().tolist())
jobtype_options = sorted(df_job[jobtype_col].dropna().astype(str).unique().tolist())

# Defaults: select all (so chart shows something immediately)
selected_status = st.multiselect(
    "Job Status Descrp", status_options, default=status_options
)
selected_jobtype = st.multiselect("Job Type", jobtype_options, default=jobtype_options)

df_filtered = df_job.copy()
if selected_status:
    df_filtered = df_filtered[df_filtered[status_col].astype(str).isin(selected_status)]
if selected_jobtype:
    df_filtered = df_filtered[
        df_filtered[jobtype_col].astype(str).isin(selected_jobtype)
    ]

st.caption(f"Filtered rows: {len(df_filtered):,}")
st.dataframe(df_filtered.head(20), use_container_width=True)

# bar chart
st.subheader("Job Filings by Borough (Filtered)")

counts = df_filtered[borough_col].fillna("Missing").value_counts().reset_index()
counts.columns = ["Borough", "Count"]

fig = px.bar(counts, x="Borough", y="Count", title="Job Filings by Borough")

st.plotly_chart(fig, use_container_width=True)
