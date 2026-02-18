import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from datetime import datetime

st.set_page_config(page_title="NYC Evictions", layout="wide")
st.title("NYC Evictions Dashboard")

url = "https://data.cityofnewyork.us/resource/6z8x-wfk4.json"
limit = 5000

date_col = "executed_date"
borough_col = "borough"
building_col = "residential_commercial_ind"

# Only 2025 data
start_date = "2025-01-01T00:00:00"
end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


# Loading data from NYC Open Data
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

    df = pd.json_normalize(all_record)
    # convert executed_date to datetime
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])  # drop invalid dates
    df["year"] = df[date_col].dt.year
    return df


# load data
with st.spinner("Loading eviction data (pagination + 2025→today filter)..."):
    df_evic = load_paginated_date_filtered(url)

st.success(f"Loaded {len(df_evic):,} rows (2025 → today)")
st.dataframe(df_evic.head(5), use_container_width=True)

# --- Filter options ---
borough_options = sorted(df_evic[borough_col].dropna().astype(str).unique().tolist())
building_options = sorted(df_evic[building_col].dropna().astype(str).unique().tolist())

selected_borough = st.multiselect(
    "Select Borough(s)", borough_options, default=borough_options
)
selected_building = st.multiselect(
    "Select Building Type(s)", building_options, default=building_options
)

df_filtered = df_evic.copy()
if selected_borough:
    df_filtered = df_filtered[
        df_filtered[borough_col].astype(str).isin(selected_borough)
    ]
if selected_building:
    df_filtered = df_filtered[
        df_filtered[building_col].astype(str).isin(selected_building)
    ]

st.caption(f"Filtered rows: {len(df_filtered):,}")
st.dataframe(df_filtered.head(20), use_container_width=True)

# --- Bar chart: Evictions by Borough ---
st.subheader("Evictions by Borough (Filtered)")

counts = df_filtered[borough_col].fillna("Missing").value_counts().reset_index()
counts.columns = ["Borough", "Total Evictions"]

fig = px.bar(
    counts,
    x="Borough",
    y="Total Evictions",
    title="NYC Evictions by Borough",
    color="Borough",
    color_discrete_sequence=px.colors.qualitative.Dark24_r,
)

st.plotly_chart(fig, use_container_width=True)
