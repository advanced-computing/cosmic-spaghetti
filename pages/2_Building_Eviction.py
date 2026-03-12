from datetime import datetime

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

st.set_page_config(page_title="NYC Evictions", layout="wide")
st.title("NYC Evictions Dashboard")

url = "https://data.cityofnewyork.us/resource/6z8x-wfk4.json"
limit = 5000

date_col = "executed_date"
borough_col = "borough"
building_col = "residential_commercial_ind"

start_date = "2025-01-01T00:00:00"
end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


# --- Optimized data loader ---
@st.cache_data(ttl=3600)
def load_paginated_data(url: str) -> pd.DataFrame:
    all_records = []
    offset = 0
    session = requests.Session()  # persistent session

    select_cols = f"{date_col},{borough_col},{building_col}"
    where = f"{date_col} >= '{start_date}' AND {date_col} <= '{end_date}'"

    # Progress bar
    progress_bar = st.progress(0)

    while True:
        params = {"$limit": limit, "$offset": offset, "$where": where, "$select": select_cols}
        r = session.get(url, params=params)
        r.raise_for_status()
        chunk = r.json()
        if not chunk:
            break
        all_records.extend(chunk)
        offset += limit
        # Update progress bar visually (approximate)
        progress_bar.progress(min(offset / 50000, 1.0))  # assume ~50k rows max

    progress_bar.progress(1.0)
    df = pd.json_normalize(all_records)

    # Convert date
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])
    df["year"] = df[date_col].dt.year
    return df


# --- Cache GeoJSON ---
@st.cache_data(ttl=86400)
def get_geojson():
    geojson_url = "https://raw.githubusercontent.com/dwillis/nyc-maps/master/boroughs.geojson"
    response = requests.get(geojson_url)
    return response.json()


# --- Load data ---
with st.spinner("Loading eviction data (pagination + 2025→today filter)..."):
    df_evic = load_paginated_data(url)

st.success(f"Loaded {len(df_evic):,} rows (2025 → today)")
st.dataframe(df_evic.head(5), use_container_width=True)

# --- Filter options ---
borough_options = sorted(df_evic[borough_col].dropna().astype(str).unique().tolist())
building_options = sorted(df_evic[building_col].dropna().astype(str).unique().tolist())

selected_borough = st.multiselect("Select Borough(s)", borough_options, default=borough_options)
selected_building = st.multiselect(
    "Select Building Type(s)", building_options, default=building_options
)

df_filtered = df_evic.copy()
if selected_borough:
    df_filtered = df_filtered[df_filtered[borough_col].astype(str).isin(selected_borough)]
if selected_building:
    df_filtered = df_filtered[df_filtered[building_col].astype(str).isin(selected_building)]

st.caption(f"Filtered rows: {len(df_filtered):,}")
st.dataframe(df_filtered.head(20), use_container_width=True)

# --- Evictions by Borough (Bar) ---
st.subheader("Evictions by Borough (Filtered)")
st.info(
    "Currently showing borough-level data. "
    "Community District (sub-borough) map is under development."
)

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

# --- Evictions by Borough (Map) ---
st.subheader("Evictions by Borough (Map)")

if not df_filtered.empty:
    nyc_geo = get_geojson()
    borough_counts = df_filtered[borough_col].value_counts().reset_index()
    borough_counts.columns = ["Borough", "Evictions"]
    borough_counts["Borough"] = borough_counts["Borough"].str.strip().str.title()

    fig_map = px.choropleth_mapbox(
        borough_counts,
        geojson=nyc_geo,
        locations="Borough",
        featureidkey="properties.BoroName",
        color="Evictions",
        color_continuous_scale="Reds",
        mapbox_style="carto-positron",
        zoom=9.5,
        center={"lat": 40.7128, "lon": -74.0060},
        title="Evictions by Borough (Map)",
        hover_name="Borough",
        hover_data={"Evictions": True},
    )
    fig_map.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
    st.plotly_chart(fig_map, use_container_width=True)

# --- Evictions Over Time ---
st.subheader("Evictions Over Time")
bucket = st.selectbox("Time bucket", ["Monthly", "Weekly", "Daily"], index=0)
freq_map = {"Monthly": "M", "Weekly": "W-MON", "Daily": "D"}
freq = freq_map[bucket]

df_ts = df_filtered.copy()
df_ts["Period"] = df_ts[date_col].dt.to_period(freq).dt.to_timestamp()
ts = df_ts.groupby(["Period", borough_col]).size().reset_index(name="Evictions")

fig_ts = px.line(
    ts,
    x="Period",
    y="Evictions",
    color=borough_col,
    markers=True,
    title="Evictions by Borough Over Time",
)
st.plotly_chart(fig_ts, use_container_width=True)
