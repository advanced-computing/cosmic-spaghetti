import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

st.set_page_config(page_title="NYC Evictions", layout="wide")
st.title("NYC Evictions Dashboard")

date_col = "executed_date"
borough_col = "borough"
building_col = "residential_commercial_ind"

start_date = "2025-01-01T00:00:00"
end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


# --- Cache GeoJSON ---
@st.cache_data(ttl=86400)
def get_geojson():
    geojson_url = "https://raw.githubusercontent.com/dwillis/nyc-maps/master/boroughs.geojson"
    response = requests.get(geojson_url)
    return response.json()


@st.cache_data(ttl=3600)
def load_data_from_bq():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )

    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )

    query = """
    SELECT executed_date, borough, residential_commercial_ind
    FROM `sipa-adv-c-cosmic-spaghetti.cosmic_spaghetti.evictions`
    WHERE executed_date >= '2025-01-01'
    """

    df = client.query(query).to_dataframe()

    df["executed_date"] = pd.to_datetime(df["executed_date"], errors="coerce")
    df = df.dropna(subset=["executed_date"])
    df["year"] = df["executed_date"].dt.year

    return df


# --- Load data ---
with st.spinner("Loading eviction data from BigQuery..."):
    df_evic = load_data_from_bq()

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

# --- Summary Metrics ---
bucket = st.selectbox("Time bucket", ["Monthly", "Weekly", "Daily"], index=0, key="metrics_bucket")

st.subheader(f"Summary of Evictions — Current {bucket}")

if not df_filtered.empty and date_col in df_filtered.columns:
    df_metrics = df_filtered.copy()
    df_metrics[date_col] = pd.to_datetime(df_metrics[date_col], errors="coerce")

    end_period = df_metrics[date_col].max()

    offsets_map = {
        "Monthly": pd.DateOffset(months=1),
        "Weekly": pd.DateOffset(weeks=1),
        "Daily": pd.DateOffset(days=1),
    }

    offsets = offsets_map[bucket]
    period_label = bucket.lower().rstrip("ly")

    start_period = end_period - offsets
    start_prev = start_period - offsets

    current = df_metrics[df_metrics[date_col] > start_period]
    previous = df_metrics[
        (df_metrics[date_col] > start_prev) & (df_metrics[date_col] <= start_period)
    ]

    # --- 1. Total evictions ---
    current_total = len(current)
    previous_total = len(previous)

    if previous_total > 0:
        total_pct = ((current_total - previous_total) / previous_total) * 100
        total_delta = f"{total_pct:+.1f}%"
    else:
        total_delta = None

    # --- 2. Borough with highest evictions ---
    current_boro = current[borough_col].value_counts()
    previous_boro = previous[borough_col].value_counts()

    top_boro = current_boro.idxmax() if not current_boro.empty else "N/A"
    top_boro_count = int(current_boro.max()) if not current_boro.empty else 0
    prev_boro_count = int(previous_boro.get(top_boro, 0))
    boro_delta = f"{top_boro_count - prev_boro_count:+,}"

    # --- 3. Building type with highest evictions ---
    current_build = current[building_col].value_counts()
    previous_build = previous[building_col].value_counts()

    top_build = current_build.idxmax() if not current_build.empty else "N/A"
    top_build_count = int(current_build.max()) if not current_build.empty else 0
    prev_build_count = int(previous_build.get(top_build, 0))

    if prev_build_count > 0:
        build_pct = ((top_build_count - prev_build_count) / prev_build_count) * 100
        build_delta = f"{build_pct:+.1f}%"
    else:
        build_delta = None

    # --- Display metrics ---
    col1, col2, col3 = st.columns(3)

    col1.metric(
        label=f"Total Evictions (vs previous {period_label})",
        value=f"{current_total:,}",
        delta=total_delta,
        border=True,
    )

    col2.metric(
        label="Borough with Highest Evictions",
        value=top_boro,
        delta=boro_delta,
        border=True,
    )

    col3.metric(
        label="Most Affected Building Type",
        value=top_build,
        delta=build_delta,
        border=True,
    )

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
bucket = st.selectbox(
    "Time bucket", ["Monthly", "Weekly", "Daily"], index=0, key="timeseries_bucket"
)
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

# add page load time

start_time = time.time()

# --- your page code here ---

elapsed = time.time() - start_time
st.caption(f"Page loaded in {elapsed:.2f} seconds")
