from __future__ import annotations

import time

import pandas as pd
import pandas_gbq
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from google.oauth2 import service_account

from functions.theme import (
    COLOR_SCALE,
    COLOR_SCALE_RISK,
    COLOR_SEQUENCE,
    DANGER,
    DEEP_BLUE,
    FRESH_SKY,
    INK_BLACK,
    LIGHT_BLUE,
    SLATE,
    apply_chart_theme,
    apply_css,
    info_box,
    page_header,
    warning_box,
)

st.set_page_config(page_title="NYC Building Evictions", layout="wide")
apply_css()

start_time = time.time()

PROJECT_ID = "sipa-adv-c-cosmic-spaghetti"
DATASET = "cosmic_spaghetti"
DATE_COL = "executed_date"
BOROUGH_COL = "borough"
TYPE_COL = "residential_commercial_ind"

ANOMALY_STD = 2.0  # flag months > 2 std dev above mean


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


@st.cache_data(ttl=3600, show_spinner=False)
def load_evictions() -> pd.DataFrame:
    query = f"""
    SELECT executed_date, borough, residential_commercial_ind
    FROM `{PROJECT_ID}.{DATASET}.evictions`
    WHERE borough IS NOT NULL
    AND executed_date IS NOT NULL
    """
    df = pandas_gbq.read_gbq(
        query,
        project_id=PROJECT_ID,
        credentials=get_credentials(),
        progress_bar_type=None,
        dtypes={"borough": "str", "residential_commercial_ind": "str"},
    )
    df["executed_date"] = pd.to_datetime(df["executed_date"], errors="coerce")
    df = df.dropna(subset=["executed_date"])
    df["year"] = df["executed_date"].dt.year
    df["month"] = df["executed_date"].dt.to_period("M").dt.to_timestamp()

    # normalize borough names — county names → borough names
    BOROUGH_NAME_MAP = {
        "Kings": "Brooklyn",
        "Richmond": "Staten Island",
        "New York": "Manhattan",
        "Bronx": "Bronx",
        "Queens": "Queens",
        "Brooklyn": "Brooklyn",
        "Manhattan": "Manhattan",
        "Staten Island": "Staten Island",
    }
    df["borough"] = (
        df["borough"]
        .str.strip()
        .str.title()
        .map(BOROUGH_NAME_MAP)
        .fillna(df["borough"].str.strip().str.title())  # noqa: E501
    )

    # normalize type — combine C/Commercial and R/Residential
    TYPE_MAP = {
        "C": "Commercial",
        "R": "Residential",
        "Commercial": "Commercial",
        "Residential": "Residential",
    }
    df["type"] = df[TYPE_COL].str.strip().str.title().map(TYPE_MAP).fillna("Unknown")
    return df


# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading eviction data..."):
    df = load_evictions()
    nyc_geo = get_geojson()

if df.empty:
    st.error("No data returned from BigQuery.")
    st.stop()

# ── Page header ───────────────────────────────────────────────────────────────
page_header(
    "🏠 NYC Building Evictions",
    "Explore eviction trends across New York City's five boroughs : "
    "residential vs commercial, seasonal patterns, and anomaly detection.",
)

# ── Pre-compute stats ─────────────────────────────────────────────────────────
total_evictions = len(df)
top_boro = df[BOROUGH_COL].value_counts().idxmax()
residential = len(df[df["type"].str.contains("Residential", na=False)])
commercial = len(df[df["type"].str.contains("Commercial", na=False)])
res_pct = residential / total_evictions * 100 if total_evictions > 0 else 0

# anomaly detection — monthly counts
monthly_total = df.groupby("month").size().reset_index(name="Count")
mean_m = monthly_total["Count"].mean()
std_m = monthly_total["Count"].std()
anomaly_months = monthly_total[monthly_total["Count"] > mean_m + ANOMALY_STD * std_m]

if not anomaly_months.empty:
    anomaly_labels = ", ".join(anomaly_months["month"].dt.strftime("%b %Y").tolist())
    warning_box(
        f"Unusually high eviction activity detected in: <strong>{anomaly_labels}</strong> "
        f"— more than {ANOMALY_STD:.0f} standard deviations above the monthly average."
    )

# ── KPI row ───────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Evictions", f"{total_evictions:,}", border=True)
c2.metric("Most Affected Borough", top_boro, border=True)
c3.metric("Residential Evictions", f"{residential:,}", border=True)
c4.metric("Commercial Evictions", f"{commercial:,}", border=True)
c5.metric("Residential Share", f"{res_pct:.1f}%", border=True)

st.divider()

# ── Filters ───────────────────────────────────────────────────────────────────
with st.expander("🔍 Filters", expanded=False):
    fcols = st.columns(3)
    boro_opts = sorted(df[BOROUGH_COL].dropna().unique())
    selected_borough = fcols[0].multiselect("Borough", boro_opts, default=boro_opts)

    type_opts = sorted(df["type"].dropna().unique())
    selected_type = fcols[1].multiselect("Building Type", type_opts, default=type_opts)

    year_opts = sorted(df["year"].dropna().unique())
    selected_years = fcols[2].multiselect("Year", year_opts, default=year_opts)

df_f = df.copy()
if selected_borough:
    df_f = df_f[df_f[BOROUGH_COL].isin(selected_borough)]
if selected_type:
    df_f = df_f[df_f["type"].isin(selected_type)]
if selected_years:
    df_f = df_f[df_f["year"].isin(selected_years)]

st.caption(f"{len(df_f):,} evictions after filtering")
st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(
    [
        "📊  Overview",
        "🗺️  Borough Analysis",
        "🏠  Residential vs Commercial",
        "🔍  Anomaly Detection",
    ]
)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.markdown("### Eviction Trends Over Time")

    # year range slider
    year_min = int(df_f["year"].min())
    year_max = int(df_f["year"].max())
    if year_min < year_max:
        selected_year_range = st.slider(
            "Filter by year range",
            min_value=year_min,
            max_value=year_max,
            value=(year_min, year_max),
            step=1,
            key="yr_range_slider",
        )
        df_tab1 = df_f[df_f["year"].between(selected_year_range[0], selected_year_range[1])]
    else:
        df_tab1 = df_f.copy()

    bucket = st.selectbox("Time bucket", ["Monthly", "Weekly", "Daily"], key="t1_bucket")
    freq_map = {"Monthly": "M", "Weekly": "W", "Daily": "D"}
    df_tab1["Period"] = df_tab1[DATE_COL].dt.to_period(freq_map[bucket]).dt.to_timestamp()

    ts = df_tab1.groupby(["Period", BOROUGH_COL]).size().reset_index(name="Evictions")
    fig_ts = px.line(
        ts,
        x="Period",
        y="Evictions",
        color=BOROUGH_COL,
        markers=True,
        title=f"Evictions by Borough : {bucket} Trend ({selected_year_range[0]}–{selected_year_range[1]})",
        color_discrete_sequence=COLOR_SEQUENCE,
        labels={BOROUGH_COL: "Borough"},
    )
    st.plotly_chart(apply_chart_theme(fig_ts), use_container_width=True)

    # year over year
    st.markdown("### Year-over-Year Comparison")
    yearly = df_tab1.groupby("year").size().reset_index(name="Evictions")
    fig_yoy = px.bar(
        yearly,
        x="year",
        y="Evictions",
        title="Total Evictions by Year",
        color="Evictions",
        color_continuous_scale=COLOR_SCALE,
        text="Evictions",
    )
    fig_yoy.update_traces(texttemplate="%{text:,}", textposition="outside")
    st.plotly_chart(apply_chart_theme(fig_yoy), use_container_width=True)

    peak_yr = yearly.sort_values("Evictions", ascending=False).iloc[0]
    info_box(
        f"Peak eviction year: <strong>{int(peak_yr['year'])}</strong> — "
        f"<strong>{int(peak_yr['Evictions']):,}</strong> evictions executed"
    )

    # seasonal pattern — avg by month of year
    st.markdown("### Seasonal Pattern")
    df_tab1["month_name"] = df_tab1[DATE_COL].dt.strftime("%b")
    df_tab1["month_num"] = df_tab1[DATE_COL].dt.month
    seasonal = (
        df_tab1.groupby(["month_num", "month_name"])
        .size()
        .reset_index(name="Avg Evictions")
        .sort_values("month_num")
    )
    fig_seasonal = px.bar(
        seasonal,
        x="month_name",
        y="Avg Evictions",
        title="Eviction Volume by Month (All Years)",
        color="Avg Evictions",
        color_continuous_scale=COLOR_SCALE,
        text="Avg Evictions",
    )
    fig_seasonal.update_traces(texttemplate="%{text:,}", textposition="outside")
    st.plotly_chart(apply_chart_theme(fig_seasonal), use_container_width=True)

    peak_month = seasonal.sort_values("Avg Evictions", ascending=False).iloc[0]
    info_box(
        f"Historically highest eviction month: "
        f"<strong>{peak_month['month_name']}</strong> — "
        f"<strong>{int(peak_month['Avg Evictions']):,}</strong> evictions on record"
    )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — BOROUGH ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown("### Evictions by Borough")

    boro_counts = (
        df_f[BOROUGH_COL]
        .value_counts()
        .reset_index()
        .rename(columns={BOROUGH_COL: "Borough", "count": "Evictions"})
    )

    col1, col2 = st.columns(2)
    with col1:
        fig_map = px.choropleth_mapbox(
            boro_counts,
            geojson=nyc_geo,
            locations="Borough",
            featureidkey="properties.BoroName",
            color="Evictions",
            color_continuous_scale=COLOR_SCALE,
            mapbox_style="carto-positron",
            zoom=9.5,
            center={"lat": 40.7128, "lon": -74.0060},
            title="Total Evictions by Borough",
            hover_name="Borough",
        )
        st.plotly_chart(apply_chart_theme(fig_map), use_container_width=True)

    with col2:
        fig_bbar = px.bar(
            boro_counts.sort_values("Evictions", ascending=True),
            x="Evictions",
            y="Borough",
            orientation="h",
            title="Evictions by Borough",
            color="Evictions",
            color_continuous_scale=COLOR_SCALE,
            text="Evictions",
        )
        fig_bbar.update_traces(texttemplate="%{text:,}", textposition="outside")
        st.plotly_chart(apply_chart_theme(fig_bbar), use_container_width=True)

    # borough × year heatmap
    st.markdown("### Eviction Heatmap : Borough by Year")
    heat = df_f.groupby([BOROUGH_COL, "year"]).size().reset_index(name="Count")
    heat_pivot = heat.pivot_table(
        index=BOROUGH_COL, columns="year", values="Count", aggfunc="sum"
    ).fillna(0)

    fig_heat = go.Figure(
        go.Heatmap(
            z=heat_pivot.to_numpy(),
            x=[str(c) for c in heat_pivot.columns.tolist()],
            y=heat_pivot.index.tolist(),
            colorscale=[[0, LIGHT_BLUE], [0.5, FRESH_SKY], [1, DEEP_BLUE]],
            text=heat_pivot.to_numpy().astype(int),
            texttemplate="%{text:,}",
            hoverongaps=False,
        )
    )
    fig_heat.update_layout(
        title="Evictions per Borough per Year",
        xaxis_title="Year",
        yaxis_title="",
    )
    st.plotly_chart(apply_chart_theme(fig_heat), use_container_width=True)

    # borough time series
    st.markdown("### Borough Trends Over Time")
    bucket2 = st.selectbox("Time bucket", ["Monthly", "Weekly", "Daily"], key="t2_bucket")
    freq2 = {"Monthly": "M", "Weekly": "W", "Daily": "D"}[bucket2]
    df_f["Period2"] = df_f[DATE_COL].dt.to_period(freq2).dt.to_timestamp()
    ts2 = df_f.groupby(["Period2", BOROUGH_COL]).size().reset_index(name="Evictions")
    fig_ts2 = px.area(
        ts2,
        x="Period2",
        y="Evictions",
        color=BOROUGH_COL,
        title=f"Eviction Trends by Borough ({bucket2})",
        color_discrete_sequence=COLOR_SEQUENCE,
        labels={BOROUGH_COL: "Borough", "Period2": "Period"},
    )
    fig_ts2.update_traces(line_width=1)
    st.plotly_chart(apply_chart_theme(fig_ts2), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — RESIDENTIAL VS COMMERCIAL
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown("### Residential vs Commercial Evictions")

    type_counts = (
        df_f["type"]
        .value_counts()
        .reset_index()
        .rename(columns={"type": "Type", "count": "Evictions"})
    )

    col1, col2 = st.columns(2)
    with col1:
        fig_type_pie = go.Figure(
            data=[
                go.Pie(
                    labels=type_counts["Type"],
                    values=type_counts["Evictions"],
                    hole=0.55,
                    marker_colors=[DEEP_BLUE, FRESH_SKY, SLATE],
                    textinfo="label+percent",
                    textfont_size=13,
                )
            ]
        )
        fig_type_pie.update_layout(
            title="Eviction Type Breakdown",
            annotations=[
                {
                    "text": f"<b>{len(df_f):,}</b><br>total",
                    "x": 0.5,
                    "y": 0.5,
                    "font_size": 16,
                    "showarrow": False,
                    "font_color": INK_BLACK,
                }
            ],
        )
        st.plotly_chart(apply_chart_theme(fig_type_pie), use_container_width=True)

    with col2:
        # type by borough stacked bar
        type_boro = df_f.groupby([BOROUGH_COL, "type"]).size().reset_index(name="Evictions")
        fig_type_boro = px.bar(
            type_boro,
            x=BOROUGH_COL,
            y="Evictions",
            color="type",
            title="Eviction Type by Borough",
            barmode="stack",
            color_discrete_sequence=[DEEP_BLUE, FRESH_SKY, SLATE],
            labels={"type": "Type"},
        )
        st.plotly_chart(apply_chart_theme(fig_type_boro), use_container_width=True)

    # residential vs commercial over time
    st.markdown("### Type Trends Over Time")
    bucket3 = st.selectbox("Time bucket", ["Monthly", "Weekly", "Daily"], key="t3_bucket")
    freq3 = {"Monthly": "M", "Weekly": "W", "Daily": "D"}[bucket3]
    df_f["Period3"] = df_f[DATE_COL].dt.to_period(freq3).dt.to_timestamp()
    ts3 = df_f.groupby(["Period3", "type"]).size().reset_index(name="Evictions")
    fig_ts3 = px.line(
        ts3,
        x="Period3",
        y="Evictions",
        color="type",
        markers=True,
        title=f"Residential vs Commercial Evictions Over Time ({bucket3})",
        color_discrete_sequence=[DEEP_BLUE, FRESH_SKY, SLATE],
        labels={"type": "Type", "Period3": "Period"},
    )
    st.plotly_chart(apply_chart_theme(fig_ts3), use_container_width=True)

    # residential share over time
    st.markdown("### Residential Share Over Time")
    res_share = df_f.groupby(["Period3", "type"]).size().reset_index(name="Count")
    res_total = res_share.groupby("Period3")["Count"].transform("sum")
    res_share["Share (%)"] = (res_share["Count"] / res_total * 100).round(1)
    res_only = res_share[res_share["type"].str.contains("Residential", na=False)]
    fig_share = px.line(
        res_only,
        x="Period3",
        y="Share (%)",
        title="Residential Eviction Share Over Time (%)",
        markers=True,
        color_discrete_sequence=[DEEP_BLUE],
        labels={"Period3": "Period"},
    )
    fig_share.add_hline(
        y=50,
        line_dash="dash",
        line_color=SLATE,
        annotation_text="50% threshold",
        annotation_font_color=SLATE,
    )
    st.plotly_chart(apply_chart_theme(fig_share), use_container_width=True)

    res_avg = res_only["Share (%)"].mean()
    info_box(f"On average, <strong>{res_avg:.1f}%</strong> of all evictions are residential")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — ANOMALY DETECTION
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown("### Anomaly Detection — Unusual Eviction Months")

    with st.expander("📋 How is anomaly detection calculated?", expanded=False):
        st.markdown(f"""
        We calculate the **mean** and **standard deviation** of monthly eviction counts
        across all available data. Any month where evictions exceed
        **mean + {ANOMALY_STD:.0f} × standard deviation** is flagged as an anomaly.

        This method helps identify months with unusually high eviction activity
        that may warrant policy attention or further investigation.

        > ⚠️ Note: Anomalies may reflect real spikes in evictions, or data reporting
        > artifacts (e.g. backlogs being processed in a single month).
        """)

    monthly = df_f.groupby("month").size().reset_index(name="Count")
    mean_val = monthly["Count"].mean()
    std_val = monthly["Count"].std()
    threshold = mean_val + ANOMALY_STD * std_val

    monthly["Anomaly"] = monthly["Count"] > threshold
    monthly["Color"] = monthly["Anomaly"].map({True: DANGER, False: DEEP_BLUE})

    # main anomaly chart
    fig_anomaly = go.Figure()

    # normal bars
    normal = monthly[~monthly["Anomaly"]]
    anomaly = monthly[monthly["Anomaly"]]

    fig_anomaly.add_trace(
        go.Bar(
            x=normal["month"],
            y=normal["Count"],
            name="Normal",
            marker_color=DEEP_BLUE,
            opacity=0.8,
        )
    )
    fig_anomaly.add_trace(
        go.Bar(
            x=anomaly["month"],
            y=anomaly["Count"],
            name="Anomaly",
            marker_color=DANGER,
            opacity=0.9,
        )
    )
    fig_anomaly.add_hline(
        y=mean_val,
        line_dash="dot",
        line_color=SLATE,
        annotation_text=f"Mean ({mean_val:.0f})",
        annotation_font_color=SLATE,
    )
    fig_anomaly.add_hline(
        y=threshold,
        line_dash="dash",
        line_color=DANGER,
        annotation_text=f"Threshold ({threshold:.0f})",
        annotation_font_color=DANGER,
    )
    fig_anomaly.update_layout(
        title="Monthly Eviction Counts with Anomaly Detection",
        xaxis_title="Month",
        yaxis_title="Evictions",
        barmode="overlay",
        showlegend=True,
    )
    st.plotly_chart(apply_chart_theme(fig_anomaly), use_container_width=True)

    if anomaly.empty:
        info_box("No anomalous months detected in the filtered data.")
    else:
        anomaly_list = anomaly.sort_values("Count", ascending=False)
        warning_box(
            f"<strong>{len(anomaly)}</strong> anomalous month(s) detected: "
            + ", ".join(
                [
                    f"{row['month'].strftime('%b %Y')} ({row['Count']:,})"
                    for _, row in anomaly_list.iterrows()
                ]
            )
        )

    # anomaly by borough — which boroughs drive spikes
    st.markdown("### Which Boroughs Drive the Spikes?")
    if not anomaly.empty:
        anomaly_months_list = anomaly["month"].tolist()
        df_anomaly = df_f[df_f["month"].isin(anomaly_months_list)]
        anomaly_boro = (
            df_anomaly.groupby(BOROUGH_COL).size().reset_index(name="Evictions in Anomaly Months")
        )
        fig_ab = px.bar(
            anomaly_boro.sort_values("Evictions in Anomaly Months", ascending=False),
            x=BOROUGH_COL,
            y="Evictions in Anomaly Months",
            title="Evictions by Borough During Anomalous Months",
            color="Evictions in Anomaly Months",
            color_continuous_scale=COLOR_SCALE_RISK,
            text="Evictions in Anomaly Months",
        )
        fig_ab.update_traces(texttemplate="%{text:,}", textposition="outside")
        st.plotly_chart(apply_chart_theme(fig_ab), use_container_width=True)
    else:
        st.info("No anomalous months in current filter selection.")

    # rolling average
    st.markdown("### Rolling Average: Smoothed Trend")

    with st.expander("📋 What is a rolling average?", expanded=False):
        st.markdown("""
        A **rolling average** smooths out short-term fluctuations by averaging
        each month's eviction count with preceding months.

        This helps identify the **underlying trend** in eviction activity, removing
        noise caused by seasonal spikes, data reporting delays, or one-off events.

        | Term | Meaning |
        |---|---|
        | **Monthly Count** (bars) | Actual evictions recorded that month |
        | **Rolling Avg** (line) | Average of current + N prior months |

        > 📌 When the rolling average is **rising**, eviction activity is trending up.
        > When it is **falling**, the trend is improving.
        """)

    roll_window = st.slider(
        "Rolling window (months)",
        min_value=2,
        max_value=12,
        value=3,
        step=1,
        key="roll_slider",
    )

    monthly_sorted = monthly.sort_values("month").copy()
    monthly_sorted["Rolling Avg"] = (
        monthly_sorted["Count"].rolling(roll_window, min_periods=1).mean()
    )

    fig_roll = go.Figure()
    fig_roll.add_trace(
        go.Bar(
            x=monthly_sorted["month"],
            y=monthly_sorted["Count"],
            name="Monthly Count",
            marker_color=LIGHT_BLUE,
            opacity=0.6,
        )
    )
    fig_roll.add_trace(
        go.Scatter(
            x=monthly_sorted["month"],
            y=monthly_sorted["Rolling Avg"],
            name="3-Month Rolling Avg",
            line_color=DEEP_BLUE,
            line_width=2.5,
            mode="lines",
        )
    )
    fig_roll.update_layout(
        title=f"Monthly Evictions with {roll_window}-Month Rolling Average",
        xaxis_title="Month",
        yaxis_title="Evictions",
        showlegend=True,
    )
    st.plotly_chart(apply_chart_theme(fig_roll), use_container_width=True)

st.caption(f"⏱ Page loaded in {time.time() - start_time:.2f} seconds")
