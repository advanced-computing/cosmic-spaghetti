from __future__ import annotations

import time

import pandas as pd
import pandas_gbq
import plotly.express as px
import plotly.graph_objects as go
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
from functions.theme import (
    COLOR_SCALE,
    COLOR_SCALE_RISK,
    COLOR_SEQUENCE,
    DANGER,
    DEEP_BLUE,
    FACADE_COLORS,
    FRESH_SKY,
    UNSAFE_THRESHOLD,
    apply_chart_theme,
    apply_css,
    info_box,
    page_header,
    warning_box,
)

st.set_page_config(page_title="NYC Buildings Overview", layout="wide")
apply_css()

start_time = time.time()

PROJECT_ID = "sipa-adv-c-cosmic-spaghetti"
DATASET = "cosmic_spaghetti"
MIN_CONSTRUCTION_YEAR = 1900
MAX_CONSTRUCTION_YEAR = 2025
UNSAFE_RATE_WARNING = 15
AT_RISK_RATE_WARNING = 40

BOROUGH_AREA_SQ_MI = {
    "MANHATTAN": 22.8,
    "BROOKLYN": 71.0,
    "QUEENS": 109.0,
    "BRONX": 42.0,
    "STATEN ISLAND": 58.0,
}


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
    AND permit_date >= '2008-01-01'
    AND permit_date <= '2020-12-31'
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
    query = f"""
    SELECT borough, permit_date, permit_type, permit_type_desc,
           status, latitude, longitude, source
    FROM `{PROJECT_ID}.{DATASET}.permits`
    WHERE permit_type != 'NB'
    AND borough IS NOT NULL
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
df_other_2025 = df_other.copy()  # all available permit data (Jan 2025+)

# ── Pre-compute key stats ─────────────────────────────────────────────────────
total_buildings = int(df_summary["total_buildings"].sum())
total_new = len(df_new)
total_other_2025 = len(df_other_2025)

safe_count = (
    len(df_facades[df_facades["filing_status"].str.contains("SAFE", na=False)])
    if not df_facades.empty
    else 0
)
swarmp_count = (
    len(df_facades[df_facades["filing_status"].str.contains("SWARMP", na=False)])
    if not df_facades.empty
    else 0
)
unsafe_count = (
    len(df_facades[df_facades["filing_status"].str.contains("UNSAFE", na=False)])
    if not df_facades.empty
    else 0
)
total_filed = safe_count + swarmp_count + unsafe_count
unsafe_rate = unsafe_count / total_filed * 100 if total_filed > 0 else 0
at_risk_rate = (unsafe_count + swarmp_count) / total_filed * 100 if total_filed > 0 else 0

# ── Page header ───────────────────────────────────────────────────────────────
page_header(
    "🏙️ NYC Buildings Overview",
    "Explore building activity across New York City's five boroughs — "
    "total building stock, new construction, active permits, and facade safety inspections.",
)

# ── Unsafe facade warning ─────────────────────────────────────────────────────
if not df_facades.empty:
    unsafe_boros = []
    for boro in df_facades["borough"].unique():
        b = df_facades[df_facades["borough"] == boro]
        pct = (
            len(b[b["filing_status"].str.contains("UNSAFE", na=False)]) / len(b)
            if len(b) > 0
            else 0
        )
        if pct > UNSAFE_THRESHOLD:
            unsafe_boros.append(f"{boro.title()} ({pct:.0%})")
    if unsafe_boros:
        warning_box(f"High unsafe facade rate detected — {' · '.join(unsafe_boros)}")

# ── KPI row ───────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Buildings", f"{total_buildings:,}", border=True)
c2.metric("New Building Jobs (2008–2020)", f"{total_new:,}", border=True)
c3.metric("Active Construction Jobs (Jan 2025+)", f"{total_other_2025:,}", border=True)
c4.metric("Unsafe Facade Filings", f"{unsafe_count:,}", border=True)
c5.metric("Overall Unsafe Rate", f"{unsafe_rate:.1f}%", border=True)

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(
    [
        "🏙️  Total Buildings",
        "🏗️  New Building Jobs",
        "🔨  Construction & Renovation",
        "🔍  Facade Inspection (FISP)",
    ]
)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — TOTAL BUILDINGS
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.markdown("### Building Stock by Borough")
    st.caption(
        "Source: NYC Building Footprints (5zhs-2jue) — all buildings with construction year ≤ 2025"
    )

    boro_summary = (
        df_summary.groupby("borough")
        .agg(
            total_buildings=("total_buildings", "sum"),
            avg_height=("avg_height", "mean"),
        )
        .reset_index()
    )
    boro_summary["Borough"] = boro_summary["borough"].str.title()
    boro_summary["Area (Square Mile)"] = boro_summary["borough"].str.upper().map(BOROUGH_AREA_SQ_MI)
    boro_summary["Buildings per Square Mile"] = (
        (boro_summary["total_buildings"] / boro_summary["Area (Square Mile)"]).round(0).astype(int)
    )

    col1, col2 = st.columns(2)
    with col1:
        fig_map = px.choropleth_mapbox(
            boro_summary,
            geojson=nyc_geo,
            locations="Borough",
            featureidkey="properties.BoroName",
            color="total_buildings",
            color_continuous_scale=COLOR_SCALE,
            mapbox_style="carto-positron",
            zoom=9.5,
            center={"lat": 40.7128, "lon": -74.0060},
            title="Total Buildings by Borough",
            hover_name="Borough",
            hover_data={
                "total_buildings": True,
                "Buildings per Square Mile": True,
                "avg_height": ":.0f",
            },
            labels={"total_buildings": "Buildings", "avg_height": "Avg Height (ft)"},
        )
        st.plotly_chart(apply_chart_theme(fig_map), use_container_width=True)

    with col2:
        fig_tree = px.treemap(
            boro_summary,
            path=["Borough"],
            values="total_buildings",
            color="Buildings per Square Mile",
            color_continuous_scale=COLOR_SCALE,
            title="Building Stock — Size = Count · Color = Density per Square Mile",
            hover_data={"Buildings per Square Mile": True, "Area (Square Mile)": True},
        )
        fig_tree.update_traces(
            texttemplate="<b>%{label}</b><br>%{value:,} buildings<br>%{percentRoot:.1%} of NYC",
            textfont_size=13,
        )
        st.plotly_chart(apply_chart_theme(fig_tree), use_container_width=True)

    # density bar
    st.markdown("### Building Density — Buildings per Square Mile")
    fig_density = px.bar(
        boro_summary.sort_values("Buildings per Square Mile", ascending=False),
        x="Borough",
        y="Buildings per Square Mile",
        color="Buildings per Square Mile",
        color_continuous_scale=COLOR_SCALE,
        text="Buildings per Square Mile",
        title="How Densely Built Is Each Borough?",
    )
    fig_density.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig_density.update_layout(showlegend=False)
    st.plotly_chart(apply_chart_theme(fig_density), use_container_width=True)

    top_dense = boro_summary.sort_values("Buildings per Square Mile", ascending=False).iloc[0]
    info_box(
        f"<strong>{top_dense['Borough']}</strong> is the most densely built borough — "
        f"<strong>{top_dense['Buildings per Square Mile']:,}</strong> buildings per square mile "
        f"across just <strong>{top_dense['Area (Square Mile)']} Square Mile</strong>."
    )

    st.divider()

    # construction trend
    st.markdown("### Construction History")

    # ── Year range slider ─────────────────────────────────────────────────────
    year_range = st.slider(
        "Filter by construction year range",
        min_value=MIN_CONSTRUCTION_YEAR,
        max_value=MAX_CONSTRUCTION_YEAR,
        value=(1940, MAX_CONSTRUCTION_YEAR),
        step=5,
        key="yr_slider",
    )

    df_yr = (
        df_summary[
            (df_summary["cnstrct_yr"] >= year_range[0])
            & (df_summary["cnstrct_yr"] <= year_range[1])
        ]
        .groupby(["cnstrct_yr", "borough"])["total_buildings"]
        .sum()
        .reset_index()
        .rename(columns={"cnstrct_yr": "Year", "total_buildings": "Buildings"})
    )

    yr_total = int(df_yr["Buildings"].sum())
    info_box(
        f"Showing buildings constructed between <strong>{year_range[0]}</strong> and "
        f"<strong>{year_range[1]}</strong> — <strong>{yr_total:,}</strong> buildings total"
    )

    col1, col2 = st.columns(2)
    with col1:
        fig_area = px.area(
            df_yr,
            x="Year",
            y="Buildings",
            color="borough",
            title=f"Buildings Constructed Per Year ({year_range[0]}–{year_range[1]})",
            color_discrete_sequence=COLOR_SEQUENCE,
            labels={"borough": "Borough"},
        )
        fig_area.update_traces(line_width=1)
        st.plotly_chart(apply_chart_theme(fig_area), use_container_width=True)

    with col2:
        # heatmap: borough × decade
        df_decade = df_yr.copy()
        df_decade["Decade"] = (df_decade["Year"] // 10 * 10).astype(str) + "s"
        pivot = (
            df_decade.groupby(["borough", "Decade"])["Buildings"]
            .sum()
            .reset_index()
            .pivot_table(index="borough", columns="Decade", values="Buildings", aggfunc="sum")
            .fillna(0)
        )
        fig_heat = go.Figure(
            go.Heatmap(
                z=pivot.to_numpy(),
                x=pivot.columns.tolist(),
                y=[b.title() for b in pivot.index.tolist()],
                colorscale=COLOR_SCALE,
                text=pivot.to_numpy().astype(int),
                texttemplate="%{text:,}",
                hoverongaps=False,
            )
        )
        fig_heat.update_layout(
            title="Construction Intensity by Borough and Decade",
            xaxis_title="Decade",
            yaxis_title="",
        )
        st.plotly_chart(apply_chart_theme(fig_heat), use_container_width=True)

    peak_decade = df_decade.groupby("Decade")["Buildings"].sum().idxmax()
    peak_count = df_decade.groupby("Decade")["Buildings"].sum().max()
    info_box(
        f"Peak construction decade: <strong>{peak_decade}</strong> — "
        f"<strong>{int(peak_count):,}</strong> buildings added to NYC"
    )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — NEW BUILDING JOBS
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown("### New Building Jobs (2008–2020)")
    st.caption("Source: DOB Permit Issuance (ipu4-2q9a) — job_type = NB · 2008–2020 only")

    with st.expander("📋 What is a New Building Job?", expanded=False):
        st.markdown("""
        A **New Building (NB)** job is filed with the NYC Department of Buildings (DOB)
        when a property owner or developer intends to construct a brand-new building on a lot.

        | Job Type | Description |
        |---|---|
        | **NB** | New Building — construct a new structure from scratch |
        | **A1** | Major Alteration — change in use, egress, or occupancy |
        | **A2** | Minor Alteration — no change in use or occupancy |
        | **A3** | Minor Work — no plans required |
        | **DM** | Demolition — full or partial removal of an existing building |

        🔗 Learn more at the
        [NYC Department of Buildings](https://www.nyc.gov/site/buildings/index.page)
        or browse job filings at
        [NYC Open Data — DOB Permit Issuance](https://data.cityofnewyork.us/Housing-Development/DOB-Permit-Issuance/ipu4-2q9a/about_data).
        """)
    st.markdown("")

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
        new_by_boro["Area (Square Mile)"] = (
            new_by_boro["Borough"].str.upper().map(BOROUGH_AREA_SQ_MI)
        )
        new_by_boro["New Bldgs per Square Mile"] = (
            new_by_boro["New Buildings"] / new_by_boro["Area (Square Mile)"]
        ).round(1)

        col1, col2, col3 = st.columns(3)
        col1.metric("New Building Jobs (2008–2020)", f"{total_new:,}", border=True)
        col2.metric(
            "Most Active Borough",
            new_by_boro.sort_values("New Buildings", ascending=False).iloc[0]["Borough"],
            border=True,
        )
        col3.metric(
            "Peak Year",
            str(int(df_new.dropna(subset=["permit_date"])["permit_date"].dt.year.mode()[0]))
            if not df_new.dropna(subset=["permit_date"]).empty
            else "N/A",
            border=True,
        )

        st.divider()

        col1, col2 = st.columns(2)
        with col1:
            fig_new_map = px.choropleth_mapbox(
                new_by_boro,
                geojson=nyc_geo,
                locations="Borough",
                featureidkey="properties.BoroName",
                color="New Buildings",
                color_continuous_scale=COLOR_SCALE,
                mapbox_style="carto-positron",
                zoom=9.5,
                center={"lat": 40.7128, "lon": -74.0060},
                title="New Building Permits by Borough",
                hover_name="Borough",
                hover_data={"New Buildings": True, "New Bldgs per Square Mile": True},
            )
            st.plotly_chart(apply_chart_theme(fig_new_map), use_container_width=True)

        with col2:
            fig_donut = go.Figure(
                data=[
                    go.Pie(
                        labels=new_by_boro["Borough"],
                        values=new_by_boro["New Buildings"],
                        hole=0.6,
                        marker_colors=COLOR_SEQUENCE[: len(new_by_boro)],
                        textinfo="label+percent",
                        textfont_size=13,
                    )
                ]
            )
            fig_donut.update_layout(
                title="New Building Jobs by Borough (2008–2020)",
                # and annotation:
                annotations=[
                    {
                        "text": f"<b>{total_new:,}</b><br><span style='font-size:12px'>jobs</span>",
                        "x": 0.5,
                        "y": 0.5,
                        "font_size": 18,
                        "showarrow": False,
                        "font_color": DEEP_BLUE,
                    }
                ],
            )
            st.plotly_chart(apply_chart_theme(fig_donut), use_container_width=True)

        # density comparison
        st.markdown("### New Building Density by Borough")
        fig_nb_density = px.bar(
            new_by_boro.sort_values("New Bldgs per Square Mile", ascending=False),
            x="Borough",
            y="New Bldgs per Square Mile",
            color="New Bldgs per Square Mile",
            color_continuous_scale=COLOR_SCALE,
            text="New Bldgs per Square Mile",
            title="New Building Jobs per Square Mile (2008–2020)",
        )
        fig_nb_density.update_traces(texttemplate="%{text:.1f}", textposition="outside")
        st.plotly_chart(apply_chart_theme(fig_nb_density), use_container_width=True)

        # scatter map
        df_new_coords = df_new.dropna(subset=["latitude", "longitude"])
        if not df_new_coords.empty:
            st.markdown("### Where Are New Buildings Being Built?")
            fig_scatter = px.scatter_mapbox(
                df_new_coords,
                lat="latitude",
                lon="longitude",
                color="borough",
                mapbox_style="carto-positron",
                zoom=10,
                center={"lat": 40.7128, "lon": -74.0060},
                title="New Building Permit Locations",
                opacity=0.65,
                color_discrete_sequence=COLOR_SEQUENCE,
                hover_data={"borough": True, "status": True, "permit_date": True},
            )
            st.plotly_chart(apply_chart_theme(fig_scatter), use_container_width=True)

        # yearly trend
        df_new_yr = df_new.dropna(subset=["permit_date"]).copy()
        if not df_new_yr.empty:
            df_new_yr["Year"] = df_new_yr["permit_date"].dt.year
            yearly = df_new_yr.groupby(["Year", "borough"]).size().reset_index(name="Count")
            fig_trend = px.bar(
                yearly,
                x="Year",
                y="Count",
                color="borough",
                title="New Building Permits Per Year by Borough",
                barmode="stack",
                color_discrete_sequence=COLOR_SEQUENCE,
                labels={"borough": "Borough"},
            )
            st.plotly_chart(apply_chart_theme(fig_trend), use_container_width=True)

            peak_yr = df_new_yr.groupby("Year").size().idxmax()
            peak_yr_count = df_new_yr.groupby("Year").size().max()
            info_box(
                f"Peak new building year: <strong>{peak_yr}</strong> — "
                f"<strong>{peak_yr_count:,}</strong> new building permits filed"
            )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — OTHER BUILDING JOBS
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown("### Construction & Renovation Jobs (January 2025 onwards)")
    st.caption(
        "Source: DOB NOW Build — Approved Permits (rbx6-tga4) — "  # noqa: E501
        "all construction job types except New Building"
    )

    with st.expander("📋 What are these construction job types?", expanded=False):
        st.markdown("""
        These are **work permits** issued by the NYC Department of Buildings for active
        construction or renovation work on existing buildings.

        | Job Type | Description |
        |---|---|
        | **General Construction** | Structural, facade, or major building work |
        | **Plumbing** | Water supply, drainage, gas piping |
        | **Mechanical Systems** | HVAC, ventilation, fire suppression |
        | **Structural** | Beams, columns, foundations |
        | **Full Demolition** | Complete removal of an existing building |
        | **Foundation** | Footings, piles, or underpinning |
        | **Sidewalk Shed** | Temporary protective structure over sidewalk |
        | **Solar** | Solar panel installation |
        | **Sign** | Signage installation or alteration |

        🔗 Learn more at
        [NYC DOB NOW Build](https://www.nyc.gov/site/buildings/industry/dob-now-build.page)
        or browse job filings at
        [NYC Open Data — DOB NOW Approved Permits](https://data.cityofnewyork.us/Housing-Development/DOB-NOW-Build-Approved-Permits/rbx6-tga4/about_data).
        """)
    st.markdown("")

    if df_other.empty:
        st.info("No other permits found.")
    else:
        other_by_boro = (
            df_other_2025["borough"]
            .value_counts()
            .reset_index()
            .rename(columns={"borough": "Borough", "count": "Permits"})
        )
        other_by_boro["Borough"] = other_by_boro["Borough"].str.title()

        top_type = (
            df_other_2025["permit_type_desc"]
            .fillna(df_other_2025["permit_type"])
            .value_counts()
            .idxmax()
        )

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Construction Jobs (Jan 2025+)", f"{total_other_2025:,}", border=True)
        col2.metric("Most Active Borough", other_by_boro.iloc[0]["Borough"], border=True)
        col3.metric("Top Job Type", top_type, border=True)

        st.divider()

        # sunburst + map
        sunburst_data = (
            df_other_2025.assign(
                borough=df_other_2025["borough"].str.title(),
                permit_type_desc=df_other_2025["permit_type_desc"].fillna(
                    df_other_2025["permit_type"]
                ),
            )
            .groupby(["borough", "permit_type_desc"])
            .size()
            .reset_index(name="Count")
        )

        col1, col2 = st.columns(2)
        with col1:
            fig_map3 = px.choropleth_mapbox(
                other_by_boro,
                geojson=nyc_geo,
                locations="Borough",
                featureidkey="properties.BoroName",
                color="Permits",
                color_continuous_scale=COLOR_SCALE,
                mapbox_style="carto-positron",
                zoom=9.5,
                center={"lat": 40.7128, "lon": -74.0060},
                title="Active Construction Jobs by Borough (Jan 2025+)",
                hover_name="Borough",
            )
            st.plotly_chart(apply_chart_theme(fig_map3), use_container_width=True)

        with col2:
            fig_sun = px.sunburst(
                sunburst_data,
                path=["borough", "permit_type_desc"],
                values="Count",
                title="Permit Breakdown by Borough and Type",
                color_discrete_sequence=COLOR_SEQUENCE,
            )
            fig_sun.update_traces(
                textinfo="label+percent parent",
                insidetextfont_size=12,
            )
            st.plotly_chart(apply_chart_theme(fig_sun), use_container_width=True)

        # heatmap
        st.markdown("### Permit Type Heatmap by Borough")
        top_types = (
            sunburst_data.groupby("permit_type_desc")["Count"].sum().nlargest(10).index.tolist()
        )
        pivot = (
            sunburst_data[sunburst_data["permit_type_desc"].isin(top_types)]
            .pivot_table(index="borough", columns="permit_type_desc", values="Count", aggfunc="sum")
            .fillna(0)
        )
        fig_pheat = go.Figure(
            go.Heatmap(
                z=pivot.to_numpy(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale=COLOR_SCALE,
                text=pivot.to_numpy().astype(int),
                texttemplate="%{text:,}",
                hoverongaps=False,
            )
        )
        fig_pheat.update_layout(
            title="Top 10 Permit Types by Borough (Jan 2025+)",
            xaxis_title="Permit Type",
            yaxis_title="",
            xaxis_tickangle=-30,
            height=350,
        )
        st.plotly_chart(apply_chart_theme(fig_pheat), use_container_width=True)

        # scatter map
        df_coords = df_other_2025.dropna(subset=["latitude", "longitude"])
        if not df_coords.empty:
            st.markdown("### Permit Locations (Jan 2025+)")
            fig_scat = px.scatter_mapbox(
                df_coords,
                lat="latitude",
                lon="longitude",
                color="permit_type_desc",
                mapbox_style="carto-positron",
                zoom=10,
                center={"lat": 40.7128, "lon": -74.0060},
                title="Where Are Permits Being Filed?",
                opacity=0.55,
                color_discrete_sequence=COLOR_SEQUENCE,
                hover_data={"borough": True, "permit_type_desc": True, "status": True},
            )
            st.plotly_chart(apply_chart_theme(fig_scat), use_container_width=True)

        st.divider()
        st.markdown("### Detailed View — Construction & Renovation Jobs (Last 12 Months)")

        date_col = first_column(df_other, ["permit_date"])
        borough_col = "borough" if "borough" in df_other.columns else None
        type_col = first_column(
            df_other, ["permit_type_desc", "permit_type", "work_type", "job_type"]
        )
        status_col = first_column(df_other, ["status", "permit_status"])
        df_detail = filter_last_12_months(df_other, date_col=date_col)

        with st.expander("Filters", expanded=False):
            cols = st.columns(3)
            if borough_col:
                boro_opts = sorted(df_detail[borough_col].dropna().astype(str).unique())
                selected_borough = cols[0].multiselect(
                    "Borough", boro_opts, default=boro_opts, key="t3b"
                )
            else:
                selected_borough = None
            if type_col:
                type_opts = sorted(df_detail[type_col].dropna().astype(str).unique())
                selected_types = cols[1].multiselect(
                    "Permit Type", type_opts, default=type_opts, key="t3t"
                )
            else:
                selected_types = None
            if status_col:
                stat_opts = sorted(df_detail[status_col].dropna().astype(str).unique())
                selected_status = cols[2].multiselect(
                    "Status", stat_opts, default=stat_opts, key="t3s"
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
        st.caption(f"{len(df_filtered):,} rows after filtering")

        bucket = st.selectbox("Time bucket", ["Monthly", "Weekly", "Daily"], key="t3bucket")
        freq = {"Monthly": "MS", "Weekly": "W-MON", "Daily": "D"}[bucket]

        if not df_filtered.empty and date_col in df_filtered.columns:
            df_filtered[date_col] = pd.to_datetime(df_filtered[date_col], errors="coerce")
            end_p = df_filtered[date_col].max()
            off = {
                "Monthly": pd.DateOffset(months=1),
                "Weekly": pd.DateOffset(weeks=1),
                "Daily": pd.DateOffset(days=1),
            }[bucket]
            pl = bucket.lower().rstrip("ly")
            cur = df_filtered[df_filtered[date_col] > end_p - off]
            prev = df_filtered[
                (df_filtered[date_col] > end_p - 2 * off) & (df_filtered[date_col] <= end_p - off)
            ]

            cur_n, prev_n = len(cur), len(prev)
            delta = f"{((cur_n - prev_n) / prev_n * 100):+.1f}%" if prev_n > 0 else None

            top_b = (
                cur[borough_col].value_counts().idxmax() if borough_col and not cur.empty else "N/A"
            )
            top_b_prev = prev[borough_col].value_counts().get(top_b, 0) if borough_col else 0
            top_b_delta = (
                f"{len(cur[cur[borough_col] == top_b]) - top_b_prev:+,}" if borough_col else None
            )

            top_t = cur[type_col].value_counts().idxmax() if type_col and not cur.empty else "N/A"

            c1, c2, c3 = st.columns(3)
            c1.metric(f"Construction Jobs (vs prev {pl})", f"{cur_n:,}", delta=delta, border=True)
            c2.metric(f"Top Borough (vs prev {pl})", top_b, delta=top_b_delta, border=True)
            c3.metric("Top Job Type", top_t, border=True)

        if not df_filtered.empty and borough_col:
            bc = (
                df_filtered[borough_col]
                .value_counts()
                .reset_index()
                .rename(columns={borough_col: "Borough", "count": "Count"})
            )
            bc["Borough"] = bc["Borough"].str.strip().str.title()
            fig_dm = px.choropleth_mapbox(
                bc,
                geojson=nyc_geo,
                locations="Borough",
                featureidkey="properties.BoroName",
                color="Count",
                color_continuous_scale=COLOR_SCALE,
                mapbox_style="carto-positron",
                zoom=9.5,
                center={"lat": 40.7128, "lon": -74.0060},
                title="Construction Jobs by Borough (Filtered)",
                hover_name="Borough",
            )
            st.plotly_chart(apply_chart_theme(fig_dm), use_container_width=True)

        ts = permit_timeseries_by_borough(
            df_filtered,
            date_col=date_col,
            borough_col=borough_col or "borough",
            status_col=None,
            freq=freq,
        )
        if not ts.empty:
            fig_ts = px.line(
                ts,
                x="Period",
                y="Count",
                color="Borough",
                markers=True,
                title="Permit Activity by Borough Over Time",
                color_discrete_sequence=COLOR_SEQUENCE,
            )
            st.plotly_chart(apply_chart_theme(fig_ts), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — FACADE INSPECTION
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown("### Facade Inspection Safety Program (FISP)")
    st.caption(
        "Buildings taller than 6 stories must be inspected every 5 years. "
        "Current cycle: Cycle 10 (2025–2030). Data available: 2001–present. "
        "Classifications: Safe · SWARMP (needs repair within 5 years) · Unsafe."
    )

    if df_facades.empty:
        st.info("No facade inspection data found.")
    else:
        with st.expander("📋 What is FISP and why does it matter?", expanded=False):
            st.markdown("""
            The **Facade Inspection Safety Program (FISP)**, also known as **Local Law 11**,
            requires all buildings taller than 6 stories in NYC to have their facades
            inspected by a Qualified Exterior Wall Inspector (QEWI) every **5 years**.

            **Classifications:**

            | Status | Meaning | Action Required |
            |---|---|---|
            | ✅ **SAFE** | No problems found | None — next inspection in 5 years |
            | ⚠️ **SWARMP** | Safe now but needs repair | Repairs required before next cycle |
            | 🚨 **UNSAFE** | Immediate danger to public | Emergency repairs required; violations |
            | 📋 **No Report Filed** | Owner has not submitted | Subject to $1,000/month penalty |

            **Current Cycle:** Cycle 10 (2025–2030)

            🔗 Learn more:
            - [NYC DOB — Facade Inspection Safety Program](https://www.nyc.gov/site/buildings/property/facades.page)
            - [Local Law 11 Overview](https://www.nyc.gov/site/buildings/property/facades.page)
            - [NYC Open Data — FISP Filings](https://data.cityofnewyork.us/Housing-Development/DOB-NOW-Safety-Facade-Compliance-Filings/xubg-57si/about_data)
            """)
        st.markdown("")
        if unsafe_rate > UNSAFE_RATE_WARNING:
            warning_box(
                f"{unsafe_rate:.1f}% of all facade filings are UNSAFE — "  # noqa: E501
                "immediate attention required."
            )
        elif at_risk_rate > AT_RISK_RATE_WARNING:
            warning_box(
                f"{at_risk_rate:.1f}% of all facade filings are at risk (UNSAFE or SWARMP)."
            )

        # gauge row
        def make_gauge(value, title, color, suffix="%", max_val=100):
            fig = go.Figure(
                go.Indicator(
                    mode="gauge+number",
                    value=round(value, 1),
                    title={"text": title, "font": {"color": DEEP_BLUE, "size": 13}},
                    number={
                        "suffix": suffix,
                        "font": {"color": DEEP_BLUE, "size": 22},
                        "valueformat": ".1f",
                    },
                    gauge={
                        "axis": {
                            "range": [0, max_val],
                            "tickcolor": DEEP_BLUE,
                            "tickfont": {"size": 10},
                        },
                        "bar": {"color": color, "thickness": 0.7},
                        "bordercolor": DEEP_BLUE,
                        "borderwidth": 1,
                        "steps": [{"range": [0, max_val * 0.5], "color": "rgba(0,0,0,0.03)"}],
                    },
                )
            )
            fig.update_layout(
                height=180,
                margin={"t": 50, "b": 10, "l": 20, "r": 20},
                paper_bgcolor="rgba(0,0,0,0)",
            )
            return fig

        safe_rate = safe_count / total_filed * 100 if total_filed > 0 else 0
        swarmp_rate = swarmp_count / total_filed * 100 if total_filed > 0 else 0

        g1, g2, g3, g4 = st.columns(4)
        with g1:
            st.plotly_chart(make_gauge(safe_rate, "Safe Rate", "#1D9E75"), use_container_width=True)
        with g2:
            st.plotly_chart(
                make_gauge(swarmp_rate, "SWARMP Rate", FRESH_SKY), use_container_width=True
            )
        with g3:
            st.plotly_chart(
                make_gauge(unsafe_rate, "Unsafe Rate", DANGER), use_container_width=True
            )
        with g4:
            st.plotly_chart(
                make_gauge(at_risk_rate, "At Risk Rate", DEEP_BLUE), use_container_width=True
            )

        st.divider()

        available_cycles = sorted(
            [int(c) for c in df_facades["cycle"].dropna().unique() if str(c).isdigit()]
        )
        min_c, max_c = available_cycles[0], available_cycles[-1]
        selected_cycles = st.slider(
            "Filter by inspection cycle",
            min_value=min_c,
            max_value=max_c,
            value=(min_c, max_c),
            step=1,
            key="cycle_slider",
            help="Cycle 9 = 2020–2024 · Cycle 10 = 2025–2030",
        )
        df_cycle9 = df_facades[
            df_facades["cycle"].dropna().astype(int).between(selected_cycles[0], selected_cycles[1])
        ]
        st.caption(
            f"Showing cycles {selected_cycles[0]}–{selected_cycles[1]} · {len(df_cycle9):,} filings"
        )

        col1, col2 = st.columns(2)
        with col1:
            sc9 = (
                df_cycle9["filing_status"]
                .fillna("UNKNOWN")
                .value_counts()
                .reset_index()
                .rename(columns={"filing_status": "Status", "count": "Count"})
            )
            fig_pie = px.pie(
                sc9,
                names="Status",
                values="Count",
                title=f"Filing Status — Cycles {selected_cycles[0]}–{selected_cycles[1]}",
                color="Status",
                color_discrete_map=FACADE_COLORS,
                hole=0.5,
            )
            fig_pie.update_traces(
                textinfo="label+percent",
                textfont_size=12,
            )
            st.plotly_chart(apply_chart_theme(fig_pie), use_container_width=True)

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
                title=f"Unsafe Filings by Borough, Cycle {selected_cycles[0]}–{selected_cycles[1]}",
                color="Unsafe Filings",
                color_continuous_scale=COLOR_SCALE_RISK,
                text="Unsafe Filings",
            )
            fig_unsafe.update_traces(textposition="outside")
            st.plotly_chart(apply_chart_theme(fig_unsafe), use_container_width=True)

        # at-risk map — connected to slider
        fisp_risk = (
            df_cycle9[df_cycle9["filing_status"].str.contains("UNSAFE|SWARMP", na=False)]
            .groupby("borough")["filing_status"]
            .count()
            .reset_index()
            .rename(columns={"borough": "Borough", "filing_status": "At Risk Filings"})
        )
        fisp_risk["Borough"] = fisp_risk["Borough"].str.title()
        fig_risk_map = px.choropleth_mapbox(
            fisp_risk,
            geojson=nyc_geo,
            locations="Borough",
            featureidkey="properties.BoroName",
            color="At Risk Filings",
            color_continuous_scale=COLOR_SCALE_RISK,
            mapbox_style="carto-positron",
            zoom=9.5,
            center={"lat": 40.7128, "lon": -74.0060},
            title=f"At Risk Facade(SWARMP+Unsafe):Cycles {selected_cycles[0]}–{selected_cycles[1]}",
            hover_name="Borough",
        )
        st.plotly_chart(apply_chart_theme(fig_risk_map), use_container_width=True)

        # unsafe rate per borough — connected to slider
        st.markdown("### Unsafe Rate by Borough")
        rates = []
        for boro in df_cycle9["borough"].unique():
            b = df_cycle9[df_cycle9["borough"] == boro]
            u = len(b[b["filing_status"].str.contains("UNSAFE", na=False)])
            t = len(b)
            rates.append(
                {
                    "Borough": boro.title(),
                    "Unsafe Rate (%)": round(u / t * 100, 1) if t > 0 else 0,
                    "Total Filings": t,
                }
            )
        df_rates = pd.DataFrame(rates).sort_values("Unsafe Rate (%)", ascending=False)
        fig_rate = px.bar(
            df_rates,
            x="Borough",
            y="Unsafe Rate (%)",
            title=f"Unsafe Facade Rate by Borough: Cycle {selected_cycles[0]}–{selected_cycles[1]}",
            color="Unsafe Rate (%)",
            color_continuous_scale=COLOR_SCALE_RISK,
            text="Unsafe Rate (%)",
        )
        fig_rate.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_rate.add_hline(
            y=UNSAFE_THRESHOLD * 100,
            line_dash="dash",
            line_color=DANGER,
            annotation_text=f"Warning threshold ({UNSAFE_THRESHOLD:.0%})",
            annotation_font_color=DANGER,
        )
        st.plotly_chart(apply_chart_theme(fig_rate), use_container_width=True)

        # cycle trend — stacked
        if "cycle" in df_facades.columns:
            st.markdown("### Filing Trends Across Inspection Cycles")
            cycle_data = (
                df_facades.groupby(["cycle", "filing_status"]).size().reset_index(name="Count")
            )
            fig_cycle = px.bar(
                cycle_data,
                x="cycle",
                y="Count",
                color="filing_status",
                title="Facade Filing Status by Inspection Cycle",
                barmode="stack",
                color_discrete_map=FACADE_COLORS,
                labels={"filing_status": "Status", "cycle": "Cycle"},
            )
            st.plotly_chart(apply_chart_theme(fig_cycle), use_container_width=True)

            # trend line: unsafe count over cycles
            unsafe_trend = (
                df_facades[df_facades["filing_status"].str.contains("UNSAFE", na=False)]
                .groupby("cycle")
                .size()
                .reset_index(name="Unsafe Count")
            )
            fig_ut = px.line(
                unsafe_trend,
                x="cycle",
                y="Unsafe Count",
                title="Unsafe Filing Count Over Inspection Cycles",
                markers=True,
                color_discrete_sequence=[DANGER],
            )
            fig_ut.update_traces(line_width=2.5, marker_size=8)
            st.plotly_chart(apply_chart_theme(fig_ut), use_container_width=True)

st.caption(f"⏱ Page loaded in {time.time() - start_time:.2f} seconds")
