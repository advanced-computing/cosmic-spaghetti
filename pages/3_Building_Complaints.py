from __future__ import annotations

import time

import pandas as pd
import pandas_gbq
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from google.oauth2 import service_account

from complaint_categories import COMPLAINT_CATEGORY_MAP
from functions.theme import (
    COLOR_SCALE,
    COLOR_SCALE_RISK,
    COLOR_SEQUENCE,
    DANGER,
    DEEP_BLUE,
    FRESH_SKY,
    LIGHT_BLUE,
    SLATE,
    WARNING,
    apply_chart_theme,
    apply_css,
    caution_box,
    info_box,
    page_header,
    warning_box,
)

st.set_page_config(page_title="NYC Building Complaints", layout="wide")
apply_css()

start_time = time.time()

PROJECT_ID = "sipa-adv-c-cosmic-spaghetti"
DATASET = "cosmic_spaghetti"
TABLE = "complaints"

DATE_COL = "date_entered"
BOROUGH_COL = "borough"
CATEGORY_COL = "complaint_category"
STATUS_COL = "status"

# Priority labels from DOB
PRIORITY_LABELS = {"A": "Emergency (A)", "B": "Urgent (B)", "C": "Normal (C)", "D": "Low (D)"}
PRIORITY_COLORS = {
    "Emergency (A)": DANGER,
    "Urgent (B)": WARNING,
    "Normal (C)": FRESH_SKY,
    "Low (D)": SLATE,
}

# Analytics thresholds
HIGH_COMPLAINT_THRESHOLD = 500


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
def load_complaints() -> pd.DataFrame:
    query = f"""
    SELECT
        community_board,
        date_entered,
        complaint_category,
        status,
        disposition_date,
        inspection_date
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE date_entered IS NOT NULL
    LIMIT 200000
    """
    df = pandas_gbq.read_gbq(
        query,
        project_id=PROJECT_ID,
        credentials=get_credentials(),
        progress_bar_type=None,
        dtypes={
            "community_board": "str",
            "complaint_category": "str",
            "status": "str",
        },
    )
    df["date_entered"] = pd.to_datetime(df["date_entered"], errors="coerce")
    df["disposition_date"] = pd.to_datetime(df["disposition_date"], errors="coerce")
    df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce")
    df = df.dropna(subset=["date_entered"])

    # extract borough from community_board first digit
    borough_map = {
        "1": "Manhattan",
        "2": "Bronx",
        "3": "Brooklyn",
        "4": "Queens",
        "5": "Staten Island",
    }
    df["borough"] = df["community_board"].str[0].map(borough_map).fillna("Unknown")

    # add readable category description
    df["complaint_desc"] = (
        df["complaint_category"].map(COMPLAINT_CATEGORY_MAP).fillna(df["complaint_category"])
    )

    # add priority from category map (A/B/C/D)
    priority_map = {
        "01": "A",
        "03": "A",
        "10": "A",
        "12": "A",
        "13": "A",
        "14": "A",
        "16": "A",
        "18": "A",
        "20": "A",
        "30": "A",
        "37": "A",
        "50": "A",
        "56": "A",
        "62": "A",
        "65": "A",
        "67": "A",
        "76": "A",
        "81": "A",
        "82": "A",
        "86": "A",
        "89": "A",
        "91": "A",
        "5B": "A",
        "5C": "A",
        "2B": "A",
        "1E": "A",
        "2E": "A",
        "04": "B",
        "05": "B",
        "06": "B",
        "09": "B",
        "15": "B",
        "21": "B",
        "23": "B",
        "45": "B",
        "52": "B",
        "54": "B",
        "58": "B",
        "59": "B",
        "63": "B",
        "66": "B",
        "71": "B",
        "75": "B",
        "78": "B",
        "83": "B",
        "88": "B",
        "92": "B",
        "93": "B",
        "1A": "B",
        "1B": "B",
        "1D": "B",
        "1G": "B",
        "2A": "B",
        "2C": "B",
        "2D": "B",
        "3A": "B",
        "4A": "B",
        "4B": "B",
        "4G": "B",
        "5A": "B",
        "5F": "B",
        "5G": "B",
        "29": "C",
        "31": "C",
        "49": "C",
        "73": "C",
        "74": "C",
        "77": "C",
        "79": "C",
        "85": "C",
        "90": "C",
        "94": "C",
        "2G": "C",
        "4W": "C",
        "6A": "C",
        "35": "D",
        "53": "D",
        "55": "D",
        "80": "D",
        "1K": "D",
        "1Z": "D",
        "2F": "D",
        "2H": "D",
        "2J": "D",
        "2K": "D",
        "2L": "D",
        "2M": "D",
        "4C": "D",
        "4D": "D",
        "4F": "D",
        "4J": "D",
        "4K": "D",
        "4L": "D",
        "4M": "D",
        "4N": "D",
        "4P": "D",
    }
    df["priority"] = df["complaint_category"].map(priority_map).fillna("C")
    df["priority_label"] = df["priority"].map(PRIORITY_LABELS).fillna("Normal (C)")

    # response time in days
    df["resp_days"] = (df["disposition_date"] - df["date_entered"]).dt.days.clip(lower=0)

    df["year"] = df["date_entered"].dt.year
    df["month"] = df["date_entered"].dt.to_period("M").dt.to_timestamp()
    return df


# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading complaints data..."):
    df = load_complaints()
    nyc_geo = get_geojson()

if df.empty:
    st.error("No data returned from BigQuery.")
    st.stop()

# ── Page header ───────────────────────────────────────────────────────────────
page_header(
    "⚠️ NYC Building Complaints",
    "Explore building complaints filed with the NYC Department of Buildings — "
    "categories, priorities, borough trends, and response times.",
)

# ── Top-level warning ─────────────────────────────────────────────────────────
emergency_count = len(df[df["priority"] == "A"])
open_count = len(df[df["status"].str.upper().str.contains("OPEN|ACTIVE", na=False)])
if emergency_count > HIGH_COMPLAINT_THRESHOLD:
    warning_box(
        f"{emergency_count:,} emergency (Priority A) complaints on record — "
        "these require immediate DOB response."
    )

# ── KPI row ───────────────────────────────────────────────────────────────────
total = len(df)
resolved = len(df[df["status"].str.upper().str.contains("CLOSED|RESOLVED|DONE", na=False)])
resolution_rate = resolved / total * 100 if total > 0 else 0
avg_response = df["resp_days"].dropna().mean()
top_boro = df["borough"].value_counts().idxmax() if not df.empty else "N/A"

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Complaints", f"{total:,}", border=True)
c2.metric("Emergency (Priority A)", f"{emergency_count:,}", border=True)
c3.metric("Resolution Rate", f"{resolution_rate:.1f}%", border=True)
c4.metric("Avg Response Time", f"{avg_response:.0f} days" if avg_response else "N/A", border=True)
c5.metric("Most Complaints", top_boro, border=True)

st.divider()

# ── Filters ───────────────────────────────────────────────────────────────────
with st.expander("🔍 Filters", expanded=False):
    fcols = st.columns(3)
    borough_opts = sorted(df[BOROUGH_COL].dropna().unique())
    selected_borough = fcols[0].multiselect("Borough", borough_opts, default=borough_opts)

    status_opts = sorted(df[STATUS_COL].dropna().unique())
    selected_status = fcols[1].multiselect("Status", status_opts, default=status_opts)

    priority_opts = sorted(df["priority_label"].dropna().unique())
    selected_priority = fcols[2].multiselect("Priority", priority_opts, default=priority_opts)

df_f = df.copy()
if selected_borough:
    df_f = df_f[df_f[BOROUGH_COL].isin(selected_borough)]
if selected_status:
    df_f = df_f[df_f[STATUS_COL].isin(selected_status)]
if selected_priority:
    df_f = df_f[df_f["priority_label"].isin(selected_priority)]

st.caption(f"{len(df_f):,} complaints after filtering")
st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(
    [
        "📊  Overview",
        "🗺️  Borough Analysis",
        "📋  Complaint Categories",
        "⏱️  Response & Status",
    ]
)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.markdown("### Complaint Volume Over Time")

    with st.expander("📋 Understanding Complaint Priorities", expanded=False):
        st.markdown("""
        The NYC Department of Buildings assigns a **priority level** to each complaint
        based on the potential risk to public safety:

        | Priority | Label | Meaning | Target Response |
        |---|---|---|---|
        | **A** | Emergency | Immediate danger to life or property | Same day inspection |
        | **B** | Urgent | Significant safety risk | Within 5 business days |
        | **C** | Normal | Non-urgent code violation | Within 30 days |
        | **D** | Low | Administrative or tracking complaint | As resources allow |

        🔗 Source: [NYC DOB Complaint Categories](https://www.nyc.gov/assets/buildings/pdf/complaint_category.pdf)
        """)

    bucket = st.selectbox("Time bucket", ["Monthly", "Weekly", "Daily"], key="t1_bucket")
    freq = {"Monthly": "MS", "Weekly": "W-MON", "Daily": "D"}[bucket]

    df_f["Period"] = (
        df_f[DATE_COL]
        .dt.to_period({"Monthly": "M", "Weekly": "W", "Daily": "D"}[bucket])
        .dt.to_timestamp()
    )

    ts = df_f.groupby(["Period", BOROUGH_COL]).size().reset_index(name="Complaints")

    fig_ts = px.line(
        ts,
        x="Period",
        y="Complaints",
        color=BOROUGH_COL,
        markers=True,
        title=f"Complaints by Borough — {bucket} Trend",
        color_discrete_sequence=COLOR_SEQUENCE,
        labels={BOROUGH_COL: "Borough"},
    )
    st.plotly_chart(apply_chart_theme(fig_ts), use_container_width=True)

    # priority breakdown over time — stacked area
    st.markdown("### Priority Breakdown Over Time")
    ts_priority = df_f.groupby(["Period", "priority_label"]).size().reset_index(name="Count")
    fig_pri_ts = px.area(
        ts_priority,
        x="Period",
        y="Count",
        color="priority_label",
        title=f"Complaint Priority Over Time ({bucket})",
        color_discrete_map=PRIORITY_COLORS,
        labels={"priority_label": "Priority"},
    )
    fig_pri_ts.update_traces(line_width=1)
    st.plotly_chart(apply_chart_theme(fig_pri_ts), use_container_width=True)

    # year over year comparison
    st.markdown("### Year-over-Year Comparison")
    yearly = df_f.groupby("year").size().reset_index(name="Complaints")
    fig_yoy = px.bar(
        yearly,
        x="year",
        y="Complaints",
        title="Total Complaints by Year",
        color="Complaints",
        color_continuous_scale=COLOR_SCALE,
        text="Complaints",
    )
    fig_yoy.update_traces(texttemplate="%{text:,}", textposition="outside")
    st.plotly_chart(apply_chart_theme(fig_yoy), use_container_width=True)

    peak_yr = yearly.sort_values("Complaints", ascending=False).iloc[0]
    info_box(
        f"Peak complaint year: <strong>{int(peak_yr['year'])}</strong> — "
        f"<strong>{int(peak_yr['Complaints']):,}</strong> complaints filed"
    )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — BOROUGH ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown("### Complaints by Borough")

    boro_counts = (
        df_f[BOROUGH_COL]
        .value_counts()
        .reset_index()
        .rename(columns={BOROUGH_COL: "Borough", "count": "Complaints"})
    )
    boro_counts["Borough"] = boro_counts["Borough"].str.title()

    col1, col2 = st.columns(2)
    with col1:
        fig_bmap = px.choropleth_mapbox(
            boro_counts,
            geojson=nyc_geo,
            locations="Borough",
            featureidkey="properties.BoroName",
            color="Complaints",
            color_continuous_scale=COLOR_SCALE,
            mapbox_style="carto-positron",
            zoom=9.5,
            center={"lat": 40.7128, "lon": -74.0060},
            title="Total Complaints by Borough",
            hover_name="Borough",
        )
        st.plotly_chart(apply_chart_theme(fig_bmap), use_container_width=True)

    with col2:
        # bar chart instead of redundant donut
        fig_bbar = px.bar(
            boro_counts.sort_values("Complaints", ascending=True),
            x="Complaints",
            y="Borough",
            orientation="h",
            title="Complaints by Borough",
            color="Complaints",
            color_continuous_scale=COLOR_SCALE,
            text="Complaints",
        )
        fig_bbar.update_traces(texttemplate="%{text:,}", textposition="outside")
        st.plotly_chart(apply_chart_theme(fig_bbar), use_container_width=True)

    # borough × priority heatmap
    st.markdown("### Priority Heatmap by Borough")
    heat_data = df_f.groupby([BOROUGH_COL, "priority_label"]).size().reset_index(name="Count")
    heat_pivot = heat_data.pivot_table(
        index=BOROUGH_COL, columns="priority_label", values="Count", aggfunc="sum"
    ).fillna(0)

    fig_heat = go.Figure(
        go.Heatmap(
            z=heat_pivot.to_numpy(),
            x=heat_pivot.columns.tolist(),
            y=[b.title() for b in heat_pivot.index.tolist()],
            colorscale=[[0, LIGHT_BLUE], [0.5, FRESH_SKY], [1, DEEP_BLUE]],
            text=heat_pivot.to_numpy().astype(int),
            texttemplate="%{text:,}",
            hoverongaps=False,
        )
    )
    fig_heat.update_layout(
        title="Complaint Priority by Borough",
        xaxis_title="Priority",
        yaxis_title="",
    )
    st.plotly_chart(apply_chart_theme(fig_heat), use_container_width=True)

    # complaint type by borough — sunburst
    st.markdown("### Complaint Types by Borough")
    sun_data = df_f.groupby([BOROUGH_COL, "complaint_desc"]).size().reset_index(name="Count")
    sun_data[BOROUGH_COL] = sun_data[BOROUGH_COL].str.title()
    # keep top 15 categories to avoid clutter
    top15 = df_f["complaint_desc"].value_counts().head(15).index.tolist()
    sun_data = sun_data[sun_data["complaint_desc"].isin(top15)]

    fig_sun = px.sunburst(
        sun_data,
        path=[BOROUGH_COL, "complaint_desc"],
        values="Count",
        title="Top 15 Complaint Types by Borough",
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    fig_sun.update_traces(textinfo="label+percent parent", insidetextfont_size=11)
    st.plotly_chart(apply_chart_theme(fig_sun), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — COMPLAINT CATEGORIES
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown("### Top Complaint Categories")
    st.caption(
        "Complaint categories follow NYC DOB classification codes. "
        "Priority A = Emergency, B = Urgent, C = Normal, D = Low."
    )

    n_top = st.slider("Number of top categories to show", 5, 30, 15, key="cat_slider")

    top_cats = (
        df_f["complaint_desc"]
        .value_counts()
        .head(n_top)
        .reset_index()
        .rename(columns={"complaint_desc": "Category", "count": "Count"})
    )

    col1, col2 = st.columns(2)
    with col1:
        fig_hbar = px.bar(
            top_cats,
            x="Count",
            y="Category",
            orientation="h",
            title=f"Top {n_top} Complaint Categories",
            color="Count",
            color_continuous_scale=COLOR_SCALE,
            text="Count",
        )
        fig_hbar.update_layout(yaxis={"categoryorder": "total ascending"})
        fig_hbar.update_traces(texttemplate="%{text:,}", textposition="outside")
        st.plotly_chart(apply_chart_theme(fig_hbar), use_container_width=True)

    with col2:
        # treemap of categories
        fig_tree = px.treemap(
            top_cats,
            path=["Category"],
            values="Count",
            title=f"Top {n_top} Categories — Treemap",
            color="Count",
            color_continuous_scale=COLOR_SCALE,
        )
        fig_tree.update_traces(
            texttemplate="<b>%{label}</b><br>%{value:,}",
            textfont_size=12,
        )
        st.plotly_chart(apply_chart_theme(fig_tree), use_container_width=True)

    # priority distribution
    st.markdown("### Complaints by Priority Level")

    pri_counts = (
        df_f["priority_label"]
        .value_counts()
        .reset_index()
        .rename(columns={"priority_label": "Priority", "count": "Count"})
    )

    col1, col2 = st.columns(2)
    with col1:
        fig_pri = px.bar(
            pri_counts.sort_values("Count", ascending=True),
            x="Count",
            y="Priority",
            orientation="h",
            title="Total Complaints by Priority Level",
            color="Priority",
            color_discrete_map=PRIORITY_COLORS,
            text="Count",
        )
        fig_pri.update_traces(texttemplate="%{text:,}", textposition="outside")
        st.plotly_chart(apply_chart_theme(fig_pri), use_container_width=True)

    with col2:
        # line chart per priority over time
        ts_pri = df_f.groupby(["Period", "priority_label"]).size().reset_index(name="Count")
        fig_pri_line = px.line(
            ts_pri,
            x="Period",
            y="Count",
            color="priority_label",
            markers=True,
            title="Priority Trend Over Time",
            color_discrete_map=PRIORITY_COLORS,
            labels={"priority_label": "Priority"},
        )
        fig_pri_line.update_traces(line_width=2)
        st.plotly_chart(apply_chart_theme(fig_pri_line), use_container_width=True)

    # emergency complaints by borough
    st.markdown("### Emergency Complaints (Priority A) by Borough")
    emerg = (
        df_f[df_f["priority"] == "A"][BOROUGH_COL]
        .value_counts()
        .reset_index()
        .rename(columns={BOROUGH_COL: "Borough", "count": "Emergency Complaints"})
    )
    emerg["Borough"] = emerg["Borough"].str.title()
    fig_emerg = px.bar(
        emerg,
        x="Borough",
        y="Emergency Complaints",
        title="Emergency (Priority A) Complaints by Borough",
        color="Emergency Complaints",
        color_continuous_scale=COLOR_SCALE_RISK,
        text="Emergency Complaints",
    )
    fig_emerg.update_traces(texttemplate="%{text:,}", textposition="outside")
    st.plotly_chart(apply_chart_theme(fig_emerg), use_container_width=True)

    emerg_top = emerg.iloc[0] if not emerg.empty else None
    if emerg_top is not None:
        warning_box(
            f"<strong>{emerg_top['Borough']}</strong> has the most emergency complaints — "
            f"<strong>{int(emerg_top['Emergency Complaints']):,}</strong> Priority A filings."
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — RESPONSE & STATUS
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown("### Complaint Status Breakdown")

    status_counts = (
        df_f[STATUS_COL]
        .value_counts()
        .reset_index()
        .rename(columns={STATUS_COL: "Status", "count": "Count"})
    )

    col1, col2 = st.columns(2)
    with col1:
        fig_status = px.pie(
            status_counts,
            names="Status",
            values="Count",
            title="Complaint Status Distribution",
            color_discrete_sequence=COLOR_SEQUENCE,
            hole=0.45,
        )
        fig_status.update_traces(textinfo="label+percent", textfont_size=12)
        st.plotly_chart(apply_chart_theme(fig_status), use_container_width=True)

    with col2:
        fig_status_bar = px.bar(
            status_counts.sort_values("Count", ascending=False),
            x="Status",
            y="Count",
            title="Complaint Status Counts",
            color="Count",
            color_continuous_scale=COLOR_SCALE,
            text="Count",
        )
        fig_status_bar.update_traces(texttemplate="%{text:,}", textposition="outside")
        st.plotly_chart(apply_chart_theme(fig_status_bar), use_container_width=True)

    # response time analysis
    st.markdown("### Response Time Analysis")

    with st.expander("📋 How is response time calculated?", expanded=False):
        st.markdown("""
        **Response time** is calculated as the number of days between when a complaint was
        filed (`date_entered`) and when it was resolved (`disposition_date`).

        | Priority | DOB Target Response Time |
        |---|---|
        | **A — Emergency** | Same day inspection |
        | **B — Urgent** | Within 5 business days |
        | **C — Normal** | Within 30 days |
        | **D — Low** | As resources allow |

        > ⚠️ Complaints without a `disposition_date` are still open and excluded from
        > response time calculations.
        """)

    df_resp = df_f.dropna(subset=["resp_days"])

    if df_resp.empty:
        st.info("No response time data available.")
    else:
        avg_resp = df_resp["resp_days"].mean()
        median_resp = df_resp["resp_days"].median()
        max_resp = df_resp["resp_days"].max()

        r1, r2, r3 = st.columns(3)
        r1.metric("Avg Response Time", f"{avg_resp:.1f} days", border=True)
        r2.metric("Median Response Time", f"{median_resp:.0f} days", border=True)
        r3.metric("Longest Response", f"{max_resp:.0f} days", border=True)

        # response time by borough
        resp_boro = (
            df_resp.groupby(BOROUGH_COL)["resp_days"]
            .mean()
            .round(1)
            .reset_index()
            .rename(columns={BOROUGH_COL: "Borough", "resp_days": "Avg Days"})
            .sort_values("Avg Days", ascending=False)
        )
        resp_boro["Borough"] = resp_boro["Borough"].str.title()

        fig_resp = px.bar(
            resp_boro,
            x="Borough",
            y="Avg Days",
            title="Average Response Time by Borough (days)",
            color="Avg Days",
            color_continuous_scale=COLOR_SCALE_RISK,
            text="Avg Days",
        )
        fig_resp.update_traces(texttemplate="%{text:.1f}", textposition="outside")
        st.plotly_chart(apply_chart_theme(fig_resp), use_container_width=True)

        # response time by priority
        resp_pri = (
            df_resp.groupby("priority_label")["resp_days"]
            .mean()
            .round(1)
            .reset_index()
            .rename(columns={"priority_label": "Priority", "resp_days": "Avg Days"})
        )
        fig_resp_pri = px.bar(
            resp_pri.sort_values("Avg Days", ascending=False),
            x="Priority",
            y="Avg Days",
            title="Average Response Time by Priority Level (days)",
            color="Priority",
            color_discrete_map=PRIORITY_COLORS,
            text="Avg Days",
        )
        fig_resp_pri.update_traces(texttemplate="%{text:.1f}d", textposition="outside")
        st.plotly_chart(apply_chart_theme(fig_resp_pri), use_container_width=True)

        slowest_boro = resp_boro.iloc[0]
        if slowest_boro["Avg Days"] > 30:  # noqa: PLR2004
            caution_box(
                f"<strong>{slowest_boro['Borough']}</strong> has the slowest average response time "
                f"<strong>{slowest_boro['Avg Days']:.1f} days</strong> on average."
            )
        else:
            info_box(
                f"Average response time across all boroughs: <strong>{avg_resp:.1f} days</strong>"
            )

        # response time distribution histogram
        st.markdown("### Response Time Distribution")
        year = 365
        fig_hist = px.histogram(
            df_resp[df_resp["resp_days"] <= year],
            x="resp_days",
            nbins=50,
            title="Distribution of Response Times (days, capped at 1 year)",
            color_discrete_sequence=[DEEP_BLUE],
            labels={"resp_days": "Days to Resolution"},
        )
        fig_hist.update_layout(bargap=0.05)
        st.plotly_chart(apply_chart_theme(fig_hist), use_container_width=True)

st.caption(f"⏱ Page loaded in {time.time() - start_time:.2f} seconds")
