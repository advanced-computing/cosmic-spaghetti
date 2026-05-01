import streamlit as st

from functions.theme import apply_css, info_box, page_header

st.set_page_config(
    page_title="NYC Building Insights",
    layout="wide",
)
apply_css()

page_header(
    "NYC Building Insights",
    "Unraveling the Web of NYC Building Data",
)

# ── Team ──────────────────────────────────────────────────────────────────────
st.markdown("### Cosmic Spaghetti Team")
st.markdown("""
**Mery Hotma Situmorang** (mhs2231) and **Najihah Ahmad Fikri** (na3183)
Advanced Computing for Policy, Spring 2026, Columbia SIPA
""")

st.divider()

# ── About the dashboard ───────────────────────────────────────────────────────
st.markdown("### About This Dashboard")
st.write("""
This dashboard explores NYC building activity data sourced from the NYC Department of Buildings
and related city agencies. It combines six datasets into a unified view, allowing users to
investigate construction trends, eviction patterns, building complaints, and facade safety
across New York City's five boroughs.

All data is loaded from NYC Open Data APIs into Google BigQuery and refreshed automatically
every day at 6am UTC via GitHub Actions. The dashboard reads directly from BigQuery for fast,
cached queries.
""")

st.divider()

# ── Pages ─────────────────────────────────────────────────────────────────────
st.markdown("### What You Can Explore")

col1, col2 = st.columns(2)

with col1:
    with st.container(border=True):
        st.markdown("**Buildings Overview**")
        st.write("""
        Explore NYC's total building stock, new construction activity (2008 to 2020),
        active construction and renovation jobs (January 2025 onwards), and facade
        inspection safety data from the Facade Inspection Safety Program (FISP).
        Includes building density analysis by borough, permit type breakdowns,
        and unsafe facade rate tracking across inspection cycles.
        """)

    with st.container(border=True):
        st.markdown("**Building Evictions**")
        st.write("""
        Track eviction trends across the five boroughs from 2017 onwards.
        Explore residential vs commercial breakdowns, seasonal patterns,
        year-over-year comparisons, and anomaly detection that flags months
        with unusually high eviction activity relative to historical averages.
        """)

with col2:
    with st.container(border=True):
        st.markdown("**Building Complaints**")
        st.write("""
        Analyze complaints filed with the NYC Department of Buildings by category,
        priority level, borough, and status. Includes response time analysis
        showing how long complaints take to resolve by borough and priority,
        and trend charts tracking complaint volume over time.
        """)

    with st.container(border=True):
        st.markdown("**Proposal**")
        st.write("""
        Read the original project proposal including research questions, target
        visualizations, and how the project evolved from its initial scope to the
        full dashboard you see today.
        """)

st.divider()

# ── Data sources ──────────────────────────────────────────────────────────────
st.markdown("### Data Sources")

with st.container(border=True):
    st.markdown("""
| Dataset | Source | Coverage |
|---|---|---|
| DOB NOW Build: Approved Permits | NYC Open Data (rbx6-tga4) | January 2025 onwards |
| DOB Permit Issuance | NYC Open Data (ipu4-2q9a) | 2008 to 2020 (New Building jobs) |
| NYC Evictions | NYC Open Data (6z8x-wfk4) | 2017 onwards |
| DOB Complaints Received | NYC Open Data (eabe-havv) | Most recent 200,000 records |
| DOB NOW: Safety Facade Compliance (FISP) | NYC Open Data (xubg-57si) | 2001 onwards |
| NYC Building Footprints | NYC Open Data (5zhs-2jue) | All buildings up to 2025 |
""")

st.divider()

# ── Technical notes ───────────────────────────────────────────────────────────
st.markdown("### Technical Notes")
st.write("""
All datasets are stored in Google BigQuery under the project
sipa-adv-c-cosmic-spaghetti, dataset cosmic_spaghetti.
Data is refreshed daily using the truncate method (full replace on each run)
because BigQuery's free tier does not support DML operations.
The Streamlit app uses st.cache_data with a one-hour TTL to keep page loads fast
after the initial query.
""")

info_box(
    "Data is refreshed daily at 6am UTC via GitHub Actions. "
    "If charts appear outdated, try clearing the Streamlit cache by pressing <strong>C</strong> on your keyboard."
)
