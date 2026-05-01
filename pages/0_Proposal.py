import time

import streamlit as st

from functions.theme import apply_css, page_header

st.set_page_config(page_title="Proposal", layout="wide")
apply_css()

start_time = time.time()

page_header(
    "Project Proposal",
    "NYC Building Insights: Unraveling the Web of NYC Building Data",
)

# ── Project Overview ──────────────────────────────────────────────────────────
st.markdown("### Project Overview")
st.write("""
We are planning to explore several NYC open datasets to better understand the relationship
between construction activity, housing conditions, and socioeconomic factors across
New York City.
""")

st.divider()

# ── Main Datasets ─────────────────────────────────────────────────────────────
with st.container(border=True):
    st.markdown("### Main Datasets")
    st.markdown("""
**1. DOB NOW Build: Approved Permits**
Records historical job permits in NYC such as work type (new building, demolition, etc.).
Provides insights about construction patterns across NYC.
[View Dataset](https://data.cityofnewyork.us/Housing-Development/DOB-NOW-Build-Approved-Permits/rbx6-tga4/about_data)

**2. DOB Permit Issuance**
Records permits issued by the NYC Department of Buildings including job type,
borough, and filing dates. Covers new buildings, alterations, and demolitions.
[View Dataset](https://data.cityofnewyork.us/Housing-Development/DOB-Permit-Issuance/ipu4-2q9a/about_data)

**3. NYC Evictions**
Executed residential evictions across the five boroughs since 2017, sortable by borough,
building type, and date.
[View Dataset](https://data.cityofnewyork.us/City-Government/Evictions/6z8x-wfk4/about_data)

**4. DOB Complaints Received**
Records complaints submitted by tenants or members of the public. Includes complaint category,
source, location, and response status.
[View Dataset](https://data.cityofnewyork.us/Housing-Development/DOB-Complaints-Received/eabe-havv/about_data)

**5. DOB NOW: Safety Facade Compliance Filings (FISP)**
Records facade inspection filings for buildings taller than 6 stories, including
filing status (Safe, SWARMP, Unsafe) and inspection cycle.
[View Dataset](https://data.cityofnewyork.us/Housing-Development/DOB-NOW-Safety-Facade-Compliance-Filings/xubg-57si/about_data)

**6. NYC Building Footprints**
Contains building geometry, construction year, height, and borough for all
buildings in New York City.
[View Dataset](https://data.cityofnewyork.us/Housing-Development/Building-Footprints/nqwf-w8eh/about_data)
""")

st.write("")

# ── Additional Datasets ───────────────────────────────────────────────────────
with st.container(border=True):
    st.markdown("### Additional Datasets Considered")
    st.markdown("""
**1. DOB Violations**
Records violations recorded by the DOB including violation type, severity, location, and status.
Reflects compliance issues related to building safety, zoning, and construction regulations.
[View Dataset](https://data.cityofnewyork.us/Housing-Development/DOB-Violations/3h2n-5cm9/about_data)

**2. DOB Disciplinary Actions**
Records disciplinary actions taken against professionals or entities (e.g. contractors, engineers)
for violations or misconduct. Includes action types, outcomes, and associated cases.
[View Dataset](https://data.cityofnewyork.us/Housing-Development/DOB-Disciplinary-Actions/ndq3-kuef/about_data)

**3. ACS Census Income Data**
Median household income aggregated at the county (borough) level from the U.S. Census Bureau's
American Community Survey. Updated annually or every five years.
""")

st.divider()

# ── Research Questions ────────────────────────────────────────────────────────
with st.container(border=True):
    st.markdown("### Research Questions")
    st.write("""
Following the feedback on our initial proposal, we combined multiple datasets to better
understand relationships between construction activity, housing conditions, and eviction
trends across New York City.
""")
    st.markdown("""
**1. How does construction activity relate to eviction patterns across NYC boroughs?**
We are interested in exploring whether areas with higher levels of construction activity
(such as new building permits or major renovations) also experience higher eviction rates.
This may reveal patterns related to redevelopment or potential housing displacement.

**2. Are building complaints and facade conditions associated with eviction outcomes?**
The dashboard allows users to investigate whether buildings with more complaints or unsafe
facade filings are also more likely to experience evictions, helping identify possible links
between housing conditions and tenant displacement.

**3. How do socioeconomic conditions relate to housing enforcement and evictions?**
We are also interested in exploring whether boroughs with lower median household income levels
experience higher rates of complaints, violations, or evictions.
""")

st.write("")

# ── Target Visualizations ─────────────────────────────────────────────────────
with st.container(border=True):
    st.markdown("### Target Visualizations")
    st.markdown("""
Our main visualization is an **interactive dashboard** displaying housing and building-related
data by borough across New York City. Users can explore patterns in construction activity,
evictions, complaints, and facade inspections geographically and over time.

Additional visualizations include:

- **Choropleth maps** showing eviction rates, unsafe facade counts, and permit activity by borough.
- **Time-series charts** showing trends in construction filings, complaints, and evictions over time.
- **Heatmaps** comparing boroughs across indicators such as permit types and construction decades.
- **Anomaly detection charts** flagging months with unusually high eviction activity.
- **Gauge charts** showing facade safety rates (Safe, SWARMP, Unsafe).
""")

st.write("")

# ── How the App Evolved ───────────────────────────────────────────────────────
with st.container(border=True):
    st.markdown("### How the Project Evolved")
    st.markdown("""
Our original proposal focused primarily on evictions and income data. Through the course of
the project, we expanded the scope significantly to include:

- **Building footprints** to analyze construction density by borough.
- **DOB permit data** from two separate systems (DOB NOW and DOB Permit Issuance)
  that required schema normalization and combined loading.
- **Facade inspection data (FISP)** to track building safety trends across inspection cycles.
- **DOB complaints data** with priority classification and response time analysis.

We also built a full ETL pipeline with automated daily refresh via GitHub Actions,
storing all data in Google BigQuery for fast dashboard queries.
""")

st.write("")

# ── Known Unknowns + Challenges ───────────────────────────────────────────────
with st.container(border=True):
    st.markdown("### Known Unknowns and Anticipated Challenges")
    st.markdown("""
**Known Unknowns**

- Differences in geographic granularity across datasets (borough vs ZIP vs address level).
- Difficulty joining datasets due to inconsistent column names and date formats across APIs.
- Uncertainty about how strong relationships between datasets will appear in the data.
- Changes in NYC Open Data APIs that affect column availability or data freshness.

**Anticipated Challenges**

- Cleaning and standardizing large NYC open datasets with inconsistent schemas.
- Handling API limitations (no SELECT on geometry columns, null date fields, rate limits).
- Designing visualizations that communicate complex relationships clearly.
- Managing BigQuery free tier constraints (no DML, truncate-only refresh strategy).
""")

st.caption(f"Page loaded in {time.time() - start_time:.2f} seconds")
