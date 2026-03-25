import streamlit as st

st.set_page_config(page_title="Proposal")


st.title("Project Proposal: NYC Building Insights: Unraveling the Web of NYC Building Data")

# Original Proposal
st.subheader("Project Overview")

st.write("""
We are planning to explore several NYC open datasets to better understand the relationship
between construction activity, housing conditions, and socioeconomic factors across New York City.
""")
st.divider()

# Main datasets
with st.container(border=True):
    st.subheader("Main Datasets")

    st.markdown("""
**1. DOB-Now Job Permit**
Records historical job permit in NYC such as work type (new building, demolition, etc.).
Provides insights about construction patterns across NYC.
[Dataset](https://data.cityofnewyork.us/Housing-Development/DOB-NOW-Build-Approved-Permits/rbx6-tga4/about_data)

**2. NYC Evictions**
Executed residential evictions across the five boroughs since 2017, sortable by borough,
building type, and date.
[Dataset](https://data.cityofnewyork.us/City-Government/Evictions/6z8x-wfk4/about_data)

**3. ACS Census Income Data**
Median household income aggregated at the county (borough) level from the U.S. Census Bureau's
American Community Survey. Updated annually or every five years.
""")

st.write("")

# Additional / Potential Datasets Box
with st.container(border=True):
    st.subheader("Additional datasets being considered")

    st.markdown("""
**1. DOB Violations**
Records violations recorded by the DOB including violation type, severity, location, and status.
Reflects compliance issues related to building safety, zoning, and construction regulations.
[Dataset](https://data.cityofnewyork.us/Housing-Development/DOB-Violations/3h2n-5cm9/about_data)

**2. DOB Complaints Received**
Records complaints submitted by tenants or members of the public. Includes complaint category,
source, location, and response status.
[Dataset](https://data.cityofnewyork.us/Housing-Development/DOB-Complaints-Received/eabe-havv/about_data)

**3. DOB Disciplinary Actions**
Records disciplinary actions taken against professionals or entities (e.g., contractors, engineers)
for violations or misconduct. Includes action types, outcomes, and associated cases.
[Dataset](https://data.cityofnewyork.us/Housing-Development/DOB-Disciplinary-Actions/ndq3-kuef/about_data)
""")

st.divider()

# Research Questions
with st.container(border=True):
    st.subheader("Research Questions")
    st.write("""Following the feedback on our initial proposal, we are planning to combine
             different datasets to better understand relationships between construction activity,
             housing conditions, and eviction trends across New York City.
""")
    st.markdown("""
**1. How does construction activity relate to eviction patterns across NYC boroughs?**
We are interested in exploring whether areas with higher levels of construction activity
(such as new building permits or major renovations) also experience higher eviction rates.
This may reveal patterns related to redevelopment or potential housing displacement.

**2. Are building complaints and violations associated with eviction outcomes?**
The dashboard will allow users to investigate whether buildings with more complaints or safety
violations are also more likely to experience evictions, helping identify possible links
between housing conditions and tenant displacement.

**3. How do socioeconomic conditions relate to housing enforcement and evictions?**
We are also interested in exploring whether boroughs with lower median household income levels
experience higher rates of complaints, violations, or evictions.
""")

st.write("")

# Target Visualization
with st.container(border=True):
    st.subheader("Target Visualizations")

    st.markdown("""
Our main visualization will be an **interactive map of New York City** displaying housing
and building-related data by borough or neighborhood. Users will be able to explore patterns
in construction activity, evictions, and housing violations geographically.

Additional visualizations may include:

- **Time-series charts** showing trends in construction filings and evictions over time.
- **Bar charts** comparing boroughs across indicators such as complaints, violations,
  and income levels.
""")

st.write("")

# Known Unknowns + Challenges
with st.container(border=True):
    st.subheader("Known Unknowns and Anticipated Challenges")

    st.markdown("""
**Known Unknowns**

- Differences in geographic granularity across datasets (borough vs ZIP vs address level).
- Difficulty joining datasets due to inconsistent formats or identifiers.
- Uncertainty about how strong relationships between datasets will appear in the data.
- Changes in API that affects functions and/or visualization pages.

**Anticipated Challenges**

- Cleaning and standardizing large NYC open datasets.
- Handling large dataset sizes efficiently in the dashboard.
- Designing visualizations that communicate complex relationships clearly without
  overwhelming users.
- Understanding the difference of loading streamlit in local computer and web.
""")
