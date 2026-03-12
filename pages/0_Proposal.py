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

# Core datasets
with st.container(border=True):
    st.subheader("Core Datasets")

    st.markdown("""
**1. DOB Job Application Filings**  
Records historical job applications in NYC such as job type (new building, demolition, etc.).  
Provides insights about construction patterns across NYC.  
[Dataset](https://data.cityofnewyork.us/Housing-Development/DOB-Job-Application-Filings/ic3t-wcy2/about_data)

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
    st.subheader("Additional Datasets Under Consideration")

    st.markdown("""
**1. DOB Violations**  
Records violations recorded by the DOB including violation type, severity, location, and status.  
Reflects compliance issues related to building safety, zoning, and construction regulations.  
[Dataset](https://data.cityofnewyork.us/Housing-Development/DOB-Violations/3h2n-5cm9/about_data)

**2. DOB Complaints Received**  
Records complaints submitted by tenants or members of the public. Includes complaint category,
source, location, and response status — offering insight into public concerns and enforcement demand.  
[Dataset](https://data.cityofnewyork.us/Housing-Development/DOB-Complaints-Received/eabe-havv/about_data)

**3. DOB Disciplinary Actions**  
Records disciplinary actions taken against professionals or entities (e.g., contractors, engineers)
for violations or misconduct. Includes action types, outcomes, and associated cases.  
[Dataset](https://data.cityofnewyork.us/Housing-Development/DOB-Disciplinary-Actions/ndq3-kuef/about_data)
""")
