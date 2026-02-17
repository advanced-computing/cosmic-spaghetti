# import necessary libraries
from datetime import datetime

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

# import NYC Building Job Filing dataset with pagination


url = "https://data.cityofnewyork.us/resource/w9ak-ipjd.json"

# the dataset has 865K records, this large data might take hours to load. So, we cut the dataset from 2025 to today.

limit = 5000

date_data = "Filing_Date"


# pagination loop
def today():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


# filtering data form 2025 to today
start_date = "2025-01-01T00:00:00"
end_date = today()


@st.cache_data(ttl=3600)
def load_data(url, start_date, end_date):
    all_records = []
    offset = 0

    where = f"{date_data} >= '{start_date}' AND {date_data} <= '{end_date}'"

    while True:
        params = {"$limit": limit, "$offset": offset, "$where": where}
        r = requests.get(url, params=params)
        r.raise_for_status()
        data = r.json()

        # stop to last page
        if not data:
            break

        all_records.extend(data)
        offset += limit
    return pd.json_normalize(all_records)


st.title("NYC Building Job Filing (2025 to today)")

with st.spinner("Loading data..."):
    df_job = load_data(url, start_date, end_date)

st.success(f"Loaded{len(df_job):,} rows")
st.dataframe(df_job.head(50), use_container_width=True)
