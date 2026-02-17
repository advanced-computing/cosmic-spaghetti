import streamlit as st
import pandas as pd
import plotly.express as px
import requests


# Loading NYC Eviction Data with Pagniation
@st.cache_data
def load_nyc_eviction_data():
    url = "https://data.cityofnewyork.us/resource/6z8x-wfk4.json"
    all_records = []
    limit = 1000
    offset = 0

    while True:
        params = {"$limit": limit, "$offset": offset}
        response = requests.get(url, params=params)
        data = response.json()
        if not data:
            break
        all_records.extend(data)
        offset += limit

    df = pd.json_normalize(all_records)
    return df


# Load data
df = load_nyc_eviction_data()
st.title("NYC Eviction Dashboard")

# Check if 'borough' column exists
if "borough" not in df.columns:
    st.error("No 'borough' column found in dataset!")
else:
    # Count total evictions by borough
    eviction_by_borough = df["borough"].value_counts().reset_index()
    eviction_by_borough.columns = ["borough", "total_evictions"]

    # Create bar chart
    fig = px.bar(
        eviction_by_borough,
        x="borough",
        y="total_evictions",
        title="Total Evictions by Borough",
        labels={"total_evictions": "Total Evictions", "borough": "Borough"},
        color="borough",
        color_discrete_sequence=px.colors.qualitative.Dark24_r,
        category_orders={
            "borough": eviction_by_borough.sort_values(
                "total_evictions", ascending=False
            )["borough"]
        },
    )

    # Display in Streamlit
    st.plotly_chart(fig)
